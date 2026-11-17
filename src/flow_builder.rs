//! Translated from `taskflow/core/flow_builder.hpp` and the editing half of
//! `taskflow/core/taskflow.hpp` (Taskflow v4.1.0).
//!
//! Upstream's `tf::FlowBuilder` is a base class holding a `Graph&`, which
//! `tf::Taskflow` and `tf::Subflow` inherit to get the task-building API.
//! Inheritance for code reuse translates to a trait with provided methods, so
//! here [`FlowBuilder`] asks implementors for one thing -- the graph they
//! build into, borrowed either way -- and builds everything else on top. M4's
//! `Subflow` then reuses the whole trait by answering that one question,
//! exactly as upstream reuses the base.
//!
//! One group of upstream `FlowBuilder` members is deliberately absent: the
//! algorithm builders (`for_each`, `reduce`, `sort`, ...), which are M6.
//! Nothing stands in for them. Everything else upstream builds is here,
//! including the four dynamic-tasking builders --
//! [`FlowBuilder::emplace_subflow`], [`FlowBuilder::emplace_runtime`],
//! [`FlowBuilder::composed_of`] and [`FlowBuilder::adopt`] -- whose node kinds
//! the executor does not run yet.

use std::sync::{Arc, Mutex};

use crate::graph::{Graph, Handle, RuntimeWork, SubflowWork, Task, Work};
use crate::runtime::Runtime;
use crate::subflow::Subflow;
use crate::task::{TaskMut, TaskRef};
use crate::taskflow::Taskflow;

/// The task-building half of a taskflow: upstream's `tf::FlowBuilder`.
///
/// Implementors supply the graph, as [`FlowBuilder::graph`] and
/// [`FlowBuilder::graph_mut`]; every other method is provided. The trait is
/// generic over the callable types it takes, so it is not usable as
/// `dyn FlowBuilder` -- nothing in this port needs that, and upstream's base
/// class is likewise only ever used statically.
///
/// [`Taskflow`] and [`Subflow`] are the two implementors, which is upstream's
/// pair of derived classes. [`Runtime`] is deliberately not one and so has no
/// [`task`](FlowBuilder::task): the graph a runtime task runs in is the
/// enclosing one, which it schedules into but never edits.
///
/// ```
/// use rustflow::prelude::*;
///
/// let mut taskflow = Taskflow::new();
/// let (a, b) = taskflow.emplace_n((|| println!("a"), || println!("b")));
/// let c = taskflow.placeholder();
/// taskflow.linearize([a, b, c]);
///
/// assert_eq!(taskflow.num_tasks(), 3);
/// assert_eq!(taskflow.graph().successors(a), [b].as_slice());
/// ```
pub trait FlowBuilder {
    /// Returns the graph this builder builds into.
    ///
    /// Upstream's `FlowBuilder::_graph` member, read-only. Both borrows are
    /// required because a provided method that only queries --
    /// [`FlowBuilder::task_ref`] -- must not ask for `&mut`, and both
    /// implementors have an inherent `graph()` of this shape already.
    fn graph(&self) -> &Graph;

    /// Returns the graph this builder builds into, mutably.
    ///
    /// This is upstream's `FlowBuilder::_graph` member again, and the
    /// requirement the building methods below are written against.
    fn graph_mut(&mut self) -> &mut Graph;

    /// Creates a static task running `callable`, and returns its handle.
    ///
    /// Upstream overloads `emplace` on the callable's signature to pick a node
    /// kind (static, condition, subflow, ...). Rust has no overloading, so each
    /// kind arrives as a named method instead: this one is the static task, and
    /// [`FlowBuilder::emplace_condition`] and
    /// [`FlowBuilder::emplace_multi_condition`] are the two conditioners.
    ///
    /// The `Send + 'static` bound is what lets the executor own the closure and
    /// run it on a worker thread. An already-boxed closure satisfies it --
    /// `Box<dyn FnMut() + Send>` is itself `FnMut()` -- which is upstream's
    /// `std::function<void()>` case.
    fn emplace<C>(&mut self, callable: C) -> Task
    where
        C: FnMut() + Send + 'static,
    {
        self.graph_mut()
            .add_node(Handle::Static(Work::new(callable)))
    }

    /// Creates a condition task running `callable`, and returns its handle.
    ///
    /// Upstream's `emplace` overload for a `ConditionTaskLike` callable. The
    /// `i32` it returns indexes this task's own successor list, so `0` runs the
    /// first successor and nothing else; a negative or out-of-range value
    /// selects nothing, which is how a branch ends.
    ///
    /// Nothing schedules on that yet -- the conditional branch belongs to the
    /// executor -- so what this builds today is a graph citizen that reports
    /// [`crate::TaskType::Condition`] and makes a dependency on it weak.
    ///
    /// ```
    /// use rustflow::prelude::*;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let (init, yes, no) = taskflow.emplace_n((|| {}, || {}, || {}));
    /// // Upstream's if-else block: `yes` on 0, `no` on 1.
    /// let cond = taskflow.emplace_condition(|| 0);
    /// taskflow.task(cond).succeed([init]).precede([yes, no]);
    ///
    /// assert!(taskflow.graph().node(cond).handle().is_conditioner());
    /// assert_eq!(taskflow.task_ref(yes).num_weak_dependencies(), 1);
    /// ```
    fn emplace_condition<C>(&mut self, callable: C) -> Task
    where
        C: FnMut() -> i32 + Send + 'static,
    {
        self.graph_mut()
            .add_node(Handle::Condition(Work::new(callable)))
    }

    /// Creates a multi-condition task running `callable`, and returns its
    /// handle.
    ///
    /// Upstream's `emplace` overload for a `MultiConditionTaskLike` callable.
    /// Where [`FlowBuilder::emplace_condition`] names one successor, this names
    /// as many as it likes -- `vec![0, 2]` runs the first and third -- and the
    /// same out-of-range rule applies to each index independently, so a list
    /// may select none of them.
    ///
    /// ```
    /// use rustflow::prelude::*;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let branches: Vec<Task> = (0..3).map(|_| taskflow.emplace(|| {})).collect();
    /// let cond = taskflow.emplace_multi_condition(|| vec![0, 2]);
    /// taskflow.task(cond).precede(branches.iter().copied());
    ///
    /// assert!(taskflow.graph().node(cond).handle().is_conditioner());
    /// assert_eq!(taskflow.graph().node(cond).num_successors(), 3);
    /// ```
    fn emplace_multi_condition<C>(&mut self, callable: C) -> Task
    where
        C: FnMut() -> Vec<i32> + Send + 'static,
    {
        self.graph_mut()
            .add_node(Handle::MultiCondition(Work::new(callable)))
    }

    /// Creates a subflow task running `callable`, and returns its handle.
    ///
    /// Upstream's `emplace` overload for a `SubflowTaskLike` callable. The body
    /// is handed the [`Subflow`] it builds into, so what the task spawns can
    /// depend on anything known only while it runs; the executor then runs
    /// that child graph before this task's own successors.
    ///
    /// The node owns the graph the body builds -- see [`Handle::Subflow`] --
    /// and a fresh entry to the same node starts from an empty one, which is
    /// what makes a subflow inside a loop behave.
    ///
    /// The body may also run its subgraph itself, with
    /// [`Subflow::join`](crate::Subflow::join), and read what the children
    /// left behind before it returns.
    ///
    /// ```
    /// use rustflow::prelude::*;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let spawner = taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
    ///     let children: Vec<Task> =
    ///         (0..3).map(|_| subflow.emplace(|| {})).collect();
    ///     subflow.linearize(children);
    /// });
    ///
    /// assert!(taskflow.graph().node(spawner).handle().is_subflow());
    /// ```
    fn emplace_subflow<C>(&mut self, callable: C) -> Task
    where
        C: for<'a> FnMut(&mut Subflow<'a>) + Send + 'static,
    {
        self.graph_mut().add_node(Handle::Subflow {
            work: SubflowWork::new(callable),
            subgraph: Mutex::new(Graph::new()),
        })
    }

    /// Creates a runtime task running `callable`, and returns its handle.
    ///
    /// Upstream's `emplace` overload for a `RuntimeTaskLike` callable. The body
    /// is handed the [`Runtime`] it is running on, which is upstream's way of
    /// giving a task the scheduler itself: it can schedule a task of its own
    /// graph outright and wait for work it spawned.
    ///
    /// Upstream's second runtime shape, the one taking a
    /// `tf::NonpreemptiveRuntime&`, is outside this milestone; it would be a
    /// third method here rather than an overload of this one.
    ///
    /// Running one is issue #51, and [`Runtime`]'s scheduling methods land with
    /// it: today this builds a graph citizen that reports
    /// [`crate::TaskType::Runtime`], and a run that reaches it is refused.
    ///
    /// ```
    /// use rustflow::prelude::*;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let task = taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     // What a body does with the handle is #51's; where it is running
    ///     // it can already ask.
    ///     let _ = runtime.worker().id();
    /// });
    ///
    /// assert!(taskflow.graph().node(task).handle().is_runtime());
    /// ```
    fn emplace_runtime<C>(&mut self, callable: C) -> Task
    where
        C: for<'a> FnMut(&mut Runtime<'a>) + Send + 'static,
    {
        self.graph_mut()
            .add_node(Handle::Runtime(RuntimeWork::new(callable)))
    }

    /// Creates a module task standing for `target`, and returns its handle.
    ///
    /// Upstream's `composed_of`, which is how one taskflow is composed into
    /// another: running this task runs the whole of `target`'s graph, and this
    /// task's successors wait for all of it.
    ///
    /// Upstream takes anything graph-like by reference, as
    /// `taskflow.composed_of(other)`, and leaves the caller to keep `other`
    /// alive for as long as any run of the composing taskflow may be in
    /// flight. A safe Rust API cannot state
    /// that obligation, so the target is an [`Arc`] and the module shares it;
    /// composing one taskflow into two others, or twice into one, hands out
    /// more shares of the same graph rather than copying it. The owned form of
    /// the same thing is [`FlowBuilder::adopt`].
    ///
    /// What the share does *not* buy is permission to run one target twice at
    /// once: a graph's per-node counters belong to one run at a time, which is
    /// upstream's rule too (`composed_of` is "thread-safe as long as the target
    /// is not concurrently ran by two or more threads"). Two module tasks over
    /// one target are therefore fine when nothing can run them at the same
    /// time -- an edge between them is enough -- and a caller's error when
    /// they can. Runs of one taskflow serialise, so queueing several runs of a
    /// composition is not that error; `executor::module` sets out both sides,
    /// and the failure a worker reports if the rule is broken.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use rustflow::prelude::*;
    ///
    /// let mut library = Taskflow::with_name("library");
    /// library.emplace(|| println!("shared work"));
    /// let library = Arc::new(library);
    ///
    /// let mut taskflow = Taskflow::new();
    /// let first = taskflow.composed_of(Arc::clone(&library));
    /// let second = taskflow.composed_of(Arc::clone(&library));
    /// taskflow.linearize([first, second]);
    ///
    /// // Two module tasks, one target.
    /// assert!(taskflow.graph().node(first).handle().is_module());
    /// assert_eq!(Arc::strong_count(&library), 3);
    /// ```
    fn composed_of(&mut self, target: Arc<Taskflow>) -> Task {
        self.graph_mut().add_node(Handle::Module(target))
    }

    /// Creates a module task owning `graph`, and returns its handle.
    ///
    /// Upstream's `adopt(Graph&&)`: the same composition as
    /// [`FlowBuilder::composed_of`] with the ownership question settled the
    /// other way -- the graph is moved into the task, so there is no target to
    /// keep alive and no share to count. It is the form to reach for when the
    /// composed graph has no other owner.
    ///
    /// Running one runs the whole of `graph` before this task's successors,
    /// exactly as a composed target runs: the two forms differ only in who
    /// owns the target graph, which is a question the scheduler answers for
    /// all four kinds of nested graph in one place (#74) and
    /// `executor::module` says how for these two.
    ///
    /// ```
    /// use rustflow::prelude::*;
    ///
    /// let mut module = Graph::new();
    /// module.add_node(Handle::Static(Work::new(|| println!("adopted"))));
    ///
    /// let mut taskflow = Taskflow::new();
    /// let task = taskflow.adopt(module);
    ///
    /// assert!(taskflow.graph().node(task).handle().is_module());
    /// ```
    fn adopt(&mut self, graph: Graph) -> Task {
        self.graph_mut().add_node(Handle::AdoptedModule(graph))
    }

    /// Creates one static task per callable in a tuple, and returns a tuple of
    /// their handles in the same order.
    ///
    /// This is upstream's variadic `emplace` returning a `std::tuple`, which
    /// structured binding then unpacks. Rust has no variadics, so the tuple is
    /// the *argument* as well as the result and [`Callables`] carries the
    /// mapping; destructuring the result reads the same as upstream:
    ///
    /// ```
    /// use rustflow::prelude::*;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let (a, b, c, d) = taskflow.emplace_n((|| {}, || {}, || {}, || {}));
    /// taskflow.linearize([a, b, c, d]);
    /// assert_eq!(taskflow.num_tasks(), 4);
    /// ```
    ///
    /// Implemented for tuples of two to eight callables. Upstream's variadic
    /// has no such ceiling, but past a handful of closures a loop over an array
    /// of them reads better than either spelling, so the cap costs nothing.
    fn emplace_n<C>(&mut self, callables: C) -> C::Tasks
    where
        C: Callables,
    {
        callables.emplace_all(self.graph_mut())
    }

    /// Creates a task with no work attached yet, and returns its handle.
    ///
    /// Upstream's `placeholder()`: a real node in the graph, distinct from an
    /// empty `tf::Task` handle that points at no node at all. This port has no
    /// empty handle to confuse it with -- a [`Task`] always names a slot -- so
    /// the distinction shows up as [`Handle::is_placeholder`] instead.
    fn placeholder(&mut self) -> Task {
        self.graph_mut().add_node(Handle::Placeholder)
    }

    /// Removes a task and every dependency touching it.
    ///
    /// A handle that names nothing live -- out of range, or erased already --
    /// does nothing, as upstream's `erase` does nothing for a task outside the
    /// graph. The index stays spent: see [`Graph::erase`] for why erasing
    /// tombstones the slot rather than compacting the arena, and what that
    /// means for the handles that remain.
    fn erase(&mut self, task: Task) {
        self.graph_mut().erase(task);
    }

    /// Adds a dependency between each adjacent pair of `tasks`, so that
    /// `[a, b, c]` becomes `a -> b -> c`.
    ///
    /// Upstream overloads `linearize` for `std::vector<Task>&` and for an
    /// initializer list; one `IntoIterator` covers both, and a slice, an array
    /// or any iterator besides.
    ///
    /// # Panics
    ///
    /// Panics if any handle does not name a live node of this graph, which is
    /// [`Graph::add_edge`]'s rule.
    fn linearize<I>(&mut self, tasks: I)
    where
        I: IntoIterator<Item = Task>,
    {
        let mut tasks = tasks.into_iter();
        let Some(mut previous) = tasks.next() else {
            return;
        };
        for task in tasks {
            self.graph_mut().add_edge(previous, task);
            previous = task;
        }
    }

    /// Removes every `from -> to` dependency, leaving both tasks in the graph.
    ///
    /// Upstream's `Taskflow::remove_dependency`, which lives on `Taskflow`
    /// rather than on `FlowBuilder` there. It belongs with the rest of the
    /// editing API, and putting it on the trait means `Subflow` gets it too.
    ///
    /// Duplicate dependencies all go: a task preceded twice and then released
    /// once is released outright, as upstream's `_remove_successors` does.
    ///
    /// # Panics
    ///
    /// Panics if either handle does not name a live node of this graph.
    fn remove_dependency(&mut self, from: Task, to: Task) {
        self.graph_mut().remove_edge(from, to);
    }

    /// Returns an editing handle to `task`: upstream's `tf::Task`.
    ///
    /// Upstream's `emplace` hands the handle straight back, and it goes on
    /// aliasing the graph the caller keeps mutating. Here the lasting handle
    /// is the [`Task`] index `emplace` returns and the fluent methods live on
    /// [`TaskMut`], a short-lived borrow of the builder -- which is why they
    /// are reached through this method rather than being the return value
    /// (README's "Graph as an arena").
    ///
    /// Being on the trait is what makes a subflow body read like the taskflow
    /// half of the same program. Upstream's subflow example ports as:
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let b = taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
    ///     let b1 = subflow.emplace(|| {});
    ///     subflow.task(b1).name("B1");
    ///     let b2 = subflow.emplace(|| {});
    ///     subflow.task(b2).name("B2");
    ///     let b3 = subflow.emplace(|| {});
    ///     subflow.task(b3).name("B3").succeed([b1, b2]);
    /// });
    /// taskflow.task(b).name("B");
    ///
    /// Executor::new(2).run(&taskflow).wait().unwrap();
    /// ```
    ///
    /// Naming a task is two statements rather than upstream's
    /// `emplace(..).name("B")` chain, and that is a decision rather than a
    /// gap: README's "Design decisions already taken" has the reason.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a task of this builder's graph.
    fn task(&mut self, task: Task) -> TaskMut<'_> {
        TaskMut::new(self.graph_mut(), task)
    }

    /// Returns a read-only handle to `task`, the query half of the pair.
    ///
    /// This is upstream's `tf::TaskView` as much as its `tf::Task`; the two
    /// differ only in the editing methods, which are on [`TaskMut`].
    ///
    /// The trait is all it takes to reach either handle, so importing it is
    /// enough -- with or without the rest of the prelude:
    ///
    /// ```
    /// use rustflow::{FlowBuilder, Taskflow};
    ///
    /// let mut taskflow = Taskflow::new();
    /// let a = taskflow.emplace(|| {});
    /// taskflow.task(a).name("a");
    ///
    /// assert_eq!(taskflow.task_ref(a).name(), "a");
    /// assert!(taskflow.task_ref(a).has_work());
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a task of this builder's graph.
    #[must_use]
    fn task_ref(&self, task: Task) -> TaskRef<'_> {
        TaskRef::new(self.graph(), task)
    }
}

/// A tuple of callables that [`FlowBuilder::emplace_n`] can turn into a tuple
/// of task handles.
///
/// Implemented for tuples of two to eight callables, which is as far as
/// upstream's variadic `emplace` is worth following. One callable is
/// [`FlowBuilder::emplace`].
pub trait Callables {
    /// The tuple of [`Task`] handles emplacing this tuple produces: one handle
    /// per callable, in the same order.
    type Tasks;

    /// Emplaces every callable into `graph` as a static task, left to right.
    fn emplace_all(self, graph: &mut Graph) -> Self::Tasks;
}

/// Expands to `Task`, discarding the identifier it is handed. The macro below
/// needs one `Task` per callable type in a type position, and this is the way
/// to spell "repeat this type once per element of that list".
macro_rules! task_handle {
    ($ignored:ident) => {
        Task
    };
}

/// Implements [`Callables`] for one tuple arity. Takes the type parameter and
/// the binding name for each element, since the expansion needs both.
macro_rules! impl_callables {
    ($($callable:ident $binding:ident),+) => {
        impl<$($callable,)+> Callables for ($($callable,)+)
        where
            $($callable: FnMut() + Send + 'static,)+
        {
            type Tasks = ($(task_handle!($callable),)+);

            fn emplace_all(self, graph: &mut Graph) -> Self::Tasks {
                let ($($binding,)+) = self;
                // Tuple expression elements evaluate left to right, so the
                // handles come back in the order the callables were written.
                ($(graph.add_node(Handle::Static(Work::new($binding))),)+)
            }
        }
    };
}

impl_callables!(C0 c0, C1 c1);
impl_callables!(C0 c0, C1 c1, C2 c2);
impl_callables!(C0 c0, C1 c1, C2 c2, C3 c3);
impl_callables!(C0 c0, C1 c1, C2 c2, C3 c3, C4 c4);
impl_callables!(C0 c0, C1 c1, C2 c2, C3 c3, C4 c4, C5 c5);
impl_callables!(C0 c0, C1 c1, C2 c2, C3 c3, C4 c4, C5 c5, C6 c6);
impl_callables!(C0 c0, C1 c1, C2 c2, C3 c3, C4 c4, C5 c5, C6 c6, C7 c7);

impl FlowBuilder for Taskflow {
    fn graph(&self) -> &Graph {
        // As below: the inherent accessor, not a recursive call into the
        // trait.
        Taskflow::graph(self)
    }

    fn graph_mut(&mut self) -> &mut Graph {
        // Inherent methods win over trait methods in resolution, so this is
        // `Taskflow`'s own accessor and not a recursive call into the trait.
        Taskflow::graph_mut(self)
    }
}

impl Taskflow {
    /// Applies `visitor` to the handle of every task in this taskflow, in
    /// creation order.
    ///
    /// Upstream's `Taskflow::for_each_task`, which walks `_graph` and hands the
    /// visitor a `tf::Task`. Erased tasks are not visited -- [`Graph::iter`]
    /// skips the tombstones they leave.
    ///
    /// The visitor takes a handle rather than a `&Node` because a handle is
    /// what the editing API consumes; `self.graph().iter()` is the nodes.
    pub fn for_each_task<V>(&self, mut visitor: V)
    where
        V: FnMut(Task),
    {
        for task in self.graph().tasks() {
            visitor(task);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::task::TaskType;

    /// Runs every task's work once, in creation order, standing in for the
    /// executor that does not exist yet.
    fn run_in_order(taskflow: &Taskflow) {
        for (_, node) in taskflow.graph().iter() {
            if let Handle::Static(work) = node.handle() {
                work.invoke();
            }
        }
    }

    #[test]
    fn emplace_attaches_work_and_placeholder_does_not() {
        let mut taskflow = Taskflow::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&counter);
        let worker = taskflow.emplace(move || {
            seen.fetch_add(1, Ordering::Relaxed);
        });
        let idle = taskflow.placeholder();

        assert_eq!(taskflow.num_tasks(), 2);
        assert!(taskflow.graph().node(worker).handle().is_static());
        assert!(taskflow.graph().node(idle).handle().is_placeholder());

        run_in_order(&taskflow);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn the_two_conditioner_builders_attach_their_own_handles() {
        let mut taskflow = Taskflow::new();
        let still = taskflow.emplace(|| {});
        let one = taskflow.emplace_condition(|| 1);
        let many = taskflow.emplace_multi_condition(|| vec![0, 2]);

        assert_eq!(taskflow.num_tasks(), 3);
        assert!(taskflow.graph().node(still).handle().is_static());
        assert!(taskflow.graph().node(one).handle().is_condition());
        assert!(taskflow.graph().node(many).handle().is_multi_condition());

        // The builder hands the callable over whole: what comes back out of
        // the node is what was emplaced, return value and all.
        let Handle::Condition(work) = taskflow.graph().node(one).handle() else {
            panic!("emplace_condition left something else behind");
        };
        assert_eq!(work.invoke(), 1);
        let Handle::MultiCondition(work) = taskflow.graph().node(many).handle() else {
            panic!("emplace_multi_condition left something else behind");
        };
        assert_eq!(work.invoke(), vec![0, 2]);

        // The run-in-order stand-in invokes static work only, so a conditioner
        // is inert until the executor learns to branch on it.
        run_in_order(&taskflow);
    }

    /// A boxed callable is upstream's `std::function<int()>` case, and the
    /// bound has to admit it for the same reason [`FlowBuilder::emplace`]'s
    /// does: `Box<dyn FnMut() -> i32 + Send>` is itself `FnMut() -> i32`.
    #[test]
    fn a_conditioner_builder_takes_an_already_boxed_callable() {
        let mut taskflow = Taskflow::new();
        let boxed: Box<dyn FnMut() -> i32 + Send> = Box::new(|| -1);
        let one = taskflow.emplace_condition(boxed);
        let boxed: Box<dyn FnMut() -> Vec<i32> + Send> = Box::new(Vec::new);
        let many = taskflow.emplace_multi_condition(boxed);

        assert!(taskflow.graph().node(one).handle().is_conditioner());
        assert!(taskflow.graph().node(many).handle().is_conditioner());
    }

    /// Every dynamic-tasking builder attaches the handle it is named for, and
    /// nothing else about the graph changes: four builders, four node kinds,
    /// one node each.
    #[test]
    fn the_dynamic_tasking_builders_attach_their_own_handles() {
        let mut taskflow = Taskflow::new();
        let spawner = taskflow.emplace_subflow(|_| {});
        let runtime = taskflow.emplace_runtime(|_| {});
        let module = taskflow.composed_of(Arc::new(Taskflow::with_name("target")));
        let adopted = taskflow.adopt(Graph::new());

        assert_eq!(taskflow.num_tasks(), 4);
        assert!(taskflow.graph().node(spawner).handle().is_subflow());
        assert!(taskflow.graph().node(runtime).handle().is_runtime());
        assert!(taskflow.graph().node(module).handle().is_module());
        assert!(taskflow.graph().node(adopted).handle().is_module());

        // Each reports its own task type, with both module forms sharing one
        // name as upstream's `TaskType` does.
        assert_eq!(taskflow.task_ref(spawner).task_type(), TaskType::Subflow);
        assert_eq!(taskflow.task_ref(runtime).task_type(), TaskType::Runtime);
        assert_eq!(taskflow.task_ref(module).task_type(), TaskType::Module);
        assert_eq!(taskflow.task_ref(adopted).task_type(), TaskType::Module);

        // None of them is a conditioner, so a dependency on one is strong.
        for task in [spawner, runtime, module, adopted] {
            assert!(!taskflow.graph().node(task).handle().is_conditioner());
        }
    }

    /// A subflow node carries an empty graph of its own from the moment it is
    /// built, which is the node field the body writes into.
    #[test]
    fn a_fresh_subflow_task_owns_an_empty_subgraph() {
        let mut taskflow = Taskflow::new();
        let spawner = taskflow.emplace_subflow(|_| {});

        let Handle::Subflow { subgraph, .. } = taskflow.graph().node(spawner).handle() else {
            panic!("emplace_subflow left something else behind");
        };
        assert!(subgraph.lock().expect("uncontended").is_empty());
    }

    /// Composing one taskflow twice hands out two shares of the same target
    /// rather than copying it, which is the whole of the `Arc<Taskflow>`
    /// decision: upstream's `Graph&` would be two references to one graph, and
    /// this is the safe spelling of that.
    #[test]
    fn composed_of_twice_shares_one_target() {
        let mut target = Taskflow::with_name("target");
        target.emplace(|| {});
        let target = Arc::new(target);
        assert_eq!(Arc::strong_count(&target), 1);

        let mut taskflow = Taskflow::new();
        let first = taskflow.composed_of(Arc::clone(&target));
        let second = taskflow.composed_of(Arc::clone(&target));

        // Two module nodes, two shares beside the caller's own.
        assert_eq!(Arc::strong_count(&target), 3);
        let Handle::Module(from_first) = taskflow.graph().node(first).handle() else {
            panic!("composed_of left something else behind");
        };
        let Handle::Module(from_second) = taskflow.graph().node(second).handle() else {
            panic!("composed_of left something else behind");
        };
        assert!(Arc::ptr_eq(from_first, from_second));
        assert!(Arc::ptr_eq(from_first, &target));
        assert_eq!(from_first.name(), "target");
        assert_eq!(from_first.graph().len(), 1);

        // Erasing one module task drops one share and leaves the other.
        taskflow.erase(first);
        assert_eq!(Arc::strong_count(&target), 2);
    }

    /// `adopt` moves the graph in: the task owns it afterwards, which is what
    /// the strong count of something the graph captured can show.
    #[test]
    fn adopt_moves_the_graph_into_the_task() {
        let witness = Arc::new(());
        let held = Arc::clone(&witness);
        let mut module = Graph::new();
        module.add_named_node(
            "inner",
            Handle::Static(Work::new(move || {
                let _ = &held;
            })),
        );
        module.add_node(Handle::Placeholder);
        assert_eq!(Arc::strong_count(&witness), 2);

        let mut taskflow = Taskflow::new();
        let task = taskflow.adopt(module);

        // The adopted graph is reachable through the handle, nodes and all,
        // and the taskflow's own graph is still one node big.
        let Handle::AdoptedModule(adopted) = taskflow.graph().node(task).handle() else {
            panic!("adopt left something else behind");
        };
        assert_eq!(adopted.len(), 2);
        assert_eq!(adopted.node(Task::new(0)).name(), "inner");
        assert_eq!(taskflow.num_tasks(), 1);
        // Moved in, not cloned: still exactly one holder of the witness.
        assert_eq!(Arc::strong_count(&witness), 2);

        // And the task owning it means erasing the task drops it.
        taskflow.erase(task);
        assert_eq!(Arc::strong_count(&witness), 1);
    }

    /// The bodies are `FnMut`, so the same closure must survive being handed
    /// over already boxed, as [`FlowBuilder::emplace`]'s does.
    #[test]
    fn the_dynamic_builders_take_already_boxed_callables() {
        let mut taskflow = Taskflow::new();
        let boxed: Box<dyn for<'a> FnMut(&mut Subflow<'a>) + Send> = Box::new(|_| {});
        let spawner = taskflow.emplace_subflow(boxed);
        let boxed: Box<dyn for<'a> FnMut(&mut Runtime<'a>) + Send> = Box::new(|_| {});
        let runtime = taskflow.emplace_runtime(boxed);

        assert!(taskflow.graph().node(spawner).handle().is_subflow());
        assert!(taskflow.graph().node(runtime).handle().is_runtime());
    }

    #[test]
    fn emplace_n_returns_one_handle_per_callable_in_order() {
        let mut taskflow = Taskflow::new();
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let record = |label: char| {
            let order = Arc::clone(&order);
            move || order.lock().expect("uncontended").push(label)
        };

        let (a, b, c) = taskflow.emplace_n((record('a'), record('b'), record('c')));
        assert_eq!(vec![a, b, c], taskflow.graph().tasks().collect::<Vec<_>>());

        run_in_order(&taskflow);
        assert_eq!(*order.lock().expect("uncontended"), vec!['a', 'b', 'c']);
    }

    /// Every arity the macro generates has to be instantiated somewhere or it
    /// is never type-checked, so this walks all of them.
    #[test]
    fn emplace_n_covers_the_whole_supported_arity_range() {
        let mut taskflow = Taskflow::new();
        let mut handles = Vec::new();

        let (a, b) = taskflow.emplace_n((|| {}, || {}));
        handles.extend([a, b]);
        let (a, b, c) = taskflow.emplace_n((|| {}, || {}, || {}));
        handles.extend([a, b, c]);
        let (a, b, c, d) = taskflow.emplace_n((|| {}, || {}, || {}, || {}));
        handles.extend([a, b, c, d]);
        let (a, b, c, d, e) = taskflow.emplace_n((|| {}, || {}, || {}, || {}, || {}));
        handles.extend([a, b, c, d, e]);
        let (a, b, c, d, e, f) = taskflow.emplace_n((|| {}, || {}, || {}, || {}, || {}, || {}));
        handles.extend([a, b, c, d, e, f]);
        let (a, b, c, d, e, f, g) =
            taskflow.emplace_n((|| {}, || {}, || {}, || {}, || {}, || {}, || {}));
        handles.extend([a, b, c, d, e, f, g]);
        let (a, b, c, d, e, f, g, h) =
            taskflow.emplace_n((|| {}, || {}, || {}, || {}, || {}, || {}, || {}, || {}));
        handles.extend([a, b, c, d, e, f, g, h]);

        // 2 + 3 + ... + 8 tasks, handed out in call order across every arity.
        assert_eq!(handles.len(), 35);
        assert_eq!(taskflow.num_tasks(), 35);
        assert_eq!(handles, taskflow.graph().tasks().collect::<Vec<_>>());
    }

    #[test]
    fn linearize_chains_adjacent_pairs_only() {
        let mut taskflow = Taskflow::new();
        let tasks: Vec<Task> = (0..4).map(|_| taskflow.placeholder()).collect();
        taskflow.linearize(tasks.iter().copied());

        let graph = taskflow.graph();
        assert_eq!(graph.successors(tasks[0]), [tasks[1]].as_slice());
        assert_eq!(graph.successors(tasks[1]), [tasks[2]].as_slice());
        assert_eq!(graph.successors(tasks[2]), [tasks[3]].as_slice());
        assert!(graph.successors(tasks[3]).is_empty());
        assert!(graph.predecessors(tasks[0]).is_empty());
        assert_eq!(graph.predecessors(tasks[3]), [tasks[2]].as_slice());
    }

    #[test]
    fn linearize_of_nothing_or_one_task_adds_no_edges() {
        let mut taskflow = Taskflow::new();
        taskflow.linearize([]);
        let only = taskflow.placeholder();
        taskflow.linearize([only]);
        assert_eq!(taskflow.graph().node(only).num_successors(), 0);
        assert_eq!(taskflow.graph().node(only).num_predecessors(), 0);
    }

    #[test]
    fn erase_through_the_trait_drops_the_work_it_was_holding() {
        let mut taskflow = Taskflow::new();
        // The closure owns a clone of the Arc, so the strong count tells us
        // whether erasing actually dropped the node's work.
        let witness = Arc::new(());
        let held = Arc::clone(&witness);
        let task = taskflow.emplace(move || {
            let _ = &held;
        });
        assert_eq!(Arc::strong_count(&witness), 2);

        taskflow.erase(task);
        assert_eq!(Arc::strong_count(&witness), 1);
        assert!(taskflow.is_empty());
        // Erasing a handle that names nothing live is a no-op, not a panic.
        taskflow.erase(task);
        taskflow.erase(Task::new(99));
    }

    #[test]
    fn for_each_task_visits_live_tasks_in_creation_order() {
        let mut taskflow = Taskflow::new();
        let a = taskflow.placeholder();
        let b = taskflow.placeholder();
        let c = taskflow.placeholder();
        taskflow.erase(b);

        let mut visited = Vec::new();
        taskflow.for_each_task(|task| visited.push(task));
        assert_eq!(visited, vec![a, c]);
    }

    /// The acceptance bar for issue #120: both handle accessors are provided
    /// methods, so code that knows only that it holds *a* builder can name a
    /// task and wire it up -- which is exactly what a subflow body knows
    /// about itself.
    #[test]
    fn the_task_handles_are_reachable_through_the_trait_alone() {
        fn build(builder: &mut impl FlowBuilder) -> (Task, Task) {
            let first = builder.emplace(|| {});
            let second = builder.emplace(|| {});
            builder.task(second).name("second").succeed([first]);
            (first, second)
        }

        // The query half takes `&self`, which is why the trait needs both
        // borrows of the graph and not just the mutable one.
        fn name_of(builder: &impl FlowBuilder, task: Task) -> String {
            builder.task_ref(task).name().to_string()
        }

        let mut taskflow = Taskflow::new();
        let (first, second) = build(&mut taskflow);

        assert_eq!(name_of(&taskflow, second), "second");
        assert_eq!(taskflow.task_ref(first).num_successors(), 1);
        assert_eq!(taskflow.task_ref(second).num_predecessors(), 1);
    }

    #[test]
    fn remove_dependency_leaves_both_tasks_behind() {
        let mut taskflow = Taskflow::new();
        let a = taskflow.placeholder();
        let b = taskflow.placeholder();
        taskflow.graph_mut().add_edge(a, b);

        taskflow.remove_dependency(a, b);
        assert_eq!(taskflow.num_tasks(), 2);
        assert_eq!(taskflow.graph().node(a).num_successors(), 0);
        assert_eq!(taskflow.graph().node(b).num_predecessors(), 0);
    }
}
