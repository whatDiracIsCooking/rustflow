//! Translated from `unittests/test_basics.cpp` (Taskflow v4.1.0): the `Type`,
//! `Creation`, `Taskflow.RemoveDependency` and `STDFunction` cases, and the
//! per-taskflow half of `Iterators`; plus, from
//! `unittests/test_control_flow.cpp`, the `Cond.Types` and `MultiCond.Types`
//! cases and the graph half of `loop_cond`.
//!
//! Upstream runs most of these through a `tf::Executor`, which is M2. Where a
//! case's assertions depend on execution, the translation keeps the part that
//! is about the graph that was *built* and says what it dropped; where it
//! depends on a node kind this port does not have yet (`STDFunction` emplaces a
//! condition task), the same. Nothing here reaches for an executor.

use rustflow::TaskType;
use rustflow::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Invokes the work of every task once, in creation order.
///
/// The stand-in for `executor.run(taskflow).wait()`: it says nothing about
/// dependency order, so a test may only use it to count invocations.
fn invoke_every_task(taskflow: &Taskflow) {
    for (_, node) in taskflow.graph().iter() {
        if let Handle::Static(work) = node.handle() {
            work.invoke();
        }
    }
}

/// Builds ten taskflows over a shared vector, as upstream's `create_taskflow`
/// lambda does. Upstream builds one `for_each` task per taskflow over the whole
/// range, which is M6; one static task per element is the same shape of work --
/// a closure per element, holding a handle to data the test owns -- without it.
fn create_taskflows(dummy: &Arc<Vec<i32>>) {
    for _ in 0..10 {
        let mut taskflow = Taskflow::new();
        for index in 0..dummy.len() {
            let elements = Arc::clone(dummy);
            taskflow.emplace(move || assert_eq!(elements[index], -1));
        }
        assert_eq!(taskflow.num_tasks(), dummy.len());
    }
}

/// `TEST_CASE("Creation")`, subcases One, Two and Four: building taskflows
/// concurrently must not disturb the data their tasks capture.
#[test]
fn creation() {
    let dummy = Arc::new(vec![-1; 1000]);

    for threads in [1, 2, 4] {
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| create_taskflows(&dummy));
            }
        });

        assert_eq!(dummy.len(), 1000);
        for item in dummy.iter() {
            assert_eq!(*item, -1);
        }
        // Dropping each taskflow dropped the closures it owned, so the only
        // handle on the vector left is this test's.
        assert_eq!(Arc::strong_count(&dummy), 1);
    }
}

/// `TEST_CASE("Taskflow.RemoveDependency")`.
///
/// Upstream builds the dependencies with `a.precede(b, c, d)`, which is
/// `Task`'s API rather than the builder's; the graph-level `add_edge` is the
/// same two pushes underneath.
#[test]
fn remove_dependency() {
    let mut taskflow = Taskflow::new();
    let a = taskflow.placeholder();
    let b = taskflow.placeholder();
    let c = taskflow.placeholder();
    let d = taskflow.placeholder();
    for (task, name) in [(a, "a"), (b, "b"), (c, "c"), (d, "d")] {
        taskflow.graph_mut().node_mut(task).set_name(name);
    }

    assert_eq!(taskflow.graph().node(a).num_successors(), 0);
    assert_eq!(taskflow.graph().node(a).num_predecessors(), 0);

    for successor in [b, c, d] {
        taskflow.graph_mut().add_edge(a, successor);
    }
    assert_eq!(taskflow.graph().node(a).num_successors(), 3);
    assert_eq!(taskflow.graph().node(b).num_predecessors(), 1);
    assert_eq!(taskflow.graph().node(c).num_predecessors(), 1);
    assert_eq!(taskflow.graph().node(d).num_predecessors(), 1);

    taskflow.remove_dependency(a, b);
    assert_eq!(taskflow.graph().node(a).num_successors(), 2);
    assert_eq!(taskflow.graph().node(b).num_predecessors(), 0);

    taskflow.remove_dependency(a, c);
    assert_eq!(taskflow.graph().node(a).num_successors(), 1);
    assert_eq!(taskflow.graph().node(c).num_predecessors(), 0);

    taskflow.remove_dependency(a, d);
    assert_eq!(taskflow.graph().node(a).num_successors(), 0);
    assert_eq!(taskflow.graph().node(d).num_predecessors(), 0);

    // The second half of the case: every duplicate of a dependency goes with
    // one `remove_dependency`, and no task leaves the graph.
    for successor in [b, b, c, c, d, d] {
        taskflow.graph_mut().add_edge(a, successor);
    }
    assert_eq!(taskflow.graph().node(a).num_successors(), 6);
    assert_eq!(taskflow.graph().node(b).num_predecessors(), 2);

    taskflow.remove_dependency(a, b);
    assert_eq!(taskflow.graph().node(a).num_successors(), 4);
    assert_eq!(taskflow.graph().node(b).num_predecessors(), 0);

    taskflow.remove_dependency(a, c);
    assert_eq!(taskflow.graph().node(a).num_successors(), 2);
    assert_eq!(taskflow.graph().node(c).num_predecessors(), 0);

    taskflow.remove_dependency(a, d);
    assert_eq!(taskflow.graph().node(a).num_successors(), 0);
    assert_eq!(taskflow.graph().node(d).num_predecessors(), 0);

    assert_eq!(taskflow.num_tasks(), 4);
}

/// `TEST_CASE("STDFunction")`: emplacing a callable that is already boxed,
/// which is upstream's `std::function<void()>`.
///
/// Upstream's counts come from running the graph, where its `func2` is a
/// *condition* task whose return value picks one successor. The handle for that
/// exists now, but nothing schedules on it yet, so the assertions here count
/// invocations of every task instead of the subset a condition would select;
/// what the case is really testing, that a type-erased callable survives
/// `emplace` and can be swapped afterwards, translates unchanged.
#[test]
fn std_function() {
    let counter = Arc::new(AtomicUsize::new(0));
    let bump = |counter: &Arc<AtomicUsize>| -> Box<dyn FnMut() + Send> {
        let counter = Arc::clone(counter);
        Box::new(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        })
    };
    let idle = || -> Box<dyn FnMut() + Send> { Box::new(|| {}) };

    // Scenario 1: emplace four boxed closures, three of which count.
    let mut taskflow = Taskflow::new();
    let a = taskflow.emplace(bump(&counter));
    let b = taskflow.emplace(bump(&counter));
    let c = taskflow.emplace(idle());
    let d = taskflow.emplace(bump(&counter));
    taskflow.graph_mut().add_edge(a, b);
    taskflow.graph_mut().add_edge(b, c);
    taskflow.graph_mut().add_edge(b, d);
    assert_eq!(taskflow.num_tasks(), 4);
    invoke_every_task(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), 3);

    // Scenario 2: replace the work of two tasks, keeping the graph. Upstream
    // spells this `C.work(func4)`, which is `Task`'s API; `set_handle` is the
    // node-level equivalent.
    counter.store(0, Ordering::Relaxed);
    taskflow
        .graph_mut()
        .node_mut(c)
        .set_handle(Handle::Static(Work::new(bump(&counter))));
    taskflow
        .graph_mut()
        .node_mut(d)
        .set_handle(Handle::Static(Work::new(idle())));
    invoke_every_task(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), 3);
    assert_eq!(taskflow.num_tasks(), 4);
    assert_eq!(taskflow.graph().node(b).num_successors(), 2);

    // Scenario 3: clear, then emplace the same four boxed closures in one
    // call, which is upstream's variadic `emplace` plus `std::tie`.
    taskflow.clear();
    counter.store(0, Ordering::Relaxed);
    let (a, b, c, d) = taskflow.emplace_n((bump(&counter), bump(&counter), idle(), bump(&counter)));
    taskflow.graph_mut().add_edge(a, b);
    taskflow.graph_mut().add_edge(b, c);
    taskflow.graph_mut().add_edge(b, d);
    assert_eq!(taskflow.num_tasks(), 4);
    invoke_every_task(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), 3);
}

/// `TEST_CASE("Cond.Types")`: both ways of writing a condition callable produce
/// a task that reports `TaskType::Condition`.
///
/// Upstream's two `static_assert(tf::is_condition_task_v<...>)` lines are the
/// concept check that picks the `emplace` overload. Rust has no overloading, so
/// what stands in for them is that `emplace_condition` accepts both callables
/// at all: a closure whose `i32` is inferred, and one that spells the return
/// type out. Neither compiles if the bound is wrong.
#[test]
fn cond_types() {
    let mut taskflow = Taskflow::new();

    let explicit_c = || 1;
    let implicit_c = || -> i32 { 2 };

    let explicit_task = taskflow.emplace_condition(explicit_c);
    let implicit_task = taskflow.emplace_condition(implicit_c);

    assert_eq!(
        taskflow.task_ref(explicit_task).task_type(),
        TaskType::Condition
    );
    assert_eq!(
        taskflow.task_ref(implicit_task).task_type(),
        TaskType::Condition
    );
}

/// `TEST_CASE("MultiCond.Types")`: the same for a multi-condition callable,
/// which upstream also reports as `TaskType::CONDITION`.
///
/// Upstream returns a `tf::SmallVector<int>`; `Vec<i32>` stands in for it.
#[test]
fn multi_cond_types() {
    let mut taskflow = Taskflow::new();

    let explicit_mc = Vec::new;
    let implicit_mc = || -> Vec<i32> { vec![1, 2, 3, 9] };

    let explicit_task = taskflow.emplace_multi_condition(explicit_mc);
    let implicit_task = taskflow.emplace_multi_condition(implicit_mc);

    assert_eq!(
        taskflow.task_ref(explicit_task).task_type(),
        TaskType::Condition
    );
    assert_eq!(
        taskflow.task_ref(implicit_task).task_type(),
        TaskType::Condition
    );
    // A condition and a multi-condition share one task type, so the handle is
    // what tells them apart.
    assert!(
        taskflow
            .graph()
            .node(explicit_task)
            .handle()
            .is_multi_condition()
    );
}

/// The graph half of `void loop_cond(unsigned w)`: `A -> B`, `B -> B, C`, and
/// the dependency counts that a self-looping condition task implies.
///
/// Upstream then runs the taskflow and asserts the loop executed a hundred
/// times, which needs the conditional scheduling that is still to come. What
/// translates today is the half before `executor.run`, which is also the half
/// that pins down weak dependencies: `B` is preceded by `A` and by itself, and
/// because it is the conditioner, one of those two is weak.
#[test]
fn loop_cond_dependency_counts() {
    let mut taskflow = Taskflow::new();
    // Upstream's `A` and `C` reset and check a counter the run walks, which
    // only means anything once the run exists; `B`'s body is the one this test
    // still has a use for, so it is the one kept.
    let a = taskflow.emplace(|| {});
    let b = taskflow.emplace_condition({
        let mut counter = 0;
        move || {
            counter += 1;
            if counter < 100 { 0 } else { 1 }
        }
    });
    let c = taskflow.emplace(|| {});

    taskflow.task(a).precede([b]);
    taskflow.task(b).precede([b, c]);

    let handle = taskflow.task_ref(a);
    assert_eq!(handle.num_strong_dependencies(), 0);
    assert_eq!(handle.num_weak_dependencies(), 0);
    assert_eq!(handle.num_predecessors(), 0);

    let handle = taskflow.task_ref(b);
    assert_eq!(handle.num_strong_dependencies(), 1);
    assert_eq!(handle.num_weak_dependencies(), 1);
    assert_eq!(handle.num_predecessors(), 2);

    // Not upstream's, but the other end of the same edge: `C` is reachable only
    // through the conditioner, so its one dependency is weak.
    let handle = taskflow.task_ref(c);
    assert_eq!(handle.num_strong_dependencies(), 0);
    assert_eq!(handle.num_weak_dependencies(), 1);
    assert_eq!(handle.num_predecessors(), 1);

    // The condition itself still returns what it was written to return, which
    // is what #31 will schedule on: 0 ninety-nine times, then 1.
    let Handle::Condition(work) = taskflow.graph().node(b).handle() else {
        panic!("emplace_condition left something else behind");
    };
    let selected: Vec<i32> = (0..100).map(|_| work.invoke()).collect();
    assert_eq!(selected.iter().filter(|branch| **branch == 0).count(), 99);
    assert_eq!(selected[99], 1);
}

/// `A.work([&]() { return conds; })` from `multiple_branches`: a placeholder
/// becomes a multi-condition task after its branches are wired up.
///
/// Upstream's `conds` runs from -10 to 110 over a graph of a hundred branches
/// and asserts that only the indices in range selected anything. Which of them
/// run is the executor's business (#31); what belongs here is that the whole
/// signed range survives the round trip to the node and back, since that is the
/// reason the index is an `i32` and not a `usize`.
#[test]
fn a_placeholder_becomes_a_multi_condition() {
    let mut taskflow = Taskflow::new();
    let a = taskflow.placeholder();
    let branches: Vec<Task> = (0..100).map(|_| taskflow.emplace(|| {})).collect();
    taskflow.task(a).precede(branches.iter().copied());

    assert_eq!(taskflow.task_ref(a).task_type(), TaskType::Placeholder);

    let conds: Vec<i32> = (-10..=110).step_by(10).collect();
    taskflow.task(a).multi_condition_work({
        let conds = conds.clone();
        move || conds.clone()
    });

    assert_eq!(taskflow.task_ref(a).task_type(), TaskType::Condition);
    assert_eq!(taskflow.task_ref(a).num_successors(), 100);
    // Every branch now hangs off a conditioner, so every one of those hundred
    // dependencies is weak.
    for branch in &branches {
        assert_eq!(taskflow.task_ref(*branch).num_weak_dependencies(), 1);
        assert_eq!(taskflow.task_ref(*branch).num_strong_dependencies(), 0);
    }

    let Handle::MultiCondition(work) = taskflow.graph().node(a).handle() else {
        panic!("multi_condition_work left something else behind");
    };
    assert_eq!(work.invoke(), conds);
    // The out-of-range indices are carried through untouched: selecting
    // nothing is the callable's prerogative, not an error the node rejects.
    assert_eq!(work.invoke().first(), Some(&-10));
    assert_eq!(work.invoke().last(), Some(&110));
}

/// `TEST_CASE("Iterators")`, the per-taskflow half of the Generic subcase:
/// `for_each_task` visits every task, and the handles it hands out are the ones
/// the graph was built with. The `for_each_successor`/`for_each_predecessor`
/// half belongs to `Task`.
#[test]
fn iterators() {
    let mut taskflow = Taskflow::new();
    let built: Vec<Task> = ["A", "B", "C", "D", "E"]
        .into_iter()
        .map(|name| {
            let task = taskflow.emplace(|| {});
            taskflow.graph_mut().node_mut(task).set_name(name);
            task
        })
        .collect();

    let mut visited = Vec::new();
    taskflow.for_each_task(|task| visited.push(task));
    assert_eq!(visited, built);
    assert!(visited.contains(&built[0]));

    // Upstream's visitor renames the tasks it is handed, which is how it shows
    // the handle addresses the real node and not a copy.
    for task in visited {
        taskflow.graph_mut().node_mut(task).set_name("visited");
    }
    let names: Vec<&str> = taskflow
        .graph()
        .iter()
        .map(|(_, node)| node.name())
        .collect();
    assert_eq!(names, vec!["visited"; 5]);
}

/// Issue #10's own bar for `erase`: take out a middle task, then check the
/// neighbours' counts and that every other handle still resolves.
///
/// This is the acceptance test for choosing tombstones over a swap-remove: a
/// compacting erase would have moved `e` into the erased slot and left `b`
/// through `e` addressing the wrong nodes.
#[test]
fn erase_a_middle_task() {
    let mut taskflow = Taskflow::new();
    let (a, b, c, d, e) = taskflow.emplace_n((|| {}, || {}, || {}, || {}, || {}));
    for (task, name) in [(a, "a"), (b, "b"), (c, "c"), (d, "d"), (e, "e")] {
        taskflow.graph_mut().node_mut(task).set_name(name);
    }
    // a -> c, b -> c, c -> d, c -> e: c is the middle of a diamond.
    for (from, to) in [(a, c), (b, c), (c, d), (c, e)] {
        taskflow.graph_mut().add_edge(from, to);
    }

    taskflow.erase(c);

    // The neighbours lost exactly the edges that went through c.
    assert_eq!(taskflow.graph().node(a).num_successors(), 0);
    assert_eq!(taskflow.graph().node(b).num_successors(), 0);
    assert_eq!(taskflow.graph().node(d).num_predecessors(), 0);
    assert_eq!(taskflow.graph().node(e).num_predecessors(), 0);

    // Every other handle still resolves, and to the node it always named.
    for (task, name) in [(a, "a"), (b, "b"), (d, "d"), (e, "e")] {
        assert!(taskflow.graph().contains(task));
        assert_eq!(taskflow.graph().node(task).name(), name);
    }

    // The erased handle names nothing live, and the task count follows.
    assert!(!taskflow.graph().contains(c));
    assert!(taskflow.graph().get(c).is_none());
    assert_eq!(taskflow.num_tasks(), 4);

    let mut visited = Vec::new();
    taskflow.for_each_task(|task| visited.push(task));
    assert_eq!(visited, vec![a, b, d, e]);

    // The tombstoned slot is never refilled, so a later task cannot inherit
    // c's index and make the stale handle resolve again.
    let f = taskflow.emplace(|| {});
    assert_ne!(f, c);
    assert!(!taskflow.graph().contains(c));
    assert_eq!(taskflow.num_tasks(), 5);
}

/// `TEST_CASE("Type")`: every builder reports the task type upstream's
/// `type()` gives it, over all four node kinds dynamic tasking adds.
///
/// Upstream's `t7`, the `[](tf::NonpreemptiveRuntime&){}` body, is left out:
/// that second runtime shape is outside this milestone, and it reports
/// `RUNTIME` there only because upstream folds the two together. Upstream's
/// `static_assert(tf::is_subflow_task_v<...>)` block has no translation
/// either, for the reason `cond_types` gives: the bound on each method *is*
/// the concept check here, and a body of the wrong shape is a compile error
/// rather than a failing assertion. `adopt` is not in upstream's case at all;
/// it belongs beside `composed_of`, so it is here.
#[test]
fn task_type() {
    let mut taskflow = Taskflow::new();
    let target = Arc::new(Taskflow::with_name("target"));

    let t1 = taskflow.emplace(|| {});
    let t2 = taskflow.emplace_condition(|| 1);
    let t3 = taskflow.emplace_subflow(|_: &mut Subflow<'_>| {});
    let t4 = taskflow.composed_of(Arc::clone(&target));
    let t5 = taskflow.emplace_multi_condition(|| vec![1, 2]);
    let t6 = taskflow.emplace_runtime(|_: &mut Runtime<'_>| {});
    let t7 = taskflow.adopt(Graph::new());

    assert_eq!(taskflow.task_ref(t1).task_type(), TaskType::Static);
    assert_eq!(taskflow.task_ref(t2).task_type(), TaskType::Condition);
    assert_eq!(taskflow.task_ref(t3).task_type(), TaskType::Subflow);
    assert_eq!(taskflow.task_ref(t4).task_type(), TaskType::Module);
    assert_eq!(taskflow.task_ref(t5).task_type(), TaskType::Condition);
    assert_eq!(taskflow.task_ref(t6).task_type(), TaskType::Runtime);
    // Upstream has no `ADOPTED_MODULE` task type: the owned form reports
    // `MODULE` like the shared one.
    assert_eq!(taskflow.task_ref(t7).task_type(), TaskType::Module);

    // The handle predicates agree with the task types, and the module pair is
    // the one place where two handles answer one predicate.
    assert!(taskflow.graph().node(t3).handle().is_subflow());
    assert!(taskflow.graph().node(t6).handle().is_runtime());
    assert!(taskflow.graph().node(t4).handle().is_module());
    assert!(taskflow.graph().node(t7).handle().is_module());
    assert_eq!(taskflow.num_tasks(), 7);
}

/// Composing one taskflow into another twice hands out two shares of the same
/// target rather than copying it.
///
/// Upstream's `composed_of(taskflow2)` takes a reference and leaves the
/// lifetime to the caller; an `Arc<Taskflow>` is this port's answer, so what
/// there is to check from outside the crate is the share count and that both
/// module tasks point at the one graph.
#[test]
fn composed_of_shares_one_target() {
    let mut target = Taskflow::with_name("target");
    let (first_inner, second_inner) = target.emplace_n((|| {}, || {}));
    target.linearize([first_inner, second_inner]);
    let target = Arc::new(target);

    let mut taskflow = Taskflow::new();
    let first = taskflow.composed_of(Arc::clone(&target));
    let second = taskflow.composed_of(Arc::clone(&target));
    taskflow.linearize([first, second]);

    // One target, two module tasks, three shares: theirs and this test's.
    assert_eq!(Arc::strong_count(&target), 3);
    let Handle::Module(from_first) = taskflow.graph().node(first).handle() else {
        panic!("composed_of left something else behind");
    };
    let Handle::Module(from_second) = taskflow.graph().node(second).handle() else {
        panic!("composed_of left something else behind");
    };
    assert!(Arc::ptr_eq(from_first, from_second));
    assert!(Arc::ptr_eq(from_first, &target));

    // The target is reachable through the module task, which is what running
    // one will need: its name and its whole graph.
    assert_eq!(from_first.name(), "target");
    assert_eq!(from_first.num_tasks(), 2);
    assert_eq!(
        from_first.graph().successors(first_inner),
        [second_inner].as_slice()
    );

    // Erasing one module task gives up one share and leaves the other's.
    taskflow.erase(second);
    assert_eq!(Arc::strong_count(&target), 2);
    assert_eq!(target.num_tasks(), 2);
}

/// `adopt` moves the graph into the task: upstream's `adopt(Graph&&)`, whose
/// doc comment is "transfers ownership of the given tf::Graph into the task".
///
/// The move is a compile-time fact -- the graph is passed by value and the
/// caller has no handle on it afterwards -- so what is left to assert is that
/// the task kept all of it, and that it is the task which owns it now.
#[test]
fn adopt_moves_the_graph_into_the_task() {
    let witness = Arc::new(());
    let held = Arc::clone(&witness);

    // Upstream builds the adopted graph with `tf::FlowBuilder{g}.emplace(...)`.
    // There is no such borrowing builder here -- `FlowBuilder` is implemented
    // for the types that own a graph -- so the nodes go in through the arena.
    let mut module = Graph::new();
    let inner = module.add_named_node(
        "adopted",
        Handle::Static(Work::new(move || {
            let _ = &held;
        })),
    );
    module.add_named_node("second", Handle::Placeholder);
    module.add_edge(inner, Task::new(1));
    assert_eq!(Arc::strong_count(&witness), 2);

    let mut taskflow = Taskflow::new();
    let task = taskflow.adopt(module);
    taskflow.graph_mut().node_mut(task).set_name("module");

    // The task holds the whole graph, edges and all, and the composing
    // taskflow is still one task big.
    let Handle::AdoptedModule(adopted) = taskflow.graph().node(task).handle() else {
        panic!("adopt left something else behind");
    };
    assert_eq!(adopted.len(), 2);
    assert_eq!(adopted.node(inner).name(), "adopted");
    assert_eq!(adopted.successors(inner), [Task::new(1)].as_slice());
    assert_eq!(taskflow.num_tasks(), 1);
    // Moved in, not cloned: the witness still has exactly one holder besides
    // this test's own.
    assert_eq!(Arc::strong_count(&witness), 2);

    // And the task owning the graph is what erasing it proves.
    taskflow.erase(task);
    assert_eq!(Arc::strong_count(&witness), 1);
}

/// A subflow task owns an empty graph from the moment it is built, and what
/// the body will build goes in there rather than into the taskflow.
///
/// The body itself cannot run outside a worker -- a [`Subflow`] borrows the
/// executor and the worker that spawned it -- so the builder half of that is
/// asserted in `src/subflow.rs`, where the crate can construct one.
#[test]
fn a_subflow_task_carries_its_own_graph() {
    let mut taskflow = Taskflow::new();
    let spawner = taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
        let children: Vec<Task> = (0..3).map(|_| subflow.emplace(|| {})).collect();
        subflow.linearize(children);
    });
    let after = taskflow.emplace(|| {});
    taskflow.task(spawner).precede([after]);

    let Handle::Subflow { subgraph, .. } = taskflow.graph().node(spawner).handle() else {
        panic!("emplace_subflow left something else behind");
    };
    assert!(subgraph.lock().expect("uncontended").is_empty());

    // The subflow node is an ordinary citizen of the graph it lives in: not a
    // conditioner, so the dependency it hands `after` is strong.
    assert_eq!(taskflow.num_tasks(), 2);
    assert_eq!(taskflow.task_ref(after).num_strong_dependencies(), 1);
    assert_eq!(taskflow.task_ref(after).num_weak_dependencies(), 0);
}

/// The two runtime handles are told apart by their own predicates, which is
/// what `preempts_itself` reads: a `Handle::Runtime` sets `PREEMPTED` before
/// its body runs and a `Handle::NonpreemptiveRuntime` never does, so a
/// dispatch that confused them would resume a node that is not waiting to be.
/// The flat `TaskType` folds both into `Runtime` and cannot say which.
#[test]
fn the_two_runtime_handles_are_told_apart_by_their_predicates() {
    let mut taskflow = Taskflow::new();
    let preemptive = taskflow.emplace_runtime(|_| {});
    let nonpreemptive = taskflow.emplace_nonpreemptive_runtime(|_| {});

    assert_eq!(taskflow.task_ref(preemptive).task_type(), TaskType::Runtime);
    assert_eq!(
        taskflow.task_ref(nonpreemptive).task_type(),
        TaskType::Runtime
    );

    let graph = taskflow.graph();
    assert!(graph.node(preemptive).handle().is_runtime());
    assert!(!graph.node(preemptive).handle().is_nonpreemptive_runtime());
    assert!(
        graph
            .node(nonpreemptive)
            .handle()
            .is_nonpreemptive_runtime()
    );
    assert!(!graph.node(nonpreemptive).handle().is_runtime());
}
