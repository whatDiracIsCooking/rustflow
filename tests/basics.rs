//! Translated from `unittests/test_basics.cpp` (Taskflow v4.1.0).
//!
//! The cases ported here are the ones that exercise the task handle API:
//! `Type`, `Builder`, `Task.RemoveDependencies`, the per-task half of
//! `Iterators`, and `Hash`. Three standing adaptations, because the port is
//! not yet where upstream is:
//!
//! - There is no executor, so where upstream calls `executor.run(taskflow)`
//!   and then asserts on a counter, [`run_in_creation_order`] invokes each
//!   task's work in the order the tasks were created. Every shape ported below
//!   is built so that creation order *is* a topological order, which is what
//!   makes the counter assertions upstream writes inside the tasks meaningful
//!   without a scheduler. The subcases whose point is the scheduling itself
//!   (`EmptyFlow` over 1..32 workers) keep only their structural half.
//! - There is no `FlowBuilder`, so tasks are created straight off the graph
//!   arena and `placeholder()`/`emplace()`/`linearize()` are spelled out by
//!   hand here.
//! - `Iterators/Generic` renames tasks from inside `for_each_successor`, which
//!   an index-handle visitor cannot do while the walk borrows the graph; the
//!   walk collects the indices and the renaming follows it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::{Handle, Task, TaskType, Taskflow};

/// Adds a placeholder task, upstream's `taskflow.placeholder()`.
fn placeholder(taskflow: &mut Taskflow, name: &str) -> Task {
    taskflow
        .graph_mut()
        .add_named_node(name, Handle::Placeholder)
}

/// Adds a task with work attached, upstream's `taskflow.emplace(callable)`.
fn emplace<C>(taskflow: &mut Taskflow, callable: C) -> Task
where
    C: FnMut() + Send + 'static,
{
    let task = taskflow.graph_mut().add_node(Handle::Placeholder);
    taskflow.task(task).work(callable);
    task
}

/// Runs every task's work once, in creation order.
///
/// The stand-in for `executor.run(taskflow).get()` until the executor lands;
/// see the module docs for why creation order is enough for these shapes.
fn run_in_creation_order(taskflow: &Taskflow) {
    for task in taskflow.graph().tasks() {
        if let Handle::Static(work) = taskflow.graph().node(task).handle() {
            work.invoke();
        }
    }
}

/// Upstream's `Taskflow::linearize`, which belongs to `FlowBuilder` and is not
/// ported yet: chain each task into the next.
fn linearize(taskflow: &mut Taskflow, tasks: &[Task]) {
    for pair in tasks.windows(2) {
        taskflow.task(pair[0]).precede([pair[1]]);
    }
}

// --------------------------------------------------------
// Testcase: Type
// --------------------------------------------------------

#[test]
fn task_type() {
    let mut taskflow = Taskflow::new();

    // Upstream builds one task per callable shape and checks the type each
    // one lands on. Two of the seven are constructible so far; the rest name
    // the milestones that will add them.
    let t1 = emplace(&mut taskflow, || {});
    let t2 = placeholder(&mut taskflow, "t2");

    assert_eq!(taskflow.task_ref(t1).task_type(), TaskType::Static);
    assert_eq!(taskflow.task_ref(t2).task_type(), TaskType::Placeholder);

    // Attaching and dropping work moves the task between the two.
    taskflow.task(t2).work(|| {});
    assert_eq!(taskflow.task_ref(t2).task_type(), TaskType::Static);
    taskflow.task(t2).reset_work();
    assert_eq!(taskflow.task_ref(t2).task_type(), TaskType::Placeholder);

    // Upstream's `to_string(TaskType)` over the whole enumeration.
    assert_eq!(TaskType::Placeholder.to_string(), "placeholder");
    assert_eq!(TaskType::Static.to_string(), "static");
    assert_eq!(TaskType::Runtime.to_string(), "runtime");
    assert_eq!(TaskType::Subflow.to_string(), "subflow");
    assert_eq!(TaskType::Condition.to_string(), "condition");
    assert_eq!(TaskType::Module.to_string(), "module");
    assert_eq!(TaskType::Async.to_string(), "async");
    assert_eq!(TaskType::Undefined.to_string(), "undefined");
}

// --------------------------------------------------------
// Testcase: Builder
// --------------------------------------------------------

/// How many tasks each `Builder` subcase builds, upstream's `num_tasks`.
const NUM_TASKS: usize = 100;

#[test]
fn builder_empty_flow() {
    let taskflow = Taskflow::new();
    assert_eq!(taskflow.num_tasks(), 0);
    assert!(taskflow.is_empty());
}

#[test]
fn builder_placeholder() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(0));

    let silent_tasks: Vec<Task> = (0..NUM_TASKS)
        .map(|i| placeholder(&mut taskflow, &i.to_string()))
        .collect();

    for (i, task) in silent_tasks.iter().enumerate() {
        let handle = taskflow.task_ref(*task);
        assert_eq!(handle.name(), i.to_string());
        assert_eq!(handle.num_predecessors(), 0);
        assert_eq!(handle.num_successors(), 0);
        assert!(!handle.has_work());
        assert!(handle.is_empty());
    }

    for task in &silent_tasks {
        let bumped = Arc::clone(&counter);
        taskflow.task(*task).work(move || {
            bumped.fetch_add(1, Ordering::Relaxed);
        });
        assert!(taskflow.task_ref(*task).has_work());
    }

    run_in_creation_order(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), NUM_TASKS);
}

#[test]
fn builder_embarrassingly_parallel() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..NUM_TASKS {
        let bumped = Arc::clone(&counter);
        emplace(&mut taskflow, move || {
            bumped.fetch_add(1, Ordering::Relaxed);
        });
    }

    assert_eq!(taskflow.num_tasks(), NUM_TASKS);
    run_in_creation_order(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), NUM_TASKS);
    assert_eq!(taskflow.num_tasks(), 100);

    counter.store(0, Ordering::Relaxed);

    for _ in 0..NUM_TASKS {
        let bumped = Arc::clone(&counter);
        emplace(&mut taskflow, move || {
            bumped.fetch_add(1, Ordering::Relaxed);
        });
    }

    assert_eq!(taskflow.num_tasks(), NUM_TASKS * 2);
    run_in_creation_order(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), NUM_TASKS * 2);
    assert_eq!(taskflow.num_tasks(), 200);
}

#[test]
fn builder_binary_sequence() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let mut tasks: Vec<Task> = Vec::new();

    for i in 0..NUM_TASKS {
        let counter = Arc::clone(&counter);
        let task = if i % 2 == 0 {
            emplace(&mut taskflow, move || {
                assert_eq!(counter.load(Ordering::Relaxed), 0);
                counter.fetch_add(1, Ordering::Relaxed);
            })
        } else {
            emplace(&mut taskflow, move || {
                assert_eq!(counter.load(Ordering::Relaxed), 1);
                counter.fetch_sub(1, Ordering::Relaxed);
            })
        };
        tasks.push(task);

        if i > 0 {
            taskflow.task(tasks[i - 1]).precede([tasks[i]]);
        }

        if i == 0 {
            assert_eq!(taskflow.task_ref(tasks[i]).num_predecessors(), 0);
        } else {
            assert_eq!(taskflow.task_ref(tasks[i]).num_predecessors(), 1);
        }
    }

    run_in_creation_order(&taskflow);
}

#[test]
fn builder_linear_counter() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let mut tasks: Vec<Task> = Vec::new();

    for i in 0..NUM_TASKS {
        let counter = Arc::clone(&counter);
        tasks.push(emplace(&mut taskflow, move || {
            assert_eq!(counter.load(Ordering::Relaxed), i);
            counter.fetch_add(1, Ordering::Relaxed);
        }));
        if i > 0 {
            taskflow.task(tasks[i - 1]).precede([tasks[i]]);
        }
    }

    run_in_creation_order(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), NUM_TASKS);
    assert_eq!(taskflow.num_tasks(), NUM_TASKS);
}

#[test]
fn builder_broadcast() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(0));

    let bumped = Arc::clone(&counter);
    let src = emplace(&mut taskflow, move || {
        bumped.fetch_add(1, Ordering::Relaxed);
    });

    let mut silent_tasks: Vec<Task> = Vec::new();
    for _ in 1..NUM_TASKS {
        let observed = Arc::clone(&counter);
        let task = emplace(&mut taskflow, move || {
            assert_eq!(observed.load(Ordering::Relaxed), 1);
        });
        silent_tasks.push(task);
        taskflow.task(src).precede([task]);
    }

    assert_eq!(taskflow.task_ref(src).num_successors(), NUM_TASKS - 1);
    for task in &silent_tasks {
        assert_eq!(taskflow.task_ref(*task).num_predecessors(), 1);
    }

    run_in_creation_order(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    assert_eq!(taskflow.num_tasks(), NUM_TASKS);
}

#[test]
fn builder_succeed() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(0));

    // Upstream creates `dst` first and has every source succeed it, so the
    // destination is task 0; running in creation order would put it before its
    // dependencies. Its assertion is therefore checked after the run instead.
    let dst = placeholder(&mut taskflow, "dst");

    for _ in 1..NUM_TASKS {
        let bumped = Arc::clone(&counter);
        let task = emplace(&mut taskflow, move || {
            bumped.fetch_add(1, Ordering::Relaxed);
        });
        taskflow.task(dst).succeed([task]);
    }

    assert_eq!(taskflow.task_ref(dst).num_predecessors(), NUM_TASKS - 1);
    assert_eq!(
        taskflow.task_ref(dst).num_strong_dependencies(),
        NUM_TASKS - 1
    );
    assert_eq!(taskflow.task_ref(dst).num_weak_dependencies(), 0);

    run_in_creation_order(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), NUM_TASKS - 1);
    assert_eq!(taskflow.num_tasks(), NUM_TASKS);
}

#[test]
fn builder_map_reduce() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(7));

    let reset = Arc::clone(&counter);
    let src = emplace(&mut taskflow, move || {
        reset.store(0, Ordering::Relaxed);
    });

    let mut silent_tasks: Vec<Task> = Vec::new();
    for _ in 0..NUM_TASKS {
        let bumped = Arc::clone(&counter);
        let task = emplace(&mut taskflow, move || {
            bumped.fetch_add(1, Ordering::Relaxed);
        });
        silent_tasks.push(task);
        taskflow.task(src).precede([task]);
    }

    // `dst` is created last so that creation order remains topological.
    let observed = Arc::clone(&counter);
    let dst = emplace(&mut taskflow, move || {
        assert_eq!(observed.load(Ordering::Relaxed), NUM_TASKS);
    });
    for task in &silent_tasks {
        taskflow.task(dst).succeed([*task]);
    }

    assert_eq!(taskflow.task_ref(src).num_successors(), NUM_TASKS);
    assert_eq!(taskflow.task_ref(dst).num_predecessors(), NUM_TASKS);

    run_in_creation_order(&taskflow);
    assert_eq!(taskflow.num_tasks(), NUM_TASKS + 2);
}

#[test]
fn builder_linearize() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(0));

    let silent_tasks: Vec<Task> = (0..NUM_TASKS)
        .map(|i| {
            let counter = Arc::clone(&counter);
            emplace(&mut taskflow, move || {
                assert_eq!(counter.load(Ordering::Relaxed), i);
                counter.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();

    linearize(&mut taskflow, &silent_tasks);

    assert_eq!(taskflow.task_ref(silent_tasks[0]).num_predecessors(), 0);
    for task in &silent_tasks[1..] {
        assert_eq!(taskflow.task_ref(*task).num_predecessors(), 1);
    }

    run_in_creation_order(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), NUM_TASKS);
    assert_eq!(taskflow.num_tasks(), NUM_TASKS);
}

#[test]
fn builder_kite() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(7));

    let reset = Arc::clone(&counter);
    let src = emplace(&mut taskflow, move || {
        reset.store(0, Ordering::Relaxed);
    });

    let silent_tasks: Vec<Task> = (0..NUM_TASKS)
        .map(|i| {
            let counter = Arc::clone(&counter);
            let task = emplace(&mut taskflow, move || {
                assert_eq!(counter.load(Ordering::Relaxed), i);
                counter.fetch_add(1, Ordering::Relaxed);
            });
            taskflow.task(src).precede([task]);
            task
        })
        .collect();

    linearize(&mut taskflow, &silent_tasks);

    let observed = Arc::clone(&counter);
    let dst = emplace(&mut taskflow, move || {
        assert_eq!(observed.load(Ordering::Relaxed), NUM_TASKS);
    });
    for task in &silent_tasks {
        taskflow.task(dst).succeed([*task]);
    }

    // The spine of the kite: each middle task is both linearized and fanned to.
    assert_eq!(taskflow.task_ref(src).num_successors(), NUM_TASKS);
    assert_eq!(taskflow.task_ref(silent_tasks[0]).num_predecessors(), 1);
    assert_eq!(taskflow.task_ref(silent_tasks[1]).num_predecessors(), 2);
    assert_eq!(taskflow.task_ref(dst).num_predecessors(), NUM_TASKS);

    run_in_creation_order(&taskflow);
    assert_eq!(taskflow.num_tasks(), NUM_TASKS + 2);
}

// --------------------------------------------------------
// Testcase: Task.RemoveDependencies
// --------------------------------------------------------

#[test]
fn task_remove_dependencies() {
    let mut taskflow = Taskflow::new();
    let a = placeholder(&mut taskflow, "a");
    let b = placeholder(&mut taskflow, "b");
    let c = placeholder(&mut taskflow, "c");
    let d = placeholder(&mut taskflow, "d");

    assert_eq!(taskflow.task_ref(a).num_successors(), 0);
    assert_eq!(taskflow.task_ref(a).num_predecessors(), 0);

    taskflow.task(a).precede([b, c, d]);
    assert_eq!(taskflow.task_ref(a).num_successors(), 3);
    assert_eq!(taskflow.task_ref(b).num_predecessors(), 1);
    assert_eq!(taskflow.task_ref(c).num_predecessors(), 1);
    assert_eq!(taskflow.task_ref(d).num_predecessors(), 1);

    // `c` is a successor of `a`, not a predecessor, so removing it as one
    // changes nothing -- exactly as upstream asserts.
    taskflow.task(a).remove_predecessors([c]);
    assert_eq!(taskflow.task_ref(b).num_predecessors(), 1);
    assert_eq!(taskflow.task_ref(c).num_predecessors(), 1);
    assert_eq!(taskflow.task_ref(d).num_predecessors(), 1);
    assert_eq!(taskflow.task_ref(a).num_successors(), 3);
    assert_eq!(taskflow.task_ref(a).num_predecessors(), 0);

    taskflow.task(a).remove_successors([c]);
    assert_eq!(taskflow.task_ref(a).num_successors(), 2);
    assert_eq!(taskflow.task_ref(a).num_predecessors(), 0);
    assert_eq!(taskflow.task_ref(b).num_predecessors(), 1);
    assert_eq!(taskflow.task_ref(c).num_predecessors(), 0);
    assert_eq!(taskflow.task_ref(d).num_predecessors(), 1);

    // The surviving successors keep their order; `c` left no hole behind.
    let successors = taskflow.graph().successors(a).to_vec();
    assert_eq!(successors, vec![b, d]);
}

// --------------------------------------------------------
// Testcase: Iterators (the per-task half)
// --------------------------------------------------------

#[test]
fn iterators_order() {
    let mut taskflow = Taskflow::new();
    let a = placeholder(&mut taskflow, "A");
    let b = placeholder(&mut taskflow, "B");
    let c = placeholder(&mut taskflow, "C");
    let d = placeholder(&mut taskflow, "D");
    let e = placeholder(&mut taskflow, "E");

    taskflow.task(a).precede([b, c, d, e]);
    taskflow.task(e).succeed([b, c, d]);

    let mut visited = Vec::new();
    taskflow
        .task_ref(a)
        .for_each_successor(|successor| visited.push(successor));
    assert_eq!(visited, vec![b, c, d, e]);

    let mut visited = Vec::new();
    taskflow
        .task_ref(e)
        .for_each_predecessor(|predecessor| visited.push(predecessor));
    assert_eq!(visited, vec![a, b, c, d]);
}

#[test]
fn iterators_generic() {
    let mut taskflow = Taskflow::new();
    let a = placeholder(&mut taskflow, "A");
    let b = placeholder(&mut taskflow, "B");
    let c = placeholder(&mut taskflow, "C");
    let d = placeholder(&mut taskflow, "D");
    let e = placeholder(&mut taskflow, "E");

    // Upstream's `taskflow.for_each_task`, which `Graph::tasks` already is.
    let tasks: Vec<Task> = taskflow.graph().tasks().collect();
    assert!(tasks.contains(&a));

    taskflow.task(a).precede([b]);

    let mut visited = Vec::new();
    taskflow.task_ref(a).for_each_successor(|s| visited.push(s));
    assert_eq!(visited, vec![b]);
    let mut visited = Vec::new();
    taskflow
        .task_ref(b)
        .for_each_predecessor(|p| visited.push(p));
    assert_eq!(visited, vec![a]);

    taskflow.task(a).precede([c]);
    taskflow.task(a).precede([d]);
    taskflow.task(a).precede([e]);
    taskflow.task(c).precede([b]);
    taskflow.task(d).precede([b]);
    taskflow.task(e).precede([b]);

    let mut counter = 0;
    let mut seen = [0; 5];
    taskflow.task_ref(a).for_each_successor(|s| {
        counter += 1;
        seen[s.index()] += 1;
    });
    assert_eq!(counter, taskflow.task_ref(a).num_successors());
    assert_eq!(seen, [0, 1, 1, 1, 1]);

    let mut counter = 0;
    let mut seen = [0; 5];
    taskflow.task_ref(b).for_each_predecessor(|p| {
        counter += 1;
        seen[p.index()] += 1;
    });
    assert_eq!(counter, taskflow.task_ref(b).num_predecessors());
    assert_eq!(seen, [1, 0, 1, 1, 1]);

    // Upstream renames each visited task from inside the visitor. An index
    // handle cannot: the walk holds the graph. Collect, then rename.
    let mut successors = Vec::new();
    taskflow
        .task_ref(a)
        .for_each_successor(|s| successors.push(s));
    for successor in successors {
        taskflow.task(successor).name("A");
    }
    assert_eq!(taskflow.task_ref(a).name(), "A");
    assert_eq!(taskflow.task_ref(b).name(), "A");
    assert_eq!(taskflow.task_ref(c).name(), "A");
    assert_eq!(taskflow.task_ref(d).name(), "A");
    assert_eq!(taskflow.task_ref(e).name(), "A");

    let mut predecessors = Vec::new();
    taskflow
        .task_ref(b)
        .for_each_predecessor(|p| predecessors.push(p));
    for predecessor in predecessors {
        taskflow.task(predecessor).name("B");
    }
    assert_eq!(taskflow.task_ref(a).name(), "B");
    assert_eq!(taskflow.task_ref(b).name(), "A");
    assert_eq!(taskflow.task_ref(c).name(), "B");
    assert_eq!(taskflow.task_ref(d).name(), "B");
    assert_eq!(taskflow.task_ref(e).name(), "B");
}

// --------------------------------------------------------
// Testcase: Hash
// --------------------------------------------------------

#[test]
fn hash() {
    let mut taskflow = Taskflow::new();

    let t1 = emplace(&mut taskflow, || {});
    let mut t2 = emplace(&mut taskflow, || {});

    let hash_of = |taskflow: &Taskflow, task: Task| taskflow.task_ref(task).hash_value();

    // Distinct tasks: upstream allows a collision, but then the handles must
    // still compare unequal.
    assert!(hash_of(&taskflow, t1) != hash_of(&taskflow, t2) || t1 != t2);

    // A copy of a handle is the same task, so it hashes the same. Upstream
    // reaches this by assigning `t2 = t1`.
    t2 = t1;
    assert_eq!(hash_of(&taskflow, t1), hash_of(&taskflow, t2));
    assert_eq!(t1, t2);

    // The hash is over identity, not content: editing the task does not move
    // it, and two tasks built the same way are still two tasks.
    let before = hash_of(&taskflow, t1);
    taskflow.task(t1).name("renamed").reset();
    assert_eq!(hash_of(&taskflow, t1), before);

    let t3 = emplace(&mut taskflow, || {});
    assert_ne!(t1, t3);
    assert_ne!(hash_of(&taskflow, t1), hash_of(&taskflow, t3));

    // And it is usable the way `std::hash<tf::Task>` is: as a map key.
    let mut set = std::collections::HashSet::new();
    assert!(set.insert(t1));
    assert!(!set.insert(t2));
    assert!(set.insert(t3));
    assert_eq!(set.len(), 2);
}
