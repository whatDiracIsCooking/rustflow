//! Translated from `unittests/test_basics.cpp` (Taskflow v4.1.0).
//!
//! The cases ported here are the ones that exercise the task handle API --
//! `Type`, `Builder`, `Task.RemoveDependencies`, the per-task half of
//! `Iterators`, `Hash` -- and the ones that exercise running a graph at all:
//! `RunAndWait` and `WorkerID`. Three standing adaptations, because the port
//! is not yet where upstream is:
//!
//! - `corun`, `run_n` and `run_until` are later issues, so a case that nests
//!   one run inside another says how it stands in for `corun` where it does;
//!   `RunAndWait.Complex` is the one that cannot stand in at all, and says why.
//!   The task bodies that assert on a counter are otherwise upstream's, run by
//!   [`run`] through the real executor.
//! - There is no `FlowBuilder`, so tasks are created straight off the graph
//!   arena and `placeholder()`/`emplace()`/`linearize()` are spelled out by
//!   hand here.
//! - `Iterators/Generic` renames tasks from inside `for_each_successor`, which
//!   an index-handle visitor cannot do while the walk borrows the graph; the
//!   walk collects the indices and the renaming follows it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::{Executor, Handle, RunError, RunHandle, Task, TaskType, Taskflow};

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

/// Runs `taskflow` to completion, upstream's `executor.run(taskflow).wait()`.
///
/// A fresh executor per call, as upstream's cases build one per test. The
/// `expect` is what keeps the assertions upstream writes *inside* its task
/// bodies meaningful: a failed assertion in a task is a panic, the run reports
/// it, and this turns it back into a failed test.
fn run(taskflow: &Taskflow) {
    let executor = Executor::new(4);
    executor
        .run(taskflow)
        .wait()
        .expect("a task of this taskflow panicked");
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

    // Upstream runs the empty taskflow at every worker count from one to 32.
    // Eight is where this port's worker sweeps stop, and the subcase is about
    // the executor rather than the builder: an empty run has no source task,
    // so it must complete without ever reaching a worker.
    for workers in 1..=8 {
        let executor = Executor::new(workers);
        executor
            .run(&taskflow)
            .wait()
            .expect("an empty run cannot panic");
        assert_eq!(executor.num_topologies(), 0);
    }
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

    run(&taskflow);
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
    run(&taskflow);
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
    run(&taskflow);
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

    run(&taskflow);
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

    run(&taskflow);
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

    run(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    assert_eq!(taskflow.num_tasks(), NUM_TASKS);
}

#[test]
fn builder_succeed() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(0));

    // Upstream creates `dst` first and has every source succeed it, so the
    // destination is task 0 and runs last all the same -- which is the part a
    // real scheduler gets right and running in creation order did not.
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

    run(&taskflow);
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

    let observed = Arc::clone(&counter);
    let dst = emplace(&mut taskflow, move || {
        assert_eq!(observed.load(Ordering::Relaxed), NUM_TASKS);
    });
    for task in &silent_tasks {
        taskflow.task(dst).succeed([*task]);
    }

    assert_eq!(taskflow.task_ref(src).num_successors(), NUM_TASKS);
    assert_eq!(taskflow.task_ref(dst).num_predecessors(), NUM_TASKS);

    run(&taskflow);
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

    run(&taskflow);
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

    run(&taskflow);
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

// --------------------------------------------------------
// Testcase: RunAndWait
// --------------------------------------------------------

#[test]
fn run_and_wait_simple() {
    // Two workers, as upstream. The tasks of the main taskflow each block on
    // an inner run, so a worker has to be left over to make progress on it --
    // which is exactly why upstream reaches for `corun` here, and `corun` is a
    // later issue. What makes the blocking stand-in safe is the shape of the
    // graph: the main taskflow is a chain, so only ever one of its tasks is
    // running, and only ever one worker is blocked.
    let executor = Arc::new(Executor::new(2));
    let counter = Arc::new(AtomicUsize::new(0));

    // The taskflow the main one runs: `A -> B`, each bumping the counter.
    let others = Arc::new({
        let mut others = Taskflow::new();
        let a = {
            let counter = Arc::clone(&counter);
            emplace(&mut others, move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        };
        let b = {
            let counter = Arc::clone(&counter);
            emplace(&mut others, move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        };
        others.task(a).precede([b]);
        others
    });

    // The main taskflow: `C -> D`, each of which runs `others` and then checks
    // what the counter must be by the time it comes back.
    let mut taskflow = Taskflow::with_name("Demo");
    let observe = |expected: usize| {
        let executor = Arc::clone(&executor);
        let others = Arc::clone(&others);
        let counter = Arc::clone(&counter);
        move || {
            executor
                .run(&others)
                .wait()
                .expect("the inner run reported a panic");
            assert_eq!(counter.load(Ordering::Relaxed), expected);
        }
    };
    let c = emplace(&mut taskflow, observe(2));
    let d = emplace(&mut taskflow, observe(4));
    taskflow.task(c).precede([d]);

    executor
        .run(&taskflow)
        .wait()
        .expect("a task of the main taskflow panicked");

    // And `others` runs once more from this thread, as upstream's last line
    // does: the same taskflow, run again after two runs of it have finished.
    executor.run(&others).wait().expect("a task panicked");

    assert_eq!(counter.load(Ordering::Relaxed), 6);
    executor.wait_for_all();
    assert_eq!(executor.num_topologies(), 0);
}

#[test]
fn run_and_wait_complex() {
    const N: usize = 100;
    const T: usize = 1000;

    let executor = Executor::new(2);
    let counter = Arc::new(AtomicUsize::new(0));

    let taskflows: Vec<Taskflow> = (0..N)
        .map(|_| {
            let mut taskflow = Taskflow::new();
            for _ in 0..T {
                let counter = Arc::clone(&counter);
                emplace(&mut taskflow, move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            }
            taskflow
        })
        .collect();

    // Upstream runs the hundred inner taskflows from the hundred tasks of one
    // outer taskflow, each corunning one of them. The blocking stand-in that
    // works for `RunAndWait.Simple` cannot work here, and the reason is worth
    // writing down: a hundred *independent* outer tasks would put two workers
    // into a blocking wait at once and leave nobody to run the inner graphs.
    // So the hundred runs are started from this thread and overlap through
    // their handles, which puts the same hundred topologies in flight at the
    // same time -- and does it without a `corun` the port has not got yet.
    let handles: Vec<RunHandle<'_>> = taskflows.iter().map(|tf| executor.run(tf)).collect();
    for handle in handles {
        handle.wait().expect("a task panicked");
    }

    assert_eq!(counter.load(Ordering::Relaxed), N * T);
    executor.wait_for_all();
    assert_eq!(executor.num_topologies(), 0);
}

// --------------------------------------------------------
// Testcase: WorkerID
// --------------------------------------------------------

/// Upstream's `worker_id(unsigned)`, minus the subflow half of it: a subflow
/// is a later milestone, so only the static task `A` survives and `B` becomes
/// a second one of it.
fn worker_id(workers: usize) {
    let executor = Arc::new(Executor::new(workers));
    assert!(
        executor.this_worker_id().is_none(),
        "the thread that built the executor is not one of its workers"
    );

    let mut taskflow = Taskflow::new();
    let check = || {
        let executor = Arc::clone(&executor);
        move || {
            let id = executor
                .this_worker_id()
                .expect("a task body runs on a worker of its own executor");
            assert!(id < workers, "worker id {id} is outside 0..{workers}");
            assert!(
                executor.this_worker().is_some(),
                "worker {id} could not find itself"
            );
        }
    };

    for _ in 0..1000 {
        let a = emplace(&mut taskflow, check());
        let b = emplace(&mut taskflow, check());
        taskflow.task(a).precede([b]);
    }

    // Upstream's `run_n(taskflow, 100)` is a later issue, so the repetition is
    // ten sequential runs instead -- ten rather than a hundred only to keep
    // the eight worker counts of this case inside a reasonable test runtime.
    // The repetition itself is upstream's point and is kept: an id a worker
    // reports correctly only on its first job would still pass if run once.
    for _ in 0..10 {
        executor
            .run(&taskflow)
            .wait()
            .expect("a task reported a bad worker id");
    }
}

#[test]
fn worker_id_1_thread() {
    worker_id(1);
}

#[test]
fn worker_id_2_threads() {
    worker_id(2);
}

#[test]
fn worker_id_3_threads() {
    worker_id(3);
}

#[test]
fn worker_id_4_threads() {
    worker_id(4);
}

#[test]
fn worker_id_5_threads() {
    worker_id(5);
}

#[test]
fn worker_id_6_threads() {
    worker_id(6);
}

#[test]
fn worker_id_7_threads() {
    worker_id(7);
}

#[test]
fn worker_id_8_threads() {
    worker_id(8);
}

// --------------------------------------------------------
// Testcase: a panicking task, and a graph deep enough to steal from
//
// Neither is upstream's. Upstream's exception cases live in
// `unittests/test_exception.cpp` and test cancellation along with the throw,
// which is a later milestone; these two are the issue's own bar for the first
// end-to-end run.
// --------------------------------------------------------

/// How many tasks run beside the panicking one.
const SIBLINGS: usize = 16;

#[test]
fn a_panicking_task_is_reported_and_the_rest_of_the_run_still_completes() {
    let executor = Executor::new(4);
    let siblings = Arc::new(AtomicUsize::new(0));
    let sinks = Arc::new(AtomicUsize::new(0));

    // One task that panics, sixteen beside it that do not, and a sink that
    // succeeds all seventeen. The sink is the part that matters: a panicking
    // task must not cancel the run, because cancellation is a later milestone
    // and silently skipping the rest of the graph would be the wrong half of
    // it to have.
    let mut taskflow = Taskflow::new();
    let mut sources = vec![emplace(&mut taskflow, || panic!("boom"))];
    for _ in 0..SIBLINGS {
        let siblings = Arc::clone(&siblings);
        sources.push(emplace(&mut taskflow, move || {
            siblings.fetch_add(1, Ordering::Relaxed);
        }));
    }
    let sink = {
        let sinks = Arc::clone(&sinks);
        emplace(&mut taskflow, move || {
            sinks.fetch_add(1, Ordering::Relaxed);
        })
    };
    taskflow.task(sink).succeed(sources.iter().copied());

    // The default panic hook prints the panic on its way past. The test
    // harness captures that, so it shows up only when this test fails, which
    // is when it is worth seeing; `--nocapture` is where to look for it
    // otherwise. Replacing the hook would silence every other test running
    // beside this one, so it stays as it is.
    let error = executor
        .run(&taskflow)
        .wait()
        .expect_err("the panic was not reported");
    if let RunError::Panicked(payload) = &error {
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
    } else {
        panic!("unexpected error: {error:?}");
    }

    assert_eq!(siblings.load(Ordering::Relaxed), SIBLINGS);
    assert_eq!(sinks.load(Ordering::Relaxed), 1, "the sink did not run");

    // The same taskflow again. The panicking task poisoned its own work on the
    // way out, so this is also the check that a poisoned task is still
    // runnable and still reports the panic rather than a lock error.
    let error = executor
        .run(&taskflow)
        .wait()
        .expect_err("the second run reported no panic");
    assert!(matches!(error, RunError::Panicked(_)));
    assert_eq!(siblings.load(Ordering::Relaxed), SIBLINGS * 2);
    assert_eq!(sinks.load(Ordering::Relaxed), 2);

    // And the executor itself is no worse for it.
    let mut after = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(0));
    for _ in 0..NUM_TASKS {
        let counter = Arc::clone(&counter);
        emplace(&mut after, move || {
            counter.fetch_add(1, Ordering::Relaxed);
        });
    }
    executor
        .run(&after)
        .wait()
        .expect("the executor is unusable after a task panicked");
    assert_eq!(counter.load(Ordering::Relaxed), NUM_TASKS);
}

/// Builds a chain of diamonds -- `src -> {left, right} -> join`, with each
/// diamond's join the next one's source -- of exactly [`NUM_TASKS`] tasks,
/// every one of which bumps `counter`.
///
/// The shape is the point: a diamond makes two tasks runnable at once, so the
/// successors a worker pushes onto its own queue are there for another worker
/// to steal, and the join makes the two of them meet again. A chain of a
/// hundred tasks is also deep enough that a run cannot finish before the next
/// one starts in the loop below.
fn diamond_chain(counter: &Arc<AtomicUsize>) -> Taskflow {
    let mut taskflow = Taskflow::new();
    let bump = |taskflow: &mut Taskflow| {
        let counter = Arc::clone(counter);
        emplace(taskflow, move || {
            counter.fetch_add(1, Ordering::Relaxed);
        })
    };

    let mut previous = bump(&mut taskflow);
    while taskflow.num_tasks() + 3 <= NUM_TASKS {
        let left = bump(&mut taskflow);
        let right = bump(&mut taskflow);
        let join = bump(&mut taskflow);
        taskflow.task(previous).precede([left, right]);
        taskflow.task(join).succeed([left, right]);
        previous = join;
    }

    assert_eq!(taskflow.num_tasks(), NUM_TASKS);
    taskflow
}

#[test]
fn a_thousand_runs_of_a_diamond_chain() {
    const RUNS: usize = 1000;

    let counter = Arc::new(AtomicUsize::new(0));
    let taskflow = diamond_chain(&counter);

    // One worker and eight: the first has no sibling to steal from and takes
    // every job through the shared buffers, the second has seven and takes
    // most of them by stealing.
    for workers in [1, 8] {
        let executor = Executor::new(workers);
        counter.store(0, Ordering::Relaxed);

        for _ in 0..RUNS {
            executor.run(&taskflow).wait().expect("a task panicked");
        }

        assert_eq!(
            counter.load(Ordering::Relaxed),
            RUNS * NUM_TASKS,
            "at {workers} workers"
        );
        assert_eq!(executor.num_topologies(), 0);
    }
}
