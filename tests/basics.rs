//! Translated from `unittests/test_basics.cpp` (Taskflow v4.1.0).
//!
//! The cases ported here are the ones that exercise the task handle API --
//! `Type`, `Builder`, `Task.RemoveDependencies`, the per-task half of
//! `Iterators`, `Hash` -- and the ones that exercise running a graph at all:
//! `Future`, `SerialRuns`, `ParallelRuns`, `RunAndWait` and `WorkerID`. Three
//! standing adaptations, because the port is not yet where upstream is:
//!
//! - `corun` is a later issue, so a case that nests one run inside another says
//!   how it stands in for `corun` where it does; `RunAndWait.Complex` is the
//!   one that cannot stand in at all, and says why. The task bodies that assert
//!   on a counter are otherwise upstream's, run by [`run`] through the real
//!   executor.
//! - The task-building helpers below predate [`FlowBuilder`] and are kept
//!   rather than rewritten, so `placeholder()`, `emplace()` and `linearize()`
//!   are spelled out by hand here; what they build is the same graph the
//!   builder would.
//! - `Iterators/Generic` renames tasks from inside `for_each_successor`, which
//!   an index-handle visitor cannot do while the walk borrows the graph; the
//!   walk collects the indices and the renaming follows it.

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use rustflow::prelude::*;
use rustflow::{Executor, RunError, RunHandle, TaskType};

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
// Testcase: Future
// --------------------------------------------------------

/// Wakes the thread that parked itself waiting for a run: the whole of the
/// runtime [`block_on`] needs, and the shape any real one has.
struct Unpark(thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Drives `future` to completion on this thread, with no runtime and no
/// dependency: poll, park, and let the waker unpark us.
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);

    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::park();
    }
}

/// Upstream's `Future` case is about `std::future`'s own surface -- `valid()`
/// and `get()` on a promise somebody else fulfilled -- which in this port is
/// `impl Future for RunHandle`: a run can be awaited instead of waited for.
/// So the case becomes what that impl owes, driven by the four lines of
/// executor above rather than by a runtime the crate does not depend on.
#[test]
fn future() {
    let executor = Executor::new(4);
    let counter = Arc::new(AtomicUsize::new(0));

    // A run that has nothing to run is ready on the first poll, which is
    // upstream's "a future from an already-fulfilled promise is valid".
    let empty = Taskflow::new();
    block_on(executor.run(&empty)).expect("an empty run cannot panic");

    // A run that does have something to run is pending first and woken by the
    // worker that finishes it.
    let mut taskflow = Taskflow::new();
    let (a, b) = {
        let first = Arc::clone(&counter);
        let second = Arc::clone(&counter);
        taskflow.emplace_n((
            move || {
                first.fetch_add(1, Ordering::Relaxed);
            },
            move || {
                second.fetch_add(1, Ordering::Relaxed);
            },
        ))
    };
    taskflow.task(a).precede([b]);

    block_on(executor.run_n(&taskflow, 3)).expect("no task panicked");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        6,
        "three runs of two tasks"
    );

    // An awaited handle is not consumed, so it is still there to drop
    // afterwards -- and it reports a panic the way `wait` does.
    let mut failing = Taskflow::new();
    failing.emplace(|| panic!("boom"));
    let handle = executor.run(&failing);
    let error = block_on(handle).expect_err("the panic was not reported");
    assert!(matches!(error, RunError::Panicked(_)));

    executor.wait_for_all();
    assert_eq!(executor.num_topologies(), 0);
}

// --------------------------------------------------------
// Testcase: SerialRuns
// --------------------------------------------------------

/// Upstream's `sequential_runs(W)`, subcase by subcase.
fn sequential_runs(workers: usize) {
    let executor = Executor::new(workers);
    let counter = Arc::new(AtomicUsize::new(0));

    let mut taskflow = Taskflow::new();
    for _ in 0..NUM_TASKS {
        let counter = Arc::clone(&counter);
        emplace(&mut taskflow, move || {
            counter.fetch_add(1, Ordering::Relaxed);
        });
    }

    // SUBCASE("RunOnce")
    let handle = executor.run(&taskflow);
    assert_eq!(taskflow.num_tasks(), NUM_TASKS);
    handle.wait().expect("a task panicked");
    assert_eq!(counter.load(Ordering::Relaxed), NUM_TASKS);

    // SUBCASE("WaitForAll"): upstream drops the future on the floor and waits
    // on the executor instead. Dropping a handle here would block, which would
    // make `wait_for_all` the second thing to wait rather than the first, so
    // the handle is held *across* the wait -- the one spelling of this that
    // still asks `wait_for_all` to do the waiting.
    counter.store(0, Ordering::Relaxed);
    let handle = executor.run(&taskflow);
    executor.wait_for_all();
    assert_eq!(counter.load(Ordering::Relaxed), NUM_TASKS);
    assert_eq!(executor.num_topologies(), 0);
    drop(handle);

    // SUBCASE("MultipleRuns"): four taskflows, two hundred runs of each, none
    // of them waited for as it is issued. Upstream's point is the executor
    // soaking up eight hundred overlapping runs; this port's addition is that
    // two hundred of them are runs of the *same* taskflow, which is exactly
    // the queue `Taskflow` keeps -- they serialise behind each other instead
    // of two workers meeting on one node.
    const RUNS: usize = 200;
    let sizes = [16, 1024, 32, 128];

    let count = Arc::new(AtomicUsize::new(0));
    let taskflows: Vec<Taskflow> = sizes
        .iter()
        .map(|&size| {
            let mut taskflow = Taskflow::new();
            for _ in 0..size {
                let count = Arc::clone(&count);
                emplace(&mut taskflow, move || {
                    count.fetch_add(1, Ordering::Relaxed);
                });
            }
            taskflow
        })
        .collect();

    let mut handles: Vec<RunHandle<'_>> = Vec::with_capacity(RUNS * sizes.len());
    for _ in 0..RUNS {
        for taskflow in &taskflows {
            handles.push(executor.run(taskflow));
        }
    }
    executor.wait_for_all();
    assert_eq!(
        count.load(Ordering::Relaxed),
        RUNS * sizes.iter().sum::<usize>()
    );
    drop(handles);

    // SUBCASE, this port's own: the same two hundred runs as `run_n`, which is
    // one topology re-armed by its own tear-down rather than two hundred
    // queued behind each other. Upstream reaches this path from `run_n` in
    // `WorkerID` and from `moved_run`; at eight worker counts it is worth
    // having here too.
    counter.store(0, Ordering::Relaxed);
    executor
        .run_n(&taskflow, RUNS)
        .wait()
        .expect("a task panicked");
    assert_eq!(counter.load(Ordering::Relaxed), RUNS * NUM_TASKS);
    assert_eq!(executor.num_topologies(), 0);
}

#[test]
fn serial_runs_1_thread() {
    sequential_runs(1);
}

#[test]
fn serial_runs_2_threads() {
    sequential_runs(2);
}

#[test]
fn serial_runs_3_threads() {
    sequential_runs(3);
}

#[test]
fn serial_runs_4_threads() {
    sequential_runs(4);
}

#[test]
fn serial_runs_5_threads() {
    sequential_runs(5);
}

#[test]
fn serial_runs_6_threads() {
    sequential_runs(6);
}

#[test]
fn serial_runs_7_threads() {
    sequential_runs(7);
}

#[test]
fn serial_runs_8_threads() {
    sequential_runs(8);
}

// --------------------------------------------------------
// Testcase: ParallelRuns
// --------------------------------------------------------

/// How many threads issue runs at once, upstream's 32.
const ISSUERS: usize = 32;

/// How many `A -> B` pairs each of their taskflows holds. Upstream's 1024,
/// quartered: the case is about many threads hitting one executor at once, and
/// the port runs it at eight worker counts where upstream runs it at four.
const PAIRS: usize = 256;

/// Upstream's `parallel_runs(w)`: many threads, each with its own taskflow,
/// all running on one executor.
fn parallel_runs(workers: usize) {
    let counter = Arc::new(AtomicUsize::new(0));

    let make_taskflow = |counter: &Arc<AtomicUsize>| {
        let mut taskflow = Taskflow::new();
        for _ in 0..PAIRS {
            let bump = |counter: &Arc<AtomicUsize>| {
                let counter = Arc::clone(counter);
                move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            };
            let (a, b) = taskflow.emplace_n((bump(counter), bump(counter)));
            taskflow.task(a).precede([b]);
        }
        taskflow
    };

    // SUBCASE("RunAndWait")
    {
        let executor = Executor::new(workers);
        counter.store(0, Ordering::Relaxed);

        thread::scope(|scope| {
            for _ in 0..ISSUERS {
                scope.spawn(|| {
                    let taskflow = make_taskflow(&counter);
                    executor.run(&taskflow).wait().expect("a task panicked");
                });
            }
        });

        assert_eq!(counter.load(Ordering::Relaxed), ISSUERS * PAIRS * 2);
        assert_eq!(executor.num_topologies(), 0);
    }

    // SUBCASE("RunAndWaitForAll"): upstream issues all 32 runs from 32 threads,
    // counts them in with an atomic barrier and then waits on the executor. The
    // handles stand in for that barrier here -- a run that has been issued is a
    // handle somebody is holding -- so the taskflows live out here and the
    // threads only start the runs.
    {
        let executor = Executor::new(workers);
        counter.store(0, Ordering::Relaxed);

        let taskflows: Vec<Taskflow> = (0..ISSUERS).map(|_| make_taskflow(&counter)).collect();
        let handles = Mutex::new(Vec::with_capacity(ISSUERS));

        thread::scope(|scope| {
            for taskflow in &taskflows {
                scope.spawn(|| {
                    let handle = executor.run(taskflow);
                    handles
                        .lock()
                        .expect("the handle list is never poisoned")
                        .push(handle);
                });
            }
        });

        executor.wait_for_all();
        assert_eq!(counter.load(Ordering::Relaxed), ISSUERS * PAIRS * 2);
        assert_eq!(
            handles
                .lock()
                .expect("the handle list is never poisoned")
                .len(),
            ISSUERS
        );
    }
}

#[test]
fn parallel_runs_1_thread() {
    parallel_runs(1);
}

#[test]
fn parallel_runs_2_threads() {
    parallel_runs(2);
}

#[test]
fn parallel_runs_3_threads() {
    parallel_runs(3);
}

#[test]
fn parallel_runs_4_threads() {
    parallel_runs(4);
}

#[test]
fn parallel_runs_5_threads() {
    parallel_runs(5);
}

#[test]
fn parallel_runs_6_threads() {
    parallel_runs(6);
}

#[test]
fn parallel_runs_7_threads() {
    parallel_runs(7);
}

#[test]
fn parallel_runs_8_threads() {
    parallel_runs(8);
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

    // Upstream's `run_n(taskflow, 100)`, with ten runs rather than a hundred
    // only to keep the eight worker counts of this case inside a reasonable
    // test runtime. The repetition itself is upstream's point and is kept: an
    // id a worker reports correctly only on its first job would still pass if
    // run once.
    executor
        .run_n(&taskflow, 10)
        .wait()
        .expect("a task reported a bad worker id");
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
// Testcase: the run family's own bar
//
// Upstream's `run_n`/`run_until` overloads have no unit test of their own
// beyond the uses in `SerialRuns`, `WorkerID` and `test_movable.cpp`; these are
// issue #15's bar for them.
// --------------------------------------------------------

/// A graph of three tasks, each bumping `counter` once per run.
fn counting_chain(counter: &Arc<AtomicUsize>) -> Taskflow {
    let mut taskflow = Taskflow::new();
    let bump = |counter: &Arc<AtomicUsize>| {
        let counter = Arc::clone(counter);
        move || {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    };
    let (a, b, c) = taskflow.emplace_n((bump(counter), bump(counter), bump(counter)));
    taskflow.linearize([a, b, c]);
    taskflow
}

/// `run_until` with a counter predicate runs the graph exactly as many times as
/// the counter allows and calls the callback exactly once -- including when
/// that number is zero, where the graph must not run at all and the callback
/// must still fire.
///
/// The predicate is upstream's `[repeat]() mutable { return repeat-- == 0; }`,
/// which is also what `run_n` passes, so this pins down both.
#[test]
fn run_until_runs_the_graph_k_times_and_fires_the_callback_once() {
    const TASKS: usize = 3;

    for workers in [1, 4] {
        for runs in 0..5_usize {
            let executor = Executor::new(workers);
            let counter = Arc::new(AtomicUsize::new(0));
            let taskflow = counting_chain(&counter);

            let asked = Arc::new(AtomicUsize::new(0));
            let called = Arc::new(AtomicUsize::new(0));

            let predicate = {
                let asked = Arc::clone(&asked);
                let mut remaining = runs;
                move || {
                    asked.fetch_add(1, Ordering::Relaxed);
                    let stop = remaining == 0;
                    remaining = remaining.saturating_sub(1);
                    stop
                }
            };
            let on_finish = {
                let called = Arc::clone(&called);
                move || {
                    called.fetch_add(1, Ordering::Relaxed);
                }
            };

            executor
                .run_until_with(&taskflow, predicate, on_finish)
                .wait()
                .expect("a task panicked");

            assert_eq!(
                counter.load(Ordering::Relaxed),
                runs * TASKS,
                "{runs} runs at {workers} workers"
            );
            assert_eq!(
                called.load(Ordering::Relaxed),
                1,
                "the callback fires once per run handle, whatever the count"
            );
            assert_eq!(
                asked.load(Ordering::Relaxed),
                runs + 1,
                "the predicate is asked once before the first run and once after each"
            );
            assert_eq!(executor.num_topologies(), 0);
        }
    }
}

/// `run_n(.., 0)` is upstream's already-true predicate: nothing runs, the
/// callback still fires, and no topology is ever counted.
#[test]
fn running_a_taskflow_no_times_still_calls_the_callback() {
    let executor = Executor::new(2);
    let counter = Arc::new(AtomicUsize::new(0));
    let taskflow = counting_chain(&counter);

    let called = Arc::new(AtomicUsize::new(0));
    let handle = {
        let called = Arc::clone(&called);
        executor.run_n_with(&taskflow, 0, move || {
            called.fetch_add(1, Ordering::Relaxed);
        })
    };
    assert_eq!(executor.num_topologies(), 0, "nothing was scheduled");
    assert_eq!(
        called.load(Ordering::Relaxed),
        1,
        "called before the handle"
    );
    handle.wait().expect("nothing ran");
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

/// The callback runs before the handle is released, so a `wait` that has
/// returned has already seen whatever the callback did. That is upstream's
/// order -- `_on_finish()` before `_carry_out_promise()` -- and it is what lets
/// a caller use the callback to publish a result the waiter then reads.
#[test]
fn the_callback_is_visible_by_the_time_the_wait_returns() {
    const RUNS: usize = 200;

    let executor = Executor::new(2);
    for _ in 0..RUNS {
        let counter = Arc::new(AtomicUsize::new(0));
        let taskflow = counting_chain(&counter);
        let published = Arc::new(AtomicUsize::new(0));

        let handle = {
            let counter = Arc::clone(&counter);
            let published = Arc::clone(&published);
            executor.run_n_with(&taskflow, 2, move || {
                published.store(counter.load(Ordering::Relaxed), Ordering::SeqCst);
            })
        };
        handle.wait().expect("a task panicked");

        assert_eq!(published.load(Ordering::SeqCst), 6, "the callback ran late");
    }
}

/// A panic out of the callback must be reported rather than left to unwind out
/// of the tear-down, which would skip the release of the waiting thread.
#[test]
fn a_panicking_callback_is_reported_and_does_not_strand_the_waiter() {
    let executor = Executor::new(2);
    let counter = Arc::new(AtomicUsize::new(0));
    let taskflow = counting_chain(&counter);

    let error = executor
        .run_with(&taskflow, || panic!("callback"))
        .wait()
        .expect_err("the callback's panic was not reported");
    if let RunError::Panicked(payload) = &error {
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"callback"));
    } else {
        panic!("unexpected error: {error:?}");
    }

    assert_eq!(
        counter.load(Ordering::Relaxed),
        3,
        "the run itself finished"
    );
    assert_eq!(executor.num_topologies(), 0);
    executor.wait_for_all();
}

/// Runs of one taskflow queue up behind each other: the second run's graph does
/// not start until the first run's last task is done. The chain's counter is
/// what shows it -- a task of run `k` that saw a count from run `k + 1` would
/// mean two runs were sharing the graph.
#[test]
fn runs_of_one_taskflow_do_not_overlap() {
    const RUNS: usize = 50;
    const WIDTH: usize = 8;

    for workers in [1, 2, 8] {
        let executor = Executor::new(workers);

        // One source fanning out to `WIDTH` tasks and a sink behind them, so a
        // run has work in flight on every worker at once.
        let running = Arc::new(AtomicUsize::new(0));
        let overlaps = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();

        let enter = {
            let running = Arc::clone(&running);
            let overlaps = Arc::clone(&overlaps);
            taskflow.emplace(move || {
                if running.fetch_add(1, Ordering::SeqCst) != 0 {
                    overlaps.fetch_add(1, Ordering::SeqCst);
                }
            })
        };
        let leave = {
            let running = Arc::clone(&running);
            let overlaps = Arc::clone(&overlaps);
            taskflow.emplace(move || {
                if running.fetch_sub(1, Ordering::SeqCst) != 1 {
                    overlaps.fetch_add(1, Ordering::SeqCst);
                }
            })
        };
        for _ in 0..WIDTH {
            let middle = taskflow.emplace(|| {});
            taskflow.task(enter).precede([middle]);
            taskflow.task(leave).succeed([middle]);
        }

        // Half the runs queued behind one another, half as one topology run
        // over and over: the two paths the tear-down takes.
        let handles: Vec<RunHandle<'_>> = (0..RUNS).map(|_| executor.run(&taskflow)).collect();
        drop(handles);
        executor
            .run_n(&taskflow, RUNS)
            .wait()
            .expect("a task panicked");

        assert_eq!(
            overlaps.load(Ordering::SeqCst),
            0,
            "two runs shared the graph at {workers} workers"
        );
        assert_eq!(running.load(Ordering::SeqCst), 0);
        assert_eq!(executor.num_topologies(), 0);
    }
}

// --------------------------------------------------------
// Testcase: a panicking task, and a graph deep enough to steal from
//
// Neither is upstream's. Upstream's exception cases live in
// `unittests/test_exceptions.cpp`, whose four runnable cases are translated
// in `tests/exceptions.rs`; this one is the issue's own bar for the first
// end-to-end run, and asserts what a panic does to the tasks beside it rather
// than what it is reported as.
// --------------------------------------------------------

/// How many tasks run beside the panicking one.
const SIBLINGS: usize = 16;

/// A panic is reported through the handle, and it cancels the rest of its own
/// run on the terms cancellation always has: a task already running is left to
/// finish, and a task not yet scheduled does not run.
///
/// So the sibling count has no single right answer -- how many of the sixteen
/// were in flight when the panic landed is the race between the panic and the
/// pool -- while the sink has: it cannot be scheduled until every one of the
/// seventeen has released it, the panicking one included, so by the time a
/// worker could dequeue it the run is already cancelled and the early-out
/// skips it. That asymmetry is the whole of the behaviour, which is why this
/// case is a fan-in rather than a chain; `tests/exceptions.rs` has upstream's
/// chain, where the counts are exact.
#[test]
fn a_panicking_task_is_reported_and_cancels_what_had_not_started() {
    let executor = Executor::new(4);
    let siblings = Arc::new(AtomicUsize::new(0));
    let sinks = Arc::new(AtomicUsize::new(0));

    // One task that panics, sixteen beside it that do not, and a sink that
    // succeeds all seventeen.
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

    assert!(
        siblings.load(Ordering::Relaxed) <= SIBLINGS,
        "a sibling ran more than once"
    );
    assert_eq!(
        sinks.load(Ordering::Relaxed),
        0,
        "the sink cannot be scheduled before the panic that cancels the run"
    );

    // The same taskflow again. The panicking task poisoned its own work on the
    // way out, so this is also the check that a poisoned task is still
    // runnable and still reports the panic rather than a lock error -- and
    // that a run after a failed one starts clean, since the cancellation is
    // the topology's and each run has its own.
    let before = siblings.load(Ordering::Relaxed);
    let error = executor
        .run(&taskflow)
        .wait()
        .expect_err("the second run reported no panic");
    assert!(matches!(error, RunError::Panicked(_)));
    assert!(
        (before..=before + SIBLINGS).contains(&siblings.load(Ordering::Relaxed)),
        "the second run ran a sibling more than once"
    );
    assert_eq!(sinks.load(Ordering::Relaxed), 0, "the sink ran after all");

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
