//! Translated from `unittests/test_basics.cpp` (Taskflow v4.1.0).
//!
//! The cases ported here are the ones that exercise the task handle API --
//! `Type`, `Builder`, `Taskflow.RemoveDependency`, `Task.RemoveDependencies`,
//! the per-task half of `Iterators`, `Hash` -- and the ones that exercise
//! running a graph at all: `Future`, `SerialRuns`, `ParallelRuns`,
//! `RunAndWait` and `WorkerID`. Three standing adaptations, because the port
//! is not yet where upstream is:
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
//!
//! Two further sections, between `Task.RemoveDependencies` and `Iterators`,
//! are not upstream's either: each one's own header says why.
//!
//! The last two sections are not upstream's at all. Upstream has no
//! `RunHandle` and therefore no case for leaking one, while this port's
//! answer to "do not mutate or destroy a running taskflow" is that handle --
//! so what happens when a caller leaks one is a property of the port, and
//! issue #40 is where it needed pinning down. The one after it is upstream's
//! `NSTATE` word, whose state bits share a `u32` with a node's
//! strong-dependency count: upstream masks the two apart and a port that did
//! not would hang, so the mask has a case of its own (issue #47).

use std::future::Future;
use std::mem;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use rustflow::prelude::*;
use rustflow::{AsyncWork, Executor, Node, OnceWork, RunError, RunHandle, TaskType};

// The topics this file calls every item of: the matrix macro, the counting
// body its `emplace` calls attach, and the spin its two leaked-handle cases
// wait on. `common/counters.rs` is deliberately not among them -- some of the
// counters here are read with `SeqCst` where `count` reads `Relaxed`, so it
// would fit some of these assertions and not others, and a topic file pulled
// for part of itself is the dead code `common`'s own docs are about.
#[macro_use]
mod common;
#[path = "common/bodies.rs"]
mod bodies;
#[path = "common/spin.rs"]
mod spin;

use bodies::bump;
use spin::spin_until;

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
// Testcase: Taskflow.RemoveDependency
// --------------------------------------------------------

#[test]
fn taskflow_remove_dependency() {
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

    taskflow.remove_dependency(a, b);
    assert_eq!(taskflow.task_ref(a).num_successors(), 2);
    assert_eq!(taskflow.task_ref(b).num_predecessors(), 0);

    taskflow.remove_dependency(a, c);
    assert_eq!(taskflow.task_ref(a).num_successors(), 1);
    assert_eq!(taskflow.task_ref(c).num_predecessors(), 0);

    taskflow.remove_dependency(a, d);
    assert_eq!(taskflow.task_ref(a).num_successors(), 0);
    assert_eq!(taskflow.task_ref(d).num_predecessors(), 0);

    // A duplicated dependency: `remove_dependency` drops every occurrence of
    // it, as upstream's does.
    taskflow.task(a).precede([b, b, c, c, d, d]);
    assert_eq!(taskflow.task_ref(a).num_successors(), 6);
    assert_eq!(taskflow.task_ref(b).num_predecessors(), 2);

    taskflow.remove_dependency(a, b);
    assert_eq!(taskflow.task_ref(a).num_successors(), 4);
    assert_eq!(taskflow.task_ref(b).num_predecessors(), 0);

    taskflow.remove_dependency(a, c);
    assert_eq!(taskflow.task_ref(a).num_successors(), 2);
    assert_eq!(taskflow.task_ref(c).num_predecessors(), 0);

    taskflow.remove_dependency(a, d);
    assert_eq!(taskflow.task_ref(a).num_successors(), 0);
    assert_eq!(taskflow.task_ref(d).num_predecessors(), 0);

    // Not upstream's case: calling it again on an edge that is already gone
    // is a no-op rather than a panic or an underflow, which is the half of
    // "a no-op on an edge that is not there" nothing above exercises.
    taskflow.remove_dependency(a, b);
    assert_eq!(taskflow.task_ref(a).num_successors(), 0);
    assert_eq!(taskflow.task_ref(b).num_predecessors(), 0);
    // And on two tasks that were never joined at all.
    taskflow.remove_dependency(b, c);
    assert_eq!(taskflow.task_ref(b).num_successors(), 0);
    assert_eq!(taskflow.task_ref(c).num_predecessors(), 0);
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
// Testcase: the data slot a run does not touch (issue #191)
//
// Not upstream's: `examples/attach_data.cpp` reads the slot from inside the
// task's own body through the aliasing `tf::Task` the closure captures,
// which this port's `FnMut() + Send + 'static` body has no way to do -- see
// `src/task.rs` for why. What is upstream's is the property the example
// relies on, that a run leaves the slot alone, which this checks from
// outside the body instead.
// --------------------------------------------------------

#[test]
fn task_data_survives_a_run() {
    let mut taskflow = Taskflow::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let bumped = Arc::clone(&counter);
    let a = emplace(&mut taskflow, move || {
        bumped.fetch_add(1, Ordering::Relaxed);
    });

    taskflow.task(a).set_data(7i32);
    assert_eq!(taskflow.task_ref(a).data::<i32>(), Some(&7));

    run(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    // The run did not touch the slot: still there, still the right type.
    assert_eq!(taskflow.task_ref(a).data::<i32>(), Some(&7));

    // Survives more than one run too, which is the iterative use
    // `attach_data.cpp` makes of it.
    run(&taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), 2);
    assert_eq!(taskflow.task_ref(a).data::<i32>(), Some(&7));
}

/// The slot is editable in place and can be taken back out again: the other
/// two halves of the attachment the case above only reads. A taken slot leaves
/// the task with no data rather than a stale copy of it, and a take that names
/// the wrong type hands the box back to the task instead of losing it.
#[test]
fn task_data_is_editable_in_place_and_can_be_taken_back() {
    let mut taskflow = Taskflow::new();
    let a = emplace(&mut taskflow, || {});

    taskflow.task(a).set_data(7i32);
    *taskflow
        .task(a)
        .data_mut::<i32>()
        .expect("the slot holds the i32 just attached") += 35;
    assert_eq!(taskflow.task_ref(a).data::<i32>(), Some(&42));

    assert_eq!(taskflow.task(a).take_data::<u8>(), None);
    assert_eq!(
        taskflow.task_ref(a).data::<i32>(),
        Some(&42),
        "a take of the wrong type leaves the slot as it was"
    );

    assert_eq!(taskflow.task(a).take_data::<i32>(), Some(42));
    assert_eq!(taskflow.task_ref(a).data::<i32>(), None);
}

// --------------------------------------------------------
// Testcase: the arena under the task handles (not upstream's)
//
// Upstream has no case for these because it has no arena: a `tf::Task` is a
// `Node*` and `tf::Graph` is the vector it was allocated out of, so walking
// the graph mutably and reading a node's work back are things its tests do
// through the task handles above. Here `Graph` is public, a `Task` indexes it
// and `DESIGN.md`'s "Graph as an arena" is the decision -- so the arena's own
// surface is this port's to pin.
// --------------------------------------------------------

/// `Graph::iter_mut` reaches every live node and no tombstone: the mutable
/// half of the walk `Graph::iter` and `Graph::tasks` make, and the one way a
/// caller holding the graph edits the nodes in it.
#[test]
fn iter_mut_reaches_every_live_node() {
    let mut graph = Graph::new();
    let a = graph.add_named_node("a", Handle::Placeholder);
    let b = graph.add_named_node("b", Handle::Placeholder);
    let c = graph.add_named_node("c", Handle::Placeholder);
    assert!(graph.erase(b));

    let mut walked = Vec::new();
    for (task, node) in graph.iter_mut() {
        walked.push(task);
        node.set_name(format!("{}!", node.name()));
    }
    assert_eq!(walked, vec![a, c], "the erased slot is not walked");

    let named: Vec<&str> = graph.iter().map(|(_, node)| node.name()).collect();
    assert_eq!(named, vec!["a!", "c!"], "the walk's edits landed");
}

/// `Graph::clear` drops every node *and* every tombstone, so the handles a
/// cleared graph hands out start over from zero. That is what separates it
/// from erasing every node one at a time, which empties the graph just as
/// well but leaves the arena's next index where it was.
#[test]
fn clear_drops_the_tombstones_too_and_restarts_the_handles() {
    let mut graph = Graph::new();
    let a = graph.add_named_node("a", Handle::Placeholder);
    let b = graph.add_named_node("b", Handle::Placeholder);
    graph.add_edge(a, b);

    // The other way to empty a graph, and not this one.
    assert!(graph.erase(a));
    assert!(graph.erase(b));
    assert!(graph.is_empty());
    assert_eq!(
        graph
            .add_named_node("after erase", Handle::Placeholder)
            .id(),
        2,
        "an erased slot is not reused"
    );

    graph.clear();
    assert_eq!(graph.len(), 0);
    assert!(graph.is_empty());
    assert_eq!(graph.tasks().count(), 0, "and nothing is left to walk");

    let reborn = graph.add_named_node("reborn", Handle::Placeholder);
    assert_eq!(reborn.id(), 0, "a cleared graph starts over from zero");
    assert_eq!(graph.node(reborn).name(), "reborn");
    assert_eq!(graph.len(), 1);
}

/// `Node::handle_mut` hands back *this* node's handle, so work replaced
/// through it is the work the node holds afterwards -- `TaskMut::work`'s claim
/// one level up, for a graph a caller owns outright rather than one a
/// `Taskflow` does.
#[test]
fn handle_mut_replaces_the_work_the_node_holds() {
    let mut graph = Graph::new();
    let task = graph.add_node(Handle::Placeholder);
    assert!(graph.node(task).handle().is_placeholder());

    *graph.node_mut(task).handle_mut() = Handle::Static(Work::new(|| {}));
    assert!(graph.node(task).handle().is_static());
}

/// Every `Handle` predicate answers `true` for exactly the variants it names,
/// over all eleven of them.
///
/// `Testcase: Type` above covers what upstream's own tests ask of a node's
/// handle, which is `tf::Task::type()`; these predicates are what the port
/// gives a caller instead of upstream's private `std::holds_alternative`
/// checks, and two of the distinctions they draw are ones `TaskType` flattens
/// away on purpose: `is_conditioner` is true of *both* condition kinds,
/// `is_module` of both module kinds, and `is_async`/`is_dependent_async` keep
/// the two async kinds apart where `TaskType` folds them together.
#[test]
fn every_handle_predicate_answers_for_exactly_its_own_variant() {
    // One `Handle` predicate under the name this case reports it by.
    type Predicate = (&'static str, fn(&Handle) -> bool);

    let predicates: [Predicate; 11] = [
        ("placeholder", Handle::is_placeholder),
        ("static", Handle::is_static),
        ("condition", Handle::is_condition),
        ("multi_condition", Handle::is_multi_condition),
        ("subflow", Handle::is_subflow),
        ("runtime", Handle::is_runtime),
        ("nonpreemptive_runtime", Handle::is_nonpreemptive_runtime),
        ("module", Handle::is_module),
        ("async", Handle::is_async),
        ("dependent_async", Handle::is_dependent_async),
        ("conditioner", Handle::is_conditioner),
    ];
    let answers = |handle: &Handle| -> Vec<&str> {
        predicates
            .iter()
            .filter(|(_, predicate)| predicate(handle))
            .map(|(name, _)| *name)
            .collect()
    };

    // The nine kinds a builder produces, each named by the method that makes
    // it and the predicates that must answer for it.
    let mut taskflow = Taskflow::new();
    let rows = [
        ("placeholder", taskflow.placeholder(), vec!["placeholder"]),
        ("emplace", taskflow.emplace(|| {}), vec!["static"]),
        (
            "emplace_condition",
            taskflow.emplace_condition(|| 0),
            vec!["condition", "conditioner"],
        ),
        (
            "emplace_multi_condition",
            taskflow.emplace_multi_condition(Vec::new),
            vec!["multi_condition", "conditioner"],
        ),
        (
            "emplace_subflow",
            taskflow.emplace_subflow(|_| {}),
            vec!["subflow"],
        ),
        (
            "emplace_runtime",
            taskflow.emplace_runtime(|_| {}),
            vec!["runtime"],
        ),
        (
            "emplace_nonpreemptive_runtime",
            taskflow.emplace_nonpreemptive_runtime(|_| {}),
            vec!["nonpreemptive_runtime"],
        ),
        (
            "composed_of",
            taskflow.composed_of(Arc::new(Taskflow::new())),
            vec!["module"],
        ),
        ("adopt", taskflow.adopt(Graph::new()), vec!["module"]),
    ];

    let graph = taskflow.graph();
    for (built_by, task, expected) in rows {
        assert_eq!(answers(graph.node(task).handle()), expected, "{built_by}");
    }

    // The two async kinds, which no builder makes: their nodes live outside a
    // taskflow's arena, so the handles are put in a graph of their own here.
    let mut free = Graph::new();
    let spawned = free.add_node(Handle::Async(AsyncWork::plain(|| {})));
    let dependent = free.add_node(Handle::DependentAsync(AsyncWork::plain(|| {})));
    assert_eq!(answers(free.node(spawned).handle()), vec!["async"]);
    assert_eq!(
        answers(free.node(dependent).handle()),
        vec!["dependent_async"],
        "a dependent-async handle is not an `is_async` one",
    );
}

/// A `OnceWork` slot holds its body until somebody takes it, and exactly one
/// caller can.
///
/// Upstream needs no equivalent -- its async node is recycled by the worker
/// that ran it -- so this is the port's own: the node here lives as long as
/// the last handle on it, and the slot being emptied by the first taker is
/// what keeps a body from being run twice. `is_spent` is the question a second
/// would-be runner asks, and the predicate a node's `Debug` prints.
#[test]
fn a_once_work_slot_hands_its_body_to_one_taker() {
    let ran = Arc::new(AtomicUsize::new(0));
    let slot: OnceWork<dyn FnOnce() + Send + 'static> = OnceWork::new(Box::new({
        let ran = Arc::clone(&ran);
        move || {
            ran.fetch_add(1, Ordering::Relaxed);
        }
    }));

    assert!(!slot.is_spent(), "an untaken body is still in the slot");
    let body = slot.take().expect("the first taker gets the body");
    assert!(
        slot.is_spent(),
        "the slot is empty the instant the body is out"
    );
    assert!(slot.take().is_none(), "no second taker gets anything");

    assert_eq!(
        ran.load(Ordering::Relaxed),
        0,
        "taking a body does not run it"
    );
    body();
    assert_eq!(ran.load(Ordering::Relaxed), 1);
}

/// `Graph::remove_edge` removes *every* copy of a duplicated edge and returns
/// how many it removed, which is the half of it `taskflow_remove_dependency`
/// above cannot see: `Taskflow::remove_dependency` discards the count.
#[test]
fn remove_edge_removes_every_copy_of_an_edge_and_counts_them() {
    let mut graph = Graph::new();
    let a = graph.add_node(Handle::Placeholder);
    let b = graph.add_node(Handle::Placeholder);
    let c = graph.add_node(Handle::Placeholder);

    graph.add_edge(a, b);
    graph.add_edge(a, b);
    graph.add_edge(a, c);
    assert_eq!(graph.successors(a).to_vec(), vec![b, b, c]);
    assert_eq!(graph.predecessors(b).to_vec(), vec![a, a]);

    assert_eq!(
        graph.remove_edge(a, b),
        2,
        "a task preceded twice loses both edges at once"
    );
    assert_eq!(graph.successors(a).to_vec(), vec![c]);
    assert!(graph.predecessors(b).is_empty());

    assert_eq!(
        graph.remove_edge(a, b),
        0,
        "an edge that is not there removes nothing"
    );
    assert_eq!(
        graph.remove_edge(c, a),
        0,
        "and an edge is directed: `c -> a` is not `a -> c`"
    );
    assert_eq!(
        graph.successors(a).to_vec(),
        vec![c],
        "neither no-op touched the edges that are there"
    );
}

/// A node's own edge lists are the two halves of one vector, split at its
/// successor count: `Node::successors` and `Node::predecessors` are the
/// slices `Graph::successors` and `Graph::predecessors` hand on, and the two
/// counts beside them are where the split falls.
///
/// Upstream keeps two vectors, `_successors` and `_predecessors`, so the one
/// claim here it has no counterpart for is what a single vector costs:
/// pushing a successor swaps the first predecessor to the back to keep the
/// halves contiguous, which is why `Node::predecessors`' own doc promises
/// insertion order only up to that swap.
#[test]
fn a_nodes_edge_halves_are_one_vector_split_at_its_successor_count() {
    let mut graph = Graph::new();
    let a = graph.add_named_node("a", Handle::Placeholder);
    let b = graph.add_named_node("b", Handle::Placeholder);
    let c = graph.add_named_node("c", Handle::Placeholder);
    let d = graph.add_named_node("d", Handle::Placeholder);

    graph.add_edge(a, b);
    graph.add_edge(a, b);
    graph.add_edge(c, b);

    let node = graph.node(b);
    assert_eq!(node.num_successors(), 0);
    assert_eq!(node.num_predecessors(), 3, "the duplicate edge is kept");
    assert!(node.successors().is_empty(), "b precedes nothing yet");
    assert_eq!(node.predecessors().to_vec(), vec![a, a, c]);
    assert_eq!(
        graph.predecessors(b),
        graph.node(b).predecessors(),
        "the graph's query is the node's own half, handed on"
    );
    assert_eq!(graph.successors(a), graph.node(a).successors());
    assert_eq!(graph.node(a).successors().to_vec(), vec![b, b]);
    assert_eq!(graph.node(a).num_successors(), 2);

    // The halves stay contiguous as `b` gains a successor of its own, so the
    // predecessor it displaces goes to the back rather than staying put.
    graph.add_edge(b, d);
    let node = graph.node(b);
    assert_eq!(node.successors().to_vec(), vec![d]);
    assert_eq!(node.num_successors(), 1);
    assert_eq!(node.num_predecessors(), 3, "and no predecessor was lost");
    assert_eq!(
        node.predecessors().to_vec(),
        vec![a, c, a],
        "the first predecessor was swapped to the back to make room"
    );
}

/// The two task handles can be taken on a graph a caller owns outright, with
/// no `Taskflow` in between: `TaskRef::new` and `TaskMut::new` are what
/// `FlowBuilder::task_ref` and `FlowBuilder::task` hand back one level up,
/// and the index `TaskMut::task` reads out of one is the node it was taken
/// for.
///
/// No upstream case: a `tf::Task` wraps a `Node*` a builder hands out, and
/// nothing upstream owns a graph a second handle could be taken against.
#[test]
fn the_two_task_handles_borrow_a_graph_a_caller_owns() {
    let mut graph = Graph::new();
    let a = graph.add_named_node("a", Handle::Placeholder);
    let b = graph.add_named_node("b", Handle::Placeholder);

    let read = TaskRef::new(&graph, b);
    assert_eq!(
        read.name(),
        "b",
        "the handle reads the node its index names"
    );
    assert_eq!(read.task(), b);
    assert!(read.is_empty(), "a placeholder holds no work");

    let mut edit = TaskMut::new(&mut graph, b);
    assert_eq!(edit.task(), b);
    edit.name("renamed").succeed([a]);
    let taken = edit.task();

    assert_eq!(taken, b, "the index outlives the borrow it was read from");
    assert_eq!(graph.node(b).name(), "renamed", "the edits landed on `b`");
    assert_eq!(graph.predecessors(b).to_vec(), vec![a]);
    assert_eq!(
        TaskRef::new(&graph, taken).name(),
        "renamed",
        "and addresses the same node once the borrow is over"
    );
    assert_eq!(
        TaskRef::new(&graph, a).name(),
        "a",
        "neither handle answered about the graph's first node"
    );
}

/// `TaskRef::new` refuses an index that addresses no node of the graph it is
/// handed, instead of reading whatever is in that slot. The check is once,
/// at construction, which is what makes every query on the handle
/// infallible where upstream dereferences a `Node*` on trust.
#[test]
#[should_panic(expected = "Task(7) is out of range for a graph of 1 nodes")]
fn a_read_handle_refuses_an_index_the_graph_does_not_hold() {
    let mut graph = Graph::new();
    graph.add_node(Handle::Placeholder);
    let _ = TaskRef::new(&graph, Task::new(7));
}

/// `TaskMut::new`'s half of the check above.
#[test]
#[should_panic(expected = "Task(7) is out of range for a graph of 1 nodes")]
fn an_editing_handle_refuses_an_index_the_graph_does_not_hold() {
    let mut graph = Graph::new();
    graph.add_node(Handle::Placeholder);
    let _ = TaskMut::new(&mut graph, Task::new(7));
}

// --------------------------------------------------------
// Testcase: the reborrow between the two task handles (not upstream's)
//
// Upstream's `tf::Task` is one type for both reading a task and editing it,
// so the seam between `TaskMut` and `TaskRef` is the port's own and has no
// upstream case to translate.
// --------------------------------------------------------

/// `TaskMut::to_ref` reborrows the mutable handle as the read-only one, over
/// the same task, and sees every edit already made through it.
///
/// What a caller needs of it is those two things: that it answers about *this*
/// task rather than about the graph's first, and that it answers as of now
/// rather than as of when the mutable handle was taken.
#[test]
fn to_ref_reads_the_task_the_mutable_handle_holds() {
    let mut taskflow = Taskflow::new();
    let first = taskflow.placeholder();
    let second = taskflow.placeholder();

    let mut handle = taskflow.task(second);
    assert_eq!(handle.to_ref().name(), "");
    assert_eq!(handle.to_ref().task_type(), TaskType::Placeholder);
    assert_eq!(handle.to_ref().num_successors(), 0);

    handle.name("second").work(|| {}).succeed([first]);

    assert_eq!(handle.to_ref().name(), "second", "the edits are visible");
    assert_eq!(handle.to_ref().task_type(), TaskType::Static);
    assert_eq!(handle.to_ref().num_predecessors(), 1);

    assert_eq!(
        taskflow.task(first).to_ref().name(),
        "",
        "the reborrow answered about the task its own handle holds"
    );
}

/// A node's `Debug` names the work it holds and the state words a run writes.
///
/// Every one of those is a hand-written impl, and for one reason: a closure, a
/// `Box<dyn Any>` and a payload slot behind a `Mutex` have no `Debug` to
/// derive, so each prints what it *is* instead of what it holds. A node that
/// printed nothing would be the one thing they are all there to avoid.
#[test]
fn a_nodes_debug_names_the_work_it_holds() {
    let mut taskflow = Taskflow::new();
    let static_task = taskflow.emplace(|| {});
    let condition = taskflow.emplace_condition(|| 0);
    let subflow = taskflow.emplace_subflow(|_| {});
    let runtime = taskflow.emplace_runtime(|_| {});
    let nonpreemptive = taskflow.emplace_nonpreemptive_runtime(|_| {});
    taskflow.task(static_task).name("printed");

    let graph = taskflow.graph();
    let printed = |task| format!("{:?}", graph.node(task));

    let text = printed(static_task);
    assert!(text.contains("Work(..)"), "{text}");
    assert!(text.contains("name: \"printed\""), "{text}");
    assert!(text.contains("NodeBase"), "{text}");
    assert!(text.contains("DispatchClaim"), "{text}");

    assert!(printed(condition).contains("Work(..)"));
    assert!(printed(subflow).contains("SubflowWork(..)"));
    assert!(printed(runtime).contains("RuntimeWork(..)"));
    assert!(
        printed(nonpreemptive).contains("NonpreemptiveRuntimeWork(..)"),
        "{}",
        printed(nonpreemptive)
    );

    // An async node's body is a slot the one worker that runs it takes the
    // call out of, so whether it is still there is what its `Debug` prints.
    let mut graph = Graph::new();
    let spawned = graph.add_node(Handle::Async(AsyncWork::plain(|| {})));
    let text = format!("{:?}", graph.node(spawned));
    assert!(text.contains("OnceWork(\"pending\")"), "{text}");
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
// Invented: the arena index a Task handle carries
// --------------------------------------------------------

/// A [`Task`] *is* the arena index of its node, and `Task::id` is that index
/// read back out: the handles a graph hands out run 0, 1, 2 in creation
/// order, `id`, [`Task::index`] and `u32::from` are three readings of the one
/// number, and a handle rebuilt from the number addresses the node the
/// original was taken for.
///
/// Invented: upstream's handle is a `Node*`, and `task.hpp` at the pin offers
/// nothing to read out of it but `hash_value`, so the index this port hands
/// out in its place has no upstream case. What it pins is the one promise
/// `Task`'s docs make -- that a handle is only meaningful together with the
/// graph that produced it -- from the index side.
#[test]
fn a_task_handle_is_the_arena_index_it_reads_back() {
    let mut taskflow = Taskflow::new();
    let first = placeholder(&mut taskflow, "first");
    let second = placeholder(&mut taskflow, "second");

    assert_eq!(first.id(), 0, "the first node of a graph is index zero");
    assert_eq!(second.id(), 1, "and the next one is the one after it");

    // The three readings of one index.
    assert_eq!(
        first.index(),
        usize::try_from(first.id()).expect("an index fits in a usize here")
    );
    assert_eq!(u32::from(second), second.id());
    assert_eq!(
        Task::try_from(second.index()).expect("an index fits in a u32 here"),
        second
    );

    // And the number addresses the node, not just itself.
    assert_eq!(taskflow.graph().node(Task::new(first.id())).name(), "first");
    assert_eq!(taskflow.task_ref(Task::new(second.id())).name(), "second");

    // A handle outlives an edit of its node, the index being the identity.
    taskflow.task(second).name("renamed");
    assert_eq!(second.id(), 1);
    assert_eq!(taskflow.task_ref(Task::new(second.id())).name(), "renamed");
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
        let a = emplace(&mut others, bump(&counter));
        let b = emplace(&mut others, bump(&counter));
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
                emplace(&mut taskflow, bump(&counter));
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
        emplace(&mut taskflow, bump(&counter));
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

worker_matrix! {
    sequential_runs {
        serial_runs_1_thread(1),
        serial_runs_2_threads(2),
        serial_runs_3_threads(3),
        serial_runs_4_threads(4),
        serial_runs_5_threads(5),
        serial_runs_6_threads(6),
        serial_runs_7_threads(7),
        serial_runs_8_threads(8),
    }
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

worker_matrix! {
    parallel_runs {
        parallel_runs_1_thread(1),
        parallel_runs_2_threads(2),
        parallel_runs_3_threads(3),
        parallel_runs_4_threads(4),
        parallel_runs_5_threads(5),
        parallel_runs_6_threads(6),
        parallel_runs_7_threads(7),
        parallel_runs_8_threads(8),
    }
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

worker_matrix! {
    worker_id {
        worker_id_1_thread(1),
        worker_id_2_threads(2),
        worker_id_3_threads(3),
        worker_id_4_threads(4),
        worker_id_5_threads(5),
        worker_id_6_threads(6),
        worker_id_7_threads(7),
        worker_id_8_threads(8),
    }
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
        emplace(&mut after, bump(&counter));
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
    let counting_task = |taskflow: &mut Taskflow| emplace(taskflow, bump(counter));

    let mut previous = counting_task(&mut taskflow);
    while taskflow.num_tasks() + 3 <= NUM_TASKS {
        let left = counting_task(&mut taskflow);
        let right = counting_task(&mut taskflow);
        let join = counting_task(&mut taskflow);
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

// --------------------------------------------------------
// what a leaked run handle does: issue #40
// --------------------------------------------------------

/// How long a case here waits for the pool to reach a state before it gives
/// up and lets the assertions report what went wrong. Generous: it is only
/// ever reached when the test is about to fail anyway, and a spin that gave up
/// early would turn a slow machine into a failure.
const GIVE_UP_AFTER: Duration = Duration::from_secs(10);

/// Waits until `flag` is set, or until [`GIVE_UP_AFTER`] has passed.
///
/// A spin rather than a condvar because one end of every use of it is a task
/// body: a worker that parked would be a worker not running the rest of the
/// run, and these cases are about what the pool does while a run is in
/// flight. Bounded, so that nothing here can hang a test binary -- including
/// when the thread that would have set the flag is unwinding.
///
/// [`spin_until`] with this file's own give-up folded into the predicate,
/// which is the one part of it that cannot come from the shared spin: that
/// one *fails* on its deadline, and a failure raised from inside a task body
/// here would be a panic thrown during the very unwind the case below is
/// about, in place of the assertions after the wait that are this file's
/// diagnostic. The shared deadline is therefore never reached, and the
/// message says what the wait was for in case a later edit drops the bound.
fn wait_or_give_up(flag: &AtomicBool) {
    let deadline = Instant::now() + GIVE_UP_AFTER;
    spin_until("a flag the pool should have set", || {
        flag.load(Ordering::Acquire) || Instant::now() >= deadline
    });
}

/// Sets a flag when it is dropped, including during an unwind.
struct ReleaseOnDrop(Arc<AtomicBool>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Upstream has no `RunHandle` and so no case for this: a handle the caller
/// leaks rather than drops.
///
/// [`std::mem::forget`] skips the blocking `Drop` the handle's borrow leans
/// on, and under NLL the borrow ends at the handle's last use -- the `forget`
/// call -- so this program compiles and the taskflow is moved and then freed
/// with the run still in flight. It is issue #40's probe, which used to be a
/// use-after-free: workers held raw pointers into storage the main thread had
/// just dropped.
///
/// What makes it safe now is ownership rather than the handle: every queued
/// job holds a share of the run and of the taskflow's storage, so neither can
/// be freed while a job can name it. The run therefore finishes, every task
/// runs, and the executor ends up idle; what leaks is the topology of the
/// handle nobody dropped.
#[test]
fn a_leaked_handle_leaks_rather_than_freeing_a_graph_under_the_pool() {
    const TASKS: usize = 200;

    let executor = Executor::new(4);
    let counter = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));

    let mut taskflow = Taskflow::new();
    for _ in 0..TASKS {
        let counter = Arc::clone(&counter);
        let running = Arc::clone(&running);
        let released = Arc::clone(&released);
        taskflow.emplace(move || {
            // The first task to run says so, and every task waits for the
            // main thread below: that is what makes the overlap this case is
            // about a certainty rather than a race won on most machines.
            running.store(true, Ordering::Release);
            wait_or_give_up(&released);
            counter.fetch_add(1, Ordering::Relaxed);
        });
    }

    mem::forget(executor.run(&taskflow));
    wait_or_give_up(&running);

    // Both of the things the borrow was there to prevent, with the run
    // provably in flight: the taskflow is moved out of, which no job notices
    // because what a job names is the storage and a move does not touch it,
    // and then what was moved out is freed.
    let moved = mem::take(&mut taskflow);
    assert_eq!(taskflow.num_tasks(), 0, "the taskflow was moved out");
    drop(moved);

    released.store(true, Ordering::Release);
    executor.wait_for_all();

    assert_eq!(
        counter.load(Ordering::Relaxed),
        TASKS,
        "the run did not finish on storage its taskflow had let go of"
    );
    assert_eq!(executor.num_topologies(), 0, "the run is accounted for");
}

/// The other half of what a leaked handle can reach: editing the taskflow
/// instead of freeing it.
///
/// The borrow makes this a compile error for a handle that is still alive, so
/// only a leak gets here -- and then the graph is one the workers are walking,
/// which an `emplace` would reallocate under them. So it panics, which is the
/// price of `Arc::get_mut` being what `Taskflow::graph_mut` asks: a run in
/// flight is a run holding shares of the storage.
///
/// The run is held open by a task waiting on `released`, so the panic is not a
/// race either: a share is outstanding for certain when the edit lands. The
/// guard is what lets the pool come down afterwards, since it is dropped --
/// and releases that task -- on the way out of the unwind.
#[test]
#[should_panic(expected = "a taskflow cannot be edited while a run of it is in flight")]
fn editing_a_taskflow_whose_handle_was_leaked_panics_rather_than_racing() {
    let executor = Executor::new(2);
    let running = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));

    let mut taskflow = Taskflow::new();
    taskflow.emplace({
        let running = Arc::clone(&running);
        let released = Arc::clone(&released);
        move || {
            running.store(true, Ordering::Release);
            wait_or_give_up(&released);
        }
    });

    mem::forget(executor.run(&taskflow));
    wait_or_give_up(&running);

    let _release = ReleaseOnDrop(Arc::clone(&released));
    taskflow.emplace(|| {});
}

/// A taskflow whose runs have all *finished* is editable at once, which is the
/// other thing the shares have to be right about: the panic above must not be
/// reachable one instruction after a `wait` returns.
///
/// Every run of the loop below edits the graph the moment the run before it
/// reported done, so a share released a moment too late -- by a job already
/// torn down, or by a tear-down that let its own go after releasing the
/// waiter -- would show up here as that panic. A thousand rounds at two
/// worker counts because the window it guards is a handful of instructions
/// wide, which is also why the edit is inside the loop rather than after it.
#[test]
fn a_taskflow_is_editable_the_instant_its_run_reports_done() {
    const ROUNDS: usize = 1000;

    for workers in [1, 4] {
        let executor = Executor::new(workers);
        let counter = Arc::new(AtomicUsize::new(0));

        let mut taskflow = Taskflow::new();
        for _ in 0..8 {
            taskflow.emplace(bump(&counter));
        }

        for round in 0..ROUNDS {
            executor.run(&taskflow).wait().expect("a task panicked");
            // The edit a stale share would turn into a panic. `set_name`
            // never asks, since a run reads no name; `graph_mut` -- which
            // `clear` and the builder both go through -- always does.
            taskflow.clear();
            assert_eq!(taskflow.num_tasks(), 0, "round {round} at {workers}w");

            taskflow.emplace(bump(&counter));
        }

        // Eight tasks for the first run, then one per round after it.
        assert_eq!(counter.load(Ordering::Relaxed), 8 + ROUNDS - 1);
    }
}

/// The same promise for a handle that was dropped without being waited on,
/// since `Drop` and `wait` owe the same thing, and for the `run_owned` family
/// -- where the last share of the storage is the handle's own, so the graph is
/// freed by the drop rather than kept by the caller.
#[test]
fn a_dropped_handle_leaves_its_taskflow_editable_and_its_owned_graph_freed() {
    let executor = Executor::new(4);
    let drops = Arc::new(AtomicUsize::new(0));

    /// Counts its own drop, which is what says when the graph was freed.
    struct CountOnDrop(Arc<AtomicUsize>);

    impl Drop for CountOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let mut borrowed = Taskflow::new();
    for _ in 0..16 {
        borrowed.emplace(|| {});
    }

    for _ in 0..100 {
        drop(executor.run(&borrowed));
        // No `wait`, so the only thing that can have released the run's share
        // of the storage is the tear-down.
        borrowed.emplace(|| {});
    }
    assert_eq!(borrowed.num_tasks(), 116);

    for round in 0..100 {
        let mut owned = Taskflow::new();
        for _ in 0..4 {
            let counted = CountOnDrop(Arc::clone(&drops));
            owned.emplace(move || {
                let _ = &counted;
            });
        }

        drop(executor.run_owned(owned));
        assert_eq!(
            drops.load(Ordering::Relaxed),
            4 * (round + 1),
            "the handle did not free the graph it owned"
        );
    }
}

// --------------------------------------------------------
// the `nstate` word: a state bit is not a dependency (issue #47)
// --------------------------------------------------------

/// Not upstream's, and it exists because the port could get this wrong
/// silently. A node's `nstate` holds its strong-dependency count in the low
/// 28 bits with the scheduler's state bits above it, and the scheduler re-arms
/// the node's join counter from that word every time it runs the node.
/// Upstream masks the count out of it -- `NSTATE::STRONG_DEPENDENCIES_MASK` --
/// and a port that read the whole word would add the bit's value, hundreds of
/// millions, to the counter. The node would never become ready again and the
/// failure would be a hang.
///
/// So the case asserts the counter rather than waiting for one: with the mask,
/// a node with one strong dependency is re-armed to exactly one however many
/// state bits it carries.
///
/// Setting a bit where the scheduler will see it takes some arranging, which
/// is itself the point: set-up clears the word, so the bit has to be set
/// *during* the run, by a task that runs before the marked one. A task body
/// cannot capture the taskflow it belongs to -- that would be a share of a
/// graph held by a closure inside that same graph -- so it captures a `Weak`,
/// which is what [`Arc::new_cyclic`] is for.
#[test]
fn a_state_bit_on_a_node_is_not_added_to_its_join_counter() {
    // Upstream's three `NSTATE` bits this port has. `PREEMPTED` is the one the
    // scheduler sets itself, and the one that also sends the marked node
    // through the fast path at the top of `invoke`.
    for bit in [Node::PREEMPTED, Node::JOINED_SUBFLOW, Node::RETAIN_SUBFLOW] {
        let executor = Executor::new(2);
        let stale = Arc::new(AtomicUsize::new(0));
        let marked = Task::new(0);

        let taskflow = Arc::new_cyclic(|weak: &Weak<Taskflow>| {
            let mut taskflow = Taskflow::new();
            // The first task emplaced is arena index zero, which is the
            // handle `marked` names from outside this closure.
            let target = taskflow.emplace(|| {});
            assert_eq!(target, marked);

            let marker = taskflow.emplace({
                let weak = Weak::clone(weak);
                let stale = Arc::clone(&stale);
                move || {
                    let taskflow = weak.upgrade().expect("the test holds the taskflow");
                    let node = taskflow.graph().node(target);
                    // A bit left by the previous run would be one set-up did
                    // not clear, which is what upstream's
                    // `_nstate = NSTATE::NONE` promises not to leave behind.
                    if node.nstate().load(Ordering::Relaxed) & bit != 0 {
                        stale.fetch_add(1, Ordering::Relaxed);
                    }
                    node.nstate().fetch_or(bit, Ordering::Relaxed);
                }
            });
            taskflow.task(marker).precede([target]);
            taskflow
        });

        // Twice, because the second run is what shows that a bit the first run
        // left does not accumulate into anything -- neither the word nor the
        // counter.
        for run in 1..=2 {
            executor.run(&taskflow).wait().expect("no task panicked");

            let node = taskflow.graph().node(marked);
            assert_ne!(
                node.nstate().load(Ordering::Relaxed) & bit,
                0,
                "run {run} of {bit:#x}: the marker did not set the bit"
            );
            assert_eq!(
                node.join_counter().load(Ordering::Relaxed),
                1,
                "run {run} of {bit:#x}: the join counter was re-armed from the \
                 whole state word instead of from the dependency count"
            );
        }

        assert_eq!(
            stale.load(Ordering::Relaxed),
            0,
            "{bit:#x} survived set-up into the next run"
        );
        executor.wait_for_all();
    }
}
