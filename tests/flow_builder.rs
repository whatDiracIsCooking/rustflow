//! Translated from `unittests/test_basics.cpp` (Taskflow v4.1.0): the
//! `Creation`, `Taskflow.RemoveDependency` and `STDFunction` cases, and the
//! per-taskflow half of `Iterators`.
//!
//! Upstream runs most of these through a `tf::Executor`, which is M2. Where a
//! case's assertions depend on execution, the translation keeps the part that
//! is about the graph that was *built* and says what it dropped; where it
//! depends on a node kind this port does not have yet (`STDFunction` emplaces a
//! condition task), the same. Nothing here reaches for an executor.

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
/// *condition* task whose return value picks one successor -- two node kinds
/// this port does not have (M5). So the assertions here count invocations of
/// every task instead of the subset a condition would select; what the case is
/// really testing, that a type-erased callable survives `emplace` and can be
/// swapped afterwards, translates unchanged.
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
