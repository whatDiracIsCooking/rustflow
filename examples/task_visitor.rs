//! Translated from `examples/task_visitor.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates traversing a taskflow with `Taskflow::for_each_task`,
//! `TaskRef::for_each_successor`/`for_each_predecessor`, and, for the one
//! task that spawned a subflow, `TaskRef::for_each_subflow_task`.
//!
//! Subflows are cleared when they join unless the body calls
//! [`Subflow::set_retain(true)`](rustflow::Subflow::set_retain), so task B's
//! body below does that -- upstream's `retain(true)`, this port's rename, as
//! `DESIGN.md` has it -- and the taskflow must run once before the subflow's
//! tasks exist to visit at all.
//!
//! Upstream's visitor closures capture `tf::Task` by value and read `.name()`
//! straight off it, the handle still aliasing the live graph. The
//! `for_each_*` visitors here hand back the bare [`Task`](rustflow::Task)
//! index instead of a borrowing handle for the same reason the attach_data
//! example gives: [`FlowBuilder::task_ref`](rustflow::FlowBuilder::task_ref)
//! looks the index back up each time a name is wanted.
//! [`TaskRef::for_each_subflow_task`] is the one exception, handing back a
//! [`TaskRef`](rustflow::TaskRef) already scoped to the subgraph -- but that
//! scoping is also what makes a *second* subgraph index (one of its
//! successors or predecessors) impossible to resolve from inside the first
//! call: the subgraph sits behind a lock the call already holds, so a second
//! call from inside the first would try to lock it again on the same thread
//! and hang. The fix below is the one the module docs for
//! `TaskRef::for_each_successor` suggest for the bare-`Task` case too --
//! collect first, resolve after -- just run twice: one pass over
//! `for_each_subflow_task` to build a name lookup, a second to print.
//!
//! Upstream prints every line as it walks; this port collects the same
//! lines and asserts the whole trail, so a non-zero exit is the test.

use std::collections::HashMap;

use rustflow::prelude::*;
use rustflow::{Executor, Task};

fn main() {
    // Create a taskflow graph with three static tasks and one subflow task.
    let mut taskflow = Taskflow::with_name("visitor");
    let executor = Executor::default();

    let a = taskflow.emplace(|| println!("TaskA"));
    let b = taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
        println!("TaskB is spawning B1, B2, and B3 ...");
        let b1 = subflow.emplace(|| println!("  Subtask B1"));
        let b2 = subflow.emplace(|| println!("  Subtask B2"));
        let b3 = subflow.emplace(|| println!("  Subtask B3"));
        subflow.task(b1).name("B1").precede([b3]);
        subflow.task(b2).name("B2").precede([b3]);
        subflow.task(b3).name("B3");
        subflow.set_retain(true); // retains the subflow
    });
    let c = taskflow.emplace(|| println!("TaskC"));
    let d = taskflow.emplace(|| println!("TaskD"));
    taskflow.task(a).name("A");
    taskflow.task(b).name("B");
    taskflow.task(c).name("C");
    taskflow.task(d).name("D");

    taskflow.task(a).precede([b]); // B runs after A
    taskflow.task(a).precede([c]); // C runs after A
    taskflow.task(b).precede([d]); // D runs after B
    taskflow.task(c).precede([d]); // D runs after C

    assert!(executor.run(&taskflow).wait().is_ok());

    // Examine the graph.
    let dot = taskflow.dump();
    println!("{dot}");
    // A subflow task's own dump only shows its cluster once its subgraph is
    // non-empty, which is upstream's note on `dump()` as well as `retain`'s:
    // the subflow ran and kept what it built.
    assert!(dot.contains("\"B1\""));
    assert!(dot.contains("\"B2\""));
    assert!(dot.contains("\"B3\""));

    // Traverse all tasks in the taskflow, collecting what upstream prints
    // instead of printing it directly, so the trail below can be asserted.
    let mut lines = Vec::new();
    taskflow.for_each_task(|task| {
        let node = taskflow.task_ref(task);
        let line = format!("task {} [type={}]", node.name(), node.task_type());
        println!("{line}");
        lines.push(line);

        node.for_each_successor(|successor| {
            let line = format!(
                "  -> successor   task {}",
                taskflow.task_ref(successor).name()
            );
            println!("{line}");
            lines.push(line);
        });
        node.for_each_predecessor(|predecessor| {
            let line = format!(
                "  <- predecessor task {}",
                taskflow.task_ref(predecessor).name()
            );
            println!("{line}");
            lines.push(line);
        });

        // First pass: a name lookup for this subflow's own tasks, built
        // before resolving any successor or predecessor -- see the module
        // docs for why a second, nested call cannot do this instead.
        let mut subflow_names: HashMap<Task, String> = HashMap::new();
        node.for_each_subflow_task(|subtask| {
            subflow_names.insert(subtask.task(), subtask.name().to_string());
        });

        // Second pass: print each subflow task and resolve its edges
        // against the lookup just built.
        node.for_each_subflow_task(|subtask| {
            let line = format!("  subflow task {}", subtask.name());
            println!("{line}");
            lines.push(line);

            subtask.for_each_successor(|successor| {
                let line = format!("    -> successor   task {}", subflow_names[&successor]);
                println!("{line}");
                lines.push(line);
            });
            subtask.for_each_predecessor(|predecessor| {
                let line = format!("    <- predecessor task {}", subflow_names[&predecessor]);
                println!("{line}");
                lines.push(line);
            });
        });
    });

    assert_eq!(
        lines,
        [
            "task A [type=static]",
            "  -> successor   task B",
            "  -> successor   task C",
            "task B [type=subflow]",
            "  -> successor   task D",
            "  <- predecessor task A",
            "  subflow task B1",
            "    -> successor   task B3",
            "  subflow task B2",
            "    -> successor   task B3",
            "  subflow task B3",
            "    <- predecessor task B1",
            "    <- predecessor task B2",
            "task C [type=static]",
            "  -> successor   task D",
            "  <- predecessor task A",
            "task D [type=static]",
            "  <- predecessor task B",
            "  <- predecessor task C",
        ]
    );
}
