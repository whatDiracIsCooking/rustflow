//! Translated from `examples/runtime.cpp` (Taskflow v4.1.0).
//!
//! `A` is a condition task that always selects its first successor, `B`.
//! Ordinarily that leaves `C` and `D` unscheduled -- a conditioner's edges
//! are weak, so a successor it does not select never runs. `B` is a runtime
//! task, and its body forces `C` to run anyway with [`Runtime::schedule`],
//! bypassing the dependency the condition did not satisfy. `D` is reachable
//! only through the condition's own choice, so it never runs.
//!
//! Upstream's program only *prints* as it goes and dumps the graph for a
//! human to read. This milestone's examples print *and* **assert**: every
//! line upstream shows is also checked with `assert!` or `assert_eq!`, so a
//! non-zero exit from the binary is itself the test. See the README's
//! "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    let mut taskflow = Taskflow::with_name("Runtime Tasking");
    let executor = Executor::default();

    let b_ran = Arc::new(AtomicBool::new(false));
    let c_ran = Arc::new(AtomicBool::new(false));
    let d_ran = Arc::new(AtomicBool::new(false));

    // Upstream's `[] () { return 0; }`: a condition task, selecting its
    // first successor -- `B` below -- and nothing else.
    let a = taskflow.emplace_condition(|| 0);
    let c = {
        let c_ran = Arc::clone(&c_ran);
        taskflow.emplace(move || {
            println!("C");
            c_ran.store(true, Ordering::Relaxed);
        })
    };
    let b = {
        let b_ran = Arc::clone(&b_ran);
        // `C` must be captured by the body, as upstream's comment on `[&C]`
        // says, so that `rt.schedule(c)` names the right task.
        taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
            println!("B");
            b_ran.store(true, Ordering::Relaxed);
            rt.schedule(c);
        })
    };
    let d = {
        let d_ran = Arc::clone(&d_ran);
        taskflow.emplace(move || {
            println!("D");
            d_ran.store(true, Ordering::Relaxed);
        })
    };

    taskflow.task(a).name("A");
    taskflow.task(b).name("B");
    taskflow.task(c).name("C");
    taskflow.task(d).name("D");

    // Create conditional dependencies: `A` may choose `B`, `C` or `D`, but
    // its body above always picks `B`.
    taskflow.task(a).precede([b, c, d]);

    // Dump the graph structure.
    let dot = taskflow.dump();
    println!("{dot}");
    assert_eq!(
        dot,
        concat!(
            "digraph Taskflow {\n",
            "  compound=true;\n",
            "  subgraph cluster_p0 {\n",
            "    label=\"Taskflow: Runtime Tasking\";\n",
            "    p0[label=\"A\" shape=diamond color=black fillcolor=aquamarine style=filled];\n",
            "    p0 -> p2 [style=dashed label=\"0\"];\n",
            "    p0 -> p1 [style=dashed label=\"1\"];\n",
            "    p0 -> p3 [style=dashed label=\"2\"];\n",
            "    p1[label=\"C\" ];\n",
            "    p2[label=\"B\" ];\n",
            "    p3[label=\"D\" ];\n",
            "  }\n",
            "}\n",
        )
    );

    // We will see both B and C in the output.
    assert!(executor.run(&taskflow).wait().is_ok());

    assert!(b_ran.load(Ordering::Relaxed), "B is the condition's choice");
    assert!(c_ran.load(Ordering::Relaxed), "C is forced by rt.schedule");
    assert!(
        !d_ran.load(Ordering::Relaxed),
        "D is reachable only through the condition, which never chose it"
    );
}
