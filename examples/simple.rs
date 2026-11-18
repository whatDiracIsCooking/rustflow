//! Translated from `examples/simple.cpp` (Taskflow v4.1.0).
//!
//! A simple example to capture the following task dependencies:
//!
//! ```text
//!           +---+
//!     +---->| B |-----+
//!     |     +---+     |
//!   +---+           +-v-+
//!   | A |           | D |
//!   +---+           +-^-+
//!     |     +---+     |
//!     +---->| C |-----+
//!           +---+
//! ```
//!
//! Upstream's program only *prints* each task's name as it runs and dumps the
//! graph to stdout for a human to read. This milestone's examples print *and*
//! **assert**: every line upstream shows is also checked with `assert!` or
//! `assert_eq!`, so a non-zero exit from the binary is itself the test. See
//! the README's "Examples" section.

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    let executor = Executor::default();
    let mut taskflow = Taskflow::with_name("simple");

    let a = taskflow.emplace(|| println!("TaskA"));
    let b = taskflow.emplace(|| println!("TaskB"));
    let c = taskflow.emplace(|| println!("TaskC"));
    let d = taskflow.emplace(|| println!("TaskD"));

    taskflow.task(a).name("A").precede([b, c]); // A runs before B and C
    taskflow.task(b).name("B");
    taskflow.task(c).name("C");
    taskflow.task(d).name("D").succeed([b, c]); // D runs after B and C

    assert!(executor.run(&taskflow).wait().is_ok());

    // dump the graph to a DOT format, via a string rather than upstream's
    // `std::cout` -- see this crate's `dump` module for why the ids differ
    // from upstream's (addresses here would not be reproducible).
    let dot = taskflow.dump();
    println!("{dot}");

    // What upstream only shows a human by eye, the port checks outright: the
    // four tasks and the two edges `precede`/`succeed` put in place above.
    assert_eq!(
        dot,
        concat!(
            "digraph Taskflow {\n",
            "  compound=true;\n",
            "  subgraph cluster_p0 {\n",
            "    label=\"Taskflow: simple\";\n",
            "    p0[label=\"A\" ];\n",
            "    p0 -> p1;\n",
            "    p0 -> p2;\n",
            "    p1[label=\"B\" ];\n",
            "    p1 -> p3;\n",
            "    p2[label=\"C\" ];\n",
            "    p2 -> p3;\n",
            "    p3[label=\"D\" ];\n",
            "  }\n",
            "}\n",
        )
    );
}
