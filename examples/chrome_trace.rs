//! Invented: this port's own example, not a translation of anything
//! upstream ships. Taskflow has no example exercising its built-in
//! `ChromeObserver` directly -- only the bare `ObserverInterface`, which
//! `examples/observer.cpp` demonstrates and `examples/observer.rs`
//! translates. No `//!` provenance line for that reason: an invented file
//! carries none, the way `src/executor/scope.rs` and `src/observer/tfprof.rs`
//! already do.
//!
//! Demonstrates [`ChromeObserver`] (#172): registered like any other
//! [`Observer`](rustflow::observer::Observer), it records one segment per
//! task dispatch and dumps them as a [Chrome trace-event] JSON array. There
//! is no upstream print to check this example's output against, so this
//! file's whole claim is in its own assertions: the dump's event count
//! matches the number of tasks dispatched, both before and after
//! [`ChromeObserver::clear`] empties it back out.
//!
//! [Chrome trace-event]: https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview

use rustflow::Executor;
use rustflow::observer::chrome::ChromeObserver;
use rustflow::prelude::*;

fn main() {
    let executor = Executor::new(2);
    let observer = executor.make_observer(ChromeObserver::new());

    // Simple.rs's own diamond: A precedes B and C, both precede D. None of
    // the four is a subflow or runtime task, so none preempts its own node
    // and "events recorded" equals "tasks dispatched" exactly.
    let mut taskflow = Taskflow::with_name("chrome_trace");
    let a = taskflow.emplace(|| println!("TaskA"));
    let b = taskflow.emplace(|| println!("TaskB"));
    let c = taskflow.emplace(|| println!("TaskC"));
    let d = taskflow.emplace(|| println!("TaskD"));
    taskflow.task(a).name("A").precede([b, c]);
    taskflow.task(b).name("B");
    taskflow.task(c).name("C");
    taskflow.task(d).name("D").succeed([b, c]);

    assert!(executor.run(&taskflow).wait().is_ok());

    assert_eq!(observer.num_tasks(), 4);

    let trace = observer.dump_to_string();
    println!("{trace}");

    assert!(trace.starts_with('['));
    assert!(trace.trim_end().ends_with(']'));
    // One `"ph":"X"` complete event per recorded segment: see
    // `ChromeObserver::dump`'s own doc comment for the shape of each.
    assert_eq!(trace.matches("\"ph\":\"X\"").count(), 4);

    observer.clear();
    assert_eq!(observer.num_tasks(), 0);
    assert_eq!(observer.dump_to_string(), "[]\n");
}
