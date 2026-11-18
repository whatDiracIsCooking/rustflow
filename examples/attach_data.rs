//! Translated from `examples/attach_data.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates attaching data to a task and running it repeatedly with
//! changing data.
//!
//! Upstream attaches a raw `int*` and has the task's own body read it back
//! through a captured `tf::Task` -- a handle that keeps aliasing the live
//! graph while the body runs. That shape cannot survive the port unchanged:
//! a task body here is `'static` and owns nothing of the taskflow that
//! holds it, so it cannot borrow the graph it sits in to call
//! [`TaskRef::data`](rustflow::TaskRef::data) on itself -- see `DESIGN.md`'s
//! "graph as an arena". The port instead sets and reads the data from
//! *outside* each run, which is the one shape change this file needs a
//! comment for; everything the typed slot offers --
//! [`TaskMut::set_data`](rustflow::TaskMut::set_data),
//! [`TaskRef::data`](rustflow::TaskRef::data),
//! [`TaskMut::data_mut`](rustflow::TaskMut::data_mut) and
//! [`TaskMut::take_data`](rustflow::TaskMut::take_data) -- is exercised
//! below.
//!
//! Upstream prints the data each run shows; this port prints and asserts it,
//! so a non-zero exit is the test.

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    let executor = Executor::default();
    let mut taskflow = Taskflow::with_name("attach data to a task");

    // Create a task and attach data to it. The task's own body cannot read
    // the attached value back (see the module docs), so it just does
    // something independent of it; the data is read back from the main
    // thread after each run instead.
    let a = taskflow.placeholder();
    taskflow
        .task(a)
        .work(|| println!("running the data-bearing task"));

    // Run the taskflow iteratively with changing data.
    for data in 0..10 {
        taskflow.task(a).set_data(data);
        assert!(executor.run(&taskflow).wait().is_ok());

        let seen = *taskflow
            .task_ref(a)
            .data::<i32>()
            .expect("just attached as this type");
        println!("data is {seen}");
        assert_eq!(seen, data);
    }

    // `data_mut` edits the attached value in place, rather than replacing it
    // through another `set_data`.
    *taskflow
        .task(a)
        .data_mut::<i32>()
        .expect("still attached as this type") += 100;
    assert_eq!(taskflow.task_ref(a).data::<i32>(), Some(&109));

    // `take_data` lifts the value out, leaving nothing attached.
    assert_eq!(taskflow.task(a).take_data::<i32>(), Some(109));
    assert_eq!(taskflow.task_ref(a).data::<i32>(), None);
}
