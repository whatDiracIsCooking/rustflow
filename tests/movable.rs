//! Translated from `unittests/test_movable.cpp` (Taskflow v4.1.0).
//!
//! Upstream's file is about what `tf::Taskflow`'s move constructor, move
//! assignment and `run(Taskflow&&)` overloads do to the *lifetime* of the task
//! bodies: every one of its cases counts destructions of a value each task body
//! captured, so a taskflow freed one moment too early or too late shows up as a
//! wrong number. Two mappings make that translate, and both are exact rather
//! than approximate:
//!
//! - **`std::move(taskflow)` is [`std::mem::take`].** A moved-from C++
//!   `Taskflow` is left empty -- its move constructor moves the graph out -- and
//!   `mem::take` leaves a `Default`-constructed one behind, which is the same
//!   empty taskflow. So `run(std::move(tf))` is `run_owned(mem::take(&mut tf))`,
//!   and upstream's `REQUIRE(tf.num_tasks() == 0)` after a move survives
//!   verbatim. Move *assignment* translates the same way: `tf1 = std::move(tf2)`
//!   is `tf1 = mem::take(&mut tf2)`, which drops what `tf1` was holding exactly
//!   as the C++ assignment destroys it.
//! - **A moved-in taskflow is freed by its run handle, not by its tear-down.**
//!   Upstream hands the taskflow to an async task that holds it until the last
//!   run is over; here the handle owns it, and the handle does not finish
//!   dropping until the last run is over. The difference is only in which line
//!   frees it: upstream's count is right after `wait()` because the tear-down
//!   freed it, and the port's is right after the handle is dropped -- which
//!   `wait()` does, since it takes the handle by value.
//!
//! The one liberty taken is size: upstream's `N` is 10000 and this file's is a
//! tenth of that, because nothing in these cases is about scale -- they count
//! drops, and a thousand of them counts as well as ten thousand.

use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use rustflow::Executor;
use rustflow::prelude::*;

/// How many tasks a taskflow is built with: upstream's `N`, a tenth of it.
const N: usize = 1000;

/// Upstream's `CountOnDestruction`, with the increment its task bodies do
/// folded in:
/// one of these is captured by each task body, bumps the counter once when the
/// body runs, and bumps it again when it is dropped.
///
/// Upstream needs copy and move constructors that null out the source so that
/// one object is counted once however often it is copied; a Rust move is a
/// memcpy that runs no destructor, so there is nothing to null out and each
/// value is counted exactly once by construction.
struct Counted(Arc<AtomicUsize>);

impl Counted {
    /// The task body's own increment, which is not `common/bodies.rs`'s
    /// `bump`: what each body here holds is the counted value itself, so that
    /// dropping it counts a second time.
    fn ran(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// Upstream's `make_taskflow`: `tasks` task bodies, each holding one
/// [`Counted`].
fn make_taskflow(taskflow: &mut Taskflow, counter: &Arc<AtomicUsize>, tasks: usize) {
    for _ in 0..tasks {
        let counted = Counted(Arc::clone(counter));
        taskflow.emplace(move || counted.ran());
    }
}

/// Upstream's `[repeat]() mutable { return repeat-- == 0; }`, which is what
/// `run_n` passes and what `moved_run` passes by hand.
fn repeat(times: usize) -> impl FnMut() -> bool + Send + 'static {
    let mut remaining = times;
    move || {
        let stop = remaining == 0;
        remaining = remaining.saturating_sub(1);
        stop
    }
}

// ----------------------------------------------------------------------------
// test move constructor: upstream's `moved_run`
// ----------------------------------------------------------------------------

#[test]
fn moved_run() {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut taskflow = Taskflow::new();

    let count = || counter.load(Ordering::Relaxed);

    // Run the moved taskflow twice over, upstream's `run_until(std::move(tf),
    // [repeat=2]…, [](){})`: two runs of `N` tasks, then the taskflow is freed
    // and its `N` captured values with it.
    make_taskflow(&mut taskflow, &counter, N);
    Executor::default()
        .run_owned_until_with(mem::take(&mut taskflow), repeat(2), || {})
        .wait()
        .expect("a task panicked");

    assert_eq!(taskflow.num_tasks(), 0, "the taskflow was moved out");
    assert_eq!(count(), 3 * N, "two runs and one round of drops");

    // Run the original, now empty, taskflow.
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("an empty run cannot panic");
    assert_eq!(count(), 3 * N);

    // Remake it and run it borrowed: nothing is freed, so only the run counts.
    make_taskflow(&mut taskflow, &counter, N);
    assert_eq!(taskflow.num_tasks(), N);
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("a task panicked");
    assert_eq!(count(), 4 * N);
    assert_eq!(taskflow.num_tasks(), N, "a borrowed run keeps the graph");

    // Hand the same taskflow over by value: one run, then `N` drops.
    Executor::default()
        .run_owned(mem::take(&mut taskflow))
        .wait()
        .expect("a task panicked");
    assert_eq!(count(), 6 * N);
    assert_eq!(taskflow.num_tasks(), 0);

    // Hand over the empty one: nothing runs and nothing is freed.
    Executor::default()
        .run_owned(mem::take(&mut taskflow))
        .wait()
        .expect("an empty run cannot panic");
    assert_eq!(count(), 6 * N);
    assert_eq!(taskflow.num_tasks(), 0);

    // Three runs of a moved-in taskflow, upstream's `run_n(std::move(tf), 3)`.
    make_taskflow(&mut taskflow, &counter, N);
    assert_eq!(taskflow.num_tasks(), N);
    Executor::default()
        .run_owned_n(mem::take(&mut taskflow), 3)
        .wait()
        .expect("a task panicked");
    assert_eq!(count(), 10 * N);
    assert_eq!(taskflow.num_tasks(), 0);

    // A moved-in *empty* taskflow with a callback: the callback still fires,
    // which is upstream's early return calling `c()` before it hands back a
    // ready future.
    let bump_by_n = {
        let counter = Arc::clone(&counter);
        move || {
            counter.fetch_add(N, Ordering::Relaxed);
        }
    };
    Executor::default()
        .run_owned_with(mem::take(&mut taskflow), bump_by_n)
        .wait()
        .expect("an empty run cannot panic");
    assert_eq!(count(), 11 * N);
    assert_eq!(taskflow.num_tasks(), 0);

    // And the same with something to run: one run, one round of drops, one
    // callback.
    let bump_by_n = {
        let counter = Arc::clone(&counter);
        move || {
            counter.fetch_add(N, Ordering::Relaxed);
        }
    };
    make_taskflow(&mut taskflow, &counter, N);
    Executor::default()
        .run_owned_with(mem::take(&mut taskflow), bump_by_n)
        .wait()
        .expect("a task panicked");
    assert_eq!(count(), 14 * N);
    assert_eq!(taskflow.num_tasks(), 0);
}

// ----------------------------------------------------------------------------
// test move assignment: upstream's `moved_taskflows`
// ----------------------------------------------------------------------------

#[test]
fn moved_taskflows() {
    let counter = Arc::new(AtomicUsize::new(0));
    let count = || counter.load(Ordering::Relaxed);

    {
        let mut taskflow1 = Taskflow::new();
        let mut taskflow2 = Taskflow::new();

        make_taskflow(&mut taskflow1, &counter, N);
        make_taskflow(&mut taskflow2, &counter, N / 2);
        assert_eq!(taskflow1.num_tasks(), N);
        assert_eq!(taskflow2.num_tasks(), N / 2);

        // Upstream's `taskflow1 = std::move(taskflow2)`, which destroys what
        // `taskflow1` was holding: `N` drops.
        taskflow1 = mem::take(&mut taskflow2);
        assert_eq!(count(), N);
        assert_eq!(taskflow1.num_tasks(), N / 2);
        assert_eq!(taskflow2.num_tasks(), 0);

        {
            let executor = Executor::default();

            let half = executor.run_owned(mem::take(&mut taskflow1));
            let nothing = executor.run_owned(mem::take(&mut taskflow2));
            assert_eq!(taskflow1.num_tasks(), 0);
            assert_eq!(taskflow2.num_tasks(), 0);

            // Both are refilled while their old contents are still running,
            // which is the point of the subcase: the runs own what they were
            // handed, so the variables are free to hold something else.
            make_taskflow(&mut taskflow1, &counter, N);
            make_taskflow(&mut taskflow2, &counter, N);
            assert_eq!(taskflow1.num_tasks(), N);
            assert_eq!(taskflow2.num_tasks(), N);

            executor.wait_for_all();

            // Upstream's count is right here, because its tear-down freed the
            // moved-in taskflow; the port's handle is what owns it, so this is
            // the line that frees it.
            drop((half, nothing));
            assert_eq!(count(), 2 * N, "the runs and the graphs they owned");
        }
    }

    // Now both refilled taskflows have gone out of scope too.
    assert_eq!(count(), 4 * N);

    // Upstream's move-constructor block, which runs nothing: it is about what
    // a move leaves behind.
    {
        let mut taskflow1 = Taskflow::new();
        let taskflow2 = mem::take(&mut taskflow1);
        assert_eq!(taskflow1.num_tasks(), 0);
        assert_eq!(taskflow2.num_tasks(), 0);

        make_taskflow(&mut taskflow1, &counter, N);
        let mut taskflow3 = mem::take(&mut taskflow1);
        assert_eq!(count(), 4 * N, "a move frees nothing");
        assert_eq!(taskflow1.num_tasks(), 0);
        assert_eq!(taskflow3.num_tasks(), N);

        // Assigning the empty one over `taskflow3` drops `taskflow3`'s graph.
        taskflow3 = mem::take(&mut taskflow1);
        assert_eq!(count(), 5 * N);
        assert_eq!(taskflow1.num_tasks(), 0);
        assert_eq!(taskflow2.num_tasks(), 0);
        assert_eq!(taskflow3.num_tasks(), 0);
    }

    assert_eq!(count(), 5 * N);
}

// ----------------------------------------------------------------------------
// test multithreaded run: upstream's `parallel_moved_runs`
// ----------------------------------------------------------------------------

#[test]
fn parallel_moved_runs() {
    /// Upstream's 64 threads for the first block and 32 for the second.
    const MANY: usize = 64;
    const SOME: usize = 32;

    let counter = Arc::new(AtomicUsize::new(0));
    let count = || counter.load(Ordering::Relaxed);

    {
        let executor = Executor::default();

        thread::scope(|scope| {
            for _ in 0..MANY {
                scope.spawn(|| {
                    let mut taskflow = Taskflow::new();
                    make_taskflow(&mut taskflow, &counter, N);
                    // Upstream discards the future and lets `wait_for_all`
                    // below be the barrier; a dropped handle blocks here
                    // instead, which is the same wait in a different place.
                    drop(executor.run_owned(taskflow));
                });
            }
        });

        executor.wait_for_all();
    }

    assert_eq!(count(), MANY * N * 2, "one run and one drop per task");

    counter.store(0, Ordering::Relaxed);

    {
        let executor = Executor::default();

        thread::scope(|scope| {
            for _ in 0..SOME {
                scope.spawn(|| {
                    let bump = || {
                        let counter = Arc::clone(&counter);
                        move || {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    };

                    let mut taskflow1 = Taskflow::new();
                    make_taskflow(&mut taskflow1, &counter, N);
                    let mut taskflow2 = mem::take(&mut taskflow1);

                    // Four runs over two variables, two of which have already
                    // been moved out of and so run nothing. All four callbacks
                    // fire all the same.
                    drop(executor.run_owned_with(mem::take(&mut taskflow1), bump()));
                    drop(executor.run_owned_with(mem::take(&mut taskflow2), bump()));
                    drop(executor.run_owned_with(mem::take(&mut taskflow1), bump()));
                    drop(executor.run_owned_with(mem::take(&mut taskflow2), bump()));
                });
            }
        });

        executor.wait_for_all();
    }

    assert_eq!(count(), SOME * (N * 2 + 4));
}

// ----------------------------------------------------------------------------
// The two owning overloads upstream's own cases never call (not upstream's)
// ----------------------------------------------------------------------------

/// `run_owned_n_with` and `run_owned_until`, upstream's
/// `run_n(Taskflow&&, size_t, C&&)` and `run_until(Taskflow&&, P&&)`.
///
/// Both are declared in upstream's `executor.hpp` and neither is called by
/// `test_movable.cpp`, which reaches the other six members of the owning
/// family: it runs `run_n(std::move(tf), 3)` without a callback and
/// `run_until(std::move(tf), p, c)` with one. So this case is the port's own,
/// and it counts what every case above counts -- one increment per body run
/// and one per captured value dropped -- because that is what says the
/// handle, and not the call, is what frees a moved-in taskflow.
#[test]
fn moved_run_n_with_and_moved_run_until() {
    let counter = Arc::new(AtomicUsize::new(0));
    let count = || counter.load(Ordering::Relaxed);
    let mut taskflow = Taskflow::new();

    let bump_by_n = {
        let counter = Arc::clone(&counter);
        move || {
            counter.fetch_add(N, Ordering::Relaxed);
        }
    };

    // Three runs of a moved-in taskflow with a callback: `3 * N` bodies, `N`
    // drops once the handle frees the taskflow, and `N` from the callback.
    make_taskflow(&mut taskflow, &counter, N);
    Executor::default()
        .run_owned_n_with(mem::take(&mut taskflow), 3, bump_by_n)
        .wait()
        .expect("a task panicked");
    assert_eq!(count(), 5 * N);
    assert_eq!(taskflow.num_tasks(), 0, "the taskflow was moved out");

    // Two runs under a predicate and no callback: `2 * N` bodies and `N`
    // drops.
    make_taskflow(&mut taskflow, &counter, N);
    Executor::default()
        .run_owned_until(mem::take(&mut taskflow), repeat(2))
        .wait()
        .expect("a task panicked");
    assert_eq!(count(), 8 * N);
    assert_eq!(taskflow.num_tasks(), 0);

    // A predicate true from the start runs nothing -- upstream's "one true
    // from the start runs nothing", on the owning half of the family -- and
    // the moved-in taskflow is freed by the handle all the same, so the `N`
    // values it holds are dropped and nothing is run.
    make_taskflow(&mut taskflow, &counter, N);
    Executor::default()
        .run_owned_until(mem::take(&mut taskflow), || true)
        .wait()
        .expect("nothing ran, so nothing panicked");
    assert_eq!(count(), 9 * N, "the taskflow was freed but never run");
    assert_eq!(taskflow.num_tasks(), 0);

    // And the empty one: the predicate is never even asked, nothing runs and
    // there is nothing to free.
    Executor::default()
        .run_owned_until(mem::take(&mut taskflow), || panic!("not asked"))
        .wait()
        .expect("an empty run cannot panic");
    assert_eq!(count(), 9 * N);
}
