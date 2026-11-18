//! A [`loom`](https://docs.rs/loom) model of the rule the ordering section of
//! `executor::topology` states: **a thread waiting on a run is woken only by
//! the drop of the last share of that run's hold on the storage.** Issue #138
//! is where the rule got this model, after the third race of one shape.
//!
//! **This file is not part of the gate**, for `tests/loom_notifier.rs`'s
//! reason: loom needs a `cfg(loom)` build of the whole graph, which only
//! `RUSTFLAGS` sets, and the cases below are empty without it. The command is
//! the notifier model's, and both files build in one go:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=/target/rustflow-loom \
//!     cargo test --release --test loom_storage
//! ```
//!
//! `.github/workflows/loom.yml` runs it weekly and on `workflow_dispatch`,
//! alongside every other `tests/loom_*.rs`, which is the only thing watching
//! this model outside a deliberate local run (#128).
//!
//! # What is modelled, and what is not
//!
//! The model is **standalone**: it does not drive the crate's `Storage` and
//! `Topology`, it restates their protocol in a few dozen lines of loom types.
//! That is a deliberate trade and the header says so plainly. Putting the real
//! types under loom would mean `cfg(loom)` aliases for every `Arc`, `Mutex`
//! and `Condvar` in `topology.rs` and `run_handle.rs` plus everything they
//! reach -- the graph, the run queue, the worker -- which is most of the
//! executor, and loom's search is exponential in the atomics it sees. The
//! notifier is modelled on its real type because it is fifteen `unsafe`
//! blocks over one state word; this protocol is three allocations and an
//! ordering, and the ordering is the whole question.
//!
//! So the model has to earn its keep another way, and it does it by
//! **mutation**: alongside the two orderings the crate ships, it carries the
//! three it used to ship or might have, and asserts that loom *finds* each of
//! those. A model that only passes on the current code has shown nothing
//! about itself. One that fails on #109's ordering, #112's ordering and
//! #138's ordering and passes on the fix has shown it is a model of the thing
//! and not of a toy -- and when the crate's protocol changes, the matching
//! case here changes with it, or the mutation it was checked against stops
//! applying and the header has to say why.
//!
//! The pieces, and what each stands for in the crate:
//!
//! - [`Flow`] is the taskflow's storage. The waiter's assertion is the one
//!   `Taskflow::graph_mut` makes: `Arc::get_mut` on the taskflow's own share
//!   succeeds, i.e. no run still holds one.
//! - [`Hold`] is `Storage`: the run's hold on the storage, of which every job
//!   of the run owns a share, with a [`Drop`] that is the fix's whole content.
//!   Which of `decrement` and `finish` that `Drop` performs is the knob the
//!   mutations turn.
//! - [`Completion`] is `Topology`'s completion slot: `finish` and `join`,
//!   which is what `RunHandle::wait` sleeps in.
//! - [`RunCount`] is the executor's `num_topologies` with the lock and condvar
//!   `wait_for_all` sleeps on, and `decrement` is `Shared::decrement_topology`.
//! - The **tear-down** thread is the thread that finishes the run's last job
//!   and runs `tear_down_topology`. The **straggler** thread is a worker still
//!   holding a copy of a preempted node's job -- the module docs' three copies
//!   -- which it drops at a time nothing orders. That one copy is enough: the
//!   question is whether *any* share can outlive the wake, and loom tries
//!   every moment the straggler could drop it.
//!
//! What the model cannot say: anything about the graph, the queues, or how a
//! share gets copied in the first place. It takes "a share exists that the
//! finishing job does not hold" as given, because the module docs argue that
//! the copies cannot be eliminated and #109 observed one.

#![cfg(loom)]

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Condvar, Mutex};
use loom::thread;

/// The taskflow's storage, which is what the waiter wants to be the unique
/// owner of when it wakes.
struct Flow;

/// `Topology`'s completion slot: `finish` wakes whoever is in `join`.
struct Completion {
    finished: Mutex<bool>,
    woken: Condvar,
}

impl Completion {
    fn new() -> Self {
        Self {
            finished: Mutex::new(false),
            woken: Condvar::new(),
        }
    }

    /// `Topology::finish`.
    fn finish(&self) {
        let mut finished = self.finished.lock().unwrap();
        *finished = true;
        self.woken.notify_all();
    }

    /// `Topology::join`, the wait under `RunHandle::wait`.
    fn join(&self) {
        let mut finished = self.finished.lock().unwrap();
        while !*finished {
            finished = self.woken.wait(finished).unwrap();
        }
    }
}

/// The executor's run count, with the lock and condvar `wait_for_all` sleeps
/// on. One run in flight when built.
struct RunCount {
    count: AtomicUsize,
    idle: Mutex<()>,
    none: Condvar,
}

impl RunCount {
    fn new() -> Self {
        Self {
            count: AtomicUsize::new(1),
            idle: Mutex::new(()),
            none: Condvar::new(),
        }
    }

    /// `Shared::decrement_topology`: the release half is the `fetch_sub`, and
    /// the notification is taken under the lock so that a waiter between its
    /// read and its sleep cannot miss it.
    fn decrement(&self) {
        if self.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            let _idle = self.idle.lock().unwrap();
            self.none.notify_all();
        }
    }

    /// `Executor::wait_for_all`.
    fn wait_for_all(&self) {
        let mut idle = self.idle.lock().unwrap();
        while self.count.load(Ordering::Relaxed) != 0 {
            idle = self.none.wait(idle).unwrap();
        }
    }
}

/// What the last share's `Drop` does besides releasing the storage: the knob
/// the mutations turn.
#[derive(Clone, Copy)]
enum OnLastShare {
    /// Pre-#111: the hold releases the storage and nothing else. The
    /// tear-down wakes the waiter itself.
    Nothing,
    /// #111 to #138: the last share wakes the `wait`er. The tear-down still
    /// decrements the run count itself, before it drops its share.
    Finish,
    /// #138: the last share decrements the run count and then wakes the
    /// `wait`er, so both waiters are woken by the one event.
    DecrementThenFinish,
}

/// `Storage`: the run's hold on the storage, shared by every job of the run.
struct Hold {
    flow: Option<Arc<Flow>>,
    count: Arc<RunCount>,
    run: Arc<Completion>,
    on_last_share: OnLastShare,
}

impl Hold {
    fn new(
        flow: &Arc<Flow>,
        count: &Arc<RunCount>,
        run: &Arc<Completion>,
        on_last_share: OnLastShare,
    ) -> Arc<Self> {
        Arc::new(Self {
            flow: Some(Arc::clone(flow)),
            count: Arc::clone(count),
            run: Arc::clone(run),
            on_last_share,
        })
    }
}

impl Drop for Hold {
    /// `Drop for Storage`. The storage first, always: the share has to be gone
    /// before anyone is woken to look for it.
    fn drop(&mut self) {
        drop(self.flow.take());
        match self.on_last_share {
            OnLastShare::Nothing => {}
            OnLastShare::Finish => self.run.finish(),
            OnLastShare::DecrementThenFinish => {
                self.count.decrement();
                self.run.finish();
            }
        }
    }
}

/// Which wait the main thread -- the caller holding the taskflow -- is in.
#[derive(Clone, Copy)]
enum Waiter {
    /// `RunHandle::wait`, which sleeps in the completion slot.
    Wait,
    /// `Executor::wait_for_all`, which sleeps on the run count.
    WaitForAll,
}

/// One run, torn down by one thread while another still holds a copy of a
/// job, and a caller in one of the two waits who edits the taskflow the
/// instant the wait returns.
///
/// `tear_down` is `Shared::tear_down_topology`'s final-run branch, handed its
/// own share of the hold and the run's accounting so the orderings can differ
/// in what the function does and what the `Drop` does.
fn one_run(
    on_last_share: OnLastShare,
    waiter: Waiter,
    tear_down: fn(Arc<Hold>, &RunCount, &Completion),
) {
    let mut flow = Arc::new(Flow);
    let count = Arc::new(RunCount::new());
    let run = Arc::new(Completion::new());

    // The run's own share, which the tear-down drops, and the copy of a
    // preempted node's job that a frame already moving on still holds.
    let own = Hold::new(&flow, &count, &run, on_last_share);
    let copy = Arc::clone(&own);

    let straggler = thread::spawn(move || drop(copy));
    let finisher = {
        let (count, run) = (Arc::clone(&count), Arc::clone(&run));
        thread::spawn(move || tear_down(own, &count, &run))
    };

    match waiter {
        Waiter::Wait => run.join(),
        Waiter::WaitForAll => count.wait_for_all(),
    }
    // `Taskflow::graph_mut`, one line after the wait.
    assert!(
        Arc::get_mut(&mut flow).is_some(),
        "the wait returned while a run still held a share of the storage"
    );

    straggler.join().unwrap();
    finisher.join().unwrap();
}

/// The tear-down before #111: account for the run, drop this thread's share,
/// wake the waiter. Whoever still holds a copy is not consulted.
fn tear_down_waking_the_waiter_itself(own: Arc<Hold>, count: &RunCount, run: &Completion) {
    count.decrement();
    drop(own);
    run.finish();
}

/// The tear-down from #111 to #138: account for the run, then drop this
/// thread's share and let the last share -- this one or the straggler's --
/// wake the `wait`er.
fn tear_down_decrementing_then_dropping(own: Arc<Hold>, count: &RunCount, _: &Completion) {
    count.decrement();
    drop(own);
}

/// The tear-down since #138: drop this thread's share, and nothing else. The
/// last share accounts for the run and wakes whoever is waiting.
fn tear_down_dropping_only(own: Arc<Hold>, _: &RunCount, _: &Completion) {
    drop(own);
}

// --- The orderings the crate ships ----------------------------------------

/// `RunHandle::wait` wakes to a unique storage: #109's fix, modelled.
#[test]
fn a_wait_is_woken_by_the_last_share() {
    loom::model(|| {
        one_run(
            OnLastShare::DecrementThenFinish,
            Waiter::Wait,
            tear_down_dropping_only,
        );
    });
}

/// `Executor::wait_for_all` wakes to a unique storage: #138's fix, modelled.
#[test]
fn a_wait_for_all_is_woken_by_the_last_share() {
    loom::model(|| {
        one_run(
            OnLastShare::DecrementThenFinish,
            Waiter::WaitForAll,
            tear_down_dropping_only,
        );
    });
}

/// A `corun` frame does not sleep in either slot: it owns the hold and polls
/// it for uniqueness, which is #112's fix. The straggler's copy is a share of
/// that same hold, so the poll cannot say yes while the copy is alive, and
/// the flow share the caller edits through is inside the hold.
#[test]
fn a_corun_frame_that_polls_its_hold_for_uniqueness_wakes_to_a_unique_storage() {
    loom::model(|| {
        let mut flow = Arc::new(Flow);
        let count = Arc::new(RunCount::new());
        let run = Arc::new(Completion::new());
        let mut hold = Hold::new(&flow, &count, &run, OnLastShare::Nothing);
        let copy = Arc::clone(&hold);

        let straggler = thread::spawn(move || drop(copy));

        // `worker.corun_until(|| Arc::get_mut(&mut hold).is_some())`.
        while Arc::get_mut(&mut hold).is_none() {
            thread::yield_now();
        }
        drop(hold);
        assert!(
            Arc::get_mut(&mut flow).is_some(),
            "the hold was unique but the storage was not"
        );

        straggler.join().unwrap();
    });
}

// --- The orderings it does not, and loom has to find each ------------------

/// Mutation 1, #109: the tear-down wakes the waiter itself after dropping its
/// own share. The straggler's copy can still be alive when the waiter edits.
#[test]
#[should_panic(expected = "the wait returned while a run still held a share of the storage")]
fn mutation_109_a_tear_down_that_wakes_the_waiter_itself_is_found() {
    loom::model(|| {
        one_run(
            OnLastShare::Nothing,
            Waiter::Wait,
            tear_down_waking_the_waiter_itself,
        );
    });
}

/// Mutation 2, #112: a `corun` frame that polls an *anchor* the straggler's
/// copy is not counted in. The job's finish drives the anchor to zero and
/// the frame moves on, with the copy still holding the storage.
#[test]
#[should_panic(expected = "the anchor reached zero while a copy still held the storage")]
fn mutation_112_a_corun_frame_that_polls_an_anchor_is_found() {
    loom::model(|| {
        let mut flow = Arc::new(Flow);
        let count = Arc::new(RunCount::new());
        let run = Arc::new(Completion::new());
        let hold = Hold::new(&flow, &count, &run, OnLastShare::Nothing);
        let copy = Arc::clone(&hold);
        let anchor = Arc::new(AtomicUsize::new(1));

        // The last job of the coruned graph: it finishes -- the anchor's
        // decrement -- holding the run's share, which it then drops. The
        // copy on the frame that preempted is dropped by another thread,
        // whenever.
        let finisher = {
            let anchor = Arc::clone(&anchor);
            thread::spawn(move || {
                anchor.fetch_sub(1, Ordering::AcqRel);
                drop(hold);
            })
        };
        let straggler = thread::spawn(move || drop(copy));

        // `worker.corun_until(|| anchor.outstanding() == 0)` and nothing
        // after it.
        while anchor.load(Ordering::Acquire) != 0 {
            thread::yield_now();
        }
        assert!(
            Arc::get_mut(&mut flow).is_some(),
            "the anchor reached zero while a copy still held the storage"
        );

        finisher.join().unwrap();
        straggler.join().unwrap();
    });
}

/// Mutation 3, #138: the tear-down decrements the run count before it drops
/// its own share. A `wait_for_all` wakes on the decrement, and either the
/// tear-down's own share or the straggler's copy can still be alive.
#[test]
#[should_panic(expected = "the wait returned while a run still held a share of the storage")]
fn mutation_138_a_tear_down_that_decrements_before_dropping_is_found() {
    loom::model(|| {
        one_run(
            OnLastShare::Finish,
            Waiter::WaitForAll,
            tear_down_decrementing_then_dropping,
        );
    });
}

/// The control for mutation 3: the same tear-down is sound for a `wait`er,
/// which is why #109's test never saw #138. The model has to pass here, or
/// mutation 3 is failing for a reason other than the one it names.
#[test]
fn mutation_138s_tear_down_is_sound_for_a_wait() {
    loom::model(|| {
        one_run(
            OnLastShare::Finish,
            Waiter::Wait,
            tear_down_decrementing_then_dropping,
        );
    });
}
