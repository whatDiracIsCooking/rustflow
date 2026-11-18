//! A [`loom`](https://docs.rs/loom) model of the one-shot protocol behind
//! `AsyncHandle`: the `Delivery<R>`/`Slot<R>` pair in `src/executor/async.rs`
//! that a worker delivers an outcome into and that the caller waits on, polls
//! as a `Future`, or drops without ever asking. Issue #265 is where the slot
//! got this model; the five models beside it cover the executor's other
//! lock-free protocols and this one had none.
//!
//! The rule being modelled is the slot's whole contract, and it has three
//! halves: **a delivery is never lost** -- a waiter asleep in the condvar is
//! woken by it and a poller left `Pending` has its waker woken -- **a woken
//! waiter finds the outcome, not an empty slot**, and **a handle dropped
//! unwaited changes none of that**.
//!
//! **This file is not part of the gate**, for `tests/loom_notifier.rs`'s
//! reason: loom needs a `cfg(loom)` build of the whole graph, which only
//! `RUSTFLAGS` sets, and the cases below are empty without it. The command is
//! every model's, and all of them build in one go:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=/target/rustflow-loom \
//!     cargo test --release --test loom_async_handle
//! ```
//!
//! `.github/workflows/loom.yml` finds and runs it weekly and on
//! `workflow_dispatch`, alongside every other `tests/loom_*.rs`, which is the
//! only thing watching this model outside a deliberate local run (#128).
//!
//! # What is modelled, and what is not
//!
//! The model is **standalone**, in `tests/loom_storage.rs`'s sense: it
//! restates the slot's protocol in loom types rather than driving the crate's
//! own `Delivery`. That is not the trade it is in the storage model, where
//! the real types were reachable and too expensive to instrument; here
//! `executor::r#async` is `pub(crate)`, so a model built on the real
//! `Delivery` would need a `cfg(loom)` visibility seam in
//! `src/executor/mod.rs` as well as the `std`-or-`loom` import seam
//! `notifier.rs` carries, and the slot is small enough -- a `bool`, an
//! `Option<Outcome>`, an `Option<Waker>`, one mutex and one condvar -- that
//! the extraction below is the whole of it line for line. [`Delivery`]'s four
//! methods are `deliver`, `is_done`, `join` and `poll_join` with the same
//! steps in the same order, and [`Handle`] is `AsyncHandle`: a share of the
//! slot and nothing else, which is why dropping it is exactly dropping the
//! share.
//!
//! What stands outside the model, and why none of it is an interleaving
//! question:
//!
//! - The `catch_unwind` in `AsyncHandle::for_body` that turns a panicking
//!   body into `Err(RunError::Panicked(payload))`. It runs on the delivering
//!   worker before the slot is touched, so the panic path reaches this model
//!   as [`Panicked`] already in hand -- which is the thing to check, since
//!   the payload travels the same slot and sets the same `done`.
//! - The executor's accounting (`num_asyncs`, the tear-down), which happens
//!   after `deliver` returns and is `tests/loom_storage.rs`'s subject.
//! - Poisoning. The crate's `Delivery::lock` takes a poisoned guard rather
//!   than propagating it, for the reason its own docs give; loom treats a
//!   panic as a failed model instead of poisoning anything, so there is no
//!   poisoned lock here to model and the extraction `unwrap`s.
//!
//! The model earns its keep by **mutation**, `tests/loom_storage.rs`'s way:
//! [`Protocol`] carries, beside the protocol the crate ships, three ways to
//! get one step or one ordering wrong, and loom has to *find* each of them.
//! A model that only passes on the current code has shown nothing about
//! itself.
//!
//! - [`Protocol::PollRegistersAfterTheCheck`] splits `poll_join`'s single
//!   critical section in two -- read `done` under one lock, register the
//!   waker under another -- which is the hole the real method's comment says
//!   it closes, and a delivery landing in the gap wakes nobody.
//! - [`Protocol::OutcomeAfterTheWake`] sets `done` and notifies with the
//!   outcome still in the deliverer's hand, writing it in a second critical
//!   section. A waiter woken in between finds `done` true and the slot empty.
//! - [`Protocol::PollKeepsWhateverWakerIsThere`] turns `poll_join`'s
//!   `will_wake` check into `is_some`, so a future polled by a second task
//!   leaves the first task's waker in the slot and the second is never woken.
//!
//! Each mutation that is sound on *some* path also carries the control that
//! says so, so that a mutation failing for a reason other than the one it
//! names shows up as the control failing too.

#![cfg(loom)]

use std::sync::Arc as StdArc;
use std::task::{Poll, Wake, Waker};

use loom::sync::atomic::{AtomicBool, Ordering};
use loom::sync::{Arc, Condvar, Mutex, MutexGuard};
use loom::thread;

/// `RunError::Panicked`: the payload a caught panic is delivered as.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Panicked(&'static str);

/// What a body hands over: the crate's `Result<R, RunError>`.
type Outcome<R> = Result<R, Panicked>;

/// What a second taker of one outcome is told, verbatim from
/// `src/executor/async.rs`: the `expect` that carries it is what reports a
/// slot woken empty, which is [`Protocol::OutcomeAfterTheWake`]'s whole
/// detection.
const OUTCOME_TAKEN_TWICE: &str = "the outcome of an async task is handed over exactly once";

/// Which steps [`Delivery`] runs: the knob the mutations turn.
#[derive(Clone, Copy)]
enum Protocol {
    /// The protocol the crate ships.
    AsShipped,
    /// Mutation 1: `poll_join` reads `done` under one lock and registers the
    /// waker under another.
    PollRegistersAfterTheCheck,
    /// Mutation 2: `deliver` sets `done`, notifies, and only then writes the
    /// outcome.
    OutcomeAfterTheWake,
    /// Mutation 3: `poll_join` keeps whatever waker is already in the slot
    /// instead of asking `will_wake` whether it is this poller's.
    PollKeepsWhateverWakerIsThere,
}

/// The guarded half of a [`Delivery`]: `async.rs`'s `Slot<R>`.
struct Slot<R> {
    /// Whether the body has run. Separate from the outcome because the
    /// outcome is *taken* by whoever waits.
    done: bool,
    /// What the body returned, or the payload it panicked with.
    outcome: Option<Outcome<R>>,
    /// The waker a polling future left behind.
    waker: Option<Waker>,
}

/// `async.rs`'s `Delivery<R>`: where a submitted body leaves its outcome, and
/// how whoever is waiting for it is woken.
struct Delivery<R> {
    /// The outcome and whoever is to be woken by it.
    slot: Mutex<Slot<R>>,
    /// Signalled by [`Delivery::deliver`], under the lock.
    delivered: Condvar,
    /// Which of the four protocols this slot runs.
    protocol: Protocol,
}

impl<R> Delivery<R> {
    /// An empty slot, for a task that has not run.
    fn new(protocol: Protocol) -> Self {
        Self {
            slot: Mutex::new(Slot {
                done: false,
                outcome: None,
                waker: None,
            }),
            delivered: Condvar::new(),
            protocol,
        }
    }

    /// `Delivery::deliver`: put the outcome in the slot, signal the condvar
    /// under the lock, and wake the waker after it -- the waker moved out
    /// first, so waking it touches nothing a released waiter may already have
    /// dropped.
    fn deliver(&self, outcome: Outcome<R>) {
        let late = matches!(self.protocol, Protocol::OutcomeAfterTheWake);

        let mut slot = self.lock();
        slot.done = true;
        // The one thing the mutation moves: the shipped protocol writes the
        // outcome in the same critical section that sets `done` and notifies.
        let mut carried = Some(outcome);
        if !late {
            slot.outcome = carried.take();
        }
        let waker = slot.waker.take();
        self.delivered.notify_all();
        drop(slot);

        if let Some(outcome) = carried {
            self.lock().outcome = Some(outcome);
        }
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// `Delivery::is_done`.
    fn is_done(&self) -> bool {
        self.lock().done
    }

    /// `Delivery::join`: the blocking wait under `AsyncHandle::wait`.
    fn join(&self) -> Outcome<R> {
        let mut slot = self.lock();
        while !slot.done {
            slot = self.delivered.wait(slot).unwrap();
        }
        slot.outcome.take().expect(OUTCOME_TAKEN_TWICE)
    }

    /// `Delivery::poll_join`: take the outcome if the body has run, and
    /// otherwise leave `waker` to be woken when it does.
    ///
    /// Registering the waker and reading the flag happen under one lock,
    /// which is what stops a task finishing between the two and leaving
    /// nobody to wake -- and is what mutation 1 takes away.
    fn poll_join(&self, waker: &Waker) -> Poll<Outcome<R>> {
        if matches!(self.protocol, Protocol::PollRegistersAfterTheCheck) {
            {
                let mut slot = self.lock();
                if slot.done {
                    return Poll::Ready(slot.outcome.take().expect(OUTCOME_TAKEN_TWICE));
                }
            }
            self.lock().waker = Some(waker.clone());
            return Poll::Pending;
        }

        let mut slot = self.lock();
        if slot.done {
            return Poll::Ready(slot.outcome.take().expect(OUTCOME_TAKEN_TWICE));
        }

        // Replaced rather than kept, because a future may be polled by a
        // different task each time; mutation 3 keeps any waker at all, which
        // is the same thing as keeping the wrong one.
        let keep = match self.protocol {
            Protocol::PollKeepsWhateverWakerIsThere => slot.waker.is_some(),
            _ => slot
                .waker
                .as_ref()
                .is_some_and(|current| current.will_wake(waker)),
        };
        if !keep {
            slot.waker = Some(waker.clone());
        }
        Poll::Pending
    }

    /// The slot.
    fn lock(&self) -> MutexGuard<'_, Slot<R>> {
        self.slot.lock().unwrap()
    }
}

/// `AsyncHandle<R>`: one share of the slot, and the right to its outcome.
/// Dropping it detaches, which here is exactly dropping the share -- the real
/// handle has no other field either.
struct Handle<R> {
    /// The slot the body delivers into.
    delivery: Arc<Delivery<R>>,
}

impl<R> Handle<R> {
    /// `AsyncHandle::wait`.
    fn wait(self) -> Outcome<R> {
        self.delivery.join()
    }

    /// `AsyncHandle::is_done`.
    fn is_done(&self) -> bool {
        self.delivery.is_done()
    }

    /// `<AsyncHandle as Future>::poll`, minus the pinning: the real impl
    /// forwards to `poll_join` and does nothing else, and a `Pin` has no
    /// runtime representation to race on.
    fn poll(&self, waker: &Waker) -> Poll<Outcome<R>> {
        self.delivery.poll_join(waker)
    }
}

/// `AsyncHandle::pending`: an empty handle and the slot that fills it.
fn pending<R>(protocol: Protocol) -> (Handle<R>, Arc<Delivery<R>>) {
    let delivery = Arc::new(Delivery::new(protocol));
    (
        Handle {
            delivery: Arc::clone(&delivery),
        },
        delivery,
    )
}

/// The whole runtime a poll needs: a waker that records having been woken.
///
/// `AsyncHandle`'s own doc example builds this out of [`Wake`] and
/// `thread::park`; the unpark is a flag here because what the model asks is
/// whether the wake happened at all.
struct Poller {
    /// Set by [`Wake::wake`].
    woken: AtomicBool,
}

impl Poller {
    /// A poller nobody has woken yet.
    fn new() -> StdArc<Self> {
        StdArc::new(Self {
            woken: AtomicBool::new(false),
        })
    }

    /// This poller's waker. Cloning the share rather than building a second
    /// `Poller` is what makes `will_wake` say "the same task": two wakers
    /// from one share are interchangeable, two from two shares are not.
    fn waker(self: &StdArc<Self>) -> Waker {
        Waker::from(StdArc::clone(self))
    }

    /// Whether the delivery woke this poller.
    fn woken(&self) -> bool {
        self.woken.load(Ordering::Acquire)
    }
}

impl Wake for Poller {
    /// A real runtime's waker would schedule the task again; recording the
    /// wake is all a model of the slot needs.
    fn wake(self: StdArc<Self>) {
        self.woken.store(true, Ordering::Release);
    }
}

/// A body delivering `outcome` while the caller is in `AsyncHandle::wait`:
/// the wait-then-deliver class, and -- since loom schedules the delivery
/// anywhere, including before the waiter takes the lock -- the
/// deliver-then-wait one on top of it.
///
/// The assertions are the main thread's on purpose: the waiting side is where
/// a lost wakeup and an empty slot both show, and a panic there is reported
/// as this test's.
fn wait_races_delivery<R>(protocol: Protocol, outcome: Outcome<R>)
where
    R: Clone + PartialEq + core::fmt::Debug + 'static,
{
    let (handle, delivery) = pending(protocol);
    let expected = outcome.clone();
    let body = thread::spawn(move || delivery.deliver(outcome));

    assert_eq!(handle.wait(), expected, "the wait returned the wrong value");

    body.join().unwrap();
}

/// A body delivering while the caller polls the handle as a `Future` exactly
/// once, with a waker in hand.
///
/// The property is the one `poll_join`'s single critical section buys:
/// **`Pending` implies the waker is woken.** A poller that is told `Pending`
/// has no other way to learn the task finished, so a delivery that neither
/// hands over the outcome nor wakes anybody is a task that never completes.
fn poll_races_delivery(protocol: Protocol) {
    let (handle, delivery) = pending(protocol);
    let poller = Poller::new();
    let waker = poller.waker();

    let body = thread::spawn(move || delivery.deliver(Ok(42)));

    let polled = handle.poll(&waker);
    body.join().unwrap();

    match polled {
        Poll::Ready(outcome) => assert_eq!(outcome, Ok(42), "the poll returned the wrong value"),
        Poll::Pending => {
            assert!(
                poller.woken(),
                "the poller was left pending with nobody to wake it"
            );
            assert_eq!(
                handle.poll(&waker),
                Poll::Ready(Ok(42)),
                "the woken poller re-polled and the outcome was not there"
            );
        }
    }
}

/// Whose waker the second poll of the same handle carries.
#[derive(Clone, Copy)]
enum SecondPoll {
    /// A different task: `will_wake` is false and the slot's waker has to be
    /// replaced.
    AnotherTask,
    /// The same task polling again: `will_wake` is true and the clone is
    /// skipped.
    TheSameTask,
}

/// A handle polled twice before the delivery lands, which is the state
/// `poll_join`'s `will_wake` branch exists for: whichever waker the *last*
/// poll carried is the one that must be woken, because it is the only one
/// whose task will poll again.
fn two_polls_race_delivery(protocol: Protocol, second_poll: SecondPoll) {
    let (handle, delivery) = pending(protocol);
    let first = Poller::new();
    let second = match second_poll {
        SecondPoll::AnotherTask => Poller::new(),
        SecondPoll::TheSameTask => StdArc::clone(&first),
    };

    let body = thread::spawn(move || delivery.deliver(Ok(42)));

    // A future moved between tasks, or one task polling it twice -- and only
    // while it is still pending, a `Ready` being the end of it: an outcome is
    // handed over once, so polling again after it would be the caller's bug
    // rather than the slot's.
    if let Poll::Ready(outcome) = handle.poll(&first.waker()) {
        body.join().unwrap();
        assert_eq!(outcome, Ok(42), "the poll returned the wrong value");
        return;
    }
    let polled = handle.poll(&second.waker());
    body.join().unwrap();

    if let Poll::Ready(outcome) = polled {
        assert_eq!(outcome, Ok(42), "the poll returned the wrong value");
        return;
    }

    assert!(
        second.woken(),
        "the latest waker was not the one the delivery woke"
    );
    assert_eq!(
        handle.poll(&second.waker()),
        Poll::Ready(Ok(42)),
        "the woken poller re-polled and the outcome was not there"
    );
}

// --- The protocol the crate ships -----------------------------------------

/// Deliver, then wait: the body has already finished when the caller asks, so
/// the wait takes the outcome without ever reaching the condvar. The thread
/// join is what fixes the order, this being the one class loom cannot be
/// asked to produce.
#[test]
fn a_wait_after_the_delivery_takes_the_outcome() {
    loom::model(|| {
        let (handle, delivery) = pending(Protocol::AsShipped);
        let body = thread::spawn(move || delivery.deliver(Ok(42)));
        body.join().unwrap();

        assert!(
            handle.is_done(),
            "the delivery had returned and `done` was not set"
        );
        assert_eq!(handle.wait(), Ok(42), "the wait returned the wrong value");
    });
}

/// Wait, then deliver: the caller is asleep in the condvar, or about to be,
/// and the delivery has to reach it wherever it lands.
#[test]
fn a_wait_before_the_delivery_is_woken_by_it() {
    loom::model(|| wait_races_delivery(Protocol::AsShipped, Ok(42)));
}

/// A poll racing the delivery with a waker in the slot.
#[test]
fn a_poller_left_pending_is_always_woken() {
    loom::model(|| poll_races_delivery(Protocol::AsShipped));
}

/// The same handle polled by a second task: the waker that poll left is the
/// one the delivery must wake.
#[test]
fn the_latest_of_two_wakers_is_the_one_woken() {
    loom::model(|| two_polls_race_delivery(Protocol::AsShipped, SecondPoll::AnotherTask));
}

/// The same task polling twice, which is `will_wake`'s true branch: the clone
/// is skipped and the waker already in the slot is still the right one.
#[test]
fn a_repeated_poll_by_one_task_is_woken_through_the_waker_it_already_left() {
    loom::model(|| two_polls_race_delivery(Protocol::AsShipped, SecondPoll::TheSameTask));
}

/// A handle dropped unwaited, racing the delivery: detaching is a share going
/// away and nothing else, so the body still delivers, nothing blocks, nobody
/// is woken, and the outcome sits in the slot until the last share takes it
/// with it.
#[test]
fn a_detached_handle_leaves_the_delivery_to_finish_alone() {
    loom::model(|| {
        let (handle, delivery) = pending(Protocol::AsShipped);
        let mut slot = Arc::clone(&delivery);

        let detach = thread::spawn(move || drop(handle));
        let body = thread::spawn(move || delivery.deliver(Ok(42)));

        detach.join().unwrap();
        body.join().unwrap();

        assert!(
            slot.is_done(),
            "the delivery had returned and `done` was not set"
        );
        assert_eq!(
            slot.join(),
            Ok(42),
            "the detached task's outcome was not in the slot"
        );
        assert!(
            Arc::get_mut(&mut slot).is_some(),
            "a share of the slot outlived both the detach and the delivery"
        );
    });
}

/// A panicking body, whose payload travels the same slot as a value: the
/// `catch_unwind` is `for_body`'s and happens before any of this, so what the
/// model checks is that the `Err` arm sets `done` and reaches the waiter
/// unchanged.
#[test]
fn a_panicking_body_delivers_its_payload() {
    loom::model(|| {
        wait_races_delivery::<i32>(Protocol::AsShipped, Err(Panicked("not today")));
    });
}

// --- The protocols it does not, and loom has to find each ------------------

/// Mutation 1: `poll_join` reads `done` under one lock and registers the
/// waker under another. A delivery in the gap takes no waker and wakes
/// nobody, and the poller it answered `Pending` is never polled again.
#[test]
#[should_panic(expected = "the poller was left pending with nobody to wake it")]
fn mutation_a_poll_that_registers_its_waker_under_a_second_lock_is_found() {
    loom::model(|| poll_races_delivery(Protocol::PollRegistersAfterTheCheck));
}

/// The control for mutation 1: the same split is invisible to a blocking
/// wait, which never leaves a waker and is released by the notification sent
/// under the lock. Mutation 1 has to be failing for the waker's sake and not
/// for the slot's.
#[test]
fn mutation_1s_split_poll_is_sound_for_a_blocking_wait() {
    loom::model(|| {
        wait_races_delivery(Protocol::PollRegistersAfterTheCheck, Ok(42));
    });
}

/// Mutation 2: `deliver` sets `done` and notifies with the outcome still in
/// its hand. The waiter it releases can reacquire the lock before the second
/// write lands, and finds a finished task with an empty slot -- which the
/// real method's one `expect` is what reports.
#[test]
#[should_panic(expected = "handed over exactly once")]
fn mutation_an_outcome_written_after_the_wake_is_found() {
    loom::model(|| wait_races_delivery(Protocol::OutcomeAfterTheWake, Ok(42)));
}

/// Mutation 3: `poll_join` keeps whatever waker the slot holds rather than
/// asking whether it is this poller's. The first task's waker stays, the
/// second task's is dropped, and the delivery wakes a task that will never
/// poll again.
#[test]
#[should_panic(expected = "the latest waker was not the one the delivery woke")]
fn mutation_a_poll_that_keeps_a_stale_waker_is_found() {
    loom::model(|| {
        two_polls_race_delivery(
            Protocol::PollKeepsWhateverWakerIsThere,
            SecondPoll::AnotherTask,
        );
    });
}

/// The control for mutation 3: keeping the waker is right when it is the same
/// task's, which is why the mutation is a `will_wake` bug rather than a
/// registration bug.
#[test]
fn mutation_3s_kept_waker_is_sound_for_a_repeated_poll_by_one_task() {
    loom::model(|| {
        two_polls_race_delivery(
            Protocol::PollKeepsWhateverWakerIsThere,
            SecondPoll::TheSameTask,
        );
    });
}
