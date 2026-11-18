//! A [`loom`](https://docs.rs/loom) model of the rule `TaskGroup`'s counter
//! keeps: **a `corun` returns only once every child of the group has reached
//! its tear-down, carrying whatever one of them failed with** -- and nothing
//! else a child does is a signal that it has. Issue #136 was a race in
//! exactly this counter, found on an unrelated PR's CI.
//!
//! **This file is not part of the gate**, for `tests/loom_notifier.rs`'s
//! reason: loom needs a `cfg(loom)` build of the whole graph, which only
//! `RUSTFLAGS` sets, and the cases below are empty without it. The command is
//! every model's, and all of them build in one go:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=/target/rustflow-loom \
//!     cargo test --release --test loom_task_group
//! ```
//!
//! `.github/workflows/loom.yml` runs it weekly and on `workflow_dispatch`,
//! alongside every other `tests/loom_*.rs`, which is the only thing watching
//! this model outside a deliberate local run.
//!
//! # What is modelled, and what is not
//!
//! The model is **standalone**, for the trade `tests/loom_storage.rs`'s
//! header argues: it restates the protocol in a few dozen lines of loom types
//! rather than driving the real `TaskGroup`, which would mean a `cfg(loom)`
//! alias for every atomic, `Arc` and `Mutex` the executor reaches -- most of
//! the crate -- and loom's search is exponential in the atomics it sees.
//! `src/task_group.rs` therefore needs no seam for this file.
//!
//! The pieces, and what each stands for in the crate:
//!
//! - [`Anchor`] is the group's `Arc<Nested>`: the join counter every child
//!   counts down, the state word the two anchor bits and the `CAUGHT` arbiter
//!   share, and the one payload slot a `corun` takes from. Three operations
//!   are the whole protocol -- [`Anchor::count_queued`] before a child is
//!   pushed, [`Anchor::finished`] as the last thing a child does, and the
//!   [`Anchor::outstanding`] load the frame polls.
//! - [`Child`] is which of the eight spawning forms put the child there,
//!   which decides where a panic out of it goes: a `spawn` keeps its own
//!   failure in its handle, a `silent_async` has no handle, so its failure is
//!   the group's.
//! - The **frame** is this thread: the worker that opened the group, spawned
//!   its children and is now in `TaskGroup::corun`. A child another worker
//!   stole is a thread of its own; a child the frame's own `corun` pops is a
//!   call inside the wait loop, which is what [`Group::coruned`] is and what
//!   keeps a two-child case down to two threads.
//!
//! What the model cannot say: anything about the queues a child travels
//! through, about more than two children, or about a chain deeper than the
//! one link a group's children have. It takes "a child can run in its
//! entirety between any two instructions of the frame" as given, which is
//! what a stealable queue means. `TaskGroup::cancel` is not modelled
//! either: its bit is the same `fetch_or` on the same word as the `EXCEPTION`
//! bit a failing child sets, which [`Anchor::failed`] already reads, so a
//! case for it would turn the same knob twice.
//!
//! # Design
//!
//! A `corun` does not park. `corun_until` is a loop that runs other people's
//! work and re-reads the predicate, so what the model spells as
//! `while anchor.outstanding() != 0 { thread::yield_now() }` is the whole of
//! the wait, and the "wake" in the rule above is nothing more than that load
//! returning zero. That is why the counter is the only thing a frame may
//! believe: there is no notification to miss, and equally no notification to
//! be woken early by -- except the ones a *caller* can reach, which is what
//! mutation 2 is about.
//!
//! The model earns its keep by **mutation**, as every model here does:
//! alongside the orderings the crate ships it carries five it does not, and
//! asserts that loom finds each of them.
//!
//! - **Mutation 1**, the one issue #266 names: a child that counts the anchor
//!   down *before* it publishes its payload. The frame's predicate then holds
//!   while the payload is still in flight, and the `corun` returns reporting
//!   nothing. This is the group's instance of the ordering
//!   `tests/loom_anchor.rs`'s third mutation finds on a runtime node, and it
//!   is carried here as well because a group's publish is a different pair of
//!   call sites -- `silent_async`'s child reaching the anchor that
//!   `corun_payload` takes from, with no node to be resumed in between.
//! - **Mutation 2**, which is #136: a frame woken by something that is not
//!   the counter. An `AsyncHandle`'s outcome is delivered *inside* the body,
//!   so a frame that waits on a child's handle has been told the body is over
//!   while the tear-down that counts the group down has not run. #136 was a
//!   test asserting a counter balanced one line after such a wait; the
//!   mutation is that assertion, and the control beside it is the same wait
//!   followed by the `corun` that closes the window.
//!   `tests/loom_async_handle.rs` models the slot that delivery lands in,
//!   which is the half of this the handle *does* promise; the half it cannot
//!   promise is the group's counter, and that is this mutation.
//! - **Mutation 3**: a child counted *after* it is queued rather than before,
//!   which is the increment half of the counter and the one upstream's
//!   `_schedule_graph` comments on. The frame can then poll a counter that
//!   has not yet heard of a child, see zero and return before it ran.
//! - **Mutation 4**: an anchor that is anchored only while a `corun` is
//!   waiting, which is the `IMPLICITLY_ANCHORED` line `TaskGroup::new` sets
//!   deleted. A child that fails before the frame has reached its `corun`
//!   then walks past the group entirely, and the failure is reported to
//!   nobody. Its control is the same knob with the child held back until the
//!   `corun` is already waiting, where the window cannot open.
//! - **Mutation 5**: the counter's `AcqRel` and `Acquire` weakened to
//!   `Relaxed`, which leaves the arithmetic right and the publication wrong.
//!   It is the one mutation here that changes no line of control flow, and it
//!   is what makes the model a model of an *ordering*: the frame still waits
//!   for the counter to reach zero and still sees a child's own work undone.
//!
//! Mutations 2 and 4 each have a control beside them where the same knob is
//! sound, so a mutation failing for some other reason would show up as its
//! control failing too.

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

/// What the failing child panics with, which every case follows by identity.
const PAYLOAD: &str = "a silent child of the group panicked";

/// `ESTATE::CAUGHT`: the payload slot is claimed, and the atomic rather than
/// the lock is the arbiter, as it is in the crate.
const CAUGHT: usize = 1;
/// `ESTATE::EXCEPTION`: something under this anchor has failed, which is what
/// stops the children that have not started.
const EXCEPTION: usize = 2;
/// `ESTATE::EXPLICITLY_ANCHORED`: a frame is waiting on this anchor right
/// now, held by `corun_payload`'s guard for the length of its wait.
const EXPLICITLY_ANCHORED: usize = 4;
/// `NSTATE::IMPLICITLY_ANCHORED`, which a group carries for its whole life
/// rather than for one nested run. In the same word as the rest here, where
/// the crate has it in `nstate`: nothing in this model reads the two words
/// apart.
const IMPLICITLY_ANCHORED: usize = 8;

/// The group's anchor: `Nested`'s `NodeBase` as a group uses it.
struct Anchor {
    /// `_join_counter`: one per child of the group that is still outstanding.
    outstanding: AtomicUsize,
    /// The orderings [`Anchor::finished`] and [`Anchor::outstanding`] use:
    /// mutation 5's knob, and a field of the anchor because it is a property
    /// of the counter rather than of one call.
    barrier: Barrier,
    /// The state bits above, which the panic walk reads and writes.
    estate: AtomicUsize,
    /// `_exception_ptr`: what the first silent child to fail panicked with,
    /// until a `corun` takes it.
    payload: Mutex<Option<&'static str>>,
}

impl Anchor {
    /// `TaskGroup::new`: an anchor with nothing outstanding, implicitly
    /// anchored for the group's whole life unless the knob says otherwise.
    fn new(anchored: Anchored, barrier: Barrier) -> Arc<Self> {
        let estate = match anchored {
            Anchored::ForTheGroupsWholeLife => IMPLICITLY_ANCHORED,
            Anchored::OnlyWhileACorunIsWaiting => 0,
        };

        Arc::new(Self {
            outstanding: AtomicUsize::new(0),
            barrier,
            estate: AtomicUsize::new(estate),
            payload: Mutex::new(None),
        })
    }

    /// `Parent::count_queued`, one child's worth: upstream's
    /// `parent->_join_counter.fetch_add`. `Relaxed` because the push that
    /// follows is what publishes the child.
    fn count_queued(&self) {
        self.outstanding.fetch_add(1, Ordering::Relaxed);
    }

    /// The decrement `WorkerThread::finish_job` makes for a job whose parent
    /// is a group's anchor: the release half of the pair
    /// [`Anchor::outstanding`] acquires on, and the last thing a child ever
    /// does.
    fn finished(&self) {
        match self.barrier {
            Barrier::TheCountDown => self.outstanding.fetch_sub(1, Ordering::AcqRel),
            Barrier::Nothing => self.outstanding.fetch_sub(1, Ordering::Relaxed),
        };
    }

    /// `Nested::outstanding`, which is both `TaskGroup::size` and the
    /// predicate `corun_payload` polls.
    fn outstanding(&self) -> usize {
        match self.barrier {
            Barrier::TheCountDown => self.outstanding.load(Ordering::Acquire),
            Barrier::Nothing => self.outstanding.load(Ordering::Relaxed),
        }
    }

    /// `NodeBase::anchor_explicitly`. The crate's is an `ExplicitAnchor`
    /// guard so that an unwind releases the bit; nothing unwinds here, so the
    /// model has the two lines the guard is made of.
    fn anchor_explicitly(&self) {
        self.estate.fetch_or(EXPLICITLY_ANCHORED, Ordering::Relaxed);
    }

    /// `Drop for ExplicitAnchor`, which `corun_payload` reaches before it
    /// takes the payload.
    fn release_explicitly(&self) {
        self.estate
            .fetch_and(!EXPLICITLY_ANCHORED, Ordering::Relaxed);
    }

    /// `Job::record_panic` over the one link a group's chain has: the failing
    /// child's own base, which is neither anchored nor claimed, and the
    /// group's anchor above it.
    ///
    /// An anchor the walk finds unanchored is walked past, and for a group's
    /// child the next destination is the failing node itself, which no group
    /// ever reads: the model spells that as doing nothing, which is the whole
    /// content of mutation 4.
    fn record(&self, payload: &'static str) {
        if self.estate.load(Ordering::Relaxed) & (EXPLICITLY_ANCHORED | IMPLICITLY_ANCHORED) == 0 {
            return;
        }

        // `claim_panic` then `keep_panic`: the first failure under this
        // anchor is the one it reports, and `EXCEPTION` rides along because
        // an anchor with a failure to report is one whose remaining children
        // are to be skipped.
        if self.estate.fetch_or(EXCEPTION | CAUGHT, Ordering::Relaxed) & CAUGHT == 0 {
            *self.payload.lock().unwrap() = Some(payload);
        }
    }

    /// `NodeBase::take_panic`, which un-claims the slot when there was
    /// something in it.
    fn take(&self) -> Option<&'static str> {
        let payload = self.payload.lock().unwrap().take();
        if payload.is_some() {
            self.estate
                .fetch_and(!(EXCEPTION | CAUGHT), Ordering::Relaxed);
        }
        payload
    }

    /// Whether this group has already failed, which is the `EXCEPTION` half
    /// of what `Job::is_parent_cancelled` reads on a child's parent: such a
    /// child does not run its body, and the count-down is all it still owes.
    fn failed(&self) -> bool {
        self.estate.load(Ordering::Relaxed) & EXCEPTION != 0
    }
}

/// What the model watches besides the anchor: the effects a `corun` is
/// supposed to be a barrier for.
struct State {
    /// One per child whose job has reached its tear-down, run or skipped,
    /// bumped before the count-down. `Relaxed` for the crate's reason: the
    /// counter's release/acquire pair is what publishes a count a task body
    /// keeps.
    accounted: AtomicUsize,
    /// One per child that ran its body, a child skipped by
    /// [`Anchor::failed`] not being one.
    ran: AtomicUsize,
    /// `AsyncHandle`'s slot for the one `spawn`ed child a case may have:
    /// filled *inside* the body, which is the window #136 named.
    delivered: AtomicBool,
    /// Whether the children may run at all yet, which is how
    /// [`Entry::AfterTheCorunBegins`] orders a child against the frame.
    released: AtomicBool,
}

impl State {
    fn new() -> Self {
        Self {
            accounted: AtomicUsize::new(0),
            ran: AtomicUsize::new(0),
            delivered: AtomicBool::new(false),
            released: AtomicBool::new(false),
        }
    }
}

/// Which spawning form put a child under the group, which is what decides
/// where a failure out of it goes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Child {
    /// `TaskGroup::spawn`: a handle of its own, into which the wrapper
    /// `AsyncHandle::for_body` builds delivers the outcome -- inside the
    /// body, and so before the count-down.
    Handled,
    /// `TaskGroup::silent_async`: no handle, and nothing to deliver.
    Silent,
    /// A `TaskGroup::silent_async` whose body panics, which is the one form
    /// whose failure is the group's to report.
    SilentAndFailing,
}

/// Whether the group's anchor carries `IMPLICITLY_ANCHORED` for its whole
/// life: the knob mutation 4 turns.
#[derive(Clone, Copy)]
enum Anchored {
    /// `TaskGroup::new`'s `fetch_or`, which is what the crate ships.
    ForTheGroupsWholeLife,
    /// That line deleted, leaving the explicit bit a `corun` holds as the
    /// only anchor there is.
    OnlyWhileACorunIsWaiting,
}

/// What publishes everything a child did before it counted the anchor down:
/// the knob mutation 5 turns.
#[derive(Clone, Copy)]
enum Barrier {
    /// The crate's: the count-down is the `AcqRel` release and the poll is
    /// the `Acquire` that pairs with it, so a frame that sees zero sees
    /// everything every child of the group did.
    TheCountDown,
    /// Both `Relaxed`. The arithmetic is unchanged -- the counter still
    /// reaches zero exactly once -- which is the point: what the orderings
    /// buy is not the count.
    Nothing,
}

/// When a child is counted into the anchor: the knob mutation 3 turns.
#[derive(Clone, Copy)]
enum Counted {
    /// `Parent::count_queued` before the push, which is the crate's and
    /// upstream's.
    BeforeTheChildIsQueued,
    /// The child counting itself in as it starts, which is the same arithmetic
    /// one instruction too late.
    ByTheChildItself,
}

/// Whether a failing child publishes its payload before or after it counts
/// the anchor down: the knob mutation 1 turns.
#[derive(Clone, Copy)]
enum Publish {
    /// The crate's, and upstream's: `record_panic` runs inside `invoke`, and
    /// the tear-down that decrements comes after it.
    BeforeTheCountDown,
    /// The count-down first, which is the shape of the races
    /// `tests/loom_storage.rs` models.
    AfterTheCountDown,
}

/// What orders a child against the frame before the frame starts waiting.
#[derive(Clone, Copy)]
enum Entry {
    /// Nothing: the whole interleaving space, which is the shape a group
    /// whose children are in the shared buffers actually has.
    Unordered,
    /// The frame waits on the `spawn`ed child's handle first and coruns
    /// afterwards, so the child's body is over before the wait begins. A case
    /// using this must have a *stolen* [`Child::Handled`] that cannot be
    /// skipped: the model has no delivery for a child that never ran, and a
    /// frame waiting on a handle is a frame not yet running anything itself.
    AfterTheChildsHandle,
    /// The child is not picked up until the frame is already inside its
    /// `corun`, which is the group's own purpose: the opening worker runs the
    /// children it spawned.
    AfterTheCorunBegins,
}

/// What the frame waits on, which is the whole question.
#[derive(Clone, Copy)]
enum Wait {
    /// `TaskGroup::corun`, the one thing that waits for a group.
    Corun,
    /// A child's `AsyncHandle::wait` and nothing else: the knob mutation 2
    /// turns.
    TheChildsHandle,
}

/// One group, its children and the frame that waits for them, under the five
/// knobs above.
#[derive(Clone, Copy)]
struct Group {
    /// The children some other worker steals, in the order the frame spawns
    /// them: one thread each. At most one [`Child::Handled`] across this and
    /// `coruned`, the model having one delivery slot.
    stolen: &'static [Child],
    /// The child the frame's own `corun` pops and runs, which costs no
    /// thread. Modelled because it is a group's usual case rather than an
    /// exotic one: `corun_until` is a work-stealing loop, so the worker that
    /// opened the group is one of the workers that runs its children.
    coruned: Option<Child>,
    /// Mutation 4's knob.
    anchored: Anchored,
    /// Mutation 5's knob.
    barrier: Barrier,
    /// Mutation 3's knob.
    counted: Counted,
    /// Mutation 1's knob.
    publish: Publish,
    /// What orders a child against the frame, which is how the two controls
    /// close the windows their mutations open.
    entry: Entry,
    /// Mutation 2's knob.
    wait: Wait,
}

/// Every ordering the crate ships, no knob turned: one silent child, counted
/// before it is queued, publishing before it counts down, under an anchor
/// that is implicitly anchored for the group's whole life, waited for by a
/// `corun`.
const SHIPPED: Group = Group {
    stolen: &[Child::Silent],
    coruned: None,
    anchored: Anchored::ForTheGroupsWholeLife,
    barrier: Barrier::TheCountDown,
    counted: Counted::BeforeTheChildIsQueued,
    publish: Publish::BeforeTheCountDown,
    entry: Entry::Unordered,
    wait: Wait::Corun,
};

impl Group {
    /// Every child of this group, stolen or coruned.
    fn children(self) -> impl Iterator<Item = Child> {
        self.stolen.iter().copied().chain(self.coruned)
    }

    /// The payload a `corun` on this group has to come back with: `Some` for
    /// exactly the cases that have a failing silent child.
    fn failure(self) -> Option<&'static str> {
        self.children()
            .any(|kind| kind == Child::SilentAndFailing)
            .then_some(PAYLOAD)
    }
}

/// One child's job, from the queue to the count-down: upstream's `_invoke`
/// and `_tear_down_nonasync` for a node whose parent is a group's anchor.
fn run_child(anchor: &Anchor, state: &State, group: Group, kind: Child) {
    if let Counted::ByTheChildItself = group.counted {
        anchor.count_queued();
    }

    if let Entry::AfterTheCorunBegins = group.entry {
        while !state.released.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

    // `Job::is_parent_cancelled`, read once: a child of a group that has
    // already failed is skipped.
    let skipped = anchor.failed();
    if !skipped {
        state.ran.fetch_add(1, Ordering::Relaxed);
        if kind == Child::Handled {
            // The handle's outcome, delivered by the wrapper the submission
            // was built with and therefore inside the body.
            state.delivered.store(true, Ordering::Release);
        }
    }

    // The tear-down's own bookkeeping, before the two orderings that are the
    // subject here.
    state.accounted.fetch_add(1, Ordering::Relaxed);

    match (kind, skipped, group.publish) {
        (Child::SilentAndFailing, false, Publish::BeforeTheCountDown) => {
            anchor.record(PAYLOAD);
            anchor.finished();
        }
        (Child::SilentAndFailing, false, Publish::AfterTheCountDown) => {
            anchor.finished();
            anchor.record(PAYLOAD);
        }
        _ => anchor.finished(),
    }
}

/// What the frame saw at the instant its wait returned.
///
/// The readings are taken **before** the children are joined, which is the
/// point of the file -- the counter is the only thing that orders the frame
/// against them -- and asserted after it, which is what keeps a found
/// mutation reportable: a loom model that panics with a thread still queued
/// unwinds through loom's own cleanup and aborts in a destructor instead of
/// failing the test.
struct Observed {
    /// What `TaskGroup::size` would have answered.
    outstanding: usize,
    /// [`State::accounted`].
    accounted: usize,
    /// [`State::ran`].
    ran: usize,
    /// What the `corun` came back with, and `None` for a wait that is not a
    /// `corun` and has no slot to read.
    payload: Option<&'static str>,
}

/// One group opened, spawned into and waited for.
fn one_group(group: Group) {
    let anchor = Anchor::new(group.anchored, group.barrier);
    let state = Arc::new(State::new());

    // Every child is counted as it is submitted, stolen or not: the frame's
    // own is just a job nobody has popped yet.
    let mut coruned = group.coruned;
    if let Counted::BeforeTheChildIsQueued = group.counted {
        for _ in group.children() {
            anchor.count_queued();
        }
    }

    let stolen: Vec<_> = group
        .stolen
        .iter()
        .map(|&kind| {
            let (anchor, state) = (Arc::clone(&anchor), Arc::clone(&state));
            thread::spawn(move || run_child(&anchor, &state, group, kind))
        })
        .collect();

    if let Entry::AfterTheChildsHandle = group.entry {
        // `six.wait()` before the corun: what the handle tells the frame is
        // that the body is over, which says nothing about the group.
        while !state.delivered.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

    let observed = match group.wait {
        Wait::Corun => {
            // `corun_payload`: the explicit-anchor guard, the wait on the
            // counter, the guard released, then the payload -- in that order.
            anchor.anchor_explicitly();
            if let Entry::AfterTheCorunBegins = group.entry {
                state.released.store(true, Ordering::Release);
            }
            // `corun_until`: the predicate first, then a job to run, and a
            // yield when there is nothing to pop.
            while anchor.outstanding() != 0 {
                match coruned.take() {
                    Some(kind) => run_child(&anchor, &state, group, kind),
                    None => thread::yield_now(),
                }
            }
            anchor.release_explicitly();
            let payload = anchor.take();

            Observed {
                outstanding: anchor.outstanding(),
                accounted: state.accounted.load(Ordering::Relaxed),
                ran: state.ran.load(Ordering::Relaxed),
                payload,
            }
        }
        Wait::TheChildsHandle => {
            while !state.delivered.load(Ordering::Acquire) {
                thread::yield_now();
            }

            Observed {
                outstanding: anchor.outstanding(),
                accounted: state.accounted.load(Ordering::Relaxed),
                ran: state.ran.load(Ordering::Relaxed),
                payload: None,
            }
        }
    };

    for child in stolen {
        child.join().unwrap();
    }

    match group.wait {
        Wait::Corun => {
            assert_eq!(
                observed.accounted,
                group.children().count(),
                "the corun returned before every child of the group had finished its job"
            );
            assert_eq!(
                observed.outstanding, 0,
                "the group was still counting a child after its corun returned"
            );
            assert_eq!(
                observed.payload,
                group.failure(),
                "the corun did not come back with the failure of the group's children"
            );
            if group.failure().is_none() {
                // Nothing failed, so nothing was skipped: the doc example's
                // count after its `corun`.
                assert_eq!(
                    observed.ran,
                    group.children().count(),
                    "a child of the group never ran, and nothing had failed to skip it"
                );
            }
        }
        // The one question mutation 2 asks, which is the one #136's test
        // asked one line too early.
        Wait::TheChildsHandle => assert_eq!(
            observed.outstanding, 0,
            "the group was still counting a child whose body the frame had been told was over"
        ),
    }
}

/// One group dropped with its child still outstanding, which is the shape
/// `TaskGroup` documents as **not** a join (#93): the child keeps running,
/// its count lands on an anchor that is still alive, and the last share of
/// that anchor goes with the last child.
///
/// Every knob is the shipped one here -- there is no frame left to mutate the
/// ordering against -- and what this case can show is bounded by that: the
/// dangling `NodeBase` upstream's own documentation warns about is ruled out
/// by the `Arc` rather than by any interleaving, so what loom checks is that
/// the child's tear-down is sound with the drop in every position, that the
/// counter still reaches zero, and -- its own check, not an assertion here --
/// that nothing is left allocated at the end of each execution.
fn one_dropped_group(kind: Child) {
    let anchor = Anchor::new(Anchored::ForTheGroupsWholeLife, Barrier::TheCountDown);
    let state = Arc::new(State::new());
    anchor.count_queued();

    let child = {
        // `SHIPPED` for its knobs only: the child is handed over here rather
        // than read off its `stolen`.
        let (anchor, state) = (Arc::clone(&anchor), Arc::clone(&state));
        thread::spawn(move || run_child(&anchor, &state, SHIPPED, kind))
    };

    // The model's own share, which stands in for nothing in the crate: it is
    // what lets the assertions below read an anchor the group has let go of,
    // and the uniqueness check at the end is what shows it was the last share
    // there was.
    let mut probe = Arc::clone(&anchor);
    // `drop(group)` with a child outstanding, racing everything that child
    // does.
    drop(anchor);

    child.join().unwrap();

    assert_eq!(
        state.accounted.load(Ordering::Relaxed),
        1,
        "the child of a dropped group did not finish its job"
    );
    assert_eq!(
        probe.outstanding(),
        0,
        "the child of a dropped group did not count itself down"
    );
    // A failure under a group nobody is waiting for is reported to nobody: it
    // is still in the slot the `corun` that never came would have read.
    assert_eq!(
        probe.take(),
        (kind == Child::SilentAndFailing).then_some(PAYLOAD),
        "a failure under a dropped group did not reach the slot it had to go nowhere from"
    );
    assert!(
        Arc::get_mut(&mut probe).is_some(),
        "a share of the anchor outlived both the group and its last child"
    );
}

// --- The orderings the crate ships ----------------------------------------

/// The base case: a `corun` waits for the one child it was given, whichever
/// way the two interleave.
#[test]
fn a_corun_waits_for_the_child_it_was_given() {
    loom::model(|| one_group(SHIPPED));
}

/// A child that is over before the `corun` begins -- the frame having waited
/// on its handle first -- is still waited for, because the handle is not the
/// counter. The control for mutation 2.
#[test]
fn a_child_that_finishes_before_the_corun_begins_is_still_waited_for() {
    loom::model(|| {
        one_group(Group {
            stolen: &[Child::Handled],
            entry: Entry::AfterTheChildsHandle,
            ..SHIPPED
        });
    });
}

/// A `corun` already waiting is ended by the child it is waiting for, which
/// is the other order of the same pair: nothing parks, so the wait ends when
/// the count-down becomes visible to the poll.
#[test]
fn a_corun_already_waiting_is_ended_by_the_last_child() {
    loom::model(|| {
        one_group(Group {
            entry: Entry::AfterTheCorunBegins,
            ..SHIPPED
        });
    });
}

/// Two children finishing concurrently balance the counter: the `corun`
/// returns once *both* have, not once the first has.
#[test]
fn two_children_finishing_concurrently_balance_the_counter() {
    loom::model(|| {
        one_group(Group {
            stolen: &[Child::Handled],
            coruned: Some(Child::Silent),
            ..SHIPPED
        });
    });
}

/// A panicking silent child is reported to the `corun` that waited for it,
/// whichever side of the frame's guard the failure lands on -- the group's
/// anchor carrying `IMPLICITLY_ANCHORED` from the moment it is opened is what
/// makes that true.
#[test]
fn a_panicking_child_is_reported_to_the_corun_that_waited() {
    loom::model(|| {
        one_group(Group {
            stolen: &[Child::SilentAndFailing],
            ..SHIPPED
        });
    });
}

/// A panicking child stops a sibling that had not started, and the counter
/// balances all the same: a skipped child still owes the count-down, which is
/// what keeps the `corun` from waiting for ever.
#[test]
fn a_panicking_child_skips_its_sibling_without_unbalancing_the_counter() {
    loom::model(|| {
        one_group(Group {
            stolen: &[Child::SilentAndFailing],
            coruned: Some(Child::Silent),
            ..SHIPPED
        });
    });
}

/// Dropping a group with a child outstanding is not a join: the child runs
/// on, counts down an anchor that is still there, and takes the last share of
/// it with it.
#[test]
fn a_group_dropped_with_a_child_outstanding_is_not_a_join() {
    loom::model(|| one_dropped_group(Child::Silent));
}

/// A failure under a dropped group is reported to nobody, which is the half
/// of the entry above that is a promise about failures rather than about
/// memory.
#[test]
fn a_failure_under_a_dropped_group_is_reported_to_nobody() {
    loom::model(|| one_dropped_group(Child::SilentAndFailing));
}

// --- The orderings it does not, and loom has to find each ------------------

/// Mutation 1: the failing child counts the anchor down before it publishes.
/// The frame's predicate holds while the payload is still in flight, so the
/// `corun` returns reporting nothing.
#[test]
#[should_panic(expected = "the corun did not come back with the failure of the group's children")]
fn mutation_a_child_that_counts_down_before_publishing_is_found() {
    loom::model(|| {
        one_group(Group {
            stolen: &[Child::SilentAndFailing],
            publish: Publish::AfterTheCountDown,
            ..SHIPPED
        });
    });
}

/// Mutation 2, which is #136: the frame waits on its child's handle and
/// believes the group is balanced. The outcome is delivered inside the body,
/// so the tear-down that counts the group down has not necessarily run.
#[test]
#[should_panic(
    expected = "the group was still counting a child whose body the frame had been told was over"
)]
fn mutation_a_frame_woken_by_its_childs_handle_is_found() {
    loom::model(|| {
        one_group(Group {
            stolen: &[Child::Handled],
            wait: Wait::TheChildsHandle,
            ..SHIPPED
        });
    });
}

/// Mutation 3: the child counts itself in rather than being counted before it
/// is queued. The frame can poll a counter that has not heard of it, see zero
/// and return before it ran.
#[test]
#[should_panic(
    expected = "the corun returned before every child of the group had finished its job"
)]
fn mutation_a_child_counted_after_it_is_queued_is_found() {
    loom::model(|| {
        one_group(Group {
            counted: Counted::ByTheChildItself,
            ..SHIPPED
        });
    });
}

/// Mutation 4: `TaskGroup::new`'s `IMPLICITLY_ANCHORED` deleted, leaving the
/// bit a `corun` holds as the only anchor. A child that fails before the
/// frame reaches its `corun` walks past the group, and the failure reaches
/// nobody.
#[test]
#[should_panic(expected = "the corun did not come back with the failure of the group's children")]
fn mutation_an_anchor_that_is_only_anchored_while_coruning_is_found() {
    loom::model(|| {
        one_group(Group {
            stolen: &[Child::SilentAndFailing],
            anchored: Anchored::OnlyWhileACorunIsWaiting,
            ..SHIPPED
        });
    });
}

/// The control for mutation 4: that anchor is sound for a child that cannot
/// run before the `corun` is already waiting, which is why the window is a
/// window rather than a certainty. The model has to pass here, or mutation 4
/// is failing for a reason other than the one it names.
#[test]
fn mutation_4s_window_closes_when_the_child_runs_only_inside_the_corun() {
    loom::model(|| {
        one_group(Group {
            stolen: &[Child::SilentAndFailing],
            anchored: Anchored::OnlyWhileACorunIsWaiting,
            entry: Entry::AfterTheCorunBegins,
            ..SHIPPED
        });
    });
}

/// Mutation 5: the counter's `AcqRel` and `Acquire` weakened to `Relaxed`,
/// which leaves the arithmetic right and the publication wrong. The frame
/// sees the counter at zero and the child's own work not yet done.
#[test]
#[should_panic(
    expected = "the corun returned before every child of the group had finished its job"
)]
fn mutation_a_counter_that_publishes_nothing_is_found() {
    loom::model(|| {
        one_group(Group {
            barrier: Barrier::Nothing,
            ..SHIPPED
        });
    });
}
