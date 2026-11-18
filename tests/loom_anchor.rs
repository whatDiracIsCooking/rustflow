//! A [`loom`](https://docs.rs/loom) model of the rule `Job::record_panic` and
//! `enter` in `executor::runtime` keep between them: **a failure under a
//! runtime task is delivered to the frame that will read it, whichever side
//! of the spawn finishes first.** A child that fails before its parent's
//! `corun` is entered has to reach the same slot as one that fails during it,
//! and a payload no `corun` ever came for has to reach the run.
//!
//! **This file is not part of the gate**, for `tests/loom_notifier.rs`'s
//! reason: loom needs a `cfg(loom)` build of the whole graph, which only
//! `RUSTFLAGS` sets, and the cases below are empty without it. The command is
//! every model's, and all of them build in one go:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=/target/rustflow-loom \
//!     cargo test --release --test loom_anchor
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
//! rather than driving the real `Job`, `Node` and `Topology`, which would
//! mean a `cfg(loom)` alias for every atomic, `Arc` and `Mutex` the executor
//! reaches -- most of the crate -- and loom's search is exponential in the
//! atomics it sees.
//!
//! What this one has to model is a *selection* as well as an ordering, and
//! that is why it is a second file rather than a case in the first. The
//! question is not only whether a payload is visible when a counter is
//! observed; it is which of three slots the payload was put in, decided by
//! two state bits that another thread is setting while the walk reads them.
//! So the pieces are:
//!
//! - [`Slot`] is `NodeBase`'s payload slot with its `CAUGHT` arbiter in
//!   front: claim, keep, take, and the one-payload rule between them.
//! - [`Node`] is the runtime task's node -- the `ENTERED` bit `enter` sets
//!   before the body runs, the `EXPLICITLY_ANCHORED` bit a `corun` holds for
//!   the length of its wait, the join counter one reference per outstanding
//!   child, and the slot the walk may write.
//! - [`Run`] is the topology's completion slot, the destination a chain's
//!   root is.
//! - [`record_panic`] is the walk over a one-link chain: a failing child's
//!   base, the runtime node above it, the run above that. [`Priority`] is the
//!   knob that says which of the node and the run outranks the other.
//! - The **body** thread is the worker running the runtime task: it enters the
//!   node, spawns one child, perhaps coruns, and performs `enter`'s tail. The
//!   **thief** thread is the worker that stole that child: it fails, walks,
//!   and counts the node down, in whichever order [`Publish`] says.
//!
//! What the model cannot say: anything about the queues the child travels
//! through, about more than one child, or about chains deeper than one link.
//! It takes "the thief can run the whole child between any two instructions
//! of the body" as given, which is what a stealable queue means.
//!
//! The model earns its keep by **mutation**, as the storage model does:
//! alongside the pair the crate ships it carries the three orderings it does
//! not, and asserts that loom *finds* each of them.
//!
//! - **Mutation 1**, the anchor window this file was written for, and
//!   upstream's own arrangement: the run outranks an implicit anchor, which
//!   it does by being explicitly anchored for the length of the run. A child
//!   that fails between the spawn and the `corun`'s entry then delivers to
//!   the run, and the `corun` returns having seen nothing.
//! - **Mutation 2**, the companion's absence: the fix's priority with no
//!   forward on the way out of `ENTERED`. A body that never coruns leaves the
//!   payload on a node nobody will read again.
//! - **Mutation 3**, the hypothesis this file's issue started with and the one
//!   the storage model's shape would have suggested: a child that counts its
//!   parent down *before* it publishes. The `corun`'s predicate then holds
//!   while the payload is still in flight -- and, which the model found
//!   rather than confirmed, the forward of mutation 2 misses the payload for
//!   the same reason, so a body that never coruns loses it too.
//!
//! Mutations 1 and 2 each have a control beside them where the same knob is
//! sound, so a mutation that failed for some other reason would show up as
//! the control failing too. Mutation 3 has no control and two findings
//! instead: there is no shape in which publishing after the count-down is
//! sound, which is why the crate's publish ordering is the one thing here
//! that both shapes rest on.

#![cfg(loom)]

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

/// What the child panics with, which every case follows by identity.
const PAYLOAD: &str = "a";

/// One payload slot with its arbiter: `NodeBase`'s `claim_panic`,
/// `keep_panic` and `take_panic`, and `Topology`'s own three.
///
/// The arbiter is the atomic rather than the lock, as it is in the crate, so
/// the second of two failures is turned away without ever reaching the mutex.
struct Slot {
    /// `ESTATE::CAUGHT`: the slot is claimed.
    caught: AtomicUsize,
    /// The payload itself.
    payload: Mutex<Option<&'static str>>,
}

impl Slot {
    fn new() -> Self {
        Self {
            caught: AtomicUsize::new(0),
            payload: Mutex::new(None),
        }
    }

    /// `claim_panic` then `keep_panic`: the pair every caller of the walk
    /// makes, and the one-payload rule in one line.
    fn record(&self, payload: &'static str) {
        if self.caught.swap(1, Ordering::Relaxed) == 0 {
            *self.payload.lock().unwrap() = Some(payload);
        }
    }

    /// `take_panic`, which un-claims the slot when there was something in it.
    fn take(&self) -> Option<&'static str> {
        let payload = self.payload.lock().unwrap().take();
        if payload.is_some() {
            self.caught.store(0, Ordering::Relaxed);
        }
        payload
    }

    /// Whether the slot is claimed, which is what `enter` asks before it
    /// reaches for the lock: a payload is only ever put here by a claimant
    /// that set this bit first.
    fn claimed(&self) -> bool {
        self.caught.load(Ordering::Relaxed) != 0
    }
}

/// The runtime task's node: the two state words the walk reads, the join
/// counter the `corun` watches, and the slot the walk may write.
struct Node {
    /// `nstate`: `PREEMPTED | IMPLICITLY_ANCHORED`, which `enter` sets before
    /// the body runs and clears only once nothing is outstanding.
    entered: AtomicUsize,
    /// `estate`: `EXPLICITLY_ANCHORED`, which a `corun` holds for the length
    /// of its wait.
    anchored: AtomicUsize,
    /// `join_counter`: the body's own reference plus one per child.
    outstanding: AtomicUsize,
    /// The node's payload slot.
    slot: Slot,
}

/// The run at the chain's root: the completion slot a `wait` reads.
struct Run {
    slot: Slot,
}

/// Which of the node and the run the walk prefers when both could have the
/// payload: the knob mutation 1 turns.
#[derive(Clone, Copy)]
enum Priority {
    /// Upstream's, and this port's before the fix: the chain's root is
    /// `EXPLICITLY_ANCHORED` for the length of the run, so it outranks an
    /// implicit anchor below it.
    RunBeatsImplicit,
    /// `DESIGN.md`'s priority, which is what the crate ships: an implicit
    /// anchor on the chain beats the run.
    ImplicitBeatsRun,
}

/// Whether the child publishes its payload before or after it counts its
/// parent down: the knob mutation 3 turns.
#[derive(Clone, Copy)]
enum Publish {
    /// The crate's, and upstream's: `record_panic` runs inside `invoke`, and
    /// the tear-down that decrements comes after it.
    BeforeCountDown,
    /// The wake-before-publish ordering, which is the shape of the four races
    /// `tests/loom_storage.rs` models.
    AfterCountDown,
}

/// Whether the node hands a payload nobody collected on when it stops being
/// an anchor: the knob mutation 2 turns.
#[derive(Clone, Copy)]
enum Forward {
    /// The crate's: `enter` walks again from a node that has left `ENTERED`.
    OnExit,
    /// No companion at all, which is what the priority above costs without
    /// one.
    Never,
}

/// What the body does with its children before it finishes: the two shapes
/// the suite's `RuntimeAsync` case runs.
#[derive(Clone, Copy)]
enum Shape {
    /// `Runtime::try_corun`, which is the frame that asks.
    Corun,
    /// The implicit join only: nobody asks, so the run has to be told.
    NoCorun,
}

/// `Job::record_panic` over a one-link chain: the failing child's own base,
/// the runtime node above it, the run above that.
///
/// The child's base is neither anchored nor claimed in any case here, so the
/// walk's first step is always to the node, and the two bits it reads there
/// are the whole of the decision.
fn record_panic(node: &Node, run: &Run, priority: Priority, payload: &'static str) {
    let explicit = node.anchored.load(Ordering::Relaxed) != 0;
    let implicit = node.entered.load(Ordering::Relaxed) != 0;

    match (explicit, implicit, priority) {
        (true, _, _) | (false, true, Priority::ImplicitBeatsRun) => node.slot.record(payload),
        _ => run.slot.record(payload),
    }
}

/// One runtime task with one child that fails, run under the four knobs.
///
/// The body thread is this one, the thief is the spawned one, and the
/// assertions at the end are the two promises the crate makes: a `corun` is
/// told about a failure under it, and a failure nobody collected is the run's.
fn one_child_fails(shape: Shape, priority: Priority, publish: Publish, forward: Forward) {
    let node = Arc::new(Node {
        entered: AtomicUsize::new(0),
        anchored: AtomicUsize::new(0),
        outstanding: AtomicUsize::new(0),
        slot: Slot::new(),
    });
    let run = Arc::new(Run { slot: Slot::new() });

    // `enter`'s first entry: both bits before the body, then the body's own
    // reference.
    node.entered.store(1, Ordering::Relaxed);
    node.outstanding.store(1, Ordering::Release);

    // `runtime.silent_async(|| panic!("a"))`: counted before the push, as
    // every queued job is, and then stolen.
    node.outstanding.fetch_add(1, Ordering::Relaxed);
    let thief = {
        let (node, run) = (Arc::clone(&node), Arc::clone(&run));
        thread::spawn(move || {
            // The child's job: the body fails, the walk delivers, the
            // tear-down counts the parent down.
            match publish {
                Publish::BeforeCountDown => {
                    record_panic(&node, &run, priority, PAYLOAD);
                    node.outstanding.fetch_sub(1, Ordering::AcqRel);
                }
                Publish::AfterCountDown => {
                    node.outstanding.fetch_sub(1, Ordering::AcqRel);
                    record_panic(&node, &run, priority, PAYLOAD);
                }
            }
        })
    };

    // The body, and what its `corun` -- if it made one -- was told.
    let seen = match shape {
        Shape::Corun => {
            // `Runtime::try_corun`: the guard, the wait on the counter coming
            // back to the body's own one reference, the guard dropped, the
            // slot taken.
            node.anchored.store(1, Ordering::Relaxed);
            while node.outstanding.load(Ordering::Acquire) != 1 {
                thread::yield_now();
            }
            node.anchored.store(0, Ordering::Relaxed);
            node.slot.take()
        }
        Shape::NoCorun => None,
    };

    // `enter`'s tail: the body's own reference spent. A node that finds
    // something still outstanding is preempted, and the *second* entry --
    // which the last child re-queues, so it runs after that child's
    // decrement -- is what clears the bits; one thread stands in for both,
    // the wait below being the re-queue.
    if node.outstanding.fetch_sub(1, Ordering::AcqRel) != 1 {
        while node.outstanding.load(Ordering::Acquire) != 0 {
            thread::yield_now();
        }
    }
    node.entered.store(0, Ordering::Relaxed);

    // The forward, after the bits come off and not before: a walk that still
    // found this node implicitly anchored would hand the payload back to it.
    if let Forward::OnExit = forward
        && node.slot.claimed()
        && let Some(payload) = node.slot.take()
    {
        record_panic(&node, &run, priority, payload);
    }

    // Joined before the run is read, which is the crate's own ordering: the
    // child's job counts this node down, and this node counts the run down
    // only after that, so no `wait` can read the completion slot while a
    // child still has a payload to put in it.
    thief.join().unwrap();

    match shape {
        // The `corun` is the frame that asked, so it is the frame that has to
        // be told -- and the run is left alone, which is what an anchor is
        // for.
        Shape::Corun => {
            assert_eq!(
                seen,
                Some(PAYLOAD),
                "the corun returned with nothing while the child had already failed"
            );
            assert_eq!(
                run.slot.take(),
                None,
                "the corun collected the failure, so the run has none"
            );
        }
        // Nobody asked, so the payload is the run's.
        Shape::NoCorun => assert_eq!(
            run.slot.take(),
            Some(PAYLOAD),
            "nothing reported the child's failure: it was left on a node nobody reads again"
        ),
    }
}

// --- The orderings the crate ships ----------------------------------------

/// A `corun` is told about a child that failed before it was entered, which
/// is the delivery this file's issue was about.
#[test]
fn a_corun_is_told_about_a_child_that_failed_before_it_began() {
    loom::model(|| {
        one_child_fails(
            Shape::Corun,
            Priority::ImplicitBeatsRun,
            Publish::BeforeCountDown,
            Forward::OnExit,
        );
    });
}

/// A body that never coruns fails its run instead, which is what the forward
/// on the way out of `ENTERED` is for.
#[test]
fn a_body_that_never_coruns_hands_its_childs_failure_to_the_run() {
    loom::model(|| {
        one_child_fails(
            Shape::NoCorun,
            Priority::ImplicitBeatsRun,
            Publish::BeforeCountDown,
            Forward::OnExit,
        );
    });
}

// --- The orderings it does not, and loom has to find each ------------------

/// Mutation 1: the run outranks the implicit anchor, which is upstream's
/// arrangement and was this port's. The thief can deliver to the run before
/// the body has reached the `corun` that is about to ask.
#[test]
#[should_panic(expected = "the corun returned with nothing while the child had already failed")]
fn mutation_a_run_that_outranks_the_entered_node_is_found() {
    loom::model(|| {
        one_child_fails(
            Shape::Corun,
            Priority::RunBeatsImplicit,
            Publish::BeforeCountDown,
            Forward::OnExit,
        );
    });
}

/// The control for mutation 1: that priority is sound for a body that never
/// coruns, which is why the window was only ever a `corun`'s. The model has
/// to pass here, or mutation 1 is failing for a reason other than the one it
/// names.
#[test]
fn mutation_1s_priority_is_sound_for_a_body_that_never_coruns() {
    loom::model(|| {
        one_child_fails(
            Shape::NoCorun,
            Priority::RunBeatsImplicit,
            Publish::BeforeCountDown,
            Forward::OnExit,
        );
    });
}

/// Mutation 2: the shipped priority with no forward out of `ENTERED`. The
/// payload reaches the entered node and stays there, so a body that never
/// coruns loses a failure the run used to report.
#[test]
#[should_panic(expected = "nothing reported the child's failure")]
fn mutation_a_node_that_leaves_entered_without_forwarding_is_found() {
    loom::model(|| {
        one_child_fails(
            Shape::NoCorun,
            Priority::ImplicitBeatsRun,
            Publish::BeforeCountDown,
            Forward::Never,
        );
    });
}

/// The control for mutation 2: the forward is what a body that *does* corun
/// never needs, having emptied the slot itself.
#[test]
fn mutation_2s_missing_forward_costs_a_corun_nothing() {
    loom::model(|| {
        one_child_fails(
            Shape::Corun,
            Priority::ImplicitBeatsRun,
            Publish::BeforeCountDown,
            Forward::Never,
        );
    });
}

/// Mutation 3: the child counts its parent down before it publishes, which is
/// the wake-before-publish ordering of the four races the storage model
/// carries. The `corun`'s predicate holds while the payload is still in
/// flight, so the frame that asked is told nothing.
#[test]
#[should_panic(expected = "the corun returned with nothing while the child had already failed")]
fn mutation_a_child_that_counts_down_before_publishing_is_found() {
    loom::model(|| {
        one_child_fails(
            Shape::Corun,
            Priority::ImplicitBeatsRun,
            Publish::AfterCountDown,
            Forward::OnExit,
        );
    });
}

/// Mutation 3's second finding, and the one the model produced rather than
/// confirmed: the same ordering loses an *uncollected* payload as well. The
/// body observes the counter at zero, clears `ENTERED` and reaches for the
/// slot before the thief has filled it, so the forward finds nothing and the
/// payload stays on a node nobody reads again. There is therefore no shape in
/// which this knob is sound, and no control for it.
#[test]
#[should_panic(expected = "nothing reported the child's failure")]
fn mutation_a_child_that_counts_down_before_publishing_also_loses_an_uncollected_payload() {
    loom::model(|| {
        one_child_fails(
            Shape::NoCorun,
            Priority::ImplicitBeatsRun,
            Publish::AfterCountDown,
            Forward::OnExit,
        );
    });
}
