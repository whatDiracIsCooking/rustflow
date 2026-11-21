//! A [`loom`](https://docs.rs/loom) model of the predicate
//! `WorkerThread::wait_for_task` checks between `prepare_wait` and
//! `commit_wait`: the shared buffers, **then every sibling worker's queue**,
//! then the done flag. Issue #452 is where that middle term got a model, the
//! suite's only instrument for it being a pair of starvation cases that need
//! a lost-notification window to fail.
//!
//! **This file is not part of the gate**, for `tests/loom_notifier.rs`'s
//! reason: loom needs a `cfg(loom)` build of the whole graph, which only
//! `RUSTFLAGS` sets, and the cases below are empty without it. The command is
//! every model's:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=/target/rustflow-loom \
//!     cargo test --release --test loom_sleep_predicate
//! ```
//!
//! `.github/workflows/loom.yml` finds and runs it weekly and on
//! `workflow_dispatch`, alongside every other `tests/loom_*.rs`, which is the
//! only thing watching this model outside a deliberate local run (#128).
//!
//! # The property
//!
//! **A worker does not park in front of work that nobody else will reach.**
//! The three things that can be waiting for it are the three terms of the
//! predicate, and the sweep of the siblings' queues is the one whose own
//! argument for being redundant nearly holds: the only thread that pushes
//! into a worker's queue is that worker, through `WorkerThread::schedule`,
//! and a worker reaches `wait_for_task` only once its own `pop` has come back
//! empty -- so a backlog the sweep can see belongs to a worker that is still
//! running and will drain it. Except for one worker, and it is the one
//! upstream's own starvation cases build: a worker **blocked inside a task
//! body** holds a non-empty queue it is never going to drain.
//! `tests/work_stealing.rs`'s `skewed_starvation` and `wavefront_starvation`
//! block `W - 1` workers that way until one of them reaches the stop task.
//!
//! What keeps a blind sweep from hanging every such run is the notification:
//! every push is followed by `notify_one`, and `explore_task` draws every
//! queue. The sweep covers the one window a notification cannot -- a
//! `notify_one` that lands before the sleeper's `prepare_wait`, where the
//! notifier has nobody to wake and the sleeper has not yet looked. That
//! window is why no deterministic non-loom test is available for this:
//! nothing a test can call makes `notify_one` land between another thread's
//! two instructions, so the starvation matrix only sometimes builds the
//! ordering, and when it does the failure is a 120-second `spin_until`
//! deadline rather than an assertion. [`one_sleep`] builds the ordering by
//! construction, for every interleaving inside the bound.
//!
//! # What is modelled, and what is not
//!
//! The **notifier is the real one**: `src/executor/notifier.rs` compiles
//! against loom's atomics under this cfg, and the whole of the window above
//! is the Dekker pair it carries -- `prepare_wait`'s fence after its ticket,
//! `notify_one`'s fence before its load of the state word -- so a stand-in
//! for it would model nothing.
//!
//! The **queues are stood in for**, `tests/loom_dispatch_claim.rs`'s way,
//! and they have to be: a `WorkerQueue` is a `crossbeam_deque::Worker`, whose
//! atomics are `std`'s and invisible to loom whatever this crate is built
//! with. [`Depth`] restates a queue as the one fact the predicate asks of it
//! -- whether it is empty -- published by a release push, consumed by a
//! read-modify-write steal and read by a plain load, which is the pairing the
//! real deque makes and the real `Worker::is_queue_empty` reads. What a
//! stand-in cannot and need not carry is the deque's own head/tail protocol:
//! no term of the predicate asks a queue anything else, and `tests/wsq.rs`
//! is where the queue layer itself is tested.
//!
//! The sweep's **index map is the knob** ([`Sweep`]), because the map is what
//! the surviving mutants of `first_nonempty_sibling` change: `k -> k + (k >=
//! id)` over `0..workers - 1` is supposed to visit every worker but `id`
//! exactly once, and a map that visits a different subset -- including one
//! that names the sweeping worker itself -- is a sweep that cannot see the
//! backlog it is there for.
//!
//! [`Shared::explore_task`] is one steal attempt at every queue but the
//! sleeper's own, which is all of upstream's `_explore_task` this model
//! needs: the real one draws victims at random until its attempt budget runs
//! out, its own `is_done` check sits in that retry loop past the first
//! attempt, and the fast path it takes when no topology is running skips the
//! loop entirely -- so the `done` term of the predicate is what has to see a
//! shutdown, which is why [`Arrival::Shutdown`] is a case here at all.
//!
//! # The mutations this model was checked against
//!
//! The model earns its keep by **mutation**, `tests/loom_scope.rs`'s way, and
//! the first three are the three mutants of `first_nonempty_sibling` that
//! survived #432's run over `src/executor/worker.rs`:
//!
//! 1. the sweep replaced by `None` ([`Sweep::Blind`]);
//! 2. its map's `+` replaced by `*` ([`Sweep::Product`]);
//! 3. its map's `>=` replaced by `<` ([`Sweep::Flipped`]);
//! 4. the buffers term deleted from the predicate ([`Terms::buffers`]);
//! 5. the done term deleted from it ([`Terms::done`]).
//!
//! Each is asserted to make loom report the deadlock, and the predicate the
//! crate ships is asserted to pass, which is the pair that makes this a check
//! on the terms rather than on the protocol `tests/loom_notifier.rs` already
//! covers. Mutations 2 and 3 are run in two pools, because the pair of
//! workers that is enough to state the property collapses both of them into
//! the same program -- "the sweep looks at its own queue" -- and a sleeper in
//! the middle of three is where they differ from each other and from a sweep
//! that answers `None`.

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::thread;

use rustflow::executor::notifier::{NonblockingNotifier, Notifier};

/// One queue -- a worker's own or a shared buffer -- stood in for by the
/// number of jobs sitting in it.
struct Depth(AtomicUsize);

impl Depth {
    /// An empty queue.
    fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    /// `WorkerQueue::push` on the owning end, and `Buffers::push`: a release
    /// write, which is what publishes a job to anyone who comes looking.
    fn push(&self) {
        self.0.fetch_add(1, Ordering::Release);
    }

    /// `Thief::steal` and `Buffers::steal_from`, with crossbeam's third
    /// outcome already settled as `queue::settle` settles it: a
    /// read-modify-write, so it reads no stale count and a job that is there
    /// is taken.
    fn steal(&self) -> bool {
        self.0.update(Ordering::AcqRel, Ordering::Acquire, |jobs| {
            jobs.saturating_sub(1)
        }) > 0
    }

    /// `Worker::is_queue_empty`, and the `Injector::is_empty` behind
    /// `Buffers::first_nonempty`: a plain load, worth exactly what upstream's
    /// `empty()` is worth, and the same hint the real predicate acts on.
    fn is_empty(&self) -> bool {
        self.0.load(Ordering::Acquire) == 0
    }
}

/// How the pre-sleep sweep maps its loop counter to the worker whose queue it
/// looks at.
#[derive(Clone, Copy)]
enum Sweep {
    /// `WorkerThread::first_nonempty_sibling` as the crate has it:
    /// `k + usize::from(k >= id)` over `0..workers - 1`, which visits every
    /// worker but `id` exactly once.
    EveryOtherWorker,
    /// **A mutation**: the sweep replaced by `None`, so no sibling ever has
    /// work.
    Blind,
    /// **A mutation**: the map's `+` replaced by `*`, which visits `0` for
    /// every `k` below `id` and `k` itself for the rest -- the sweeping
    /// worker's own queue among them.
    Product,
    /// **A mutation**: the map's `>=` replaced by `<`, which shifts the
    /// skipped index to the other side of `id` and so visits `id` twice and
    /// the last worker never.
    Flipped,
}

impl Sweep {
    /// The `(k, id) -> worker id` map this sweep walks by, or `None` for the
    /// sweep that walks nothing at all.
    fn index_map(self) -> Option<fn(usize, usize) -> usize> {
        match self {
            Sweep::EveryOtherWorker => Some(|k, id| k + usize::from(k >= id)),
            Sweep::Product => Some(|k, id| k * usize::from(k >= id)),
            Sweep::Flipped => Some(|k, id| k + usize::from(k < id)),
            Sweep::Blind => None,
        }
    }
}

/// Which terms of `wait_for_task`'s predicate the sleeper checks, and by which
/// map it sweeps.
#[derive(Clone, Copy)]
struct Terms {
    /// The first term, `Buffers::first_nonempty`: work left by a thread with
    /// no queue of its own.
    buffers: bool,
    /// The second term, and this model's subject.
    sweep: Sweep,
    /// The third term, `Worker::is_done`: the executor asking this worker to
    /// leave the scheduling loop.
    done: bool,
}

impl Terms {
    /// The predicate the crate ships: all three terms, swept by the crate's
    /// own index map.
    const SHIPPED: Self = Self {
        buffers: true,
        sweep: Sweep::EveryOtherWorker,
        done: true,
    };

    /// The shipped predicate with `sweep` in place of its sweep.
    const fn swept_by(sweep: Sweep) -> Self {
        Self {
            buffers: true,
            sweep,
            done: true,
        }
    }
}

/// Which worker of how many goes to sleep, and whose queue the backlog lands
/// in.
#[derive(Clone, Copy)]
struct Pool {
    /// How many workers the executor has, which is the length of the sweep.
    workers: usize,
    /// The worker driving [`wait_for_task`].
    sleeper: usize,
    /// The worker whose queue [`Arrival::SiblingQueue`] pushes into, and which
    /// never drains it.
    pusher: usize,
}

impl Pool {
    /// The smallest pool the property can be stated in: worker zero asleep,
    /// worker one holding the backlog.
    const PAIR: Self = Self {
        workers: 2,
        sleeper: 0,
        pusher: 1,
    };

    /// Three workers with the sleeper in the middle, so that the two mutated
    /// index maps visit a strict, *different* subset of the siblings instead
    /// of collapsing into "its own queue and nothing else".
    const SLEEPER_IN_THE_MIDDLE: Self = Self {
        workers: 3,
        sleeper: 1,
        pusher: 2,
    };
}

/// What the sleeping worker looks at: the pool's queues, the shared buffers,
/// the done flag, and the notifier every one of them is read under.
struct Shared {
    /// One per worker, indexed by worker id: upstream's `_workers` and the
    /// `Thief` each `Worker` publishes.
    queues: Vec<Depth>,
    /// `Shared::buffers`. One is enough: what the predicate asks of them is
    /// `first_nonempty`, and a second, permanently empty buffer would add a
    /// load and no ordering.
    buffers: Depth,
    /// `Worker::done`, read and written relaxed exactly as the real flag is
    /// -- the notifier's fences are what order a shutdown against a worker
    /// going to sleep. One flag rather than one per worker, there being one
    /// sleeper here and nothing that reads another worker's.
    done: AtomicBool,
    /// The real notifier, not a stand-in.
    notifier: NonblockingNotifier,
}

impl Shared {
    /// An idle pool of `workers` workers: every queue empty, no shutdown, no
    /// waiters.
    fn new(workers: usize) -> Self {
        Self {
            queues: (0..workers).map(|_| Depth::new()).collect(),
            buffers: Depth::new(),
            done: AtomicBool::new(false),
            notifier: NonblockingNotifier::new(workers),
        }
    }

    /// `WorkerThread::explore_task`, as much of it as this model needs: one
    /// steal attempt at every queue but this worker's own -- which upstream's
    /// victim draw excludes, the sleeper's own queue being the one it just
    /// drained -- and then "nothing to steal".
    fn explore_task(&self, id: usize) -> bool {
        (0..self.queues.len())
            .filter(|&victim| victim != id)
            .any(|victim| self.queues[victim].steal())
            || self.buffers.steal()
    }

    /// `Buffers::first_nonempty`, asked as the predicate asks it.
    fn buffers_nonempty(&self) -> bool {
        !self.buffers.is_empty()
    }

    /// `WorkerThread::first_nonempty_sibling`, with its index map the knob
    /// the mutations turn.
    fn first_nonempty_sibling(&self, id: usize, sweep: Sweep) -> Option<usize> {
        let map = sweep.index_map()?;
        (0..self.queues.len() - 1)
            .map(|k| map(k, id))
            .find(|&victim| !self.queues[victim].is_empty())
    }

    /// `Worker::is_done`.
    fn is_done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }
}

/// What the modelled `wait_for_task` came back with: a job to run, or the
/// instruction to leave the scheduling loop, which the real one spells
/// `Some(job)` and `None`.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// There was work, and this worker has it.
    Job,
    /// The executor is shutting this worker down.
    Shutdown,
}

/// `WorkerThread::wait_for_task`'s own sequence over `terms`: explore, open a
/// wait, check every term of the predicate, and commit the wait only if all
/// of them say there is nothing to do -- cancelling it on each check that
/// finds work, and going round again, because a notification is a hint to
/// look rather than a delivery.
///
/// The loop is what makes the liveness property checkable, as it is in
/// `tests/loom_notifier.rs`: returning from `commit_wait` means *a*
/// notification arrived, while leaving this function means the worker
/// actually found the work or was told to stop. A predicate that cannot see
/// the one backlog in the pool leaves this parked inside a `commit_wait`
/// nobody will end, which loom reports as a deadlock.
fn wait_for_task(shared: &Shared, id: usize, terms: Terms) -> Outcome {
    loop {
        if shared.explore_task(id) {
            return Outcome::Job;
        }

        shared.notifier.prepare_wait(id);

        if terms.buffers && shared.buffers_nonempty() {
            shared.notifier.cancel_wait(id);
            continue;
        }
        if shared.first_nonempty_sibling(id, terms.sweep).is_some() {
            shared.notifier.cancel_wait(id);
            continue;
        }
        if terms.done && shared.is_done() {
            shared.notifier.cancel_wait(id);
            return Outcome::Shutdown;
        }

        shared.notifier.commit_wait(id);
    }
}

/// What the other thread does -- once -- and then stops doing anything at
/// all, which is the whole point: in all three cases the work is left where
/// it is and the sleeper is the only thread that can still reach it.
#[derive(Clone, Copy)]
enum Arrival {
    /// A worker pushing a job onto its own queue through
    /// `WorkerThread::schedule` and never draining it: a worker blocked
    /// inside a task body, which is what `skewed_starvation` and
    /// `wavefront_starvation` build `W - 1` of.
    SiblingQueue,
    /// A thread with no queue of its own pushing into the shared buffers and
    /// leaving: an external submission, a corun view, or a task body that
    /// cannot name the `WorkerThread` frame below it.
    Buffer,
    /// `Executor::drop`'s shutdown: the done flags set, then `notify_all`.
    Shutdown,
}

impl Arrival {
    /// What a sleeper that saw this arrival has to come back with.
    fn outcome(self) -> Outcome {
        match self {
            Arrival::SiblingQueue | Arrival::Buffer => Outcome::Job,
            Arrival::Shutdown => Outcome::Shutdown,
        }
    }
}

/// One run of the model: `arrival` on one thread, a worker driving
/// [`wait_for_task`] over `terms` on the other.
///
/// The sleeper runs on the model's own thread, as the notifier model's
/// notifier does, so the pool is two threads and not three. Nothing is
/// ordered between them by construction: loom is free to run the arrival
/// before the sleeper has looked at anything, between its steal attempt and
/// its `prepare_wait` -- the window the sweep exists for -- or after it is
/// already parked.
fn one_sleep(pool: Pool, arrival: Arrival, terms: Terms) {
    // Leaked rather than shared through an `Arc`, which is the one line in
    // this file that is about loom rather than about the executor. The panic
    // loom raises for a deadlock leaves its scheduler with no active thread,
    // so any loom operation performed on the way out of the model -- a
    // `loom::sync::Arc`'s decrement, or the notifier's own `Drop` reading its
    // state word where debug assertions are on -- aborts the process instead
    // of unwinding to the `#[should_panic]` that every mutation below rests
    // on. A frame owning nothing of loom's cannot perform one. The cost is
    // one `Shared` per interleaving explored, and a model that asserts a
    // deadlock has no cheaper way to be caught asserting it.
    let shared: &'static Shared = Box::leak(Box::new(Shared::new(pool.workers)));

    let other = thread::spawn(move || match arrival {
        Arrival::SiblingQueue => {
            shared.queues[pool.pusher].push();
            shared.notifier.notify_one();
        }
        Arrival::Buffer => {
            shared.buffers.push();
            shared.notifier.notify_one();
        }
        Arrival::Shutdown => {
            shared.done.store(true, Ordering::Relaxed);
            shared.notifier.notify_all();
        }
    });

    let outcome = wait_for_task(shared, pool.sleeper, terms);

    other.join().unwrap();

    assert_eq!(
        outcome,
        arrival.outcome(),
        "the sleeper left `wait_for_task` for the wrong reason"
    );
    // The notifier has to come back clean, as it does in
    // `tests/loom_notifier.rs`: a ticket resolved twice or not at all would
    // leave this non-zero and the next notification aimed at a ghost.
    assert_eq!(
        shared.notifier.num_waiters(),
        0,
        "a waiter was left on the notifier"
    );
}

/// `bounded_model`'s identical reason in every other model here: the
/// unbounded search is exponential in the atomics this protocol touches --
/// one `notify_one` alone is a fence, a load and a compare-exchange -- and
/// three preemptions is loom's own recommendation and already enough to find
/// every ordering below, the window this model is about being two.
fn bounded_model(f: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(f);
}

// --- The predicate the crate ships, against each of the three things that
// --- can be left waiting for a worker ---------------------------------------

/// A sibling's backlog, pushed at any point in the sleeper's sequence: the
/// sweep sees it wherever the `notify_one` lands, including before the
/// `prepare_wait` that would have been woken by it.
#[test]
fn a_siblings_backlog_is_seen_wherever_the_notification_lands() {
    bounded_model(|| one_sleep(Pool::PAIR, Arrival::SiblingQueue, Terms::SHIPPED));
}

/// The same, with the sleeper in the middle of three workers, which is the
/// pool the mutated index maps are told apart in: the shipped map still
/// visits the last worker, whose queue holds the job.
#[test]
fn a_backlog_past_the_sleeper_is_seen_from_the_middle_of_the_sweep() {
    bounded_model(|| {
        one_sleep(
            Pool::SLEEPER_IN_THE_MIDDLE,
            Arrival::SiblingQueue,
            Terms::SHIPPED,
        );
    });
}

/// A job left in the shared buffers by a thread that then went away: the
/// first term of the predicate, under the same window.
#[test]
fn a_job_left_in_the_buffers_is_seen_wherever_the_notification_lands() {
    bounded_model(|| one_sleep(Pool::PAIR, Arrival::Buffer, Terms::SHIPPED));
}

/// A shutdown, which no steal attempt of this model's `explore_task` can
/// report: the third term of the predicate is the only thing that gets the
/// worker out, and it has to see the flag wherever the `notify_all` lands.
#[test]
fn a_shutdown_is_seen_wherever_the_notification_lands() {
    bounded_model(|| one_sleep(Pool::PAIR, Arrival::Shutdown, Terms::SHIPPED));
}

// --- Mutation: the three surviving mutants of the sweep ---------------------

/// Mutation 1, `first_nonempty_sibling` replaced by `None`: the sleeper parks
/// in front of a queue its own `notify_one` arrived too early to point it at,
/// and loom finds the ordering.
#[test]
#[should_panic(expected = "deadlock")]
fn mutation_a_sweep_that_answers_none_parks_in_front_of_the_backlog() {
    bounded_model(|| {
        one_sleep(
            Pool::PAIR,
            Arrival::SiblingQueue,
            Terms::swept_by(Sweep::Blind),
        );
    });
}

/// Mutation 2, the map's `+` as `*`: for a sleeper with id zero every `k`
/// maps to `k` itself, so the sweep reads the sleeper's own empty queue and
/// never worker one's.
#[test]
#[should_panic(expected = "deadlock")]
fn mutation_a_sweep_whose_map_multiplies_parks_in_front_of_the_backlog() {
    bounded_model(|| {
        one_sleep(
            Pool::PAIR,
            Arrival::SiblingQueue,
            Terms::swept_by(Sweep::Product),
        );
    });
}

/// The same mutation from the middle of three workers, where it is not the
/// degenerate program above: the sweep visits workers zero and one -- itself
/// among them -- and never worker two, which is where the backlog is.
#[test]
#[should_panic(expected = "deadlock")]
fn mutation_a_multiplying_sweep_misses_the_worker_past_the_sleeper() {
    bounded_model(|| {
        one_sleep(
            Pool::SLEEPER_IN_THE_MIDDLE,
            Arrival::SiblingQueue,
            Terms::swept_by(Sweep::Product),
        );
    });
}

/// Mutation 3, the map's `>=` as `<`: the skipped index moves to the other
/// side of `id`, so for a sleeper with id zero the sweep reads its own queue
/// and stops.
#[test]
#[should_panic(expected = "deadlock")]
fn mutation_a_sweep_whose_map_flips_its_comparison_parks_in_front_of_the_backlog() {
    bounded_model(|| {
        one_sleep(
            Pool::PAIR,
            Arrival::SiblingQueue,
            Terms::swept_by(Sweep::Flipped),
        );
    });
}

/// The same mutation from the middle of three workers: the map names the
/// sleeper for both values of `k`, so the sweep visits one queue twice and
/// the pool's other two never.
#[test]
#[should_panic(expected = "deadlock")]
fn mutation_a_flipped_sweep_visits_the_sleeper_twice_and_the_backlog_never() {
    bounded_model(|| {
        one_sleep(
            Pool::SLEEPER_IN_THE_MIDDLE,
            Arrival::SiblingQueue,
            Terms::swept_by(Sweep::Flipped),
        );
    });
}

// --- Mutation: the predicate's other two terms, so that the three cases
// --- above are a check on the sweep and not on the window -------------------

/// Mutation 4, the buffers term deleted: the same window over a job no
/// worker's queue holds, which is what makes the buffers the first term
/// rather than a duplicate of the sweep.
#[test]
#[should_panic(expected = "deadlock")]
fn mutation_a_predicate_without_the_buffers_term_parks_in_front_of_a_buffer() {
    bounded_model(|| {
        one_sleep(
            Pool::PAIR,
            Arrival::Buffer,
            Terms {
                buffers: false,
                ..Terms::SHIPPED
            },
        );
    });
}

/// Mutation 5, the done term deleted: a worker that cannot see its own
/// shutdown parks whatever the notification does, since nothing else will
/// ever be pushed.
#[test]
#[should_panic(expected = "deadlock")]
fn mutation_a_predicate_without_the_done_term_never_leaves_the_loop() {
    bounded_model(|| {
        one_sleep(
            Pool::PAIR,
            Arrival::Shutdown,
            Terms {
                done: false,
                ..Terms::SHIPPED
            },
        );
    });
}
