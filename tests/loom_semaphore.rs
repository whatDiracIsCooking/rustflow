//! A [`loom`](https://docs.rs/loom) model of the rule `Semaphore` and the
//! executor keep between them: **a task that cannot have every count it asked
//! for is parked rather than run, and is rescheduled by whoever releases.**
//! Issue #267 is where that rule got this model; upstream's semaphore history
//! has had exactly the two races the cases below are built around.
//!
//! The rule has three halves, and the model asserts all three as one terminal
//! check: **every task of a constrained set runs exactly once** -- none
//! stranded on a waiting list, none run twice by two releases -- **never more
//! of them in their bodies at once than the semaphore has counts**, and
//! **every count comes back**, the counter at its maximum and the waiting
//! lists empty once the set is done.
//!
//! **This file is not part of the gate**, for `tests/loom_notifier.rs`'s
//! reason: loom needs a `cfg(loom)` build of the whole graph, which only
//! `RUSTFLAGS` sets, and the cases below are empty without it. The command is
//! every model's, and all of them build in one go:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=/target/rustflow-loom \
//!     cargo test --release --test loom_semaphore
//! ```
//!
//! `.github/workflows/loom.yml` finds and runs it weekly and on
//! `workflow_dispatch`, alongside every other `tests/loom_*.rs`, which is the
//! only thing watching this model outside a deliberate local run (#128).
//!
//! # What is modelled, and what is not
//!
//! The model is **standalone**, in `tests/loom_storage.rs`'s sense: it
//! restates the protocol in loom types rather than driving the crate's own
//! `Semaphore`. Here that is not even a trade -- `try_acquire_or_wait` and
//! `release` are `pub(crate)`, so an integration test cannot call them at all,
//! and a model built on the real type would need a visibility seam on top of
//! the `std`-or-`loom` import seam `notifier.rs` carries. The extraction is
//! the whole of it: the crate's semaphore is a counter, a maximum and a `Vec`
//! of parked jobs under one `Mutex`, and [`Semaphore`] below has the same
//! three fields with the same two methods taking the same steps in the same
//! order.
//!
//! What stands in for the executor, and what each piece is:
//!
//! - [`Scenario::invoke_one`] is `WorkerThread::invoke_one` reduced to its two
//!   semaphore hooks and the body between them. The dispatch, the join
//!   counters and the successors are no part of this question.
//! - [`Scenario::acquire_all`] and [`Scenario::release_hook`] are
//!   `Node::acquire_all` and `Node::release_all` with their call sites,
//!   rollback and all.
//! - [`Ready`] is the executor's shared buffers, as the bag they are to this
//!   model: `bulk_schedule` pushes and a worker pops, in no fixed order.
//! - [`Scenario::drain`] is the fairness assumption -- whatever reaches the
//!   queue is eventually run by some worker -- and it runs on one thread once
//!   the racing threads are joined. The queue's own concurrency belongs to
//!   `tests/wsq.rs` and the notifier's model; what is in question here is
//!   whether a parked job reaches the queue at all.
//! - [`Progress`] is what an observer would see: which tasks ran, and how many
//!   were in their bodies at once.
//!
//! What the model cannot say: anything about a parked job's accounting -- the
//! count the clone inherits against its parent, the run that cannot finish
//! while a task of it sits parked -- which is `tests/loom_storage.rs`'s rule
//! rather than this one, and nothing about cancellation, where `DESIGN.md`
//! records that a parked job stays parked.
//!
//! The model earns its keep by **mutation**, `tests/loom_storage.rs`'s way:
//! [`Protocol`] carries, beside the protocol the crate ships, three ways to
//! get one step wrong, and loom has to *find* each of them. A model that only
//! passes on the current code has shown nothing about itself.
//!
//! - [`Protocol::ParkOutsideTheTest`] splits `try_acquire_or_wait`'s single
//!   critical section in two -- test the counter under one lock, park under
//!   another -- which is the hole the module's `# Design` section says the
//!   method's shape closes: a release landing in the gap wakes nothing and
//!   leaves the job parked on a free semaphore.
//! - [`Protocol::ReleaseThatDoesNotWake`] hands the count back and leaves the
//!   waiting list alone, so the counter is free and the task that wanted it
//!   never hears.
//! - [`Protocol::ParkKeepingWhatItTook`] drops `Node::acquire_all`'s rollback,
//!   which two tasks asking for the same two semaphores in opposite orders
//!   turn into a deadlock. It carries the control that says it is sound for a
//!   task with one semaphore to acquire, where there is nothing to give back.

#![cfg(loom)]

use loom::sync::{Arc, Mutex, MutexGuard};
use loom::thread;

/// What a stranded task is reported as, and the one assertion both of the
/// races in this file are decided by: a job that parked and that no release
/// ever handed to the executor shows up here as a missing id.
const EVERY_TASK_RUNS: &str = "every task of a constrained set runs exactly once";

/// How many times [`Scenario::drain`] will run a queued job before it gives
/// up. Every scenario here has three tasks at most and parks each of them at
/// most once per schedule, so this is slack rather than a limit; it is a
/// bound at all so that a mutation able to schedule for ever fails the model
/// instead of hanging it.
const MAX_SCHEDULES: usize = 16;

/// Upstream's `Node*` in a waiting list, here an index into
/// [`Scenario::jobs`]. The crate parks a `Job`, which owns the run, the graph
/// and the node it names, and none of that ownership is an interleaving
/// question.
type JobId = usize;

/// Which steps the semaphore and the acquire loop take: the knob the
/// mutations turn.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Protocol {
    /// The protocol the crate ships.
    AsShipped,
    /// Mutation 1: `try_acquire_or_wait` tests the counter under one lock and
    /// parks under another.
    ParkOutsideTheTest,
    /// Mutation 2: `release` returns the count and leaves the waiting list
    /// where it is.
    ReleaseThatDoesNotWake,
    /// Mutation 3: `Node::acquire_all` parks without giving back the counts it
    /// has already taken.
    ParkKeepingWhatItTook,
}

/// Everything `semaphore.rs` keeps under its one lock: upstream's
/// `_cur_value`, `_max_value` and `_waiters`.
struct State {
    /// How much of the semaphore is still to be had.
    value: usize,
    /// What a release may not push the counter past.
    max_value: usize,
    /// The tasks parked on this semaphore, oldest first.
    waiters: Vec<JobId>,
}

/// `Semaphore`: the counter and the tasks parked on it, under one lock.
struct Semaphore {
    /// The crate's `Mutex<Inner>`.
    state: Mutex<State>,
    /// Which of the four protocols this semaphore runs.
    protocol: Protocol,
}

impl Semaphore {
    /// A semaphore with `max_value` to give out, full.
    fn new(max_value: usize, protocol: Protocol) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                value: max_value,
                max_value,
                waiters: Vec::new(),
            }),
            protocol,
        })
    }

    /// `Semaphore::try_acquire_or_wait`: take a count if there is one to take,
    /// and otherwise park `job` for a later release to hand back.
    ///
    /// The test and the park are one critical section, which is what mutation
    /// 1 takes away.
    fn try_acquire_or_wait(&self, job: JobId) -> bool {
        if self.protocol == Protocol::ParkOutsideTheTest {
            {
                let mut state = self.lock();
                if state.value > 0 {
                    state.value -= 1;
                    return true;
                }
            }
            // The window the shipped method does not have: a release landing
            // here finds an empty waiting list, and the push below then parks
            // the job on a semaphore that has a count to give.
            self.lock().waiters.push(job);
            return false;
        }

        let mut state = self.lock();
        if state.value > 0 {
            state.value -= 1;
            true
        } else {
            state.waiters.push(job);
            false
        }
    }

    /// `Semaphore::release`: give the count back and hand every parked task to
    /// the caller to schedule.
    ///
    /// `woken` is appended to, as the crate's is -- its `mem::swap` when the
    /// list is still empty is an allocation trick with the same effect. The
    /// maximum guard is the crate's `SemaphoreError` arm; the tasks here give
    /// back exactly what they took, so it never fires, and a count lost to it
    /// would show in [`Scenario::check`]'s final state if it did.
    fn release(&self, woken: &mut Vec<JobId>) {
        let mut state = self.lock();
        if state.value >= state.max_value {
            return;
        }
        state.value += 1;
        if self.protocol != Protocol::ReleaseThatDoesNotWake {
            woken.append(&mut state.waiters);
        }
    }

    /// The one lock. A loom model has no poisoning to take a guard out of:
    /// loom reports a panic as a failed model.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }
}

/// The executor's shared buffers: where the jobs a release wakes go, for some
/// worker to pick up.
struct Ready {
    /// Queued work, popped in no particular order -- which is what a bag of
    /// work-stealing queues is to a model of the wake.
    queue: Mutex<Vec<JobId>>,
}

impl Ready {
    /// An executor with nothing queued.
    fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
        }
    }

    /// `Shared::bulk_schedule`. An empty wake takes no lock and notifies
    /// nobody, as the real one pushes nothing and skips its notification.
    fn bulk_schedule(&self, jobs: Vec<JobId>) {
        if jobs.is_empty() {
            return;
        }
        self.queue.lock().unwrap().extend(jobs);
    }

    /// A worker taking queued work.
    fn take(&self) -> Option<JobId> {
        self.queue.lock().unwrap().pop()
    }
}

/// Which tasks ran, and how many of them were inside their bodies at once.
struct Progress {
    /// One lock, so that entering a body and recording it cannot be seen
    /// apart.
    inner: Mutex<Running>,
}

/// [`Progress`]'s guarded half.
struct Running {
    /// The id of every task that entered its body, in the order they did.
    ran: Vec<JobId>,
    /// How many are inside one now.
    holders: usize,
    /// The largest `holders` has been, which is what the semaphore's whole
    /// purpose bounds.
    peak: usize,
}

impl Progress {
    /// Nothing has run yet.
    fn new() -> Self {
        Self {
            inner: Mutex::new(Running {
                ran: Vec::new(),
                holders: 0,
                peak: 0,
            }),
        }
    }

    /// A task entering its body with every count it asked for in hand.
    fn enter(&self, job: JobId) {
        let mut running = self.lock();
        running.ran.push(job);
        running.holders += 1;
        running.peak = running.peak.max(running.holders);
    }

    /// A task leaving its body, its counts not yet given back.
    fn leave(&self) {
        self.lock().holders -= 1;
    }

    /// What has been recorded.
    fn lock(&self) -> MutexGuard<'_, Running> {
        self.inner.lock().unwrap()
    }
}

/// One task of a constrained set: upstream's `Node` reduced to its
/// `Semaphores`, nothing else about a node being in question here.
struct Job {
    /// Upstream's `to_acquire`, in order -- and its `to_release` with it,
    /// every task here giving back exactly what it took, which is upstream's
    /// five-task example and the only shape the wake rule is about. The order
    /// is the whole of the third scenario.
    to_acquire: Vec<Arc<Semaphore>>,
}

/// One scenario: a set of tasks, the semaphores constraining them, and the
/// executor they run on.
struct Scenario {
    /// Which protocol this scenario runs, for the one step that is
    /// `Node::acquire_all`'s rather than the semaphore's.
    protocol: Protocol,
    /// Every semaphore in the scenario, for the final check.
    semaphores: Vec<Arc<Semaphore>>,
    /// The constrained set, indexed by [`JobId`].
    jobs: Vec<Job>,
    /// How many of these tasks may be in their bodies at once: the smallest
    /// count among the semaphores all of them ask for.
    at_once: usize,
    /// The executor's shared buffers.
    ready: Ready,
    /// The observer's view, which [`Scenario::check`] reads.
    progress: Progress,
}

impl Scenario {
    /// `num_jobs` tasks over one semaphore of `max_value`: upstream's
    /// five-task example, in the smallest size that still races.
    fn one_semaphore(protocol: Protocol, max_value: usize, num_jobs: usize) -> Arc<Self> {
        let semaphore = Semaphore::new(max_value, protocol);
        let jobs = (0..num_jobs)
            .map(|_| Job {
                to_acquire: vec![Arc::clone(&semaphore)],
            })
            .collect();
        Self::new(protocol, vec![semaphore], jobs)
    }

    /// Two tasks over two semaphores of one, each asking for them in the
    /// opposite order: the pair `Node::acquire_all`'s rollback exists for.
    fn opposite_orders(protocol: Protocol) -> Arc<Self> {
        let first = Semaphore::new(1, protocol);
        let second = Semaphore::new(1, protocol);
        let jobs = vec![
            Job {
                to_acquire: vec![Arc::clone(&first), Arc::clone(&second)],
            },
            Job {
                to_acquire: vec![Arc::clone(&second), Arc::clone(&first)],
            },
        ];
        Self::new(protocol, vec![first, second], jobs)
    }

    /// The shared state behind both constructors.
    fn new(protocol: Protocol, semaphores: Vec<Arc<Semaphore>>, jobs: Vec<Job>) -> Arc<Self> {
        let at_once = semaphores
            .iter()
            .map(|semaphore| semaphore.lock().max_value)
            .min()
            .expect("a scenario has at least one semaphore");
        Arc::new(Self {
            protocol,
            semaphores,
            jobs,
            at_once,
            ready: Ready::new(),
            progress: Progress::new(),
        })
    }

    /// `WorkerThread::invoke_one`'s two semaphore hooks around a task's body:
    /// a task that cannot acquire does not run and is not finished either, and
    /// the only jobs it makes ready are the ones its rollback woke.
    fn invoke_one(&self, job: JobId) {
        let mut woken = Vec::new();
        if !self.acquire_all(job, &mut woken) {
            self.ready.bulk_schedule(woken);
            return;
        }

        self.progress.enter(job);
        self.progress.leave();

        self.release_hook(job);
    }

    /// `Node::acquire_all`: every semaphore in order, and if one refuses, give
    /// back what has been taken -- newest first -- and stay parked on the one
    /// that did. Mutation 3 is the rollback, skipped.
    fn acquire_all(&self, job: JobId, woken: &mut Vec<JobId>) -> bool {
        let to_acquire = &self.jobs[job].to_acquire;
        for (taken, semaphore) in to_acquire.iter().enumerate() {
            if semaphore.try_acquire_or_wait(job) {
                continue;
            }
            if self.protocol != Protocol::ParkKeepingWhatItTook {
                for held in to_acquire[..taken].iter().rev() {
                    held.release(woken);
                }
            }
            return false;
        }
        true
    }

    /// The `to_release` block: `Node::release_all` and the `bulk_schedule`
    /// that follows it. Also what a task which ran before the model started
    /// does when it finishes, that release being the event two of the
    /// scenarios are built around.
    fn release_hook(&self, job: JobId) {
        let mut woken = Vec::new();
        for semaphore in &self.jobs[job].to_acquire {
            semaphore.release(&mut woken);
        }
        self.ready.bulk_schedule(woken);
    }

    /// The task already holding its counts when the model starts: it ran
    /// before anything else and is about to release. Done on the main thread
    /// before any spawn, an acquire with nothing to race being no part of the
    /// question.
    fn already_running(&self, job: JobId) {
        assert!(
            self.acquire_all(job, &mut Vec::new()),
            "the semaphores start full, so the first task gets its counts"
        );
        self.progress.enter(job);
        self.progress.leave();
    }

    /// The mop-up: whatever the racing threads left queued, run to quiescence
    /// by this one thread, which is the rest of the run as far as the wake
    /// rule is concerned.
    fn drain(&self) {
        for _ in 0..MAX_SCHEDULES {
            let Some(job) = self.ready.take() else {
                return;
            };
            self.invoke_one(job);
        }
        panic!("the ready queue did not come to rest in {MAX_SCHEDULES} schedules");
    }

    /// The rule, as three assertions on the state everything has come to rest
    /// in. The main thread's on purpose, loom reporting a panic on a spawned
    /// thread as the model's own failure rather than as this test's.
    fn check(&self) {
        let running = self.progress.lock();
        let mut ran = running.ran.clone();
        ran.sort_unstable();
        assert_eq!(
            ran,
            (0..self.jobs.len()).collect::<Vec<JobId>>(),
            "{EVERY_TASK_RUNS}"
        );
        assert!(
            running.peak <= self.at_once,
            "more tasks were in their bodies at once than the semaphore had counts"
        );
        drop(running);

        for semaphore in &self.semaphores {
            let state = semaphore.lock();
            assert_eq!(
                state.value, state.max_value,
                "a count never came back to the semaphore"
            );
            assert!(
                state.waiters.is_empty(),
                "a task was left parked on a semaphore nothing will release"
            );
        }
    }
}

// --- The rule the crate ships ---------------------------------------------

/// Upstream's five-task example in the smallest size that races: two tasks
/// over a semaphore of one, started at once. One of them takes the count and
/// the other parks, and the first one's release is what runs it.
#[test]
fn a_semaphore_of_one_runs_its_tasks_one_at_a_time() {
    loom::model(|| {
        let scenario = Scenario::one_semaphore(Protocol::AsShipped, 1, 2);
        let second = {
            let scenario = Arc::clone(&scenario);
            thread::spawn(move || scenario.invoke_one(1))
        };

        scenario.invoke_one(0);
        second.join().unwrap();
        scenario.drain();
        scenario.check();
    });
}

/// The first race the rule is about: a release landing while an acquirer is
/// between "the count is gone" and "parked". The shipped method leaves no gap
/// for it to land in -- the test and the park are one critical section -- so
/// wherever loom puts the release, the acquirer either takes the count or is
/// on the list in time to be handed back by it.
#[test]
fn a_release_landing_as_a_task_parks_is_not_lost() {
    loom::model(|| {
        let scenario = Scenario::one_semaphore(Protocol::AsShipped, 1, 2);
        scenario.already_running(0);

        let acquirer = {
            let scenario = Arc::clone(&scenario);
            thread::spawn(move || scenario.invoke_one(1))
        };

        scenario.release_hook(0);
        acquirer.join().unwrap();
        scenario.drain();
        scenario.check();
    });
}

/// The second race: two acquirers and one count to be had. Exactly one of
/// them gets it, and the release that runs the other is the holder's or the
/// winner's, whichever the interleaving leaves last.
#[test]
fn two_acquirers_racing_one_release_both_run() {
    loom::model(|| {
        let scenario = Scenario::one_semaphore(Protocol::AsShipped, 1, 3);
        scenario.already_running(0);

        let acquirers: Vec<_> = [1, 2]
            .into_iter()
            .map(|job| {
                let scenario = Arc::clone(&scenario);
                thread::spawn(move || scenario.invoke_one(job))
            })
            .collect();

        scenario.release_hook(0);
        for acquirer in acquirers {
            acquirer.join().unwrap();
        }
        scenario.drain();
        scenario.check();
    });
}

/// Acquire-many in opposite orders, which is what the rollback in
/// `Node::acquire_all` is for: a task that gets the first count and not the
/// second gives the first back before it parks, so the two cannot sit holding
/// half of each other's set -- and the rollback's own release is a wake, so
/// neither is stranded by it either.
#[test]
fn acquire_many_in_opposite_orders_strands_neither_task() {
    loom::model(|| {
        let scenario = Scenario::opposite_orders(Protocol::AsShipped);
        let second = {
            let scenario = Arc::clone(&scenario);
            thread::spawn(move || scenario.invoke_one(1))
        };

        scenario.invoke_one(0);
        second.join().unwrap();
        scenario.drain();
        scenario.check();
    });
}

// --- The protocols it does not, and loom has to find each ------------------

/// Mutation 1: `try_acquire_or_wait` tests the counter under one lock and
/// parks under another. A release landing in the gap returns the count to an
/// empty waiting list, and the park that follows leaves the task on a free
/// semaphore with nobody left to release it.
#[test]
#[should_panic(expected = "every task of a constrained set runs exactly once")]
fn mutation_a_park_that_does_not_recheck_the_counter_is_found() {
    loom::model(|| {
        let scenario = Scenario::one_semaphore(Protocol::ParkOutsideTheTest, 1, 2);
        scenario.already_running(0);

        let acquirer = {
            let scenario = Arc::clone(&scenario);
            thread::spawn(move || scenario.invoke_one(1))
        };

        scenario.release_hook(0);
        acquirer.join().unwrap();
        scenario.drain();
        scenario.check();
    });
}

/// Mutation 2: `release` hands the count back and leaves the waiting list
/// alone. The counter says the set may run and the parked task never hears,
/// which is the same stranding by the other half of the protocol.
#[test]
#[should_panic(expected = "every task of a constrained set runs exactly once")]
fn mutation_a_release_that_does_not_wake_is_found() {
    loom::model(|| {
        let scenario = Scenario::one_semaphore(Protocol::ReleaseThatDoesNotWake, 1, 2);
        scenario.already_running(0);

        let acquirer = {
            let scenario = Arc::clone(&scenario);
            thread::spawn(move || scenario.invoke_one(1))
        };

        scenario.release_hook(0);
        acquirer.join().unwrap();
        scenario.drain();
        scenario.check();
    });
}

/// Mutation 3: `Node::acquire_all` parks on the semaphore that refused while
/// keeping the counts it already took. Two tasks asking for the same two
/// semaphores in opposite orders then park holding one each, and neither
/// count is ever given back to wake the other.
#[test]
#[should_panic(expected = "every task of a constrained set runs exactly once")]
fn mutation_a_park_that_keeps_what_it_took_is_found() {
    loom::model(|| {
        let scenario = Scenario::opposite_orders(Protocol::ParkKeepingWhatItTook);
        let second = {
            let scenario = Arc::clone(&scenario);
            thread::spawn(move || scenario.invoke_one(1))
        };

        scenario.invoke_one(0);
        second.join().unwrap();
        scenario.drain();
        scenario.check();
    });
}

/// The control for mutation 3: a task with one semaphore to acquire has
/// nothing to give back, so the missing rollback is invisible to it. Mutation
/// 3 has to be failing for the rollback's sake and not for the park's.
#[test]
fn mutation_3s_park_is_sound_for_a_single_semaphore() {
    loom::model(|| {
        let scenario = Scenario::one_semaphore(Protocol::ParkKeepingWhatItTook, 1, 2);
        scenario.already_running(0);

        let acquirer = {
            let scenario = Arc::clone(&scenario);
            thread::spawn(move || scenario.invoke_one(1))
        };

        scenario.release_hook(0);
        acquirer.join().unwrap();
        scenario.drain();
        scenario.check();
    });
}
