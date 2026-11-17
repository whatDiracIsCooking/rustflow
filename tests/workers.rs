//! Translated from `unittests/test_workers.cpp` (Taskflow v4.1.0).
//!
//! Upstream's two cases are `WorkerInterface.Basics`, run at one through eight
//! workers, and `Worker.Identifier`, and both translate directly -- the second
//! as of #94, which brought the `async` forms it asks its question from.
//!
//! Two cases are this file's own, and neither replaces anything:
//!
//! - [`Identifier`] asks `Worker.Identifier`'s question of a worker's
//!   *prologue* rather than of a task body. It was written when there was no
//!   way to put a closure on a worker at all, and it is kept because the
//!   prologue is the one place upstream never asks: a worker that did not yet
//!   know who it was while running its own prologue would pass the upstream
//!   case and fail this one. The handshake is what makes it possible to ask,
//!   since a prologue runs while the executor is still being built and so
//!   cannot be handed it as an argument.
//! - the last case is from the issue rather than from upstream: an executor
//!   that is created and dropped in a loop must not leak a thread.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use rustflow::{Executor, Worker, WorkerInterface};

/// How long a handshake in this file waits before it gives up and lets an
/// assertion report what went missing. Generous: it is only ever reached when
/// the test is about to fail, and it is what keeps a bug in the worker
/// lifecycle a failure rather than a hung test run.
const GIVE_UP_AFTER: Duration = Duration::from_secs(30);

/// Upstream's `CustomWorkerBehavior`: counts both hooks and records the id each
/// prologue was handed.
#[derive(Debug, Default)]
struct Counting {
    prologues: AtomicUsize,
    epilogues: AtomicUsize,
    panics: AtomicUsize,
    ids: Mutex<Vec<usize>>,
}

impl Counting {
    fn ids(&self) -> MutexGuard<'_, Vec<usize>> {
        self.ids.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl WorkerInterface for Counting {
    fn scheduler_prologue(&self, worker: &Worker) {
        self.prologues.fetch_add(1, Ordering::SeqCst);
        self.ids().push(worker.id());

        // Nothing can be scheduled yet, so a worker that reports work in its
        // own queue on the way in would mean the queues are being shared with
        // the wrong worker.
        assert!(
            worker.is_queue_empty(),
            "worker {} started with work",
            worker.id()
        );
    }

    fn scheduler_epilogue(&self, _worker: &Worker, panicked: Option<Box<dyn Any + Send>>) {
        if panicked.is_some() {
            self.panics.fetch_add(1, Ordering::SeqCst);
        }
        self.epilogues.fetch_add(1, Ordering::SeqCst);
    }
}

/// Upstream's `worker_interface_basics(W)`.
fn worker_interface_basics(workers: usize) {
    let counting = Arc::new(Counting::default());

    // The scope is upstream's: both hooks are only accounted for once the
    // executor has been dropped, because the epilogues run during the join.
    let interface = Arc::clone(&counting);
    drop(Executor::with_worker_interface(workers, interface));

    assert_eq!(
        counting.prologues.load(Ordering::SeqCst) + counting.epilogues.load(Ordering::SeqCst),
        workers * 2,
        "each hook runs exactly once per worker"
    );
    assert_eq!(counting.prologues.load(Ordering::SeqCst), workers);
    assert_eq!(counting.epilogues.load(Ordering::SeqCst), workers);
    assert_eq!(
        counting.panics.load(Ordering::SeqCst),
        0,
        "a clean shutdown hands the epilogue no panic"
    );

    let mut ids = counting.ids().clone();
    assert_eq!(ids.len(), workers);
    ids.sort_unstable();
    assert_eq!(
        ids,
        (0..workers).collect::<Vec<_>>(),
        "the ids handed to the prologues are not a permutation of 0..{workers}"
    );
}

#[test]
fn worker_interface_basics_1_thread() {
    worker_interface_basics(1);
}

#[test]
fn worker_interface_basics_2_threads() {
    worker_interface_basics(2);
}

#[test]
fn worker_interface_basics_3_threads() {
    worker_interface_basics(3);
}

#[test]
fn worker_interface_basics_4_threads() {
    worker_interface_basics(4);
}

#[test]
fn worker_interface_basics_5_threads() {
    worker_interface_basics(5);
}

#[test]
fn worker_interface_basics_6_threads() {
    worker_interface_basics(6);
}

#[test]
fn worker_interface_basics_7_threads() {
    worker_interface_basics(7);
}

#[test]
fn worker_interface_basics_8_threads() {
    worker_interface_basics(8);
}

/// What one worker saw when it asked who it was.
#[derive(Debug)]
struct Report {
    /// The id the prologue was handed.
    worker_id: usize,
    /// `Some(id)` if the handle arrived before [`GIVE_UP_AFTER`] ran out.
    handle: Option<Observed>,
}

/// The answers a worker got from the two executors it asked.
#[derive(Debug)]
struct Observed {
    own_id: Option<usize>,
    own_worker: Option<usize>,
    other_id: Option<usize>,
    other_worker_is_none: bool,
}

/// The handshake that puts upstream's `Worker.Identifier` checks on a worker
/// thread.
///
/// A worker's prologue runs while the executor is still being built, so it
/// cannot be handed the executor as an argument; it waits for the test to
/// publish one instead. The handle is a [`Weak`], because the executor holds
/// this interface: an `Arc` here would be a cycle, and a pool that is never
/// dropped is a pool that is never joined.
struct Identifier {
    /// An executor built *before* the one under test, which none of its
    /// workers belong to. Upstream asks four executors the same question; one
    /// other is enough to show that the map is per-executor and not a
    /// thread-local.
    other: Arc<Executor>,
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    handle: Option<Weak<Executor>>,
    reports: Vec<Report>,
}

impl Identifier {
    fn new(other: Arc<Executor>) -> Self {
        Self {
            other,
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Hands every waiting prologue the executor it belongs to.
    fn publish(&self, handle: Weak<Executor>) {
        self.lock().handle = Some(handle);
        self.changed.notify_all();
    }

    /// Blocks until `workers` prologues have reported, or until the patience
    /// runs out -- whereupon the caller's assertion on the count is what fails.
    fn wait_for_reports(&self, workers: usize) -> MutexGuard<'_, State> {
        let (state, _) = self
            .changed
            .wait_timeout_while(self.lock(), GIVE_UP_AFTER, |state| {
                state.reports.len() < workers
            })
            .unwrap_or_else(PoisonError::into_inner);
        state
    }
}

impl WorkerInterface for Identifier {
    fn scheduler_prologue(&self, worker: &Worker) {
        let (state, timed_out) = self
            .changed
            .wait_timeout_while(self.lock(), GIVE_UP_AFTER, |state| state.handle.is_none())
            .unwrap_or_else(PoisonError::into_inner);
        let handle = state.handle.clone();
        drop(state);

        let observed = (!timed_out.timed_out()).then(|| {
            let executor = handle
                .expect("the handle is published before the wait ends")
                .upgrade()
                .expect("the test holds the executor until every report is in");

            Observed {
                own_id: executor.this_worker_id(),
                own_worker: executor.this_worker().map(Worker::id),
                other_id: self.other.this_worker_id(),
                other_worker_is_none: self.other.this_worker().is_none(),
            }
        });

        let mut state = self.lock();
        state.reports.push(Report {
            worker_id: worker.id(),
            handle: observed,
        });
        self.changed.notify_all();
    }

    fn scheduler_epilogue(&self, _worker: &Worker, _panicked: Option<Box<dyn Any + Send>>) {}
}

/// `Worker.Identifier`'s question asked of a worker's prologue: a worker
/// finds itself in its own executor and nowhere else, and a thread that is
/// nobody's worker finds itself nowhere.
///
/// This port's own, and the module docs say why it is kept now that
/// `a_worker_finds_itself_in_its_own_executor_and_no_other` below asks
/// upstream's own version from a task body.
#[test]
fn a_workers_prologue_finds_itself_in_its_own_executor_and_no_other() {
    for workers in 1..=4 {
        let other = Arc::new(Executor::new(2));
        let identifier = Arc::new(Identifier::new(Arc::clone(&other)));
        let interface = Arc::clone(&identifier);
        let executor = Arc::new(Executor::with_worker_interface(workers, interface));
        identifier.publish(Arc::downgrade(&executor));

        // The thread running this test is a worker of neither executor, which
        // is upstream's `this_worker_id() == -1` from the main thread.
        assert_eq!(executor.this_worker_id(), None);
        assert!(executor.this_worker().is_none());
        assert_eq!(other.this_worker_id(), None);

        let state = identifier.wait_for_reports(workers);
        assert_eq!(
            state.reports.len(),
            workers,
            "only {} of {workers} workers reported in",
            state.reports.len()
        );

        let mut ids = Vec::with_capacity(workers);
        for report in &state.reports {
            let observed = report
                .handle
                .as_ref()
                .expect("a worker never received the executor handle");

            assert_eq!(
                observed.own_id,
                Some(report.worker_id),
                "a worker did not recognise itself"
            );
            assert_eq!(observed.own_worker, Some(report.worker_id));
            assert_eq!(
                observed.other_id, None,
                "a worker was recognised by an executor it does not belong to"
            );
            assert!(observed.other_worker_is_none);

            ids.push(report.worker_id);
        }

        ids.sort_unstable();
        assert_eq!(ids, (0..workers).collect::<Vec<_>>());

        // The reports are all in, so nothing is holding the handle any more.
        drop(state);
        drop(executor);
    }
}

/// How many rounds upstream's `Worker.Identifier` submits of each of its four
/// forms: its `for(size_t i=0; i<1000; ++i)`.
const IDENTIFIER_ROUNDS: usize = 1000;

/// Upstream's four executors, each with this many workers.
///
/// Upstream default-constructs them, which is one pool per hardware thread
/// four times over. Two apiece is enough for the question -- a worker of one
/// pool is asked about all four -- and keeps the eight threads this case
/// starts from crowding out the rest of the binary.
const IDENTIFIER_WORKERS: usize = 2;

/// Upstream's body, once per executor: the thread running it is a worker of
/// `mine` and of none of the others.
///
/// Upstream writes the four checks out four times with the `!= nullptr` moved
/// along one line each; this is that table read from `mine` instead. The
/// counter is what reports a body whose assertions were *dropped* rather than
/// raised -- `executor.silent_async` has nowhere to report a panic to, so the
/// silent form of this case would otherwise pass in silence. A count short of
/// [`IDENTIFIER_ROUNDS`] is that body having panicked, and the panic itself
/// is on stderr.
fn finds_itself(
    executors: &[Arc<Executor>],
    mine: usize,
    ran: &Arc<AtomicUsize>,
) -> impl Fn() + Clone + Send + 'static {
    let executors: Vec<Arc<Executor>> = executors.to_vec();
    let ran = Arc::clone(ran);
    move || {
        for (index, executor) in executors.iter().enumerate() {
            let id = executor.this_worker_id();
            let worker = executor.this_worker().map(Worker::id);
            if index == mine {
                assert!(
                    id.is_some(),
                    "executor {index} did not recognise its own worker"
                );
                assert_eq!(
                    worker, id,
                    "executor {index} named one worker by id and another by handle"
                );
            } else {
                assert_eq!(
                    id, None,
                    "executor {index} recognised a worker of executor {mine}"
                );
                assert!(worker.is_none());
            }
        }
        ran.fetch_add(1, Ordering::Relaxed);
    }
}

/// Upstream's `Worker.Identifier`: a worker finds itself in its own executor
/// and nowhere else, and a thread that is nobody's worker finds itself
/// nowhere.
///
/// Upstream's four forms are kept as four, one per executor -- `async`,
/// `silent_async` and `dependent_async` twice -- because what the case is
/// about is the executor-to-worker map rather than the submission: every one
/// of them puts a body on a worker of its own pool, and all four have to
/// agree about which pool that is. A thread-local "current worker" would pass
/// the first form and fail the other three.
///
/// The handles are dropped as upstream drops its futures, so the two counts
/// `wait_for_all` reads are what wait for the four thousand bodies.
#[test]
fn a_worker_finds_itself_in_its_own_executor_and_no_other() {
    let executors: Vec<Arc<Executor>> = (0..4)
        .map(|_| Arc::new(Executor::new(IDENTIFIER_WORKERS)))
        .collect();
    let ran: Vec<Arc<AtomicUsize>> = (0..executors.len())
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();

    // The thread running this test is a worker of none of them, which is
    // upstream's `this_worker_id() == -1` from the main thread.
    for (index, executor) in executors.iter().enumerate() {
        assert_eq!(executor.this_worker_id(), None, "executor {index}");
        assert!(executor.this_worker().is_none(), "executor {index}");
    }

    for _ in 0..IDENTIFIER_ROUNDS {
        executors[0].spawn(finds_itself(&executors, 0, &ran[0]));
        executors[1].silent_async(finds_itself(&executors, 1, &ran[1]));
        executors[2].dependent_async(finds_itself(&executors, 2, &ran[2]), []);
        executors[3].dependent_async(finds_itself(&executors, 3, &ran[3]), []);
    }

    for executor in &executors {
        executor.wait_for_all();
    }

    for (index, ran) in ran.iter().enumerate() {
        assert_eq!(
            ran.load(Ordering::Relaxed),
            IDENTIFIER_ROUNDS,
            "executor {index}: a body did not reach its end"
        );
    }
}

/// The process's thread count, per `/proc/self/status`, or `None` where there
/// is no procfs to read it from.
#[cfg(target_os = "linux")]
fn thread_count() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").expect("procfs is readable on Linux");
    status
        .lines()
        .find_map(|line| line.strip_prefix("Threads:"))
        .and_then(|count| count.trim().parse().ok())
}

#[cfg(not(target_os = "linux"))]
fn thread_count() -> Option<usize> {
    None
}

/// Create-and-drop in a loop: the shutdown path has to join every worker of
/// every executor, every time.
///
/// The counts are the real assertion, and they are exact: a worker reaches its
/// epilogue only by leaving the scheduling loop, and `drop` returns only after
/// it has joined every thread, so a worker that was left asleep on the notifier
/// shows up here as a missing epilogue -- or, if it is asleep for good, as the
/// run timing out rather than this test failing.
#[test]
fn creating_and_dropping_executors_joins_every_worker() {
    const ROUNDS: usize = 100;

    let before = thread_count();

    for workers in 1..=8 {
        let counting = Arc::new(Counting::default());
        for _ in 0..ROUNDS {
            let interface = Arc::clone(&counting);
            drop(Executor::with_worker_interface(workers, interface));
        }

        assert_eq!(counting.prologues.load(Ordering::SeqCst), workers * ROUNDS);
        assert_eq!(counting.epilogues.load(Ordering::SeqCst), workers * ROUNDS);
        assert_eq!(counting.panics.load(Ordering::SeqCst), 0);
    }

    // 3600 threads were spawned above. The bound is a bound rather than an
    // equality because the other tests in this binary run alongside this one
    // and hold executors of their own while it reads the counter; what it
    // catches is a leak, and a leak here is measured in thousands.
    const SIBLING_SLACK: usize = 64;
    if let (Some(before), Some(after)) = (before, thread_count()) {
        assert!(
            after <= before + SIBLING_SLACK,
            "thread count went from {before} to {after}"
        );
    }
}
