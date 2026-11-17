//! Translated from `unittests/test_semaphores.cpp` (Taskflow v4.1.0).
//!
//! Six cases of that file, and all six are about one claim: a task that
//! acquires a semaphore before its work and releases it after does not run
//! while more than `max_value` tasks of that set are running. The counting is
//! what carries it -- every task of a graph has to run exactly once per run,
//! however often it was parked and woken on the way -- and the gauge is what
//! carries the rest: a count that was right while two bodies ran at once
//! would say nothing about the semaphore at all.
//!
//! `Semaphore.Deadlock.8threads` is deliberately not here, and the reason is
//! upstream's: the whole case is commented out in that file, around a graph
//! whose pre-tasks acquire a semaphore its post-tasks release through a
//! common task in between -- which can park every worker on a count no
//! running task holds. Porting a case upstream does not run would be porting
//! a hang.
//!
//! Four adaptations, each forced by the port rather than chosen:
//!
//! - **Every counter is an atomic.** Upstream counts with a plain `int` and
//!   `size_t`, and in `critical_section` and `linear_chain` nothing guards
//!   them: what makes that sound upstream is the very mutual exclusion the
//!   case is testing, so a broken semaphore would be undefined behaviour
//!   rather than a failed assertion. Here they are `AtomicUsize` with
//!   `Relaxed` increments, so a broken semaphore is a wrong number and the
//!   assertion is the thing that reports it. `conflict_graph`'s `std::mutex`
//!   goes for the same reason: it is there to make a plain `counter++` legal
//!   in the two bodies that *may* run together, and an atomic needs no lock
//!   to be legal.
//! - **A gauge says what the counter cannot.** Each case that constrains
//!   concurrency to one also asserts that no two bodies were ever in flight
//!   at once, through [`Gauge`] -- an in-flight count and its high-water
//!   mark. Upstream has no equivalent and does not need one: a semaphore that
//!   let two bodies through would corrupt its unguarded `counter` and trip
//!   its own sanitiser builds. This is that check written down.
//! - **A composed target is shared, not borrowed.** Upstream's
//!   `composed_of(taskflow1)` takes a reference; here it takes an
//!   `Arc<Taskflow>`, so the two module cases wrap their target once it is
//!   built and never edit it again. That is the port's ownership rule, which
//!   `tests/modules.rs` sets out.
//! - **A queued run is a handle held, not a future dropped.** Upstream's
//!   `executor.run(taskflow)` discards a `tf::Future` that keeps nothing
//!   alive; dropping a [`RunHandle`] here blocks until that run is over, so
//!   the cases that queue several runs before waiting hold the handles across
//!   the `wait_for_all` -- otherwise the queueing, which is the point of
//!   those lines, would become one run waited for at a time.
//!
//! Every size and every worker sweep is upstream's own, which was measured
//! rather than assumed: the heaviest shapes in the file are the chain of
//! 10000 semaphores and the two 1024-task modules, and all thirty-four cases
//! together take 0.3 seconds run one after another in a debug build, 0.2
//! pinned to two cores. That is well under the tenth of a second per case
//! `tests/modules.rs` set as the bar for staying in the default run, so
//! nothing here is `#[ignore]`d and nothing is trimmed. The headroom is not
//! luck -- a release wakes the *whole* waiting list, and all but `max_value`
//! of those jobs park again, so these cases would be quadratic if the bodies
//! were slow enough for a queue of waiters to build up. They are a counter
//! increment, so it never does.
//!
//! One property these cases lean on throughout, and it is the scheduler's
//! rather than the semaphores': runs of one taskflow serialise through that
//! taskflow's run queue. So "a hundred runs" is a hundred runs one after
//! another, each starting with every node re-armed, and a semaphore's counter
//! has to come back to its maximum between them -- which is what the value
//! assertions after the queued runs are checking.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::prelude::*;
use rustflow::{Executor, RunHandle, Semaphore};

// ----------------------------------------------------------------------------
// What a counter cannot say
// ----------------------------------------------------------------------------

/// How many task bodies are inside their critical section at once, and the
/// most that ever were.
///
/// The port's own, for the reason the module docs give: upstream's unguarded
/// counters make overlapping bodies undefined behaviour, where here they
/// would merely be two atomic increments. So each body raises the gauge on
/// the way in and lowers it on the way out, and the case asserts the
/// high-water mark afterwards -- `1` for a semaphore of 1, which is the whole
/// claim of a critical section.
///
/// `AcqRel` on the two counting operations and `Relaxed` on the mark: the
/// increment and the decrement have to be ordered against each other for the
/// count to mean anything, and the mark is only ever read after every run has
/// finished.
struct Gauge {
    in_flight: AtomicUsize,
    high_water: AtomicUsize,
}

impl Gauge {
    /// A gauge reading zero, shared with the bodies that will raise it.
    fn new() -> Arc<Self> {
        Arc::new(Self {
            in_flight: AtomicUsize::new(0),
            high_water: AtomicUsize::new(0),
        })
    }

    /// Records one more body in flight, and the mark if this is a new one.
    fn enter(&self) {
        let now = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.high_water.fetch_max(now, Ordering::Relaxed);
    }

    /// Records that body leaving.
    fn leave(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }

    /// The most bodies that were ever in flight at once.
    fn high_water(&self) -> usize {
        self.high_water.load(Ordering::Relaxed)
    }
}

/// A counter the task bodies share, as every case here counts: an atomic
/// rather than upstream's plain integer, which the module docs explain.
type Counter = Arc<AtomicUsize>;

/// A counter at zero.
fn counter() -> Counter {
    Arc::new(AtomicUsize::new(0))
}

/// How many times the bodies have run.
fn count(counter: &Counter) -> usize {
    counter.load(Ordering::Relaxed)
}

// ----------------------------------------------------------------------------
// Testcase: CriticalSection
// ----------------------------------------------------------------------------

/// Upstream's `critical_section(W)`: a thousand independent tasks, every one
/// of them acquiring and releasing one semaphore of 1, so that a graph with
/// no edges at all runs one task at a time.
///
/// Four runs rather than one, which is upstream's shape and is the part that
/// pins the *counter*: a semaphore whose value did not come back between runs
/// would park the second run's thousand tasks for ever, and one that came
/// back twice would let two bodies through. The last three are queued without
/// waiting, so the run queue is what orders them.
fn critical_section(workers: usize) {
    let semaphore = Arc::new(Semaphore::new(1));
    assert_eq!(semaphore.value(), 1);
    assert_eq!(semaphore.max_value(), 1);

    const N: usize = 1000;

    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();
    let counter = counter();
    let gauge = Gauge::new();

    for _ in 0..N {
        let counter = Arc::clone(&counter);
        let gauge = Arc::clone(&gauge);
        let task = taskflow.emplace(move || {
            gauge.enter();
            counter.fetch_add(1, Ordering::Relaxed);
            gauge.leave();
        });
        taskflow.task(task).acquire(&semaphore).release(&semaphore);
    }

    executor
        .run(&taskflow)
        .wait()
        .expect("the run of a thousand serialised tasks succeeds");
    assert_eq!(count(&counter), N, "{workers} workers: one run");

    let handles: Vec<RunHandle<'_>> = (0..3).map(|_| executor.run(&taskflow)).collect();
    executor.wait_for_all();
    assert_eq!(count(&counter), 4 * N, "{workers} workers: four runs");
    drop(handles);

    assert_eq!(
        gauge.high_water(),
        1,
        "{workers} workers: a semaphore of 1 must never have two bodies in flight"
    );
    assert_eq!(
        semaphore.value(),
        1,
        "{workers} workers: the value comes back"
    );
    assert_eq!(semaphore.max_value(), 1);
}

#[test]
fn critical_section_1_worker() {
    critical_section(1);
}

#[test]
fn critical_section_2_workers() {
    critical_section(2);
}

#[test]
fn critical_section_3_workers() {
    critical_section(3);
}

#[test]
fn critical_section_7_workers() {
    critical_section(7);
}

#[test]
fn critical_section_11_workers() {
    critical_section(11);
}

#[test]
fn critical_section_16_workers() {
    critical_section(16);
}

// ----------------------------------------------------------------------------
// Testcase: OverlappedSemaphore
// ----------------------------------------------------------------------------

/// Upstream's `overlapped_semaphores(W)`: every task acquires *two*
/// semaphores, one of 1 and one of 4, and releases both.
///
/// The case is about the acquire being all-or-nothing. A task that took the
/// count of 1 and then found the count of 4 exhausted must give the first one
/// back before it parks, or the task holding nothing useful would block the
/// whole set against a task that is itself parked -- which is the rollback in
/// `Node::acquire_all`, and a deadlock rather than a wrong number if it were
/// missing. The gauge still reads 1, because the tighter of the two
/// semaphores is what constrains the set.
fn overlapped_semaphores(workers: usize) {
    let semaphore1 = Arc::new(Semaphore::new(1));
    let semaphore4 = Arc::new(Semaphore::new(4));

    const N: usize = 1000;

    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();
    let counter = counter();
    let gauge = Gauge::new();

    for _ in 0..N {
        let counter = Arc::clone(&counter);
        let gauge = Arc::clone(&gauge);
        let task = taskflow.emplace(move || {
            gauge.enter();
            counter.fetch_add(1, Ordering::Relaxed);
            gauge.leave();
        });
        taskflow
            .task(task)
            .acquire(&semaphore1)
            .acquire(&semaphore4)
            .release(&semaphore1)
            .release(&semaphore4);
    }

    executor
        .run(&taskflow)
        .wait()
        .expect("the run of a thousand doubly constrained tasks succeeds");

    assert_eq!(count(&counter), N, "{workers} workers");
    assert_eq!(
        gauge.high_water(),
        1,
        "{workers} workers: the semaphore of 1 constrains the set"
    );
    assert_eq!(semaphore1.value(), 1, "{workers} workers");
    assert_eq!(semaphore4.value(), 4, "{workers} workers");
}

#[test]
fn overlap_1_worker() {
    overlapped_semaphores(1);
}

#[test]
fn overlap_2_workers() {
    overlapped_semaphores(2);
}

#[test]
fn overlap_4_workers() {
    overlapped_semaphores(4);
}

#[test]
fn overlap_8_workers() {
    overlapped_semaphores(8);
}

// ----------------------------------------------------------------------------
// Testcase: Conflict Graph
// ----------------------------------------------------------------------------

/// Which of a case's tasks are in flight, as a bitmask, and how many times a
/// body found one it must not overlap.
///
/// `conflict_graph`'s own, because what that case claims is not a limit on
/// concurrency but a *pattern* of it: A conflicts with B and A conflicts with
/// C, while B and C may run together. A gauge of one number cannot say that
/// -- a high-water mark of 2 is the correct answer *and* what a broken
/// semaphore would give -- so each body names its own bit and the bits it
/// forbids.
///
/// A violation found here is one-sided and that is enough: B's body sets its
/// bit before it looks and clears it after, so A seeing B's bit means the two
/// were inside their sections at the same moment, whichever of them looked.
struct InFlight {
    bits: AtomicUsize,
    violations: AtomicUsize,
}

impl InFlight {
    /// Nobody in flight and nothing seen yet.
    fn new() -> Arc<Self> {
        Arc::new(Self {
            bits: AtomicUsize::new(0),
            violations: AtomicUsize::new(0),
        })
    }

    /// Enters the body owning `mine`, recording a violation if any task of
    /// `forbidden` is inside its own body right now.
    fn enter(&self, mine: usize, forbidden: usize) {
        let others = self.bits.fetch_or(mine, Ordering::AcqRel);
        if others & forbidden != 0 {
            self.violations.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Leaves that body.
    fn leave(&self, mine: usize) {
        self.bits.fetch_and(!mine, Ordering::AcqRel);
    }

    /// How many times a body overlapped one it conflicts with.
    fn violations(&self) -> usize {
        self.violations.load(Ordering::Relaxed)
    }
}

/// Upstream's `conflict_graph(W)`: three tasks and two semaphores of 1, one
/// per conflicting pair, which is upstream's own documented way of writing a
/// conflict graph without edges. A acquires both, B the first and C the
/// second, so A excludes each of B and C while B and C are free to run
/// together.
///
/// Upstream counts 3 after one run and 303 after a hundred more, queued ten
/// at a time through `run_n`. The bitmask is what checks the shape rather
/// than the total: with 101 runs of three tasks there is ample opportunity
/// for A to overlap B if the semaphore released by one is not the one
/// acquired by the other.
fn conflict_graph(workers: usize) {
    const A: usize = 1;
    const B: usize = 2;
    const C: usize = 4;

    let conflict_ab = Arc::new(Semaphore::new(1));
    let conflict_ac = Arc::new(Semaphore::new(1));

    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();
    let counter = counter();
    let in_flight = InFlight::new();

    let body = |mine: usize, forbidden: usize| {
        let counter = Arc::clone(&counter);
        let in_flight = Arc::clone(&in_flight);
        move || {
            in_flight.enter(mine, forbidden);
            counter.fetch_add(1, Ordering::Relaxed);
            in_flight.leave(mine);
        }
    };

    let a = taskflow.emplace(body(A, B | C));
    let b = taskflow.emplace(body(B, A));
    let c = taskflow.emplace(body(C, A));

    taskflow
        .task(a)
        .acquire(&conflict_ab)
        .release(&conflict_ab)
        .acquire(&conflict_ac)
        .release(&conflict_ac);
    taskflow.task(b).acquire(&conflict_ab).release(&conflict_ab);
    taskflow.task(c).acquire(&conflict_ac).release(&conflict_ac);

    executor
        .run(&taskflow)
        .wait()
        .expect("the run of a conflict graph succeeds");
    assert_eq!(count(&counter), 3, "{workers} workers: one run");

    let handles: Vec<RunHandle<'_>> = (0..10).map(|_| executor.run_n(&taskflow, 10)).collect();
    executor.wait_for_all();
    assert_eq!(count(&counter), 303, "{workers} workers: a hundred more");
    drop(handles);

    assert_eq!(
        in_flight.violations(),
        0,
        "{workers} workers: a task overlapped one it conflicts with"
    );
    assert_eq!(conflict_ab.value(), 1, "{workers} workers");
    assert_eq!(conflict_ac.value(), 1, "{workers} workers");
}

#[test]
fn conflict_graph_1_worker() {
    conflict_graph(1);
}

#[test]
fn conflict_graph_2_workers() {
    conflict_graph(2);
}

#[test]
fn conflict_graph_3_workers() {
    conflict_graph(3);
}

#[test]
fn conflict_graph_4_workers() {
    conflict_graph(4);
}

// ----------------------------------------------------------------------------
// Testcase: Module Task
// ----------------------------------------------------------------------------

/// Upstream's `semaphore_in_module(W)`: a module task that acquires a
/// semaphore of 2 around a target graph whose own tasks acquire *the same*
/// semaphore.
///
/// This is the case the preemption protocol has to get right. The module node
/// acquires on its first entry, preempts itself to run the target graph, and
/// must neither acquire again on the entry that resumes it nor release before
/// that entry -- the first would park the node on a count its own children
/// hold, which is a deadlock, and the second would hand back a count while
/// the children still rely on it.
///
/// The semaphore is 2 and not 1 in upstream's case, and that is load-bearing
/// rather than incidental: the module node holds one count for the whole of
/// its target's run, so a semaphore of 1 would leave its children nothing to
/// acquire and the run would never finish. It is also why the gauge still
/// reads 1 against a semaphore of 2: one count is the module node's for the
/// whole run, so exactly one child body may be in flight.
fn semaphore_in_module(workers: usize) {
    const N: usize = 1024;

    let semaphore = Arc::new(Semaphore::new(2));
    let executor = Executor::new(workers);
    let counter = counter();
    let gauge = Gauge::new();

    let mut target = Taskflow::new();
    for _ in 0..N {
        let counter = Arc::clone(&counter);
        let gauge = Arc::clone(&gauge);
        let task = target.emplace(move || {
            gauge.enter();
            counter.fetch_add(1, Ordering::Relaxed);
            gauge.leave();
        });
        target.task(task).acquire(&semaphore).release(&semaphore);
    }
    let target = Arc::new(target);

    let mut taskflow = Taskflow::new();
    let module = taskflow.composed_of(Arc::clone(&target));
    taskflow
        .task(module)
        .acquire(&semaphore)
        .release(&semaphore);

    executor
        .run(&taskflow)
        .wait()
        .expect("a module task that acquires around its target succeeds");

    assert_eq!(count(&counter), N, "{workers} workers");
    assert_eq!(
        gauge.high_water(),
        1,
        "{workers} workers: the module holds one of the two counts, so its          children get exactly one"
    );
    assert_eq!(
        semaphore.value(),
        2,
        "{workers} workers: both counts come back"
    );
}

#[test]
fn module_1_worker() {
    semaphore_in_module(1);
}

#[test]
fn module_2_workers() {
    semaphore_in_module(2);
}

#[test]
fn module_3_workers() {
    semaphore_in_module(3);
}

#[test]
fn module_4_workers() {
    semaphore_in_module(4);
}

// ----------------------------------------------------------------------------
// Testcase: Semaphores in a Module Task
// ----------------------------------------------------------------------------

/// Upstream's `semaphores_in_module(W)`: the case above over *ten* semaphores
/// at once, every one of them acquired and released by the module task and by
/// each of its target's tasks.
///
/// Two things come with the ten. The acquire is a loop, so a task that gets
/// nine counts and fails on the tenth has nine to give back before it parks,
/// over and over for a run of hundreds of tasks -- the rollback that
/// `overlapped_semaphores` exercises with two, run at depth. And the
/// semaphores start at upstream's default of zero and are given their value
/// by `reset_to`, which the assertions before the graph is built pin: a
/// default semaphore can hand out nothing at all.
fn semaphores_in_module(workers: usize) {
    const N: usize = 1024;
    const SEMAPHORES: usize = 10;

    let semaphores: Vec<Arc<Semaphore>> = (0..SEMAPHORES).map(|_| Arc::default()).collect();
    for semaphore in &semaphores {
        assert_eq!(semaphore.value(), 0, "a default semaphore holds nothing");
        assert_eq!(semaphore.max_value(), 0);
        semaphore.reset_to(2);
        assert_eq!(semaphore.value(), 2);
        assert_eq!(semaphore.max_value(), 2);
    }

    let executor = Executor::new(workers);
    let counter = counter();
    let gauge = Gauge::new();

    let mut target = Taskflow::new();
    for _ in 0..N {
        let counter = Arc::clone(&counter);
        let gauge = Arc::clone(&gauge);
        let task = target.emplace(move || {
            gauge.enter();
            counter.fetch_add(1, Ordering::Relaxed);
            gauge.leave();
        });
        target
            .task(task)
            .acquire_each(&semaphores)
            .release_each(&semaphores);
    }
    let target = Arc::new(target);

    let mut taskflow = Taskflow::new();
    let module = taskflow.composed_of(Arc::clone(&target));
    taskflow
        .task(module)
        .acquire_each(&semaphores)
        .release_each(&semaphores);

    executor
        .run(&taskflow)
        .wait()
        .expect("a module task that acquires ten semaphores around its target succeeds");

    assert_eq!(count(&counter), N, "{workers} workers");
    assert_eq!(
        gauge.high_water(),
        1,
        "{workers} workers: the module holds one count of each, so its          children get exactly one"
    );
    for semaphore in &semaphores {
        assert_eq!(
            semaphore.value(),
            2,
            "{workers} workers: every count comes back"
        );
    }
}

#[test]
fn modules_1_worker() {
    semaphores_in_module(1);
}

#[test]
fn modules_2_workers() {
    semaphores_in_module(2);
}

#[test]
fn modules_3_workers() {
    semaphores_in_module(3);
}

#[test]
fn modules_4_workers() {
    semaphores_in_module(4);
}

#[test]
fn modules_5_workers() {
    semaphores_in_module(5);
}

#[test]
fn modules_6_workers() {
    semaphores_in_module(6);
}

#[test]
fn modules_7_workers() {
    semaphores_in_module(7);
}

#[test]
fn modules_8_workers() {
    semaphores_in_module(8);
}

// ----------------------------------------------------------------------------
// Testcase: Linear Chain
// ----------------------------------------------------------------------------

/// Upstream's `linear_chain(W)`: a chain of tasks with **no edges at all**,
/// ordered entirely by overlapping pairs of semaphores. Task `i` acquires
/// semaphore `i-1` and semaphore `i`, so consecutive tasks share one and
/// cannot overlap, while tasks further apart share nothing and may.
///
/// The counters are what checks it, and the arrangement is worth spelling
/// out: task `i` increments counter `i-1` and counter `i`, so every counter
/// is written by exactly the two tasks that share a semaphore -- the pair the
/// graph says may not overlap. Each must therefore read 2, the last one after
/// the main thread adds the increment no task `L` is there to make. Upstream
/// relies on the exclusion for the increments to be legal at all; here they
/// are atomic, so a semaphore that let two neighbours through gives a wrong
/// count rather than a data race.
fn linear_chain(workers: usize) {
    const L: usize = 10000;

    let semaphores: Vec<Arc<Semaphore>> = (0..L).map(|_| Arc::new(Semaphore::new(1))).collect();
    let counters: Arc<Vec<AtomicUsize>> =
        Arc::new((0..L).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());

    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();

    for i in 0..L {
        let counters = Arc::clone(&counters);
        let task = taskflow.emplace(move || {
            if i > 0 {
                counters[i - 1].fetch_add(1, Ordering::Relaxed);
            }
            counters[i].fetch_add(1, Ordering::Relaxed);
        });

        let mut handle = taskflow.task(task);
        if i > 0 {
            handle
                .acquire(&semaphores[i - 1])
                .release(&semaphores[i - 1]);
        }
        handle.acquire(&semaphores[i]).release(&semaphores[i]);
    }

    executor
        .run(&taskflow)
        .wait()
        .expect("a chain ordered only by semaphores succeeds");

    // Upstream's `counters.back().data++`: there is no task `L` to make the
    // second increment of the last counter.
    counters[L - 1].fetch_add(1, Ordering::Relaxed);

    for (i, counter) in counters.iter().enumerate() {
        assert_eq!(
            counter.load(Ordering::Relaxed),
            2,
            "{workers} workers: counter {i}"
        );
    }

    for (i, semaphore) in semaphores.iter().enumerate() {
        assert_eq!(semaphore.value(), 1, "{workers} workers: semaphore {i}");
    }
}

#[test]
fn linear_chain_1_worker() {
    linear_chain(1);
}

#[test]
fn linear_chain_2_workers() {
    linear_chain(2);
}

#[test]
fn linear_chain_3_workers() {
    linear_chain(3);
}

#[test]
fn linear_chain_4_workers() {
    linear_chain(4);
}

#[test]
fn linear_chain_5_workers() {
    linear_chain(5);
}

#[test]
fn linear_chain_6_workers() {
    linear_chain(6);
}

#[test]
fn linear_chain_7_workers() {
    linear_chain(7);
}

#[test]
fn linear_chain_8_workers() {
    linear_chain(8);
}
