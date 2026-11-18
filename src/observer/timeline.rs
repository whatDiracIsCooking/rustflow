//! The per-worker timeline both built-in observers record into: the segment
//! type, the stack of begin timestamps that pairs each `on_exit` with the
//! `on_entry` it belongs to, and the per-worker locking around the two.
//!
//! # Design
//!
//! **Written once here, where upstream copies it per observer.**
//! `ChromeObserver` and `TFProfObserver` each carry their own `Segment`,
//! their own `Timeline` of per-worker segment vectors and their own begin
//! stacks (`Timeline::stacks` in one, `_stacks` in the other). This module is
//! the part the two share, generic over `S` -- the store of finished segments
//! the observer keeps per worker, which is where they differ: one flat list
//! per worker for
//! [`ChromeObserver`](crate::observer::chrome::ChromeObserver), one list per
//! nesting level for
//! [`TfProfObserver`](crate::observer::tfprof::TfProfObserver). Everything
//! downstream of the store -- a JSON writer, a string table, a binary block,
//! a text report -- stays with the observer that owns the format.
//!
//! **A stack of begin timestamps per worker, not one timestamp.** A
//! [`Runtime::corun`](crate::Runtime::corun), a [`Subflow`](crate::Subflow)
//! that joins, or a module task waiting on its target dispatches another job
//! from inside a worker thread already running one, nesting an inner
//! `on_entry`/`on_exit` pair inside the outer one on the same worker. A stack
//! pairs each exit with its own entry regardless of that nesting, and the
//! depth left after the pop is the level the exit ran at -- how upstream
//! reads `_timeline.segments[w][_stacks[w].size()]`. One saved timestamp per
//! worker would give neither.
//!
//! **A [`Mutex`] per worker, not upstream's bare vectors.** Every
//! [`Observer`](crate::observer::Observer) method takes `&self` and may run
//! on any worker at once -- the trait's own doc comment says so -- while
//! upstream's equivalent methods are `private`, called only from the one
//! worker `id` they are about and under no lock, because nothing else ever
//! touches `_timeline.segments[w]`. Rust has no way to grant one thread
//! exclusive access to one slot of a shared `Vec` without something that
//! enforces it; a mutex per worker is that something, and the worker index
//! that already selects *which* mutex keeps the lock itself uncontended --
//! the same `w`-indexed independence upstream relies on without a lock at
//! all.

use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Instant;

/// One finished task execution: upstream's `Segment`, in both observers'
/// copies of it. `P` is whatever else the observer recording it keeps per
/// segment -- `()` where the name and the span are the whole of it.
pub(crate) struct Segment<P> {
    /// The task's name, copied out of the
    /// [`TaskView`](crate::observer::TaskView) that named it in `on_exit`:
    /// nothing a `TaskView` borrows outlives that call.
    pub(crate) name: String,
    /// What else the recording observer keeps about this execution.
    pub(crate) payload: P,
    /// When the task started running.
    pub(crate) begin: Instant,
    /// When it finished.
    pub(crate) end: Instant,
}

/// One worker's slot of a [`Timeline`]: the segments it has finished, in
/// whatever shape its observer keeps them, and the begins not yet matched by
/// an exit. Upstream's `Timeline::segments[w]` paired with its `w`th begin
/// stack.
#[derive(Default)]
pub(crate) struct WorkerTimeline<S> {
    /// Begin timestamps not yet popped by a matching
    /// [`Timeline::on_exit`], nested innermost-last.
    stack: Vec<Instant>,
    /// This worker's finished segments, in whatever shape its observer dumps
    /// from.
    pub(crate) store: S,
}

/// Every worker's [`WorkerTimeline`] plus the instant their timestamps are
/// reported relative to: the shared half of upstream's two `Timeline`
/// structs.
#[derive(Default)]
pub(crate) struct Timeline<S> {
    /// Upstream's `Timeline::origin`, fixed once by [`Timeline::set_up`].
    origin: OnceLock<Instant>,
    /// One slot per worker, sized by [`Timeline::set_up`] and never resized
    /// after, so that the index a caller locks stays the worker's own.
    slots: OnceLock<Vec<Mutex<WorkerTimeline<S>>>>,
}

impl<S: Default> Timeline<S> {
    /// Fixes the origin and sizes this timeline to `num_workers` slots: the
    /// shared half of both observers'
    /// [`Observer::set_up`](crate::observer::Observer::set_up). The first call
    /// wins, so an observer registered with a second executor keeps the
    /// timeline -- and the worker count -- its first registration set up.
    pub(crate) fn set_up(&self, num_workers: usize) {
        let _ = self.origin.set(Instant::now());
        let _ = self.slots.set(
            (0..num_workers)
                .map(|_| Mutex::new(WorkerTimeline::default()))
                .collect(),
        );
    }
}

impl<S> Timeline<S> {
    /// The instant [`set_up`](Timeline::set_up) fixed, or `None` while this
    /// timeline has never been set up -- an observer no executor ever
    /// registered.
    pub(crate) fn origin(&self) -> Option<Instant> {
        self.origin.get().copied()
    }

    /// How many slots [`set_up`](Timeline::set_up) sized this timeline to;
    /// zero before it has run.
    pub(crate) fn num_workers(&self) -> usize {
        self.slots.get().map_or(0, Vec::len)
    }

    /// Locks each worker's slot in turn, yielding its guard: one at a time, so
    /// a caller walking the whole timeline never holds two of these locks at
    /// once and no lock order between workers arises. Yields nothing before
    /// [`set_up`](Timeline::set_up) has run.
    pub(crate) fn workers(&self) -> impl Iterator<Item = MutexGuard<'_, WorkerTimeline<S>>> {
        self.slots
            .get()
            .into_iter()
            .flatten()
            .map(|slot| slot.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Records the begin timestamp that this dispatch's own `on_exit` will
    /// pop: the whole of both observers'
    /// [`Observer::on_entry`](crate::observer::Observer::on_entry). Panics if
    /// [`set_up`](Timeline::set_up) has not run, which
    /// [`Executor::make_observer`](crate::Executor::make_observer) does
    /// before any worker can reach this.
    pub(crate) fn on_entry(&self, worker: usize) {
        self.lock(worker).stack.push(Instant::now());
    }

    /// Pops the begin timestamp this dispatch's own `on_entry` pushed and
    /// hands `record` the worker's store, the nesting level this dispatch ran
    /// at, and the span it covered. The worker's lock is held for the call,
    /// so `record` must not take another of this timeline's locks. Panics if
    /// this exit has no matching entry on the same worker.
    pub(crate) fn on_exit(
        &self,
        worker: usize,
        record: impl FnOnce(&mut S, usize, Instant, Instant),
    ) {
        let mut timeline = self.lock(worker);
        let begin = timeline
            .stack
            .pop()
            .expect("on_exit always follows a matching on_entry on the same worker");
        let end = Instant::now();
        // The depth left after the pop is the level this dispatch ran at:
        // zero unless something of this worker's own preempted above it.
        let level = timeline.stack.len();
        record(&mut timeline.store, level, begin, end);
    }

    /// Discards everything recorded so far: clears each worker's begin stack
    /// and hands its store to `clear_store`. A no-op before
    /// [`set_up`](Timeline::set_up) has run, there being nothing to discard.
    pub(crate) fn clear(&self, mut clear_store: impl FnMut(&mut S)) {
        for mut timeline in self.workers() {
            timeline.stack.clear();
            clear_store(&mut timeline.store);
        }
    }

    /// Locks one worker's slot, recovering a poisoned lock's contents rather
    /// than propagating the panic: a worker that unwound mid-task leaves this
    /// timeline readable, and no later run is worth failing over it.
    fn lock(&self, worker: usize) -> MutexGuard<'_, WorkerTimeline<S>> {
        self.slots
            .get()
            .expect("Executor::make_observer calls set_up before registering this observer")[worker]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A timeline that was never set up yields no workers, reports no origin
    /// and clears without panicking -- the state an observer no executor ever
    /// registered is left in.
    #[test]
    fn a_timeline_that_was_never_set_up_is_empty() {
        let timeline: Timeline<Vec<Segment<()>>> = Timeline::default();
        assert_eq!(timeline.num_workers(), 0);
        assert!(timeline.origin().is_none());
        assert_eq!(timeline.workers().count(), 0);
        timeline.clear(Vec::clear);
    }

    /// `on_exit` pairs each exit with the entry it belongs to and reports the
    /// nesting level it ran at: two nested dispatches on one worker report
    /// level 1 then level 0, inner span inside outer.
    #[test]
    fn nested_dispatches_pair_innermost_first_and_report_their_level() {
        let timeline: Timeline<Vec<(usize, Instant, Instant)>> = Timeline::default();
        timeline.set_up(1);

        timeline.on_entry(0);
        timeline.on_entry(0);
        timeline.on_exit(0, |store, level, begin, end| {
            store.push((level, begin, end));
        });
        timeline.on_exit(0, |store, level, begin, end| {
            store.push((level, begin, end));
        });

        let recorded = timeline
            .workers()
            .next()
            .expect("set_up(1) left one worker")
            .store
            .clone();
        assert_eq!(recorded.len(), 2);
        let (inner_level, inner_begin, inner_end) = recorded[0];
        let (outer_level, outer_begin, outer_end) = recorded[1];
        assert_eq!((inner_level, outer_level), (1, 0));
        assert!(outer_begin <= inner_begin && inner_end <= outer_end);
    }

    /// `clear` empties both halves of a worker's slot: the recorded segments
    /// through the caller's own closure, and the begin stack of any entry
    /// still open, so that a later exit cannot pair with a stale begin.
    #[test]
    fn clear_empties_the_store_and_the_open_begin_stack() {
        let timeline: Timeline<Vec<usize>> = Timeline::default();
        timeline.set_up(2);

        timeline.on_entry(1);
        timeline.on_exit(1, |store, level, _begin, _end| store.push(level));
        timeline.on_entry(1);

        timeline.clear(Vec::clear);
        assert!(timeline.workers().all(|worker| worker.store.is_empty()));

        // The open entry's begin went with the clear, so the next exit runs
        // at level 0 rather than nested under a stale begin.
        timeline.on_entry(1);
        timeline.on_exit(1, |store, level, _begin, _end| store.push(level));
        let levels: Vec<usize> = timeline.workers().flat_map(|w| w.store.clone()).collect();
        assert_eq!(levels, vec![0]);
    }
}
