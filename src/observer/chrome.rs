//! Translated from `taskflow/observer/chrome.hpp` (Taskflow v4.1.0).
//!
//! [`ChromeObserver`] is an [`Observer`] that
//! records one `(worker, task name, begin, end)` segment per task dispatch
//! and writes them out as a [Chrome trace-event] JSON array -- `"ph": "X"`
//! complete events, each carrying `pid`, `tid`, `ts` and `dur` -- the format
//! Chrome's own `about://tracing` (and Perfetto) load directly.
//!
//! ```
//! use rustflow::Executor;
//! use rustflow::observer::chrome::ChromeObserver;
//! use rustflow::prelude::*;
//!
//! let executor = Executor::new(2);
//! let observer = executor.make_observer(ChromeObserver::new());
//!
//! let mut taskflow = Taskflow::new();
//! taskflow.emplace(|| {});
//! executor.run(&taskflow).wait().expect("no task panicked");
//!
//! assert_eq!(observer.num_tasks(), 1);
//! assert!(observer.dump_to_string().starts_with('['));
//! ```
//!
//! [Chrome trace-event]: https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview
//!
//! # Design
//!
//! **A per-worker stack of begin timestamps, not one.** Upstream's own
//! `Timeline::stacks` exists for the same reason the `observer` module's
//! docs name: a [`Runtime::scope`](crate::Runtime::scope) or a `corun` loop
//! can dispatch another job from inside a worker thread already running
//! one, nesting an inner `on_entry`/`on_exit` pair inside the outer one on
//! the same worker. A stack -- `on_entry` pushes, `on_exit` pops -- pairs
//! each exit with the entry it belongs to regardless of nesting; a single
//! saved timestamp per worker would not.
//!
//! **A [`Mutex`] per worker, not upstream's bare vectors.** Every
//! [`Observer`] method takes `&self` and may run
//! on any worker at once -- the trait's own doc comment says so -- while
//! upstream's equivalent methods are `private` and called only from the one
//! worker `id` they are about, under no lock, because nothing else ever
//! touches `_timeline.segments[w]`. Rust has no way to grant one thread
//! exclusive access to one slot of a shared `Vec` without something that
//! enforces it; a `Mutex<WorkerTimeline>` per worker is that something, and
//! the worker index that already selects *which* mutex keeps the lock
//! itself uncontended between workers -- the same `w`-indexed independence
//! upstream relies on without a lock at all.
//!
//! **`dump` takes a [`Write`], not upstream's `ostream`.** [`std::io::Write`]
//! is the idiomatic equivalent, and threading it as `impl Write` rather than
//! `dyn Write` costs the caller nothing; [`dump_to_string`](ChromeObserver::dump_to_string)
//! is the `std::string`-returning overload upstream keeps alongside it,
//! built on top of [`dump`](ChromeObserver::dump) rather than duplicated.
//!
//! **Task names are JSON-escaped; upstream's are not.** A name upstream
//! writes raw into its stream breaks the output's own JSON syntax should it
//! ever contain a quote or a backslash; escaping the handful of characters
//! that matter is cheap enough that there is no reason to inherit that
//! sharp edge.

use std::io::{self, Write};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Instant;

use crate::observer::{Observer, TaskView, WorkerView};

/// One finished task execution: upstream's `ChromeObserver::Segment`.
struct Segment {
    /// The task's name, copied out of the [`TaskView`] that named it in
    /// `on_exit`: nothing a [`TaskView`] borrows outlives that call.
    name: String,
    /// When the task started running.
    begin: Instant,
    /// When it finished.
    end: Instant,
}

/// One worker's own timeline: the segments it has finished, and a stack of
/// begins for entries not yet matched with an exit.
///
/// Upstream's `ChromeObserver::Timeline` holds one `segments` vector and one
/// `stacks` vector, each indexed by worker; this is that pair's `w`th slot
/// pulled out into its own type, so each worker's slot can carry its own
/// [`Mutex`] -- see the module docs' "Design" section.
#[derive(Default)]
struct WorkerTimeline {
    /// Begin timestamps not yet popped by a matching `on_exit`, nested
    /// innermost-last so that a dispatch nested inside another task's body
    /// still pairs correctly.
    stack: Vec<Instant>,
    /// Finished segments, in the order `on_exit` recorded them.
    segments: Vec<Segment>,
}

/// An [`Observer`] that records a [Chrome trace-event] timeline: upstream's
/// `ChromeObserver`. See the module docs for the trace format and a worked
/// example.
#[derive(Default)]
pub struct ChromeObserver {
    /// The instant every segment's timestamp is reported relative to:
    /// upstream's `Timeline::origin`, set once in `set_up`.
    origin: OnceLock<Instant>,
    /// One timeline per worker, sized to the executor's worker count in
    /// `set_up` and never resized after.
    workers: OnceLock<Vec<Mutex<WorkerTimeline>>>,
}

impl ChromeObserver {
    /// A fresh observer that has recorded nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many task executions have been recorded since the last
    /// [`clear`](ChromeObserver::clear), summed over every worker. Zero
    /// before [`Observer::set_up`] has run. Upstream's `num_tasks`.
    #[must_use]
    pub fn num_tasks(&self) -> usize {
        self.workers.get().map_or(0, |workers| {
            workers
                .iter()
                .map(|worker| {
                    worker
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .segments
                        .len()
                })
                .sum()
        })
    }

    /// Discards every recorded segment, leaving
    /// [`num_tasks`](ChromeObserver::num_tasks) at zero. A no-op before
    /// [`Observer::set_up`] has run, there being nothing to discard yet.
    /// Upstream's `clear`.
    pub fn clear(&self) {
        let Some(workers) = self.workers.get() else {
            return;
        };
        for worker in workers {
            let mut timeline = worker.lock().unwrap_or_else(PoisonError::into_inner);
            timeline.segments.clear();
            timeline.stack.clear();
        }
    }

    /// Writes every recorded segment into `writer` as a [Chrome
    /// trace-event] JSON array of `"ph": "X"` complete events, one per task
    /// execution, in the order `set_up` numbered the workers and each
    /// worker recorded them. `"[]\n"` before [`Observer::set_up`] has run.
    /// Upstream's `dump(std::ostream&)`.
    ///
    /// [Chrome trace-event]: https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview
    ///
    /// # Errors
    ///
    /// Whatever writing to `writer` itself returns.
    pub fn dump(&self, writer: &mut impl Write) -> io::Result<()> {
        writer.write_all(b"[")?;
        let mut wrote_one = false;
        if let (Some(&origin), Some(workers)) = (self.origin.get(), self.workers.get()) {
            for (tid, worker) in workers.iter().enumerate() {
                let timeline = worker.lock().unwrap_or_else(PoisonError::into_inner);
                for (index, segment) in timeline.segments.iter().enumerate() {
                    if wrote_one {
                        writer.write_all(b",")?;
                    }
                    wrote_one = true;

                    let ts = segment.begin.saturating_duration_since(origin).as_micros();
                    let dur = segment
                        .end
                        .saturating_duration_since(segment.begin)
                        .as_micros();

                    write!(writer, "{{\"cat\":\"ChromeObserver\",\"name\":\"")?;
                    if segment.name.is_empty() {
                        write!(writer, "{tid}_{index}")?;
                    } else {
                        write_json_escaped(writer, &segment.name)?;
                    }
                    write!(
                        writer,
                        "\",\"ph\":\"X\",\"pid\":1,\"tid\":{tid},\"ts\":{ts},\"dur\":{dur}}}"
                    )?;
                }
            }
        }
        writer.write_all(b"]\n")?;
        Ok(())
    }

    /// [`dump`](ChromeObserver::dump) into a fresh `String`. Upstream's
    /// `dump() -> std::string` overload.
    #[must_use]
    pub fn dump_to_string(&self) -> String {
        let mut buffer = Vec::new();
        self.dump(&mut buffer)
            .expect("writing to a `Vec<u8>` buffer never fails");
        String::from_utf8(buffer).expect(
            "dump only ever writes ASCII JSON punctuation, decimal digits and escaped task names",
        )
    }
}

impl Observer for ChromeObserver {
    fn set_up(&self, num_workers: usize) {
        let _ = self.workers.set(
            (0..num_workers)
                .map(|_| Mutex::new(WorkerTimeline::default()))
                .collect(),
        );
        let _ = self.origin.set(Instant::now());
    }

    fn on_entry(&self, worker: WorkerView<'_>, _task: TaskView<'_>) {
        let workers = self
            .workers
            .get()
            .expect("Executor::make_observer calls set_up before registering this observer");
        let mut timeline = workers[worker.id()]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        timeline.stack.push(Instant::now());
    }

    fn on_exit(&self, worker: WorkerView<'_>, task: TaskView<'_>) {
        let workers = self
            .workers
            .get()
            .expect("Executor::make_observer calls set_up before registering this observer");
        let mut timeline = workers[worker.id()]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let begin = timeline
            .stack
            .pop()
            .expect("on_exit always follows a matching on_entry on the same worker");
        timeline.segments.push(Segment {
            name: task.name().to_string(),
            begin,
            end: Instant::now(),
        });
    }
}

/// Writes `s` into `writer` as the inside of a JSON string -- no
/// surrounding quotes -- escaping the characters JSON requires escaped. The
/// module docs' "Design" section says why upstream's unescaped write is not
/// repeated here.
fn write_json_escaped(writer: &mut impl Write, s: &str) -> io::Result<()> {
    for ch in s.chars() {
        match ch {
            '"' => writer.write_all(b"\\\"")?,
            '\\' => writer.write_all(b"\\\\")?,
            '\n' => writer.write_all(b"\\n")?,
            '\r' => writer.write_all(b"\\r")?,
            '\t' => writer.write_all(b"\\t")?,
            control if (control as u32) < 0x20 => write!(writer, "\\u{:04x}", control as u32)?,
            other => write!(writer, "{other}")?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh observer that was never registered reports nothing and dumps
    /// an empty array, rather than panicking for lack of `set_up`.
    #[test]
    fn an_unregistered_observer_is_empty() {
        let observer = ChromeObserver::new();
        assert_eq!(observer.num_tasks(), 0);
        assert_eq!(observer.dump_to_string(), "[]\n");
        observer.clear();
    }

    /// `clear` drops every recorded segment, and is itself safe to call on
    /// an observer that never recorded anything.
    #[test]
    fn clear_empties_a_set_up_observer() {
        let observer = ChromeObserver::new();
        observer.set_up(1);
        observer.clear();
        assert_eq!(observer.num_tasks(), 0);
        assert_eq!(observer.dump_to_string(), "[]\n");
    }

    /// `write_json_escaped` turns the characters JSON requires escaped into
    /// their escape sequences, and leaves everything else untouched.
    #[test]
    fn json_escaping_covers_quotes_backslashes_and_control_characters() {
        let mut buffer = Vec::new();
        write_json_escaped(&mut buffer, "a\"b\\c\nd\te").unwrap();
        assert_eq!(String::from_utf8(buffer).unwrap(), "a\\\"b\\\\c\\nd\\te");
    }
}
