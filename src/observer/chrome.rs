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
//! **The timeline itself is shared with [`TfProfObserver`](crate::observer::tfprof::TfProfObserver).**
//! Upstream gives each of its two observers a private copy of the per-worker
//! segment list, the stack of begins that pairs an exit with its entry, and
//! the locking those need once every [`Observer`] method takes `&self`; this
//! port has one `observer::timeline` module instead, whose own docs carry the
//! reasoning. What stays here is what is Chrome's alone: a flat list of
//! segments per worker, in the order its exits fired, and the JSON writer
//! below.
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

use crate::observer::timeline::{Segment, Timeline};
use crate::observer::{Observer, TaskView, WorkerView};

/// An [`Observer`] that records a [Chrome trace-event] timeline: upstream's
/// `ChromeObserver`. See the module docs for the trace format and a worked
/// example.
///
/// [Chrome trace-event]: https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview
#[derive(Default)]
pub struct ChromeObserver {
    /// Every worker's segments, each worker's own in the order its exits
    /// fired: upstream's `ChromeObserver::Timeline`. A segment carries no
    /// payload beyond its name and span, this trace format having nowhere to
    /// put one.
    timeline: Timeline<Vec<Segment<()>>>,
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
        self.timeline
            .workers()
            .map(|worker| worker.store.len())
            .sum()
    }

    /// Discards every recorded segment, leaving
    /// [`num_tasks`](ChromeObserver::num_tasks) at zero. A no-op before
    /// [`Observer::set_up`] has run, there being nothing to discard yet.
    /// Upstream's `clear`.
    pub fn clear(&self) {
        self.timeline.clear(Vec::clear);
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
        if let Some(origin) = self.timeline.origin() {
            for (tid, worker) in self.timeline.workers().enumerate() {
                for (index, segment) in worker.store.iter().enumerate() {
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
        self.timeline.set_up(num_workers);
    }

    fn on_entry(&self, worker: WorkerView<'_>, _task: TaskView<'_>) {
        self.timeline.on_entry(worker.id());
    }

    fn on_exit(&self, worker: WorkerView<'_>, task: TaskView<'_>) {
        // A nested dispatch's level is of no interest here: this trace
        // format is flat, and the event's own `ts` and `dur` already show a
        // nested segment sitting inside the one it preempted.
        self.timeline
            .on_exit(worker.id(), |segments, _level, begin, end| {
                segments.push(Segment {
                    name: task.name().to_string(),
                    payload: (),
                    begin,
                    end,
                });
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
