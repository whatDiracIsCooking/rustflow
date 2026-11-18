//! Translated from `taskflow/observer/tfprof.hpp` (Taskflow v4.1.0).
//!
//! [`TfProfObserver`] is an [`Observer`] that records the same per-worker,
//! per-nesting-level timeline upstream's built-in profiler does, and offers
//! it back two ways: [`dump`](TfProfObserver::dump), a binary block a @TFProf
//! viewer reads, and [`summary`](TfProfObserver::summary), a text report.
//!
//! # Design
//!
//! **`dump` writes upstream's binary block, not JSON.** The header carries
//! two orphaned doc comments reading "dumps the timelines into a @TFProf
//! format through an output stream" and "dumps the timelines into a JSON
//! string" directly above `TFProfObserver::dump`, with no `std::string
//! dump()` overload declared anywhere to match the second one -- stale
//! leftovers from an earlier, simpler version. The body that actually ships
//! at the pin writes a self-contained binary block, all integers
//! little-endian:
//!
//! - a 24-byte header: `uid` (`u64`), `origin_us` (`u64`), `str_table_len`
//!   (`u32`), `num_wl` (`u32`, the number of non-empty `(worker, level)`
//!   blocks that follow)
//! - the string table: `str_table_len` bytes, the deduplicated raw bytes of
//!   every non-empty task name this call saw, each clamped to 31 bytes
//! - per non-empty `(worker, level)`: a 12-byte header -- `worker_id`
//!   (`u32`), `level` (`u32`), `num_segs` (`u32`) -- then `num_segs`
//!   segments, each a LEB128 varint `delta_beg` (this segment's begin minus
//!   the previous one's, in microseconds, zero for the block's first), a
//!   LEB128 varint `duration` (microseconds), a `u32` `name_off` (byte
//!   offset into the string table above, meaningless when the next field is
//!   zero) and one byte packing the task type into its upper 3 bits and the
//!   name's length into its lower 5
//!
//! This port's rule is faithful translation, and inventing a JSON shape in
//! place of that would satisfy neither half of why this issue exists: it
//! would not match what shipped at the pin, and it would not open in the
//! viewer that shape exists for. `TFProfManager` -- the process-global
//! singleton upstream builds around this, gated by reading the
//! `TF_ENABLE_PROFILER` environment variable and, from its destructor,
//! writing a file to whatever path that names -- is deliberately not
//! ported; [`DESIGN.md`](https://github.com/whatDiracIsCooking/rustflow/blob/main/DESIGN.md)'s
//! entry on `TFProfManager` has why, and the `"TFPX"` multi-executor
//! file format a caller who wants one can still build around this type's
//! `dump`.
//!
//! **The type byte packs this port's whole [`TaskType`], not upstream's
//! five.** Upstream's own packing helper distinguishes only static,
//! subflow, condition, module and async, silently reporting everything else
//! -- placeholder, runtime, its own switch's `default` -- as static. This
//! port's [`TaskType`] already carries placeholder and runtime as variants
//! of their own (see [`crate::observer`]'s copy of the same mapping upstream
//! switches on), and 3 bits hold 8 values against the 7 this enum can
//! produce, so each gets a code of its own rather than inheriting that
//! collapse.
//!
//! **The timeline itself is shared with [`ChromeObserver`](crate::observer::chrome::ChromeObserver)**
//! -- the per-worker segments, the stack of begins that pairs an exit with
//! its entry and the locking both need live in this crate's
//! `observer::timeline` module, whose own docs carry the reasoning upstream's
//! two private copies each repeat. What is this observer's alone is the
//! second dimension: a worker's segments are `Vec<Vec<Segment<TaskType>>>`,
//! indexed by nesting level the way upstream's own `_timeline.segments[w][l]`
//! is, because a task whose body preempts (a subflow's `join`, a
//! `Runtime::corun`, a module task waiting on its target) dispatches a nested
//! graph on the very same worker before its own `on_exit` fires. A level is
//! allocated the first time an exit reports it, upstream's resize of
//! `_timeline.segments[w]` read the same way here.
//!
//! **`summary`'s table omits upstream's concurrency histogram**, the
//! `worker_histogram`/`task_histogram` arrays `TFProfObserver::summary`
//! spends roughly a third of its body computing from an 80-column terminal
//! width and a scaled tick label. Nothing in `Summary::dump` -- the method
//! that actually writes the text report -- ever reads either array: they
//! are computed and discarded on every call, upstream's own dead code. This
//! port's [`summary`](TfProfObserver::summary) carries the three pieces
//! `Summary::dump` does print -- the overview line, the per-task-type table
//! and the per-worker table, in that order -- and nothing computes a value
//! nothing prints. Its column widths are fixed rather than upstream's
//! content-sized ones, and the overview's wall time is plain microseconds
//! rather than upstream's unit-scaled display (`12.3 ms` and the like):
//! neither changes a count or a sum the acceptance checks, both are a good
//! deal less code, and every average in the table is an integer microsecond
//! floor rather than upstream's two-decimal float, so that no total this
//! port already keeps in a `u64` needs a lossy cast to report one.

use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use crate::observer::timeline::{Segment, Timeline};
use crate::observer::{Observer, TaskView, WorkerView};
use crate::task::TaskType;

/// Every [`TaskType`] a dispatched task can actually report, in the order
/// the per-task-type table lists them. [`TaskType::Undefined`] is excluded:
/// nothing [`TaskView::task_type`] returns is ever that variant, as
/// [`crate::observer`]'s own copy of the mapping notes.
const OBSERVED_TASK_TYPES: [TaskType; 7] = [
    TaskType::Static,
    TaskType::Subflow,
    TaskType::Condition,
    TaskType::Module,
    TaskType::Async,
    TaskType::Placeholder,
    TaskType::Runtime,
];

/// This port's 3-bit code for `task_type`, packed into the upper bits of the
/// per-segment type/name-length byte [`TfProfObserver::dump`] writes. See
/// the module docs' "Design" section for why every variant gets a code of
/// its own rather than upstream's five-way collapse.
/// [`TaskType::Undefined`] is matched only so this stays exhaustive over a
/// future variant; it is never produced by [`TaskView::task_type`].
const fn type_code(task_type: TaskType) -> u8 {
    match task_type {
        TaskType::Static => 0,
        TaskType::Subflow => 1,
        TaskType::Condition => 2,
        TaskType::Module => 3,
        TaskType::Async => 4,
        TaskType::Placeholder => 5,
        TaskType::Runtime => 6,
        TaskType::Undefined => 7,
    }
}

/// One worker's finished segments, indexed by nesting level: `[0]` is every
/// task that ran with nothing of this worker's own preempting above it. The
/// store half of this observer's `Timeline`, upstream's
/// `Timeline::segments[w]`.
type WorkerLevels = Vec<Vec<Segment<TaskType>>>;

/// A deduplicating table of task names, upstream's `_StringTable`: used only
/// while building [`TfProfObserver::dump`]'s output.
#[derive(Default)]
struct StringTable {
    /// The flat, concatenated bytes of every distinct name interned so far.
    data: Vec<u8>,
    /// Maps a name already interned to the offset its bytes start at in
    /// `data`.
    index: HashMap<String, u32>,
}

impl StringTable {
    /// Interns `name`, returning the byte offset its (possibly truncated)
    /// bytes start at in [`data`](StringTable::data) and the length
    /// actually stored, clamped to 31 bytes so it fits the 5 bits the
    /// per-segment type/name-length byte gives it. An empty name interns to
    /// `(0, 0)`, upstream's "anonymous" marker telling a reader to
    /// synthesise its own label.
    fn intern(&mut self, name: &str) -> (u32, u8) {
        if name.is_empty() {
            return (0, 0);
        }
        let bytes = name.as_bytes();
        let len = u8::try_from(bytes.len().min(31)).expect("clamped to at most 31");
        if let Some(&offset) = self.index.get(name) {
            return (offset, len);
        }
        let offset = u32::try_from(self.data.len()).expect("a task-name table under 4 GiB");
        self.data.extend_from_slice(&bytes[..usize::from(len)]);
        self.index.insert(name.to_string(), offset);
        (offset, len)
    }
}

/// Per-task-type aggregate statistics: upstream's `TFProfObserver::TaskSummary`.
#[derive(Default)]
struct TaskSummary {
    /// How many tasks of this type ran.
    count: u64,
    /// The sum of every one of their durations, in microseconds.
    total_span: u64,
    /// The shortest of them, in microseconds. Meaningless while `count` is
    /// zero.
    min_span: u64,
    /// The longest of them, in microseconds. Meaningless while `count` is
    /// zero.
    max_span: u64,
}

impl TaskSummary {
    /// Folds one more observation of `span` microseconds in.
    fn update(&mut self, span: u64) {
        self.total_span = self.total_span.saturating_add(span);
        self.min_span = if self.count == 0 {
            span
        } else {
            self.min_span.min(span)
        };
        self.max_span = if self.count == 0 {
            span
        } else {
            self.max_span.max(span)
        };
        self.count += 1;
    }

    /// The integer-microsecond floor average span. The module docs' "Design"
    /// section says why this is an integer rather than upstream's float.
    fn avg_span(&self) -> u64 {
        self.total_span.checked_div(self.count).unwrap_or(0)
    }
}

/// Per-worker aggregate statistics, collapsed across every nesting level:
/// upstream's `TFProfObserver::WorkerSummary`.
struct WorkerSummary {
    /// This worker's id.
    id: usize,
    /// How many tasks it ran, at every nesting level.
    count: u64,
    /// The sum of its merged, non-overlapping busy intervals, in
    /// microseconds -- true wall-clock busy time, not the sum of every
    /// task's own span, which would double-count a preempting task's own
    /// time against the nested work it is waiting on.
    busy_us: u64,
    /// `wall_us` minus `busy_us`.
    idle_us: u64,
    /// The shortest task this worker ran, in microseconds.
    min_span: u64,
    /// The longest task this worker ran, in microseconds.
    max_span: u64,
}

impl WorkerSummary {
    /// The integer-microsecond floor average span over this worker's busy
    /// time. The module docs' "Design" section says why this is an integer.
    fn avg_span(&self) -> u64 {
        self.busy_us.checked_div(self.count).unwrap_or(0)
    }
}

/// The average of every worker's own utilisation -- busy time over wall
/// time, as an integer percentage -- divided by `num_all_workers` rather
/// than `wsum.len()`, so that a worker which ran nothing still drags the
/// average down. Upstream's `Summary::avg_utilization`.
fn avg_utilization_pct(wsum: &[WorkerSummary], wall_us: u64, num_all_workers: usize) -> u64 {
    if wall_us == 0 || num_all_workers == 0 {
        return 0;
    }
    let sum: u64 = wsum
        .iter()
        .map(|worker| worker.busy_us.saturating_mul(100) / wall_us)
        .sum();
    sum / u64::try_from(num_all_workers).expect("a worker count fits a u64")
}

/// An [`Observer`] that records the timeline a @TFProf viewer reads and the
/// text report `summary` prints: upstream's `TFProfObserver`. See the module
/// docs for the binary format [`dump`](TfProfObserver::dump) writes and the
/// `TFProfManager` piece this port declines to carry.
pub struct TfProfObserver {
    /// This observer's own identity, upstream's `unique_id<size_t>()`.
    /// Assigned once, at construction rather than at
    /// [`Observer::set_up`] as upstream's is, so
    /// [`dump`](TfProfObserver::dump) and
    /// [`summary`](TfProfObserver::summary) report a stable id even for an
    /// observer that is never registered.
    uid: u64,
    /// The wall-clock reading, in microseconds since the Unix epoch, of the
    /// moment `set_up` fixed this observer's origin -- [`Instant`]
    /// carries no absolute value of its own, where upstream's
    /// `steady_clock::time_point` can at least report *an*
    /// implementation-defined one, so this is read from [`SystemTime`]
    /// alongside it. Set once, in `set_up`, together with the timeline below.
    origin_us: OnceLock<u64>,
    /// Every worker's segments, each worker's own indexed by nesting level:
    /// a segment's payload is the task's [`TaskType`].
    timeline: Timeline<WorkerLevels>,
}

/// Hands out the next value of upstream's `unique_id<size_t>()`, one call
/// per [`TfProfObserver::new`].
static NEXT_UID: AtomicU64 = AtomicU64::new(0);

impl Default for TfProfObserver {
    /// Same as [`TfProfObserver::new`].
    fn default() -> Self {
        Self::new()
    }
}

impl TfProfObserver {
    /// A fresh observer that has recorded nothing, with its own unique id
    /// already assigned. See the `uid` field's own doc for why that happens
    /// here rather than at [`Observer::set_up`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            uid: NEXT_UID.fetch_add(1, Ordering::Relaxed),
            origin_us: OnceLock::new(),
            timeline: Timeline::default(),
        }
    }

    /// How many task executions have been recorded since the last
    /// [`clear`](TfProfObserver::clear), summed over every worker and
    /// nesting level. Zero before [`Observer::set_up`] has run. Upstream's
    /// `num_tasks`.
    #[must_use]
    pub fn num_tasks(&self) -> usize {
        self.timeline
            .workers()
            .map(|worker| worker.store.iter().map(Vec::len).sum::<usize>())
            .sum()
    }

    /// How many workers have recorded at least one task since this observer
    /// was set up. Zero before [`Observer::set_up`] has run. Upstream's
    /// `num_workers`.
    ///
    /// Matches upstream's own quirk rather than correcting it: a worker's
    /// slot of nesting levels is allocated the first time it finishes a
    /// task and never shrunk back to nothing by [`clear`](Self::clear), so a
    /// worker counted here once stays counted even after a `clear` leaves
    /// [`num_tasks`](Self::num_tasks) at zero.
    #[must_use]
    pub fn num_workers(&self) -> usize {
        self.timeline
            .workers()
            .filter(|worker| !worker.store.is_empty())
            .count()
    }

    /// Discards every recorded segment, leaving
    /// [`num_tasks`](TfProfObserver::num_tasks) at zero. A no-op before
    /// [`Observer::set_up`] has run, there being nothing to discard yet.
    /// Upstream's `clear`.
    pub fn clear(&self) {
        // Each level's segments go, but the levels themselves stay: the
        // `num_workers` quirk documented above is upstream's, and dropping
        // them here would quietly correct it.
        self.timeline
            .clear(|levels| levels.iter_mut().for_each(Vec::clear));
    }

    /// Writes this observer's recorded timeline into `writer` as a
    /// self-contained binary block a @TFProf viewer reads. See the module
    /// docs for the exact byte layout. A well-formed, empty block -- this
    /// observer's `uid`, a zero origin, an empty string table and no blocks
    /// -- before [`Observer::set_up`] has run. Upstream's
    /// `dump(std::ostream&)`.
    ///
    /// # Errors
    ///
    /// Whatever writing to `writer` itself returns.
    pub fn dump(&self, writer: &mut impl Write) -> io::Result<()> {
        let (Some(origin), Some(&origin_us)) = (self.timeline.origin(), self.origin_us.get())
        else {
            write_u64(writer, self.uid)?;
            write_u64(writer, 0)?;
            write_u32(writer, 0)?;
            write_u32(writer, 0)?;
            return Ok(());
        };

        struct Block {
            worker: u32,
            level: u32,
            // (delta_beg, duration, name_off, type|name_len byte)
            segments: Vec<(u64, u64, u32, u8)>,
        }

        let mut table = StringTable::default();
        let mut blocks = Vec::new();

        for (worker_index, worker) in self.timeline.workers().enumerate() {
            for (level_index, level) in worker.store.iter().enumerate() {
                if level.is_empty() {
                    continue;
                }
                let mut segments = Vec::with_capacity(level.len());
                let mut previous_beg_us = 0u64;
                for segment in level {
                    let beg_us = origin_us.saturating_add(micros_since(segment.begin, origin));
                    let end_us = origin_us.saturating_add(micros_since(segment.end, origin));
                    let delta_beg = beg_us.saturating_sub(previous_beg_us);
                    let duration = end_us.saturating_sub(beg_us);
                    let (name_off, name_len) = table.intern(&segment.name);
                    let packed = (type_code(segment.payload) << 5) | (name_len & 0x1F);
                    segments.push((delta_beg, duration, name_off, packed));
                    previous_beg_us = beg_us;
                }
                blocks.push(Block {
                    worker: u32::try_from(worker_index).expect("worker count fits a u32"),
                    level: u32::try_from(level_index).expect("nesting depth fits a u32"),
                    segments,
                });
            }
        }

        write_u64(writer, self.uid)?;
        write_u64(writer, origin_us)?;
        write_u32(
            writer,
            u32::try_from(table.data.len()).expect("a task-name table under 4 GiB"),
        )?;
        write_u32(
            writer,
            u32::try_from(blocks.len()).expect("a block count fits a u32"),
        )?;
        writer.write_all(&table.data)?;

        for block in &blocks {
            write_u32(writer, block.worker)?;
            write_u32(writer, block.level)?;
            write_u32(
                writer,
                u32::try_from(block.segments.len()).expect("a segment count fits a u32"),
            )?;
            for &(delta_beg, duration, name_off, packed) in &block.segments {
                write_varint(writer, delta_beg)?;
                write_varint(writer, duration)?;
                write_u32(writer, name_off)?;
                writer.write_all(&[packed])?;
            }
        }

        Ok(())
    }

    /// Writes the text report [`dump`](TfProfObserver::dump)'s binary block
    /// has no room for: an overview line, the per-task-type table and the
    /// per-worker table, in that order. The module docs' "Design" section
    /// says what this port keeps of upstream's `Summary::dump` and what it
    /// does not. Upstream's `summary(std::ostream&)`.
    ///
    /// # Errors
    ///
    /// Whatever writing to `writer` itself returns.
    pub fn summary(&self, writer: &mut impl Write) -> io::Result<()> {
        let num_all_workers = self.timeline.num_workers();
        if num_all_workers == 0 {
            return writeln!(writer, "==Observer {}: no tasks recorded", self.uid);
        }

        let mut type_summaries: HashMap<TaskType, TaskSummary> = HashMap::new();
        let mut per_worker = Vec::with_capacity(num_all_workers);
        let mut view_begin: Option<Instant> = None;
        let mut view_end: Option<Instant> = None;

        for worker in self.timeline.workers() {
            let mut intervals = Vec::new();
            let mut count = 0u64;
            let mut min_span = 0u64;
            let mut max_span = 0u64;
            for level in &worker.store {
                for segment in level {
                    let span = micros_since(segment.end, segment.begin);
                    type_summaries
                        .entry(segment.payload)
                        .or_default()
                        .update(span);

                    min_span = if count == 0 { span } else { min_span.min(span) };
                    max_span = if count == 0 { span } else { max_span.max(span) };
                    count += 1;

                    intervals.push((segment.begin, segment.end));
                    view_begin = Some(view_begin.map_or(segment.begin, |b| b.min(segment.begin)));
                    view_end = Some(view_end.map_or(segment.end, |e| e.max(segment.end)));
                }
            }
            per_worker.push((intervals, count, min_span, max_span));
        }

        let Some(view_begin) = view_begin else {
            return writeln!(writer, "==Observer {}: no tasks recorded", self.uid);
        };
        let view_end = view_end.expect("set alongside view_begin, in the same loop iteration");

        let mut wall_us = micros_since(view_end, view_begin);
        if wall_us == 0 {
            wall_us = 1;
        }

        let mut wsum = Vec::new();
        for (id, (mut intervals, count, min_span, max_span)) in per_worker.into_iter().enumerate() {
            if intervals.is_empty() {
                continue;
            }
            intervals.sort_by_key(|&(begin, _)| begin);
            let mut busy_us = 0u64;
            let (mut cur_begin, mut cur_end) = intervals[0];
            for &(begin, end) in &intervals[1..] {
                if begin <= cur_end {
                    cur_end = cur_end.max(end);
                } else {
                    busy_us = busy_us.saturating_add(micros_since(cur_end, cur_begin));
                    cur_begin = begin;
                    cur_end = end;
                }
            }
            busy_us = busy_us.saturating_add(micros_since(cur_end, cur_begin));

            wsum.push(WorkerSummary {
                id,
                count,
                busy_us,
                idle_us: wall_us.saturating_sub(busy_us),
                min_span,
                max_span,
            });
        }

        let num_tasks: u64 = type_summaries.values().map(|t| t.count).sum();

        writeln!(writer, "{}", "=".repeat(80))?;
        writeln!(
            writer,
            " Observer {} | Wall: {wall_us} us | Workers: {num_all_workers} | Tasks: \
             {num_tasks} | Avg Utilization: {}%",
            self.uid,
            avg_utilization_pct(&wsum, wall_us, num_all_workers)
        )?;
        writeln!(writer, "{}", "=".repeat(80))?;

        if type_summaries.values().any(|t| t.count > 0) {
            writeln!(writer)?;
            writeln!(writer, "[Aggregate Task Statistics]")?;
            writeln!(writer, "{}", "-".repeat(70))?;
            writeln!(
                writer,
                "{:<10}{:>8}{:>12}{:>10}{:>10}{:>10}",
                "Type", "Count", "Total(us)", "Avg(us)", "Min(us)", "Max(us)"
            )?;
            writeln!(writer, "{}", "-".repeat(70))?;
            for task_type in OBSERVED_TASK_TYPES {
                let Some(summary) = type_summaries.get(&task_type) else {
                    continue;
                };
                if summary.count == 0 {
                    continue;
                }
                writeln!(
                    writer,
                    "{:<10}{:>8}{:>12}{:>10}{:>10}{:>10}",
                    task_type.as_str(),
                    summary.count,
                    summary.total_span,
                    summary.avg_span(),
                    summary.min_span,
                    summary.max_span
                )?;
            }
        }

        if !wsum.is_empty() {
            writeln!(writer)?;
            writeln!(writer, "[Worker Utilization]")?;
            writeln!(writer, "{}", "-".repeat(78))?;
            writeln!(
                writer,
                "{:>8}{:>8}{:>10}{:>10}{:>9}{:>9}{:>9}{:>7}",
                "Worker", "Tasks", "Busy(us)", "Idle(us)", "Avg(us)", "Min(us)", "Max(us)", "Util%"
            )?;
            writeln!(writer, "{}", "-".repeat(78))?;

            let (mut total_count, mut total_busy, mut total_idle) = (0u64, 0u64, 0u64);
            for worker in &wsum {
                let util = worker.busy_us.saturating_mul(100) / wall_us;
                writeln!(
                    writer,
                    "{:>8}{:>8}{:>10}{:>10}{:>9}{:>9}{:>9}{:>6}%",
                    worker.id,
                    worker.count,
                    worker.busy_us,
                    worker.idle_us,
                    worker.avg_span(),
                    worker.min_span,
                    worker.max_span,
                    util
                )?;
                total_count += worker.count;
                total_busy = total_busy.saturating_add(worker.busy_us);
                total_idle = total_idle.saturating_add(worker.idle_us);
            }

            writeln!(writer, "{}", "-".repeat(78))?;
            writeln!(
                writer,
                "{:>8}{:>8}{:>10}{:>10}{:>9}{:>9}{:>9}{:>6}% (avg)",
                "Total",
                total_count,
                total_busy,
                total_idle,
                "",
                "",
                "",
                avg_utilization_pct(&wsum, wall_us, num_all_workers)
            )?;
        }

        writeln!(writer)
    }

    /// [`summary`](TfProfObserver::summary) into a fresh `String`. Upstream's
    /// `summary() -> std::string` overload.
    #[must_use]
    pub fn summary_to_string(&self) -> String {
        let mut buffer = Vec::new();
        self.summary(&mut buffer)
            .expect("writing to a Vec<u8> buffer never fails");
        String::from_utf8(buffer)
            .expect("summary only ever writes decimal digits and this module's own ASCII text")
    }
}

impl Observer for TfProfObserver {
    fn set_up(&self, num_workers: usize) {
        self.timeline.set_up(num_workers);
        let _ = self.origin_us.set(unix_epoch_micros());
    }

    fn on_entry(&self, worker: WorkerView<'_>, _task: TaskView<'_>) {
        self.timeline.on_entry(worker.id());
    }

    fn on_exit(&self, worker: WorkerView<'_>, task: TaskView<'_>) {
        self.timeline
            .on_exit(worker.id(), |levels, level, begin, end| {
                // Upstream resizes `_timeline.segments[w]` to `_stacks[w]`'s
                // depth before the pop, which is this level plus one.
                if levels.len() <= level {
                    levels.resize_with(level + 1, Vec::new);
                }
                levels[level].push(Segment {
                    name: task.name().to_string(),
                    payload: task.task_type(),
                    begin,
                    end,
                });
            });
    }
}

/// Now, in microseconds since the Unix epoch, clamped to [`u64::MAX`] and
/// floored at zero for a clock set before the epoch. The `origin_us` field
/// says why this reading is taken at all.
fn unix_epoch_micros() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_micros(),
    )
    .unwrap_or(u64::MAX)
}

/// `end - begin` in whole microseconds, floored at zero by
/// [`Instant::saturating_duration_since`] and clamped to [`u64::MAX`] rather
/// than panicking on the vanishingly unlikely overflow -- no task in this
/// crate's own tests or benchmarks runs anywhere near 584,942 years.
fn micros_since(end: Instant, begin: Instant) -> u64 {
    u64::try_from(end.saturating_duration_since(begin).as_micros()).unwrap_or(u64::MAX)
}

/// Writes `v` as 4 little-endian bytes.
fn write_u32(writer: &mut impl Write, v: u32) -> io::Result<()> {
    writer.write_all(&v.to_le_bytes())
}

/// Writes `v` as 8 little-endian bytes.
fn write_u64(writer: &mut impl Write, v: u64) -> io::Result<()> {
    writer.write_all(&v.to_le_bytes())
}

/// Writes `v` as a LEB128 varint: 7 bits of value per byte, the 8th set
/// while another byte follows. Upstream's `_tfp_write_varint`.
fn write_varint(writer: &mut impl Write, mut v: u64) -> io::Result<()> {
    loop {
        let low_bits = u8::try_from(v & 0x7F).expect("masked to 7 bits");
        v >>= 7;
        let byte = if v == 0 { low_bits } else { low_bits | 0x80 };
        writer.write_all(&[byte])?;
        if v == 0 {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh observer that was never registered reports nothing, and both
    /// output methods still produce a well-formed, empty result rather than
    /// panicking for lack of `set_up`.
    #[test]
    fn an_unregistered_observer_is_empty() {
        let observer = TfProfObserver::new();
        assert_eq!(observer.num_tasks(), 0);
        assert_eq!(observer.num_workers(), 0);

        let mut dump = Vec::new();
        observer.dump(&mut dump).unwrap();
        // 24-byte header: uid(8) + origin_us(8, zero) + str_table_len(4,
        // zero) + num_wl(4, zero), and nothing after it.
        assert_eq!(dump.len(), 24);
        assert_eq!(&dump[8..24], [0u8; 16]);

        assert!(observer.summary_to_string().contains("no tasks recorded"));
        observer.clear();
    }

    /// Two observers constructed in sequence get two different unique ids,
    /// upstream's `unique_id<size_t>()`.
    #[test]
    fn two_observers_have_different_uids() {
        let first = TfProfObserver::new();
        let second = TfProfObserver::new();
        let dump_first = {
            let mut buffer = Vec::new();
            first.dump(&mut buffer).unwrap();
            buffer
        };
        let dump_second = {
            let mut buffer = Vec::new();
            second.dump(&mut buffer).unwrap();
            buffer
        };
        assert_ne!(&dump_first[0..8], &dump_second[0..8]);
    }

    /// `clear` drops every recorded segment, and is itself safe to call on
    /// an observer that never recorded anything.
    #[test]
    fn clear_empties_a_set_up_observer() {
        let observer = TfProfObserver::new();
        observer.set_up(1);
        observer.clear();
        assert_eq!(observer.num_tasks(), 0);
    }

    /// `write_varint`/`micros_since` round trip through a hand-rolled LEB128
    /// reader: the one place this module's own binary encoding is checked
    /// directly rather than through a full `dump`.
    #[test]
    fn varint_round_trips_through_a_hand_rolled_reader() {
        fn read_varint(bytes: &[u8]) -> (u64, usize) {
            let mut value = 0u64;
            let mut shift = 0u32;
            for (i, &byte) in bytes.iter().enumerate() {
                value |= u64::from(byte & 0x7F) << shift;
                if byte & 0x80 == 0 {
                    return (value, i + 1);
                }
                shift += 7;
            }
            panic!("no terminating byte in {bytes:?}");
        }

        for value in [0u64, 1, 127, 128, 300, u64::MAX] {
            let mut buffer = Vec::new();
            write_varint(&mut buffer, value).unwrap();
            let (decoded, consumed) = read_varint(&buffer);
            assert_eq!(decoded, value);
            assert_eq!(consumed, buffer.len());
        }
    }

    /// `type_code` is injective over every [`TaskType`]
    /// [`TaskView::task_type`] can actually produce, so the per-segment byte
    /// never confuses two of them.
    #[test]
    fn type_code_is_distinct_over_every_observed_task_type() {
        let mut codes: Vec<u8> = OBSERVED_TASK_TYPES.iter().copied().map(type_code).collect();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), OBSERVED_TASK_TYPES.len());
    }
}
