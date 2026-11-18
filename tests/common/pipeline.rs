//! Translated from `unittests/test_pipelines.cpp` (Taskflow v4.1.0).
//!
//! The scaffolding every untyped-`Pipeline` family in that file builds its
//! pipes out of: the `lines * pipes` scratch buffer upstream calls
//! `mybuffer`, and the three pipe bodies its families repeat verbatim.
//!
//! A topic of its own for the reason [`super`]'s docs give. The suites that
//! port the structural and composition families -- `tests/pipelines.rs` and
//! `tests/pipelines_compose.rs` -- call all of it; the nesting suite's own
//! pipes each branch or drive a sub-pipeline instead, so the builders and
//! [`Buffer::clear`] would have no caller there and that suite keeps its own
//! narrower buffer.
//!
//! # Design
//!
//! **One `Mutex` around the whole buffer.** Upstream's
//! `std::vector<std::array<int, P>> mybuffer(L)` is captured by reference by
//! every one of a pipeline's `mutable` lambdas and written by all of them at
//! once, which is sound there only because each writes a different line. A
//! [`Pipe`] callable here is `Fn(&mut Pipeflow) + Send + Sync + 'static`, so
//! the choice is a lock or `unsafe`; the lock is correctness rather than
//! contention, since a case's bodies touch the buffer a handful of times per
//! token.
//!
//! **Each builder takes its stopping point as a closure, not a number.** A
//! family that stops at a fixed `N` passes `move || n`; `Looping.Pipelines`,
//! whose condition task grows the count round by round, passes a read of the
//! `AtomicUsize` it advances. The alternative -- a `usize` parameter plus a
//! second builder for the looping case -- would have been two copies of each
//! body again, which is the thing this file exists to stop.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};

/// A pipeline's `lines * pipes` scratch buffer: upstream's
/// `std::vector<std::array<int, P>> mybuffer(L)`.
#[derive(Clone)]
pub struct Buffer {
    rows: Arc<Mutex<Vec<Vec<i32>>>>,
}

impl Buffer {
    /// A buffer of `num_lines` rows of `num_pipes` zeroed slots.
    pub fn new(num_lines: usize, num_pipes: usize) -> Self {
        Self {
            rows: Arc::new(Mutex::new(vec![vec![0; num_pipes]; num_lines])),
        }
    }

    /// Writes `value` into `line`'s `pipe` slot.
    pub fn set(&self, line: usize, pipe: usize, value: i32) {
        self.rows.lock().expect("uncontended")[line][pipe] = value;
    }

    /// Reads `line`'s `pipe` slot.
    pub fn get(&self, line: usize, pipe: usize) -> i32 {
        self.rows.lock().expect("uncontended")[line][pipe]
    }

    /// Zeroes every slot, between a case's two runs or a round and the next.
    pub fn clear(&self) {
        for row in self.rows.lock().expect("uncontended").iter_mut() {
            row.iter_mut().for_each(|slot| *slot = 0);
        }
    }
}

/// The first serial pipe nearly every family starts with: upstream's
/// `[N, &source, &j1, &mybuffer, L](auto& pf) mutable { if(j1 == N) {
/// pf.stop(); return; } ... }`.
///
/// Stops the pipeline once `counter` has reached `stop_at()`, and otherwise
/// checks the token reached the line its number says it should, writes
/// `counter + 1` into its own slot for the next pipe to read, and counts
/// itself. Upstream's own `source` vector is a `std::iota` from zero, so
/// `source[j1]` is `j1` and the vector itself has nothing left to carry.
pub fn counting_source(
    num_lines: usize,
    stop_at: impl Fn() -> usize + Send + Sync + 'static,
    counter: &Arc<AtomicUsize>,
    buffer: &Buffer,
) -> Pipe {
    let counter = Arc::clone(counter);
    let buffer = buffer.clone();
    Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
        let current = counter.load(Ordering::Relaxed);
        if current == stop_at() {
            pf.stop();
            return;
        }
        assert_eq!(pf.token() % num_lines, pf.line());
        let value = i32::try_from(current + 1).expect("a token count fits in an i32");
        buffer.set(pf.line(), pf.pipe(), value);
        counter.fetch_add(1, Ordering::Relaxed);
    })
}

/// A parallel pipe that collects what the pipe before it wrote: upstream's
/// `[N, &j2, &mybuffer, &collection, &mutex, L](auto& pf) mutable`.
///
/// `limit()` is the token count the family is running, and the ticket check
/// is upstream's `REQUIRE(j2 < N)` -- a `fetch_add` here where upstream's
/// own counter is a plain `size_t++`, since this pipe's tokens run at once.
/// What the collection then holds is a job for `common/tokens.rs`'s own
/// `assert_consecutive_from`: nothing orders a parallel pipe's own tokens.
pub fn collecting_pipe(
    num_lines: usize,
    limit: impl Fn() -> usize + Send + Sync + 'static,
    counter: &Arc<AtomicUsize>,
    buffer: &Buffer,
    collection: &Arc<Mutex<Vec<i32>>>,
) -> Pipe {
    let counter = Arc::clone(counter);
    let buffer = buffer.clone();
    let collection = Arc::clone(collection);
    Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
        let ticket = counter.fetch_add(1, Ordering::Relaxed);
        assert!(ticket < limit());
        let value = buffer.get(pf.line(), pf.pipe() - 1);
        assert_eq!(pf.token() % num_lines, pf.line());
        // The buffer is read before the collection is locked, so that no
        // body built here ever holds two of these locks at once.
        collection.lock().expect("uncontended").push(value);
    })
}

/// [`collecting_pipe`], plus the one step on: it writes what it read *plus
/// one* into its own slot, so that a pipe after it reads a value only this
/// one could have produced.
///
/// What a family puts after it tells the two apart: a serial pipe checking
/// `current + 2` pins that a parallel pipe in the middle left the pipes
/// around it serial, which is the whole of upstream's `3P(SPS)`.
pub fn propagating_pipe(
    num_lines: usize,
    limit: impl Fn() -> usize + Send + Sync + 'static,
    counter: &Arc<AtomicUsize>,
    buffer: &Buffer,
    collection: &Arc<Mutex<Vec<i32>>>,
) -> Pipe {
    let counter = Arc::clone(counter);
    let buffer = buffer.clone();
    let collection = Arc::clone(collection);
    Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
        let ticket = counter.fetch_add(1, Ordering::Relaxed);
        assert!(ticket < limit());
        let previous = buffer.get(pf.line(), pf.pipe() - 1);
        buffer.set(pf.line(), pf.pipe(), previous + 1);
        assert_eq!(pf.token() % num_lines, pf.line());
        collection.lock().expect("uncontended").push(previous);
    })
}
