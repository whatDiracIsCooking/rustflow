//! Translated from `taskflow/core/semaphore.hpp` (Taskflow v4.1.0).
//!
//! A semaphore is a concurrency constraint over a *set* of tasks rather than a
//! lock a task takes: a task that acquires one before its work and releases it
//! after limits how many tasks of that set may be running at once. Upstream's
//! five-task example with a semaphore of 1 is the whole idea -- five
//! independent tasks that nonetheless run one at a time.
//!
//! # Design
//!
//! - The waiting list holds `Job`s where upstream's holds `Node*`, so that a
//!   parked task keeps the run, the graph and the node it names alive for as
//!   long as it sits in the list: `DESIGN.md` on semaphore waiters.
//! - `try_acquire_or_wait` takes the job by reference and answers with a
//!   `bool`, cloning into the waiting list only when it parks. Handing the
//!   job back for the caller to park cannot work: the counter test and the
//!   park have to be one critical section, or a release landing between them
//!   wakes nothing and leaves the job parked on a free semaphore. The caller
//!   keeps its own job, which is what it needs on the path where the acquire
//!   *succeeds* anyway.
//! - Releasing past the maximum is a `SemaphoreError` rather than upstream's
//!   `TF_THROW`: what a task that over-releases has done is the executor's
//!   decision, not this module's. The error is `pub(crate)`, accessor and all,
//!   because nothing public hands one out: the only release a caller can reach
//!   is a task's own, through [`TaskMut::release`](crate::TaskMut::release),
//!   and `src/executor/worker.rs` turns the failure of one into the panic
//!   payload the run reports -- the text of this error, not the value. So the
//!   number reaches a caller through `Display`, and neither the type nor the
//!   accessor has a caller outside the crate to serve.
//! - Every query takes the lock, including [`Semaphore::max_value`], which
//!   upstream reads unlocked even though `reset` writes it.

use core::fmt;
use std::mem;
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::executor::topology::Job;

/// A concurrency constraint over a set of tasks: upstream's `tf::Semaphore`.
///
/// A task acquires a semaphore before its work and releases it after -- see
/// [`TaskMut::acquire`](crate::TaskMut::acquire) and
/// [`TaskMut::release`](crate::TaskMut::release) -- and the count the
/// semaphore starts with is how many of those tasks may run at once. A task
/// that cannot get the count it asked for does not run and does not spin: it
/// is parked on the semaphore and re-scheduled by whoever releases.
///
/// The counter and the parked tasks are behind one `Mutex`, as upstream has
/// one `_mtx`, and every method takes `&self`: a semaphore is shared by the
/// tasks it constrains, as an `Arc<Semaphore>` the graph holds a clone of per
/// task, so nothing can hand out a `&mut`.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use rustflow::Semaphore;
///
/// let semaphore = Arc::new(Semaphore::new(2));
/// assert_eq!(semaphore.value(), 2);
/// assert_eq!(semaphore.max_value(), 2);
/// ```
pub struct Semaphore {
    inner: Mutex<Inner>,
}

/// Everything upstream keeps under its one `_mtx`, as one struct so that the
/// lock covers it by construction.
#[derive(Default)]
struct Inner {
    /// Upstream's `_cur_value`: how much of the semaphore is still to be had.
    value: usize,
    /// Upstream's `_max_value`: what a release may not push the counter past.
    max_value: usize,
    /// Upstream's `_waiters`, as jobs rather than node pointers -- the module
    /// docs say why.
    waiters: Vec<Job>,
}

impl Semaphore {
    /// A semaphore with `max_value` to give out, which is upstream's
    /// `explicit Semaphore(size_t)`.
    ///
    /// The current value starts at the maximum, so `max_value` tasks of the
    /// constrained set may run at once.
    #[must_use]
    pub fn new(max_value: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                value: max_value,
                max_value,
                waiters: Vec::new(),
            }),
        }
    }

    /// Returns the current counter value, upstream's `value()`.
    #[must_use]
    pub fn value(&self) -> usize {
        self.lock().value
    }

    /// Returns the largest value this semaphore may hold, upstream's
    /// `max_value()`.
    #[must_use]
    pub fn max_value(&self) -> usize {
        self.lock().max_value
    }

    /// Returns the counter to its maximum and drops the parked tasks,
    /// upstream's `reset()`.
    ///
    /// Dropping the parked tasks is upstream's behaviour and is only safe
    /// between runs: a parked job *is* the scheduling of a task that has not
    /// run, so a reset during a run of the constrained graph leaves that task
    /// unscheduled for good and the run unable to finish.
    pub fn reset(&self) {
        let mut inner = self.lock();
        inner.value = inner.max_value;
        Self::drop_waiters(inner);
    }

    /// Resets the counter to `new_max_value`, which becomes both the current
    /// and the maximum value, and drops the parked tasks: upstream's
    /// `reset(size_t)`.
    ///
    /// [`Semaphore::reset`] has the caveat about resetting a semaphore a run
    /// is using; it applies here too.
    pub fn reset_to(&self, new_max_value: usize) {
        let mut inner = self.lock();
        inner.max_value = new_max_value;
        inner.value = new_max_value;
        Self::drop_waiters(inner);
    }

    /// Takes one from the counter if there is one to take, and otherwise parks
    /// `job` to be re-scheduled by a later release: upstream's
    /// `_try_acquire_or_wait`.
    ///
    /// `true` means the count was taken and the caller's task may run; `false`
    /// means a clone of `job` is now on the waiting list and the caller must
    /// *not* run that task. The module docs say why the shape is this rather
    /// than one that hands the job back.
    pub(crate) fn try_acquire_or_wait(&self, job: &Job) -> bool {
        let mut inner = self.lock();
        if inner.value > 0 {
            inner.value -= 1;
            true
        } else {
            inner.waiters.push(job.clone());
            false
        }
    }

    /// Gives one back to the counter and moves every parked task into `woken`
    /// for the caller to schedule: upstream's `_release`.
    ///
    /// `woken` is appended to rather than replaced, one task being able to
    /// release several semaphores for the caller to schedule at once:
    /// upstream's `dst`, including its swap when `dst` is still empty, which
    /// steals the waiting list's allocation instead of copying out of it.
    ///
    /// # Errors
    ///
    /// Returns [`SemaphoreError`] if the counter is already at its maximum, so
    /// that this release would push it past: upstream's *"can't release the
    /// semaphore more than its maximum value"*. The counter and the waiting
    /// list are left untouched in that case.
    pub(crate) fn release(&self, woken: &mut Vec<Job>) -> Result<(), SemaphoreError> {
        let mut inner = self.lock();

        if inner.value >= inner.max_value {
            return Err(SemaphoreError {
                max_value: inner.max_value,
            });
        }

        inner.value += 1;

        if woken.is_empty() {
            mem::swap(woken, &mut inner.waiters);
        } else {
            woken.append(&mut inner.waiters);
        }

        Ok(())
    }

    /// How many tasks are parked on this semaphore.
    ///
    /// Not upstream's API, which has no query for its `_waiters`, and
    /// `cfg(test)` because a count another thread can change the instant it
    /// is read answers nothing a caller could act on. The unit tests of this
    /// module and of [`crate::graph`] drive the semaphore from one thread and
    /// have no other window onto the list.
    #[cfg(test)]
    pub(crate) fn num_waiters(&self) -> usize {
        self.lock().waiters.len()
    }

    /// The one lock, taken the way the rest of the crate takes a lock whose
    /// data cannot be left inconsistent by a panic: a poisoned semaphore is
    /// still a counter and a list of jobs.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Empties the waiting list, dropping the jobs *after* the lock is
    /// released: a [`Job`] owns a share of its run and of the graph it
    /// indexes, so dropping the last one can run the tear-down of a whole
    /// taskflow's storage, and none of that wants to happen under a lock a
    /// worker may be waiting on.
    fn drop_waiters(mut inner: MutexGuard<'_, Inner>) {
        let waiters = mem::take(&mut inner.waiters);
        drop(inner);
        drop(waiters);
    }
}

impl Default for Semaphore {
    /// A semaphore of zero, as upstream's defaulted constructor gives: nothing
    /// can be acquired from it until [`Semaphore::reset_to`] gives it a value.
    fn default() -> Self {
        Self::new(0)
    }
}

impl fmt::Debug for Semaphore {
    /// Hand-written because the state is behind a lock and a `Job` has no
    /// `Debug`, so the waiting list is printed as its length.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.lock();
        f.debug_struct("Semaphore")
            .field("value", &inner.value)
            .field("max_value", &inner.max_value)
            .field("waiters", &inner.waiters.len())
            .finish()
    }
}

/// A release that would push a semaphore's counter past its maximum.
///
/// Upstream throws this -- `TF_THROW("can't release the semaphore more than
/// its maximum value: ", _max_value)` -- out of the release itself. Here the
/// release hands it back, because the two callers answer for it differently:
/// a task that over-releases is a failure of *that task*, which is the
/// executor's business, while a rollback inside
/// [`Node::acquire_all`](crate::Node) cannot be over-releasing at all.
///
/// One way to fail, so a struct rather than an enum, carrying the maximum --
/// the number upstream prints and what makes the message actionable.
///
/// Not `pub`: the `# Design` section above has why.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SemaphoreError {
    max_value: usize,
}

impl SemaphoreError {
    /// The maximum value the release would have gone past, which is what
    /// [`Display`](fmt::Display) prints.
    #[must_use]
    pub(crate) const fn max_value(self) -> usize {
        self.max_value
    }
}

impl fmt::Display for SemaphoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "can't release the semaphore more than its maximum value: {}",
            self.max_value()
        )
    }
}

impl std::error::Error for SemaphoreError {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::executor::topology::{GraphOwner, Parent, Topology};
    use crate::graph::Handle;
    use crate::taskflow::Taskflow;

    /// One job per name, each the job of a source task of a taskflow that is
    /// never run.
    ///
    /// A [`Job`] keeps no name of its own, so the node's is what a case here
    /// recognises a parked or woken job by: `job.node().name()`. The taskflow
    /// itself is dropped on return and the jobs are what keeps its storage
    /// alive, which is the property that makes a waiting list of jobs sound in
    /// the first place.
    fn jobs(names: &[&str]) -> Vec<Job> {
        let mut taskflow = Taskflow::new();
        for name in names {
            taskflow
                .graph_mut()
                .add_named_node(*name, Handle::Placeholder);
        }

        // Every node is a source, so the sources come out in arena order,
        // which is the order the names were added in.
        let flow = Arc::clone(taskflow.flow());
        let topology = Arc::new(Topology::new(|| true, || {}, Arc::clone(&flow)));
        let jobs: Vec<Job> =
            Job::sources(&GraphOwner::flow(&flow), &topology, &Parent::Run).collect();

        let found: Vec<&str> = jobs.iter().map(|job| job.node().name()).collect();
        assert_eq!(found, names);
        jobs
    }

    #[test]
    fn new_starts_full() {
        let semaphore = Semaphore::new(2);
        assert_eq!(semaphore.value(), 2);
        assert_eq!(semaphore.max_value(), 2);
        assert_eq!(semaphore.num_waiters(), 0);
    }

    #[test]
    fn default_is_a_semaphore_of_zero() {
        let semaphore = Semaphore::default();
        assert_eq!(semaphore.value(), 0);
        assert_eq!(semaphore.max_value(), 0);
    }

    /// Upstream's whole mechanism in one case: the counter is what runs, and
    /// the task that cannot have it goes on the list instead.
    #[test]
    fn two_acquires_fit_and_the_third_parks() {
        let semaphore = Semaphore::new(2);
        let jobs = jobs(&["a", "b", "c"]);

        assert!(semaphore.try_acquire_or_wait(&jobs[0]));
        assert!(semaphore.try_acquire_or_wait(&jobs[1]));
        assert_eq!(semaphore.value(), 0);

        assert!(!semaphore.try_acquire_or_wait(&jobs[2]));
        assert_eq!(semaphore.value(), 0);
        assert_eq!(semaphore.num_waiters(), 1);
    }

    #[test]
    fn a_release_hands_back_the_parked_job_and_the_value() {
        let semaphore = Semaphore::new(1);
        let jobs = jobs(&["a", "b"]);
        assert!(semaphore.try_acquire_or_wait(&jobs[0]));
        assert!(!semaphore.try_acquire_or_wait(&jobs[1]));

        let mut woken = Vec::new();
        assert_eq!(semaphore.release(&mut woken), Ok(()));

        assert_eq!(semaphore.value(), 1);
        assert_eq!(semaphore.num_waiters(), 0);
        let names: Vec<&str> = woken.iter().map(|job| job.node().name()).collect();
        assert_eq!(names, ["b"]);
    }

    /// `woken` is appended to, since one task releases several semaphores and
    /// the caller schedules the lot once. The `is_empty` branch of the release
    /// is the first of these two and the append branch is the second.
    #[test]
    fn a_second_release_appends_to_the_same_list() {
        let first = Semaphore::new(1);
        let second = Semaphore::new(1);
        let jobs = jobs(&["a", "b", "c", "d"]);
        assert!(first.try_acquire_or_wait(&jobs[0]));
        assert!(!first.try_acquire_or_wait(&jobs[1]));
        assert!(second.try_acquire_or_wait(&jobs[2]));
        assert!(!second.try_acquire_or_wait(&jobs[3]));

        let mut woken = Vec::new();
        assert_eq!(first.release(&mut woken), Ok(()));
        assert_eq!(second.release(&mut woken), Ok(()));

        let names: Vec<&str> = woken.iter().map(|job| job.node().name()).collect();
        assert_eq!(names, ["b", "d"]);
    }

    #[test]
    fn releasing_at_the_maximum_is_an_error() {
        let semaphore = Semaphore::new(1);
        let mut woken = Vec::new();

        let error = semaphore
            .release(&mut woken)
            .expect_err("already at 1 of 1");
        assert_eq!(error.max_value(), 1);
        assert_eq!(
            error.to_string(),
            "can't release the semaphore more than its maximum value: 1"
        );
        // The failed release changed nothing.
        assert_eq!(semaphore.value(), 1);
        assert!(woken.is_empty());
    }

    /// The error names the maximum the release would have passed, which is
    /// the number upstream prints and the only thing it carries. The case
    /// above has a maximum of 1, so it cannot tell a report of the maximum
    /// from a report of any fixed number; this one moves the maximum with
    /// `reset_to` and asks twice. #310 set its bar.
    #[test]
    fn an_over_release_names_the_maximum_it_would_have_passed() {
        let semaphore = Semaphore::new(3);

        let error = semaphore
            .release(&mut Vec::new())
            .expect_err("already at 3 of 3");
        assert_eq!(error.max_value(), 3);

        semaphore.reset_to(7);
        let error = semaphore
            .release(&mut Vec::new())
            .expect_err("already at 7 of 7");
        assert_eq!(error.max_value(), 7);
        assert_eq!(
            error.to_string(),
            "can't release the semaphore more than its maximum value: 7"
        );
    }

    #[test]
    fn a_release_of_a_semaphore_of_zero_is_an_error() {
        let semaphore = Semaphore::default();
        assert!(semaphore.release(&mut Vec::new()).is_err());
    }

    #[test]
    fn reset_refills_the_counter_and_empties_the_waiters() {
        let semaphore = Semaphore::new(1);
        let jobs = jobs(&["a", "b"]);
        assert!(semaphore.try_acquire_or_wait(&jobs[0]));
        assert!(!semaphore.try_acquire_or_wait(&jobs[1]));

        semaphore.reset();

        assert_eq!(semaphore.value(), 1);
        assert_eq!(semaphore.max_value(), 1);
        assert_eq!(semaphore.num_waiters(), 0);
    }

    #[test]
    fn reset_to_sets_both_numbers_and_empties_the_waiters() {
        let semaphore = Semaphore::new(1);
        let jobs = jobs(&["a", "b"]);
        assert!(semaphore.try_acquire_or_wait(&jobs[0]));
        assert!(!semaphore.try_acquire_or_wait(&jobs[1]));

        semaphore.reset_to(3);

        assert_eq!(semaphore.value(), 3);
        assert_eq!(semaphore.max_value(), 3);
        assert_eq!(semaphore.num_waiters(), 0);
        // Three to give out now, where the old maximum was one.
        assert!(semaphore.try_acquire_or_wait(&jobs[0]));
        assert!(semaphore.try_acquire_or_wait(&jobs[0]));
        assert!(semaphore.try_acquire_or_wait(&jobs[0]));
        assert!(!semaphore.try_acquire_or_wait(&jobs[0]));
    }

    /// The constraint holds under contention, which is the only thing the lock
    /// is there for: however the threads interleave, the count handed out at
    /// any instant never exceeds the maximum, and nothing is lost.
    #[test]
    fn concurrent_acquires_never_hand_out_more_than_the_maximum() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let semaphore = Arc::new(Semaphore::new(4));
        let holders = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let job = jobs(&["a"]).pop().expect("one job");

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let semaphore = Arc::clone(&semaphore);
                let holders = Arc::clone(&holders);
                let peak = Arc::clone(&peak);
                let job = job.clone();
                std::thread::spawn(move || {
                    for _ in 0..2_000 {
                        if semaphore.try_acquire_or_wait(&job) {
                            let held = holders.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(held, Ordering::SeqCst);
                            holders.fetch_sub(1, Ordering::SeqCst);
                            semaphore
                                .release(&mut Vec::new())
                                .expect("this thread holds a count, so it may give one back");
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("no case here panics");
        }

        assert!(peak.load(Ordering::SeqCst) <= 4);
        assert_eq!(semaphore.value(), 4);
    }
}
