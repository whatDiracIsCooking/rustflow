//! Translated from `Topology` in `taskflow/core/graph.hpp` and `_set_up_graph`
//! in `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! One run of one taskflow in three parts: `run` is the run itself, `job` the
//! queued unit of work, and `ownership` what keeps the graph a job walks alive
//! and whose counter it counts down. Each part's `//!` has its own design
//! section, and `ownership`'s collects the lock orderings all three keep.

mod job;
mod ownership;
mod run;

pub(crate) use job::{Finished, Job};
pub(crate) use ownership::{GraphOwner, Nested, Parent, Storage};
pub(crate) use run::{Cancellable, Topology, set_up_graph};

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Poll, Wake, Waker};

    use super::*;
    use crate::Executor;
    use crate::executor::async_node::AsyncNode;
    use crate::executor::queue::WorkerQueue;
    use crate::executor::worker::WorkerThread;
    use crate::graph::{AsyncWork, Handle, Node, NodeBase, SubflowWork};
    use crate::prelude::*;
    use crate::taskflow::Flow;

    /// A run that runs its graph exactly once and has nothing to call
    /// afterwards: upstream's `run(f)` policy, which is what most of these
    /// cases want.
    ///
    /// The share of a taskflow's storage it holds is the one the tear-down
    /// would take back out, and no case here reaches a tear-down: the methods
    /// that walk a graph take it as a parameter, so an empty one will do.
    fn once() -> Topology {
        Topology::new(|| true, || {}, Arc::default())
    }

    /// A diamond, which is the smallest shape with a task that is neither a
    /// source nor a sink: `a -> {b, c} -> d`.
    fn diamond() -> (Taskflow, [Task; 4]) {
        let mut taskflow = Taskflow::new();
        let (a, b, c, d) = taskflow.emplace_n((|| {}, || {}, || {}, || {}));
        taskflow.task(a).precede([b, c]);
        taskflow.task(d).succeed([b, c]);
        (taskflow, [a, b, c, d])
    }

    /// A worker thread with a queue nobody else holds the other end of, for
    /// driving the scheduler one job at a time from the test thread.
    ///
    /// The queue is a fresh one rather than the worker's own, so the pool's
    /// real threads cannot steal what these cases push; they would not look
    /// anyway, since nothing here counts a topology and a worker skips the
    /// steal loop while `num_topologies` is zero.
    fn test_worker(executor: &Executor) -> WorkerThread {
        WorkerThread::new(Arc::clone(&executor.shared), 0, WorkerQueue::new())
    }

    /// A run of `flow`'s graph, shared as the scheduling path needs it.
    fn run_of(flow: &Arc<Flow>) -> Arc<Topology> {
        Arc::new(Topology::new(|| true, || {}, Arc::clone(flow)))
    }

    /// A taskflow of `tasks` independent tasks, each bumping `counter`, which
    /// is the shape a nested graph wants here: every node is a source, so
    /// every one of them is queued whether or not its predecessors ran.
    fn fan(tasks: usize, counter: &Arc<AtomicUsize>) -> Taskflow {
        let mut taskflow = Taskflow::new();
        for _ in 0..tasks {
            let counter = Arc::clone(counter);
            taskflow.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }
        taskflow
    }

    #[test]
    fn setting_up_a_graph_counts_sources_and_arms_every_join_counter() {
        let (taskflow, [a, b, c, d]) = diamond();

        assert_eq!(set_up_graph(taskflow.graph()), 1, "only `a` is a source");

        let graph = taskflow.graph();
        for (task, expected) in [(a, 0_u32), (b, 1), (c, 1), (d, 2)] {
            let node = graph.node(task);
            assert_eq!(
                node.join_counter().load(Ordering::Relaxed),
                usize::from(u16::try_from(expected).expect("a small count"))
            );
            assert_eq!(node.nstate().load(Ordering::Relaxed), expected);
        }
    }

    /// Setting a graph up twice has to leave it exactly as the first time
    /// did, or the second run of a taskflow would start with counters the
    /// first run had already spent -- and with state bits the first run left
    /// behind, which is upstream's `node->_nstate = NSTATE::NONE`. A node
    /// still carrying `PREEMPTED` would have the next run treat its first
    /// entry as a resumption and skip its body.
    #[test]
    fn setting_up_the_same_graph_again_rearms_it() {
        let (taskflow, [_, b, _, d]) = diamond();

        set_up_graph(taskflow.graph());
        // Spend the counters and leave the state bits a cancelled run might,
        // as a run would.
        taskflow
            .graph()
            .node(b)
            .join_counter()
            .store(0, Ordering::Relaxed);
        taskflow
            .graph()
            .node(d)
            .join_counter()
            .store(0, Ordering::Relaxed);
        taskflow
            .graph()
            .node(d)
            .nstate()
            .fetch_or(Node::PREEMPTED | Node::JOINED_SUBFLOW, Ordering::Relaxed);
        taskflow
            .graph()
            .node(b)
            .estate()
            .store(Cancellable::CANCELLED, Ordering::Relaxed);

        assert_eq!(set_up_graph(taskflow.graph()), 1);
        let graph = taskflow.graph();
        assert_eq!(graph.node(d).join_counter().load(Ordering::Relaxed), 2);
        assert_eq!(
            graph.node(d).nstate().load(Ordering::Relaxed),
            2,
            "the state bits of the last run did not survive set-up"
        );
        assert_eq!(graph.node(b).estate().load(Ordering::Relaxed), 0);
    }

    /// The strong-dependency rule #31 settled, which set-up is the only place
    /// that reads: an edge out of a conditioner is not counted, so a node
    /// reached only by one is armed at zero and is still not a source.
    #[test]
    fn setting_up_a_graph_with_conditioners_counts_only_strong_dependencies() {
        let mut taskflow = Taskflow::new();
        let (plain, joined) = taskflow.emplace_n((|| {}, || {}));
        let branch = taskflow.emplace_condition(|| 0);
        let selected = taskflow.emplace(|| {});
        taskflow.task(joined).succeed([plain, branch]);
        taskflow.task(branch).precede([selected]);

        assert_eq!(
            set_up_graph(taskflow.graph()),
            2,
            "`plain` and `branch` have no predecessors"
        );

        let graph = taskflow.graph();
        for (task, strong) in [(plain, 0), (branch, 0), (joined, 1), (selected, 0)] {
            assert_eq!(
                graph.node(task).join_counter().load(Ordering::Relaxed),
                strong,
                "{task:?}"
            );
            assert_eq!(
                graph.node(task).nstate().load(Ordering::Relaxed),
                u32::try_from(strong).expect("a small count"),
                "{task:?}"
            );
        }
    }

    #[test]
    fn the_sources_are_the_tasks_with_no_predecessors() {
        let (taskflow, [a, ..]) = diamond();
        set_up_graph(taskflow.graph());

        let topology = run_of(taskflow.flow());
        let owner = GraphOwner::flow(taskflow.flow());
        let sources: Vec<Task> = Job::sources(&owner, &topology, &Parent::Run)
            .map(|job| job.task)
            .collect();
        assert_eq!(sources, vec![a]);
    }

    /// An empty graph has no sources, and so does a graph whose every node is
    /// in a cycle. Both answer zero, which is what tells `run` there is
    /// nothing to schedule.
    #[test]
    fn a_graph_with_no_source_sets_up_to_zero() {
        assert_eq!(set_up_graph(Flow::default().graph()), 0);

        let mut cyclic = Taskflow::new();
        let (a, b) = cyclic.emplace_n((|| {}, || {}));
        cyclic.task(a).precede([b]);
        cyclic.task(b).precede([a]);
        assert_eq!(set_up_graph(cyclic.graph()), 0);
    }

    /// The join counter is the whole of the run's bookkeeping, so the shape of
    /// it is worth pinning down on its own: one node finishing is not the end
    /// of a run that queued two.
    #[test]
    fn a_run_ends_when_the_last_outstanding_job_finishes() {
        let (taskflow, _) = diamond();
        let topology = once();
        Parent::Run.count_queued(&topology, set_up_graph(taskflow.graph()));

        // `a` finishes, having made `b` and `c` ready.
        Parent::Run.count_queued(&topology, 2);
        assert!(!topology.job_finished());

        // `b` finishes; `c` has not.
        assert!(!topology.job_finished());
        // `c` finishes and makes `d` ready.
        Parent::Run.count_queued(&topology, 1);
        assert!(!topology.job_finished());
        // `d` finishes, and nothing is outstanding.
        assert!(topology.job_finished());
    }

    #[test]
    fn the_first_panic_is_the_one_reported() {
        let topology = once();
        topology.record_panic(Box::new("first"));
        topology.record_panic(Box::new("second"));
        topology.finish();

        let payload = topology.join().expect("a panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"first"));
        // Taken, not cloned: the second waiter has nothing left to report.
        assert!(topology.join().is_none());
    }

    /// The other half of recording a panic, and the whole of "the rest of the
    /// run is cancelled": the bit the scheduler's early-out reads is set by
    /// the same atomic that claims the payload slot.
    #[test]
    fn recording_a_panic_cancels_the_rest_of_the_run() {
        let topology = once();
        assert!(!topology.cancelled());

        topology.record_panic(Box::new("boom"));
        assert!(topology.cancelled(), "the run was not cancelled");
        assert!(
            !topology.run_again(),
            "a failed run does not go round again"
        );

        // `CAUGHT` is not part of what `cancelled` reads, so a run cancelled
        // by its handle has a payload slot still to claim.
        let cancelled = once();
        assert!(cancelled.cancel());
        cancelled.record_panic(Box::new("boom"));
        cancelled.finish();
        let payload = cancelled.join().expect("a panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
    }

    #[test]
    fn joining_a_finished_run_does_not_block() {
        let topology = once();
        topology.finish();
        assert!(topology.join().is_none());
        assert!(topology.join().is_none());
    }

    /// The predicate is upstream's, counter and all: `run_n(f, k)` asks it
    /// once before the first run and once after each run, so a topology set up
    /// for `k` runs has to answer "again" exactly `k - 1` times.
    #[test]
    fn a_counting_predicate_runs_the_graph_the_number_of_times_asked() {
        for runs in 0..5_usize {
            let mut remaining = runs;
            let topology = Topology::new(
                move || {
                    let stop = remaining == 0;
                    remaining = remaining.saturating_sub(1);
                    stop
                },
                || {},
                Arc::default(),
            );

            // The call `run_until` makes before scheduling anything, then one
            // per tear-down, as the worker makes them.
            let mut scheduled = 0;
            if !topology.stop_requested() {
                scheduled = 1;
                while topology.run_again() {
                    scheduled += 1;
                }
            }
            assert_eq!(scheduled, runs, "a topology asked for {runs} runs");
        }
    }

    /// A predicate that panics must not unwind out of the tear-down: that would
    /// skip `finish` and leave the waiter blocked for good. It is recorded as
    /// the run's failure instead, and it means "stop".
    #[test]
    fn a_panicking_predicate_stops_the_run_and_is_reported() {
        let topology = Topology::new(|| panic!("predicate"), || {}, Arc::default());

        assert!(!topology.run_again(), "a failed predicate means stop");
        topology.finish();

        let payload = topology.join().expect("the panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"predicate"));
    }

    /// The estate word is upstream's, and the two things it owes the rest of
    /// the port are that either of the cancelling bits means "cancelled" and
    /// that a setter is told whether it was the first.
    /// [`Topology::record_panic`] leans on both: a panicking task sets
    /// `EXCEPTION`, and the first one to do it is the one whose failure the
    /// run reports.
    #[test]
    fn the_estate_word_is_either_bit_and_reports_the_first_setter() {
        let state = Cancellable::new();
        assert!(!state.cancelled());

        assert!(state.set(Cancellable::CANCELLED), "the first setter");
        assert!(state.cancelled());
        assert!(!state.set(Cancellable::CANCELLED), "the second setter");

        // The other bit on its own means cancelled too, which is what makes
        // the panic term a term of `cancelled()` rather than of `run_again`.
        let failed = Cancellable::new();
        assert!(failed.set(Cancellable::EXCEPTION));
        assert!(failed.cancelled());
        assert!(!failed.set(Cancellable::EXCEPTION));

        // And they are distinct bits, so neither setter answers for the other.
        let both = Cancellable::new();
        assert!(both.set(Cancellable::CANCELLED));
        assert!(both.set(Cancellable::EXCEPTION));

        // `CAUGHT` is the third, and the one that is not part of what
        // `cancelled` reads: it says the payload slot is taken, nothing more.
        let caught = Cancellable::new();
        assert!(caught.set(Cancellable::CAUGHT));
        assert!(!caught.cancelled());
        assert!(!caught.set(Cancellable::EXCEPTION | Cancellable::CAUGHT));
    }

    /// Cancelling is upstream's `Future::cancel`: `true` while the run is
    /// there to cancel, `false` once it has finished -- which is its
    /// `weak_ptr::lock()` failing -- and harmless twice over.
    #[test]
    fn cancelling_reports_false_only_once_the_run_has_finished() {
        let topology = once();
        assert!(!topology.cancelled());

        assert!(topology.cancel(), "a run in flight can be cancelled");
        assert!(topology.cancel(), "cancelling twice is still true");
        assert!(topology.cancelled());

        topology.finish();
        assert!(!topology.cancel(), "a finished run has nothing to cancel");
        // The bit stays set: a cancelled run that has finished is still a
        // cancelled run, which is what a `was_cancelled` query would read.
        assert!(topology.cancelled());
    }

    /// The cancellation half of upstream's
    /// `!tpg->cancelled() && !tpg->_predicate()`: a run told to go round again
    /// does not, once it is cancelled. The counting predicate is the one that
    /// shows the short-circuit, since a predicate that was never asked has not
    /// spent a round.
    #[test]
    fn a_cancelled_run_does_not_go_round_again() {
        let asked = Arc::new(AtomicUsize::new(0));
        let topology = {
            let asked = Arc::clone(&asked);
            Topology::new(
                move || {
                    asked.fetch_add(1, Ordering::Relaxed);
                    false
                },
                || {},
                Arc::default(),
            )
        };

        assert!(topology.run_again(), "the predicate says go round again");
        assert_eq!(asked.load(Ordering::Relaxed), 1);

        assert!(topology.cancel());
        assert!(!topology.run_again(), "a cancelled run stops");
        assert_eq!(
            asked.load(Ordering::Relaxed),
            1,
            "the predicate was asked by a run that will not happen"
        );
    }

    /// The callback is upstream's `_on_finish`: called once, after the last
    /// run, and a panic out of it is reported rather than propagated.
    #[test]
    fn the_callback_is_called_once_and_its_panic_is_recorded() {
        let calls = Arc::new(AtomicUsize::new(0));
        let topology = {
            let calls = Arc::clone(&calls);
            Topology::new(
                || true,
                move || {
                    calls.fetch_add(1, Ordering::Relaxed);
                },
                Arc::default(),
            )
        };

        topology.carry_out_callback();
        topology.carry_out_callback();
        assert_eq!(calls.load(Ordering::Relaxed), 1, "`FnOnce` means once");

        let panicking = Topology::new(|| true, || panic!("callback"), Arc::default());
        panicking.carry_out_callback();
        panicking.finish();
        let payload = panicking.join().expect("the panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"callback"));
    }

    /// `poll_join` is the non-blocking half of `join`: pending until the run
    /// finishes, and then the same payload `join` would have handed back.
    #[test]
    fn polling_a_run_leaves_a_waker_and_is_woken_by_finish() {
        let woken = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountingWaker(Arc::clone(&woken))));

        let topology = once();
        assert!(topology.poll_join(&waker).is_pending());
        // Polling again with the same waker keeps the one already stored.
        assert!(topology.poll_join(&waker).is_pending());
        assert_eq!(woken.load(Ordering::Relaxed), 0);

        topology.record_panic(Box::new("boom"));
        topology.finish();
        assert_eq!(woken.load(Ordering::Relaxed), 1, "the waker was not woken");

        let Poll::Ready(payload) = topology.poll_join(&waker) else {
            panic!("a finished run is ready");
        };
        let payload = payload.expect("a panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));

        // Taken, not cloned, exactly as `join` takes it.
        assert!(matches!(topology.poll_join(&waker), Poll::Ready(None)));
        assert_eq!(woken.load(Ordering::Relaxed), 1, "no second wake");
    }

    /// The ordering issue #109 was about, as the two lines that make it: a
    /// share of a run's hold on the storage outstanding when the tear-down
    /// has done everything else keeps the waiter blocked, and dropping that
    /// share is what releases it.
    ///
    /// The straggler here stands in for a copy of a preempted node's job,
    /// which is what holds one in the scheduler -- see the module docs' order
    /// section -- and the assertion is the one `Taskflow::graph_mut` makes a
    /// line later: when the waiter wakes, the taskflow's own share is the
    /// only one left.
    #[test]
    fn the_waiter_is_released_by_the_last_share_of_the_hold_on_the_storage() {
        let woken = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountingWaker(Arc::clone(&woken))));

        // The executor whose count the run is in. Declared first so that an
        // assertion failing below still drops the straggler -- and with it
        // the count -- before `Executor::drop` waits for that count.
        let executor = crate::Executor::new(1);
        let shared = Arc::clone(&executor.shared);
        shared.increment_topology();

        let mut taskflow = Taskflow::new();
        taskflow.emplace(|| {});
        let taskflow = taskflow;
        let topology = Topology::for_run(
            || true,
            || {},
            Arc::clone(taskflow.flow()),
            Arc::clone(&shared),
        );
        let straggler = topology
            .storage()
            .expect("a run that has not been torn down holds its own share");

        assert!(topology.poll_join(&waker).is_pending());

        // The whole of the tear-down's last act, with the straggler still
        // holding a share: nothing is released and nothing is decremented,
        // because the storage is not free yet.
        topology.release_flow();
        assert!(
            topology.poll_join(&waker).is_pending(),
            "a run whose storage is still held is not over"
        );
        assert_eq!(woken.load(Ordering::Relaxed), 0);
        assert_eq!(
            shared.num_topologies(),
            1,
            "a run whose storage is still held is still counted (#138)"
        );

        drop(straggler);
        assert!(
            topology.poll_join(&waker).is_ready(),
            "the last share of the hold releases the waiter"
        );
        assert_eq!(woken.load(Ordering::Relaxed), 1, "the waker was not woken");
        assert_eq!(
            shared.num_topologies(),
            0,
            "the last share of the hold takes the run out of the count (#138)"
        );
        assert_eq!(
            Arc::strong_count(taskflow.flow()),
            1,
            "the taskflow's own share is the only one left, which is what \
             `Taskflow::graph_mut` asks `Arc::get_mut`"
        );
    }

    /// A hold that names no run releases nobody, so the tear-down of such a
    /// run releases the waiter itself: a corun's topology, and every
    /// topology these tests build by hand, is one of those.
    #[test]
    fn a_run_whose_hold_names_no_waiter_is_finished_by_releasing_the_storage() {
        let taskflow = Taskflow::new();
        let topology = Topology::new(|| true, || {}, Arc::clone(taskflow.flow()));
        let straggler = topology.storage().expect("built with a hold");

        topology.release_flow();
        assert!(
            topology.join().is_none(),
            "an ungated hold is released by `release_flow` rather than by its own drop"
        );
        drop(straggler);
    }

    /// The rule the two cases above rest on is asserted where a run is
    /// finished, in debug builds (#141): a run whose hold gates its completion
    /// may not be finished while the topology still holds that hold, because
    /// the waiter it wakes would find the storage held. This is the one way
    /// to trip it -- `finish` with no `release_flow` first -- and the case
    /// exists so that the assertion is exercised rather than merely present.
    ///
    /// The unwind drops the topology, and with it the only share of the hold,
    /// whose `Drop` takes the run out of the executor's count: that is what
    /// lets `Executor::drop` return afterwards.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(
        expected = "a run is finished only after its hold on the storage has left the topology"
    )]
    fn finishing_a_gated_run_that_still_holds_its_storage_is_caught() {
        let executor = crate::Executor::new(1);
        let shared = Arc::clone(&executor.shared);
        shared.increment_topology();

        let taskflow = Taskflow::new();
        let topology = Topology::for_run(
            || true,
            || {},
            Arc::clone(taskflow.flow()),
            Arc::clone(&shared),
        );
        assert!(topology.storage().is_some(), "the run holds its own share");

        // The tear-down's last two acts, in the wrong order: the slot still
        // holds the run's share.
        topology.finish();
    }

    /// A waker that counts, in place of a runtime.
    struct CountingWaker(Arc<AtomicUsize>);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A graph scheduled under a node counts its sources against *that node*
    /// rather than against the run, which is the whole of what a nested graph
    /// is: the node's own join counter tracks its children, so the node
    /// cannot finish until they have.
    #[test]
    fn a_graph_under_a_node_parent_counts_its_sources_against_that_node() {
        let executor = Executor::new(1);
        let worker = test_worker(&executor);

        let mut outer = Taskflow::new();
        let spawner = outer.emplace(|| {});
        let flow = Arc::clone(outer.flow());
        let topology = run_of(&flow);
        let owner = GraphOwner::flow(&flow);
        set_up_graph(flow.graph());

        // The parent: the job the spawner node was dequeued as, which is what
        // a preempting arm will have in hand.
        let parent = Nested::new(Some(Job::new(spawner, &owner, &topology, &Parent::Run)));
        assert_eq!(parent.outstanding(), 0, "the spawner is a source");

        let counter = Arc::new(AtomicUsize::new(0));
        let inner = fan(3, &counter);
        executor.shared.schedule_graph(
            Some(&worker),
            &GraphOwner::flow(inner.flow()),
            &topology,
            &parent.as_parent(),
        );

        assert_eq!(parent.outstanding(), 3, "one per source of the inner graph");
        assert_eq!(
            flow.graph()
                .node(spawner)
                .join_counter()
                .load(Ordering::Relaxed),
            3,
            "the parent's counter is the node's own"
        );
        assert_eq!(
            topology.join_counter.load(Ordering::Relaxed),
            0,
            "the run's counter is untouched by a nested graph"
        );
        assert_eq!(counter.load(Ordering::Relaxed), 0, "nothing has run yet");
    }

    /// The preemption protocol's second half: the last child of a `PREEMPTED`
    /// parent re-queues that parent, and exactly one of them does it.
    #[test]
    fn the_last_child_of_a_preempted_parent_resumes_it_exactly_once() {
        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);

        let mut outer = Taskflow::new();
        let spawner = outer.emplace(|| {});
        let flow = Arc::clone(outer.flow());
        let topology = run_of(&flow);
        let owner = GraphOwner::flow(&flow);
        set_up_graph(flow.graph());
        let parent = Nested::new(Some(Job::new(spawner, &owner, &topology, &Parent::Run)));

        // What a preempting arm does before it schedules the nested graph.
        flow.graph()
            .node(spawner)
            .nstate()
            .fetch_or(Node::PREEMPTED, Ordering::Relaxed);

        let counter = Arc::new(AtomicUsize::new(0));
        let inner = fan(2, &counter);
        executor.shared.schedule_graph(
            Some(&worker),
            &GraphOwner::flow(inner.flow()),
            &topology,
            &parent.as_parent(),
        );

        let first = worker.pop().expect("a source of the inner graph");
        let second = worker.pop().expect("the other source");
        assert!(worker.pop().is_none(), "two sources, two jobs");

        worker.invoke(first);
        assert_eq!(parent.outstanding(), 1);
        assert!(
            worker.pop().is_none(),
            "the parent is not resumed while a child is outstanding"
        );

        worker.invoke(second);
        assert_eq!(parent.outstanding(), 0);
        let resumed = worker.pop().expect("the last child resumes the parent");
        assert_eq!(resumed.task, spawner, "the parent's own job is re-queued");
        assert!(worker.pop().is_none(), "resumed once, not twice");
        assert_eq!(counter.load(Ordering::Relaxed), 2, "both children ran");
    }

    /// A parent with no `PREEMPTED` bit is resumed by nobody, which is the
    /// anchor case: a [`corun`](crate::Executor::corun) schedules a graph
    /// under an anchor, the jobs drive its counter to zero, and the frame
    /// watching that counter is what the zero releases. The run's own counter
    /// is never touched, so nothing tears the topology down either.
    #[test]
    fn a_graph_under_an_anchor_drives_the_anchor_to_zero_and_leaves_the_run_alone() {
        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);

        let counter = Arc::new(AtomicUsize::new(0));
        let mut inner = Taskflow::new();
        let (first, second) = inner.emplace_n((
            {
                let counter = Arc::clone(&counter);
                move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            },
            {
                let counter = Arc::clone(&counter);
                move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            },
        ));
        inner.task(first).precede([second]);

        let topology = run_of(inner.flow());
        let anchor = Nested::new(None);
        executor.shared.schedule_graph(
            Some(&worker),
            &GraphOwner::flow(inner.flow()),
            &topology,
            &anchor.as_parent(),
        );
        assert_eq!(anchor.outstanding(), 1, "one source");

        // The worker's own loop, by hand. One turn of it runs both jobs: the
        // second is a chained successor, so since #62 it is kept out of the
        // queue and run as the first job's continuation -- counted against
        // the anchor before it is kept, which is what makes the counter
        // below reach zero exactly once.
        let mut dequeued = 0;
        while let Some(job) = worker.pop() {
            worker.invoke(job);
            dequeued += 1;
        }

        assert_eq!(
            dequeued, 1,
            "one turn: the chain's second link never reached the queue"
        );
        assert_eq!(counter.load(Ordering::Relaxed), 2, "both jobs ran");
        assert_eq!(anchor.outstanding(), 0, "the anchor is what went to zero");
        assert_eq!(
            topology.join_counter.load(Ordering::Relaxed),
            0,
            "the run's counter was never raised, so it was never spent"
        );

        // And nothing finished the run: with an anchor as the parent, that is
        // the corun frame's own job.
        let waker = Waker::from(Arc::new(CountingWaker(Arc::new(AtomicUsize::new(0)))));
        assert!(topology.poll_join(&waker).is_pending());
    }

    /// Upstream's `_is_parent_cancelled`, over a chain two levels deep: a job
    /// whose *grandparent* has failed is torn down without being run, even
    /// though the run itself is neither cancelled nor failed. The counters
    /// still reach zero, which is what keeps the nesting above it able to
    /// finish at all.
    #[test]
    fn a_child_of_a_cancelled_ancestor_is_torn_down_without_running() {
        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);

        let mut outer = Taskflow::new();
        let (grandparent, parent_task) = outer.emplace_n((|| {}, || {}));
        let flow = Arc::clone(outer.flow());
        let topology = run_of(&flow);
        let owner = GraphOwner::flow(&flow);
        set_up_graph(flow.graph());

        let grandparent_job = Job::new(grandparent, &owner, &topology, &Parent::Run);
        let outer_parent = Nested::new(Some(grandparent_job));
        let parent = Nested::new(Some(Job::new(
            parent_task,
            &owner,
            &topology,
            &outer_parent.as_parent(),
        )));

        // What a panicking subflow body leaves behind, two levels up: the
        // failure is the grandparent's, not the run's.
        flow.graph()
            .node(grandparent)
            .estate()
            .store(Cancellable::EXCEPTION, Ordering::Relaxed);

        let counter = Arc::new(AtomicUsize::new(0));
        let inner = fan(3, &counter);
        executor.shared.schedule_graph(
            Some(&worker),
            &GraphOwner::flow(inner.flow()),
            &topology,
            &parent.as_parent(),
        );
        assert_eq!(parent.outstanding(), 3);

        while let Some(job) = worker.pop() {
            worker.invoke(job);
        }

        assert_eq!(counter.load(Ordering::Relaxed), 0, "no body ran");
        assert_eq!(
            parent.outstanding(),
            0,
            "a skipped job still owes its parent the decrement"
        );
        assert!(!topology.cancelled(), "the run itself is fine");
    }

    /// The parent chain the anchor cases walk: a root, a `root` node under
    /// it, a `middle` node under that, and the jobs of a nested graph under
    /// `middle`.
    ///
    /// Built by hand rather than by running anything, because what these
    /// cases are about is the walk: the two [`Nested`] parents are exactly
    /// what a dispatch arm would have built, and the jobs are exactly what
    /// `schedule_graph` would have queued.
    struct Chain {
        /// The storage holding `root` and `middle`.
        flow: Arc<Flow>,
        /// The run at the chain's root, which the walk must leave alone
        /// whenever an anchor below it takes the payload.
        topology: Arc<Topology>,
        /// The outer parent node, two levels above a failing job.
        root: Task,
        /// The inner parent node, one level above it.
        middle: Task,
        /// The jobs of the nested graph, each of a node of its own.
        jobs: Vec<Job>,
    }

    impl Chain {
        /// A chain whose nested graph has `jobs` jobs.
        ///
        /// `root_parent` is what the `root` node's own job hangs off: the run
        /// for an ordinary nesting, or a [`Nested`] with nothing above it --
        /// the shape a corun anchor has -- for a chain that ends without
        /// reaching a run.
        fn new(jobs: usize, root_parent: &Parent) -> Self {
            let mut outer = Taskflow::new();
            let (root, middle) = outer.emplace_n((|| {}, || {}));
            let flow = Arc::clone(outer.flow());
            let topology = run_of(&flow);
            let owner = GraphOwner::flow(&flow);
            set_up_graph(flow.graph());

            let above = Nested::new(Some(Job::new(root, &owner, &topology, root_parent)));
            let parent = Nested::new(Some(Job::new(
                middle,
                &owner,
                &topology,
                &above.as_parent(),
            )));

            let inner = fan(jobs, &Arc::new(AtomicUsize::new(0)));
            set_up_graph(inner.flow().graph());
            let jobs = Job::sources(
                &GraphOwner::flow(inner.flow()),
                &topology,
                &parent.as_parent(),
            )
            .collect::<Vec<_>>();
            assert!(!jobs.is_empty(), "a chain needs a job to fail");

            // Everything the jobs need is owned by the jobs: the inner
            // taskflow's storage, the chain of parents above them and the
            // run. So the taskflow handles can go here.
            Self {
                flow,
                topology,
                root,
                middle,
                jobs,
            }
        }

        /// The state words of one of the two parent nodes.
        fn base(&self, task: Task) -> &NodeBase {
            self.flow.graph().node(task).base()
        }

        /// Whether `task`'s base was marked by the walk, without claiming
        /// anything.
        fn marked(&self, task: Task) -> bool {
            self.base(task).estate().load(Ordering::Relaxed) & Cancellable::EXCEPTION != 0
        }

        /// Whether `task`'s base holds the payload, which is the bit that
        /// says the walk stopped there rather than passing through.
        fn caught(&self, task: Task) -> bool {
            self.base(task).estate().load(Ordering::Relaxed) & Cancellable::CAUGHT != 0
        }
    }

    /// The first of the three anchor cases: a failure under an explicit
    /// anchor two levels up is delivered to *that* anchor, every base between
    /// is marked so the rest of each nested graph stops, and the run is left
    /// alone.
    #[test]
    fn a_failure_under_an_explicit_anchor_is_delivered_to_it() {
        let chain = Chain::new(1, &Parent::Run);
        let _anchor = chain.base(chain.root).anchor_explicitly();

        chain.jobs[0].record_panic(Box::new("boom"));

        // The anchor caught it, and the base between did not: it carries the
        // mark that cancels the rest of its own graph and nothing else.
        assert!(chain.caught(chain.root), "the anchor did not catch it");
        assert!(chain.marked(chain.root));
        assert!(
            chain.marked(chain.middle),
            "the intermediate node is marked"
        );
        assert!(!chain.caught(chain.middle), "it is not an anchor");
        assert!(
            chain.jobs[0].node().estate().load(Ordering::Relaxed) & Cancellable::EXCEPTION != 0,
            "the failing node is marked too"
        );

        let payload = chain
            .base(chain.root)
            .take_panic()
            .expect("the anchor has the payload");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));

        // The run is untouched: it is not cancelled, so the jobs of the
        // *outer* graph still run, and a `wait` on it reports nothing. That
        // is the whole point of an anchor -- the failure belongs to whoever
        // is waiting for the nested graph.
        assert!(!chain.topology.cancelled(), "the run was cancelled");
        chain.topology.finish();
        assert!(chain.topology.join().is_none(), "the run has a payload");

        // And nothing was left on the failing node, which is where a payload
        // with nowhere to go would have ended up.
        assert!(chain.jobs[0].node().base().take_panic().is_none());
    }

    /// The second: with no explicit anchor anywhere on the chain, the
    /// innermost `IMPLICITLY_ANCHORED` base gets the payload -- the one
    /// closest to the failure, not the outermost, and not the run the chain
    /// roots at either.
    ///
    /// Two chains, because the root is the half of this that `DESIGN.md`'s
    /// priority is about and upstream answers the other way. The first ends
    /// at a parent with nothing above it, which is a corun anchor's shape;
    /// the second at a real run, which is the shape a graph makes whenever a
    /// runtime task's body spawns children. The answer has to be the same
    /// chain in both, or where a child's failure lands depends on which side
    /// of it finished first.
    #[test]
    fn a_failure_with_no_explicit_anchor_goes_to_the_innermost_implicit_one() {
        let unanchored = Nested::new(None);
        let chain = Chain::new(1, &unanchored.as_parent());

        // Both parents volunteer; the inner one wins.
        for task in [chain.root, chain.middle] {
            chain
                .base(task)
                .nstate()
                .fetch_or(Node::IMPLICITLY_ANCHORED, Ordering::Relaxed);
        }

        chain.jobs[0].record_panic(Box::new("boom"));

        assert!(chain.caught(chain.middle), "the inner anchor did not catch");
        assert!(!chain.caught(chain.root), "the outer anchor caught it");
        assert!(chain.marked(chain.root), "the walk passed through it");
        let payload = chain
            .base(chain.middle)
            .take_panic()
            .expect("the inner anchor has the payload");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
        assert!(chain.base(chain.root).take_panic().is_none());
        assert!(!chain.topology.cancelled());

        // The same shape rooted at a real run. The run is waiting for the
        // whole chain, but the frame that means to read its slot when it is
        // resumed is closer to the failure, so the run is left with nothing
        // to report -- which is what lets a runtime body collect its own
        // child's failure and go on.
        let rooted = Chain::new(1, &Parent::Run);
        for task in [rooted.root, rooted.middle] {
            rooted
                .base(task)
                .nstate()
                .fetch_or(Node::IMPLICITLY_ANCHORED, Ordering::Relaxed);
        }

        rooted.jobs[0].record_panic(Box::new("boom"));

        assert!(
            rooted.caught(rooted.middle),
            "the inner anchor did not catch"
        );
        assert!(!rooted.caught(rooted.root), "the outer anchor caught it");
        let payload = rooted
            .base(rooted.middle)
            .take_panic()
            .expect("the inner anchor has the payload");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
        assert!(
            !rooted.topology.cancelled(),
            "the run outranked the anchor below it"
        );
        rooted.topology.finish();
        assert!(rooted.topology.join().is_none(), "and took no payload");
    }

    /// The third: two failures under one anchor keep the first, which is the
    /// rule a run has always applied, now applied at the anchor. The loser is
    /// dropped rather than parked on the node that raised it, which is where
    /// upstream's fall-through leaves it and where this port deliberately
    /// does not -- [`Job::record_panic`] says why, and this is what pins it:
    /// neither failing node holds anything afterwards.
    #[test]
    fn two_failures_under_one_anchor_keep_the_first() {
        let chain = Chain::new(2, &Parent::Run);
        let _anchor = chain.base(chain.root).anchor_explicitly();

        chain.jobs[0].record_panic(Box::new("first"));
        chain.jobs[1].record_panic(Box::new("second"));

        let payload = chain
            .base(chain.root)
            .take_panic()
            .expect("the anchor has a payload");
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"first"),
            "the anchor kept the later failure"
        );

        // And the loser is gone rather than parked on the node that raised
        // it, which is where upstream leaves it: see `Job::record_panic` for
        // why this port drops it instead. Neither failing node holds
        // anything.
        assert!(chain.jobs[0].node().base().take_panic().is_none());
        assert!(chain.jobs[1].node().base().take_panic().is_none());
    }

    /// What `executor::module` asks before it schedules a target: a job
    /// recognises its own graph and every graph above it on the parent
    /// chain, and nothing else.
    ///
    /// The chain is the one the anchor cases use, which is exactly the shape
    /// the question is about -- jobs of a nested graph two links below the
    /// graph their parents live in -- and the case that matters is the
    /// *second* link, since a walk that compared one level only would pass
    /// the first assertion and let issue #82's cycle of two taskflows
    /// through.
    #[test]
    fn a_job_knows_the_graphs_it_is_running_inside_of() {
        let chain = Chain::new(1, &Parent::Run);
        let job = &chain.jobs[0];

        assert!(job.runs_inside(job.graph()), "its own graph");
        assert!(
            job.runs_inside(chain.flow.graph()),
            "the graph both its parents live in, two links up"
        );

        let elsewhere = Taskflow::new();
        assert!(
            !job.runs_inside(elsewhere.flow().graph()),
            "a graph this run has never been in"
        );
    }

    /// A waiter that arrives before the run finishes has to be released by
    /// `finish`, which is the one piece of this file that a single-threaded
    /// test cannot reach.
    #[test]
    fn a_blocked_waiter_is_released_by_finish() {
        let topology = once();
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| topology.join());
            topology.record_panic(Box::new(String::from("boom")));
            topology.finish();
            let payload = waiter.join().expect("the waiter panicked");
            let payload = payload.expect("a panic was recorded");
            assert_eq!(
                payload.downcast_ref::<String>().map(String::as_str),
                Some("boom")
            );
        });
    }

    /// The index every one of the four cases below resolves, which is task 1
    /// of a two-node graph -- live in the nested arena and live in the outer
    /// one, with a different node behind it. That is the whole point of the
    /// collision: a job that resolved against the wrong graph would find a
    /// node rather than a panic, and run it.
    const NESTED: Task = Task::new(1);

    /// A two-node graph whose nodes are named `<what>-0` and `<what>-1`.
    fn named_pair(what: &str) -> Graph {
        let mut graph = Graph::new();
        graph.add_named_node(format!("{what}-0"), Handle::Placeholder);
        graph.add_named_node(format!("{what}-1"), Handle::Placeholder);
        graph
    }

    /// A taskflow whose graph is `root-0` and `root-1`, the second carrying
    /// `handle`: the node a nested graph hangs off.
    fn nesting(handle: Handle) -> Taskflow {
        let mut taskflow = Taskflow::new();
        taskflow
            .graph_mut()
            .add_named_node("root-0", Handle::Placeholder);
        taskflow.graph_mut().add_named_node("root-1", handle);
        taskflow
    }

    /// The first of the four kinds: a job of the run's own graph resolves in
    /// the taskflow's own arena, which is the only kind there was before #74.
    #[test]
    fn a_job_of_the_runs_own_graph_resolves_in_the_taskflows_arena() {
        let taskflow = nesting(Handle::Placeholder);
        let topology = run_of(taskflow.flow());
        let owner = GraphOwner::flow(taskflow.flow());

        let job = Job::new(NESTED, &owner, &topology, &Parent::Run);

        assert_eq!(job.node().name(), "root-1");
        assert!(std::ptr::eq(job.graph(), taskflow.graph()));
    }

    /// The second: a job of a module's target resolves in the *target's*
    /// arena, through the share the handle holds. This is the kind #49 could
    /// already schedule, and it is here so that all four are pinned in one
    /// place.
    #[test]
    fn a_job_of_a_modules_target_resolves_in_the_targets_arena() {
        let mut target = Taskflow::new();
        *target.graph_mut() = named_pair("target");
        let target = Arc::new(target);
        let taskflow = nesting(Handle::Module(Arc::clone(&target)));
        let topology = run_of(taskflow.flow());

        // What `module::invoke` hands the scheduling path: the storage of the
        // taskflow its own handle holds.
        let owner = GraphOwner::flow(target.flow());
        let job = Job::new(NESTED, &owner, &topology, &Parent::Run);

        assert_eq!(job.node().name(), "target-1");
        assert_eq!(
            taskflow.graph().node(NESTED).name(),
            "root-1",
            "the same index names another node in the composing graph"
        );
    }

    /// The third, and the one that was broken: a job of an adopted module's
    /// graph resolves *inside the handle* of the node that owns it, reached
    /// as the module node's own owner plus its index.
    ///
    /// Without that path there is nothing to resolve against but the outer
    /// owner, which answers with the composing graph's node at the same index
    /// -- `root-1` here -- and says nothing about having done so. That is why
    /// the arm that runs one refused the run until this landed.
    #[test]
    fn a_job_of_an_adopted_modules_graph_resolves_inside_its_handle() {
        let taskflow = nesting(Handle::AdoptedModule(named_pair("adopted")));
        let topology = run_of(taskflow.flow());
        let root = GraphOwner::flow(taskflow.flow());

        // The job the module node was dequeued as, which is what the arm that
        // runs one has in hand, and the path it builds out of it.
        let module = Job::new(NESTED, &root, &topology, &Parent::Run);
        let owner = module.graph_inside(None);
        let parent = Nested::new(Some(module));

        let job = Job::new(NESTED, &owner, &topology, &parent.as_parent());

        assert_eq!(job.node().name(), "adopted-1");
        let Handle::AdoptedModule(adopted) = taskflow.graph().node(NESTED).handle() else {
            panic!("the module node owns its graph");
        };
        assert!(
            std::ptr::eq(job.graph(), adopted),
            "the graph is the handle's own, not a copy of it"
        );
        assert_eq!(
            taskflow.graph().node(NESTED).name(),
            "root-1",
            "the silent answer the path replaces"
        );
    }

    /// The fourth: a job of a subflow's subgraph resolves in the graph that
    /// came *out* of the node's lock, which is the shape `executor::subflow`
    /// lands on (#50) and the module docs argue for. The scheduling arm is
    /// that issue's; what is pinned here is that the lease resolves and that
    /// the lock holds nothing while its graph is out being run.
    #[test]
    fn a_job_of_a_subflows_subgraph_resolves_in_the_leased_graph() {
        let taskflow = nesting(Handle::Subflow {
            work: SubflowWork::new(|_: &mut Subflow<'_>| {}),
            subgraph: Mutex::new(named_pair("sub")),
        });
        let topology = run_of(taskflow.flow());
        let root = GraphOwner::flow(taskflow.flow());
        let spawner = Job::new(NESTED, &root, &topology, &Parent::Run);

        let Handle::Subflow { subgraph, .. } = taskflow.graph().node(NESTED).handle() else {
            panic!("the subflow node owns its subgraph");
        };

        // What the arm does with the guard it holds: a `Mutex` is no place
        // for the jobs of a running graph to look, so the graph comes out of
        // it for the length of the run.
        let leased = std::mem::take(&mut *subgraph.lock().expect("uncontended"));
        let owner = spawner.graph_inside(Some(leased));
        let parent = Nested::new(Some(spawner));

        let job = Job::new(NESTED, &owner, &topology, &parent.as_parent());

        assert_eq!(job.node().name(), "sub-1");
        assert!(
            subgraph.lock().expect("uncontended").is_empty(),
            "the lock holds nothing while its graph is out being run"
        );
        assert_eq!(
            taskflow.graph().node(NESTED).name(),
            "root-1",
            "the same index names another node in the graph the subflow hangs off"
        );
    }

    /// A job is five words, of which the owner is two: a tag and a pointer.
    /// It was four before #74, when the only graph a job could name was a
    /// taskflow's storage.
    ///
    /// Pinned because one job is built per task on the hot path and goes
    /// through a work-stealing deque by value, so a sixth word is a decision
    /// to be measured rather than an accident -- `BENCHMARKS.md` has what the
    /// fifth one cost.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn a_job_is_five_words() {
        assert_eq!(size_of::<GraphOwner>(), 16, "a tag and one pointer");
        assert_eq!(size_of::<Job>(), 40);
    }

    /// The sentinel run's whole estate word, which every case that records a
    /// failure on a free node has to leave at zero.
    ///
    /// The word rather than [`Topology::cancelled`], because `CAUGHT` is not
    /// in that mask and a claim nobody can read back would be as wrong as a
    /// cancellation: this run is one allocation for the process, and the
    /// correct number of bits ever set on it is none.
    fn sentinel_estate() -> u32 {
        Topology::free_standing()
            .estate
            .estate
            .load(Ordering::Relaxed)
    }

    /// The free-standing node of a silent async task, which is the fifth
    /// kind of graph a job can walk.
    fn free_node() -> Arc<AsyncNode> {
        AsyncNode::new("free", Handle::Async(AsyncWork::plain(|| {})))
    }

    /// A job of a free node resolves like a job of any other graph: the one
    /// node of the task's own arena, the arena itself, and the sentinel run.
    /// That is the whole point of giving an async task a one-node graph --
    /// the dispatch learns nothing new.
    #[test]
    fn a_job_of_a_free_node_resolves_to_that_node() {
        let node = free_node();
        let job = Job::free(&node, &Parent::Run);

        let (resolved, graph, topology) = job.parts();
        assert!(
            std::ptr::eq(resolved, node.node()),
            "the job names the task's one node"
        );
        assert!(
            std::ptr::eq(graph, node.graph()),
            "and resolves it against the task's own arena"
        );
        assert!(
            std::ptr::eq(topology, &**Topology::free_standing()),
            "the run is the sentinel every free node shares"
        );
        assert_eq!(resolved.name(), "free");
        assert!(resolved.handle().is_async());
    }

    /// Both halves of what the sentinel has to make true. A free node with
    /// no parent is never skipped -- the sentinel is never cancelled, which
    /// is what makes the first term of `is_parent_cancelled` harmless -- and
    /// a free node *under* a parent still answers for that parent, which is
    /// how a task group cancels the children it owns.
    #[test]
    fn a_free_node_is_cancelled_only_by_a_parent_above_it() {
        let node = free_node();

        assert!(
            !Job::free(&node, &Parent::Run).is_parent_cancelled(),
            "nothing cancels the run a free node claims to be part of"
        );

        let parent = Nested::new(None);
        let under_parent = Job::free(&node, &parent.as_parent());
        assert!(!under_parent.is_parent_cancelled());

        // Upstream's `ESTATE::CANCELLED` on the parent, which is what a task
        // group's cancel sets on the anchor its children hang off.
        parent
            .base()
            .estate()
            .fetch_or(Cancellable::CANCELLED, Ordering::Relaxed);
        assert!(
            under_parent.is_parent_cancelled(),
            "a cancelled parent skips the free nodes under it"
        );
    }

    /// A free node whose chain roots at a **cancelled run** is skipped, and
    /// one whose chain roots at a live run is not. #92's runtime child is
    /// that shape: a free node spawned from the body of a node of a real
    /// run, hanging off that node.
    ///
    /// Issue #115's second half, and [`Job::record_panic`]'s fix one function
    /// along. The child's own job holds the sentinel -- never cancelled, by
    /// design -- so the run that is actually waiting can only be read off the
    /// job at the chain's *root*, which is why the walk carries one along.
    ///
    /// **How a regression surfaces**: as a `false` from the second assertion,
    /// which is a child of a cancelled run doing work the run has already
    /// given up on -- and, for a run cancelled because a task of it failed,
    /// work whose own failure has nowhere left to go.
    #[test]
    fn a_free_node_under_a_cancelled_run_is_cancelled_through_its_chain() {
        let mut taskflow = Taskflow::new();
        let spawner = taskflow.emplace(|| {});
        let flow = Arc::clone(taskflow.flow());
        let topology = run_of(&flow);
        let owner = GraphOwner::flow(&flow);
        set_up_graph(flow.graph());

        let parent = Nested::new(Some(Job::new(spawner, &owner, &topology, &Parent::Run)));
        let child = Job::free(&free_node(), &parent.as_parent());

        assert!(
            !child.is_parent_cancelled(),
            "a live run runs the free nodes spawned under it"
        );

        assert!(
            topology.cancel(),
            "the run is in flight, so it can be cancelled"
        );
        assert!(
            child.is_parent_cancelled(),
            "a child of a cancelled run is skipped, though its own job holds the sentinel"
        );
        assert!(
            !Topology::free_standing().cancelled(),
            "and the sentinel took no part in it"
        );
    }

    /// The sentinel never reports a last job, which is what keeps the
    /// tear-down -- a run policy and a waiter that do not exist -- out of
    /// reach. Queue and finish are paired here as the scheduler pairs them,
    /// which is the condition the phantom job is counted on top of.
    #[test]
    fn the_sentinel_run_never_finishes() {
        let node = free_node();
        let job = Job::free(&node, &Parent::Run);

        job.parent().count_queued(job.topology(), 1);
        let (parent, topology) = job.finish().into_parts();

        assert!(matches!(parent, Parent::Run));
        assert!(
            !topology.job_finished(),
            "the phantom job is what the counter can never get past"
        );
        assert!(!topology.cancelled(), "and nothing can cancel the sentinel");
    }

    /// A failure on a parentless free node stays on that node's own slot --
    /// [`Job::record_panic`]'s last case, which a free node is the first
    /// thing in the crate to reach -- and leaves the sentinel run exactly as
    /// it was found.
    ///
    /// Both halves matter and the second is issue #105's. The sentinel is one
    /// allocation for the whole process, read by every free node through
    /// [`Job::is_parent_cancelled`], so an `EXCEPTION` bit recorded on it
    /// would skip every async task submitted after the first failure for the
    /// life of that process -- and, since a skipped async job never reaches
    /// the counters its tear-down owes, hang the next `wait_for_all` as well.
    /// The first half is what [`AsyncTask::take_panic`](crate::AsyncTask)
    /// reads.
    #[test]
    fn a_parentless_free_nodes_failure_stays_on_its_own_node() {
        let node = free_node();
        Job::free(&node, &Parent::Run).record_panic(Box::new("boom"));

        let payload = node.take_panic().expect("the node's own slot has it");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
        assert!(node.take_panic().is_none(), "a payload is taken once");

        assert_eq!(
            sentinel_estate(),
            0,
            "the sentinel took no bit of the failure"
        );
        assert!(!Topology::free_standing().cancelled());
        assert!(
            !Job::free(&free_node(), &Parent::Run).is_parent_cancelled(),
            "so the next free node of the process still runs"
        );
    }

    /// A free node *with* a parent above it walks that chain exactly as
    /// before: the anchor somebody is blocked on takes the payload, and
    /// neither the failing node nor the sentinel holds anything. Only a chain
    /// that roots at the sentinel ends with nobody.
    ///
    /// The anchor here is a [`Nested`] with nothing above it and its
    /// explicit-anchor bit held, which is the shape #93's task group gives
    /// the children it owns and the shape a `corun` frame already has.
    #[test]
    fn a_failure_on_a_free_node_under_an_anchor_is_delivered_to_it() {
        let parent = Nested::new(None);
        let _anchor = parent.base().anchor_explicitly();

        let node = free_node();
        Job::free(&node, &parent.as_parent()).record_panic(Box::new("boom"));

        let payload = parent.base().take_panic().expect("the anchor has it");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
        assert!(
            node.take_panic().is_none(),
            "the failing node kept nothing: the anchor is what is waiting"
        );
        assert_eq!(
            sentinel_estate(),
            0,
            "and the run the chain never reached is untouched"
        );
    }

    /// #90's shape, and the one the sentinel used to swallow outright: a free
    /// node that preempted itself to run a nested graph is an *implicit*
    /// anchor, and a failure under it is delivered there.
    ///
    /// The walk records such an anchor and keeps going, so what it does at
    /// the chain's root decides the case: a root that claimed to be an
    /// anchor would outrank the implicit one and take the payload itself.
    #[test]
    fn a_failure_under_an_implicitly_anchored_free_node_is_delivered_to_it() {
        let above = free_node();
        above
            .node()
            .base()
            .nstate()
            .fetch_or(Node::IMPLICITLY_ANCHORED, Ordering::Relaxed);
        let parent = Nested::new(Some(Job::free(&above, &Parent::Run)));

        let node = free_node();
        Job::free(&node, &parent.as_parent()).record_panic(Box::new("boom"));

        let payload = above.take_panic().expect("the implicit anchor has it");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
        assert!(node.take_panic().is_none(), "the failing node kept nothing");
        assert_eq!(sentinel_estate(), 0, "and the sentinel did not outrank it");
    }
}
