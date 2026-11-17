//! Translated from `Topology` in `taskflow/core/graph.hpp` and `_set_up_graph`
//! in `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! A [`Topology`] is one run of one [`Taskflow`](crate::Taskflow): the counter
//! that says how much of the graph is still outstanding, the policy that
//! decides whether to run the graph again, and the slot a `wait` blocks on. A
//! [`Job`] is one queued task of one such run -- upstream's `Node*`, plus
//! what upstream reaches through the node's `_topology` and `_parent`
//! pointers.
//!
//! # Why a job owns what it names
//!
//! Upstream's links between the three are all non-owning. A queued `Node`
//! holds a raw `Topology*`, a `Topology` holds a `Taskflow&`, and the only
//! `shared_ptr<Topology>` in the program sits in the taskflow's own run
//! queue -- `tf::Future` holds a `weak_ptr` and keeps nothing alive at all.
//! What makes that sound upstream is a rule in its documentation: do not
//! destroy or modify a taskflow while it is running.
//!
//! A port cannot rest on a rule the caller can break, because breaking this
//! one would be *safe* Rust causing a use-after-free. So both links are
//! owning here, and that is the whole of the argument:
//!
//! - a [`Job`] holds an `Arc<Topology>`, so the run it belongs to outlives
//!   it;
//! - a [`Job`] holds a [`GraphOwner`] -- whatever owns the graph it walks,
//!   which for a top-level node is the taskflow's storage: the graph itself
//!   and the queue of runs of it -- so the nodes it addresses outlive it
//!   too;
//! - a [`Topology`] holds a share of that same storage for the length of the
//!   run, which is what the thread that ends a run tears it down through.
//!
//! Nothing here reconstructs a reference from a raw pointer, and no part of
//! this module is `unsafe`. The lifetime on [`RunHandle`](crate::RunHandle)
//! is still what makes "do not mutate a running taskflow" a compile error;
//! what the shares add is what happens to a caller who gets *past* that
//! lifetime by leaking the handle. [`std::mem::forget`] on one ends the
//! borrow with the run still in flight, and the consequence is now a leak
//! rather than a dangling pointer: the run finishes, the storage is freed
//! when the last share of it goes, and an edit of the taskflow in the
//! meantime panics in [`Taskflow::graph_mut`](crate::Taskflow::graph_mut)
//! rather than racing the workers walking the graph. Issue #40 is where that
//! argument was wrong; `run_handle` has the rest of it.
//!
//! # The order the shares are released in
//!
//! One thing is load-bearing beyond mere liveness. `Arc::get_mut` is what
//! `Taskflow::graph_mut` rests on, so a share outstanding one instant too
//! long would turn an ordinary edit of a taskflow whose run has finished into
//! a panic -- and the thing a caller does the instant a `wait` returns is
//! edit the taskflow.
//!
//! The rule that answers it is about the *last* share rather than about any
//! particular holder: **the thread waiting on a run is released by whoever
//! drops the last share of that run's hold on the storage.** That hold is
//! [`Storage`], every job of the run's own graph owns a share of it, and its
//! [`Drop`] releases the storage and then the waiter -- so "the storage is
//! free" and "the waiter is awake" are one event in one order instead of two
//! events a worker has to be trusted to keep in it. The tear-down's last act
//! is accordingly to drop the run's own share, not to release the waiter
//! itself: [`Topology::release_flow`].
//!
//! Two rules used to say it instead, and issue #109 is where the pair turned
//! out not to add up to the guarantee:
//!
//! - a job releases its share of the storage before it decrements the join
//!   counter that may end the run -- [`Job::finish`], which is still how it is
//!   done and still enforced by *consuming* the job rather than by a comment;
//! - the tear-down released the run's own share before releasing the waiting
//!   thread, which it still does and which is still not enough.
//!
//! What the first rule does not cover is a share that no finishing job holds.
//! A node that **preempts** -- a module task, a subflow task, a runtime task
//! -- is dequeued as a job, and that job is then *copied*: once onto the frame
//! that is still inside the dispatch arm, once into the [`Nested`] its nested
//! graph is scheduled under, and once more as the job the last child of that
//! graph re-queues to resume the node. Only the third copy is ever `finish`ed.
//! The other two are dropped a few instructions later by frames that have
//! moved on, and nothing orders those few instructions against the resumed
//! node finishing the run -- so a `wait` could return, and on CI did, with a
//! copy of a preempted node's job still holding the storage and the caller's
//! next `emplace` panicking.
//!
//! Eliminating the copies instead is not available, and the reason is this
//! module's own ownership shape rather than anything about preemption. The
//! join counter a child of a nested graph decrements lives in the parent
//! *node*, which is in a graph the copy in `Nested` is what keeps alive, so a
//! child cannot give that copy up before the decrement that may end the run:
//! it would be decrementing a counter in freed memory. The copy on the
//! preempting frame has no decrement to be ordered against at all -- it is
//! dropped when the dispatch arm returns, and the run can end while it is
//! still on the way out. Both shares therefore have to outlive the decrement,
//! and what has to move is the release of the waiter.
//!
//! One consequence is worth naming: the thread that releases a waiter is no
//! longer always the one that tore the run down. It is whichever thread drops
//! the last share, which is the tear-down in the ordinary case and a worker
//! still unwinding a preemption in the case above. Nothing on either path
//! touches the run after that drop.
//!
//! One share is deliberately held *across* a tear-down, and it is worth
//! saying why that does not contradict the job rule either. Since #62 a worker
//! keeps one ready successor out of its queue to run next -- upstream's
//! `_update_cache`, the continuation loop in `WorkerThread::invoke` -- so a
//! job of the graph exists, holding a share of the storage, while the job
//! that readied it is spent and its join counter decremented. What makes
//! that sound is the *counting*, which happens before the successor is kept:
//! `Parent::count_queued` has already counted it against the same parent the
//! finishing job is about to count down, so that counter cannot reach zero
//! while a continuation is held. The decrement that ends a run, or that
//! resumes a preempted parent, is therefore never one with a continuation
//! outstanding, and the window a job's share has to stay valid for is as
//! wide as it was: one job, from the moment it is counted to the moment it
//! is spent.
//!
//! A job addresses its node by [`Task`] index rather than by pointer, which is
//! what keeps the node itself out of the question: it comes out of the graph
//! through safe code, and a graph nobody can edit while a job names it cannot
//! invalidate an index either.
//!
//! # The five graphs a job can walk
//!
//! Upstream settles this before a job exists. `_schedule_graph` takes a
//! `Graph&`, every node of that graph then holds nothing but its own state
//! and its own pointers, and what keeps the reference valid is the same
//! sentence of documentation as the rest. A port whose jobs own what they
//! name has to say instead *who owns* each graph the scheduler can be asked
//! to run, and the five answers are not one shape: that choice is
//! [`GraphOwner`], and issue #74 is where it was made once rather than three
//! times over the hottest struct in the crate.
//!
//! Two of the five are a share of a taskflow's storage -- the run's own
//! graph, and the target of a module task composed of an `Arc<Taskflow>` --
//! which is why a module task was schedulable (#49) before this field was
//! widened at all. Two more live *inside* the handle of the node they hang
//! off, an adopted module's `Graph` and a subflow's `Mutex<Graph>`, so the
//! only thing that names one is the owner of the *outer* graph plus that
//! node's index as a path. [`Inside`] is that path, and it is recursive
//! because the nesting is: a subflow of an adopted module of a composed
//! taskflow is three links. It cannot close into a cycle, because a graph a
//! node owns cannot contain the node that owns it.
//!
//! The fifth is #86's and is the one that belongs to nobody: the one-node
//! graph an *async task* is, which is in no taskflow and under no node.
//! `executor::async_node` owns that shape and says why an async task carries
//! a whole arena for its one node; what this module adds is the owner
//! variant that resolves it and the job that names it.
//!
//! What all this buys is the thing a `Graph&` cannot say. A job of an adopted
//! module's graph used to resolve its [`Task`] against the *root* arena --
//! silently, against whatever node happened to sit at that index -- which is
//! why the arm that ran one refused the run until this landed.
//!
//! # The run a free node claims to be part of
//!
//! A [`Job`] holds an `Arc<Topology>` because every job upstream can reach a
//! `Topology*` through its node, and nothing in the scheduler asks whether
//! that pointer is null before using it: `Job::parts` hands one out,
//! `is_parent_cancelled` reads one, and the tear-down accounts a finished job
//! to one. Upstream's async node has `_topology == nullptr`, and its
//! `_tear_down_async` is a *separate* tear-down precisely so that the null
//! never reaches the code that would dereference it.
//!
//! So a free node needs an answer to "which run is this part of", and #86
//! had two to choose between: make the field an `Option`, or share one
//! sentinel run that never finishes. **It is the sentinel**, and the reason
//! is that the other answer spreads: `Topology` is reached through
//! [`Job::parts`], [`Job::topology`] and [`Job::topology_share`], so an
//! `Option` there would put a `None` case into the worker's dispatch, into
//! every arm that schedules a nested graph, into the corun path and into the
//! tear-down -- five files of plumbing to express one thing that is already
//! expressible, since a run nothing can wait for and nothing can cancel is
//! exactly what a free node wants to be part of.
//!
//! [`Topology::free_standing`] is that run, and four properties make it
//! sound rather than merely convenient:
//!
//! - **It never finishes.** Its join counter is born at one, for a job that
//!   does not exist and will never be completed. Every real job raises that
//!   counter before it is queued and lowers it as it finishes -- the
//!   scheduler's rule for every topology, not this one's -- so the counter is
//!   one plus the number of free nodes in flight and the `fetch_sub` that
//!   ends a job can never see it at one. The tear-down, which would evaluate
//!   a run policy and release a waiter that does not exist, is therefore
//!   unreachable. That is what "never finishes" has to mean here, and it is
//!   why the sentinel is not simply a `Topology::nothing_to_run`, which is
//!   finished before it is handed out.
//! - **It is never an anchor.** An anchor is a thing a failure can be
//!   *delivered* to, which is to say a thing somebody is waiting on, and
//!   this is the one run nobody can wait on. So [`Job::record_panic`] ends a
//!   chain that roots here instead of claiming it -- upstream's null
//!   `_topology`, which its `_process_exception` reaches by the same route --
//!   and the payload stays on the failing node, where
//!   [`AsyncTask::take_panic`](crate::AsyncTask::take_panic) reads it. Issue
//!   #105 is where this was a claim the walk did not keep: the sentinel took
//!   the payload of the first free node to fail, and the bullet below was
//!   false from that moment on.
//! - **It is never cancelled.** Nothing hands out a
//!   [`Canceller`](crate::Canceller) for it, and nothing records a failure on
//!   it either -- which is the bullet above, and is why that one comes first.
//!   So [`Job::is_parent_cancelled`] reads `false` for a free node with no
//!   parent, and reads the chain above it when there is one. A task group
//!   cancels its children through exactly that chain.
//! - **It holds no taskflow.** Its storage slot is `None`, so sharing it
//!   keeps nothing alive but itself: one allocation for the whole process,
//!   which is what keeps an async task at upstream's one allocation per task
//!   beyond its body.
//!
//! # Why a subgraph comes out of its lock to be run
//!
//! The subflow is the one of the five that needs more than a path, because a
//! `Mutex<Graph>` cannot be read without a guard. Three ways to answer that
//! were on the table and only one of them schedules a graph whose jobs can
//! run at the same time as each other:
//!
//! - **lock per access.** A guard is exclusive, so the jobs of one subgraph
//!   would run one at a time. It is worse than it sounds: the work a node
//!   runs *is* in the node, so `WorkerThread::invoke` holds its graph for the
//!   length of the body, and the guard would be held across user code rather
//!   than taken for a lookup. A body that coruns its own subgraph --
//!   `Subflow::join`, which is #50 -- holds that same guard while it waits,
//!   which turns the serialisation into a deadlock. [`Taskflow::dump`] meets
//!   this lock too and answers it with `try_lock`-and-skip; a scheduler has
//!   no such answer available, because a graph it declines to read is a run
//!   that never finishes.
//! - **hold one guard for the length of the nested run.** The same two costs
//!   with the lock held longer, and it does not type-check either: a
//!   `MutexGuard` carries the lifetime of the mutex it came from, and a job
//!   outlives every frame that could lend it one -- which is the argument the
//!   parent chain already rests on.
//! - **take the graph out while you have the guard**, which is what
//!   [`Inside`] does. The subgraph is moved out of its lock for the length of
//!   its run and held by every job of it through that one shared allocation,
//!   so resolving a node of a subgraph costs exactly what resolving a node of
//!   a module's target costs, and no lock is on the hot path at all.
//!
//! Taking it needs `&mut Graph`, and that is the whole of what happens if the
//! body that owns the guard is still running: a caller cannot ask for the
//! lease without exclusive access to the subgraph, so either the body hands
//! it over out of its own guard or the body has finished and dropped the
//! guard before the dispatch arm takes the lock. The scheduler never
//! contends for it, and there is no `try_lock` to lose. What a concurrent
//! dump sees in the meantime is a subgraph with nothing in it, which is what
//! it already shows for a subgraph whose lock is held, so the lease adds no
//! observable of its own.
//!
//! Putting the graph back where it came from is #50's, and the answer is
//! `Drop for Inside`: **the last share of a lease is what sends the graph
//! home**, through `subflow::return_subgraph`, which also applies the
//! clear-unless-`RETAIN_SUBFLOW` rule that decides whether it goes back at
//! all. [`Inside`] is the path that restore resolves the destination lock
//! through, which is the other half of why the type is here.
//!
//! The frame that is *waiting* for a nested run would be the obvious
//! restorer, and it cannot be, which is worth stating in this module because
//! the reason is this module's ownership shape rather than anything about
//! subflows. A job of a graph nested inside a leased subgraph decrements the
//! join counter of its parent node -- a node **of the leased graph**, so the
//! counter is inside the lease and the job must hold the lease to reach it --
//! and [`finish_job`](crate::executor::Shared::finish_job) releases that hold
//! *after* the decrement, which is the decrement that can end the leased
//! graph's own run. So a straggling share can outlive the run it belongs to by
//! a few instructions, no holder can prove it is the last, and a waiting
//! frame's `Arc::into_inner` would fail at random.
//! [`GraphOwner::take_lease`] is that attempt all the same, for the one
//! caller that benefits from the common case; `executor::subflow` has what
//! losing the race costs.
//!
//! # Why a parent travels with the job
//!
//! Upstream gives every node a `NodeBase* _parent`, written by `_set_up_graph`
//! and read by `_tear_down_nonasync`: the thing whose join counter the node
//! decrements when it finishes. For a top-level node that pointer *is* the
//! topology; for a node of a subflow's subgraph, of a module's target graph or
//! of an adopted graph it is the node that graph hangs off; for a
//! [`corun`](crate::Executor::corun) it is an anchor on the corunning
//! worker's stack frame.
//!
//! None of those three survives being stored on the node here, and the reason
//! is ownership rather than taste:
//!
//! - a **raw** parent pointer is what #40 took out of this module, and a
//!   nested graph does not need it back: the whole of the executor is safe
//!   code and the crate's only `unsafe` is in `executor::notifier`;
//! - an **owning** parent pointer would be a cycle. A node of a subgraph lives
//!   inside the handle of the node that owns it, which lives in the arena of
//!   the taskflow's `Flow`, so a parent handle stored on that child would be a
//!   share of a `Flow` held by a value inside that same `Flow` -- a leak, and
//!   one no tear-down could be trusted to break, since a run abandoned by a
//!   leaked handle never reaches its tear-down at all;
//! - a **borrowed** parent is what the stack anchor would be, and a node
//!   outlives any frame that could lend it one.
//!
//! So the parent is not on the node: it is on the [`Job`], which is the value
//! the scheduler creates and which already owns everything it names. A
//! [`Parent`] is either the run itself -- upstream's `parent ==
//! node->_topology`, as a variant rather than a pointer comparison -- or a
//! [`Nested`] share, which holds the *job of the parent node*. That makes the
//! parent chain a chain of jobs, each owning the one above it up to the root,
//! and every one of them owning the storage its own node lives in. The
//! liveness argument is therefore the one above, restated once per level, and
//! `Nested` is also what lets a finished child resume a preempted parent: the
//! job to re-queue is the one the parent was dequeued as.
//!
//! A job of a nested graph keeps a share of the taskflow's storage alive
//! through its parent chain, and *that* is the share the ordering section is
//! about: it is released when the child's own `Parent` is dropped, which is
//! after the decrement that may have ended the run rather than before it.
//! The job rule cannot reach it, for the reason that section gives -- the
//! counter the child decrements is inside what the chain keeps alive -- so
//! nothing about the chain is ordered against a `wait` returning. What makes
//! that harmless is that the waiter is released by the last share of
//! [`Storage`], and every link of a parent chain holds one: the chain is
//! therefore not a hole in the guarantee but one of the holders of it.

use std::any::Any;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::task::{Poll, Waker};

use crate::executor::async_node::AsyncNode;
use crate::executor::subflow;
use crate::graph::{Graph, Handle, Node, NodeBase, Task};
use crate::task::TaskRef;
use crate::taskflow::Flow;

/// One unit of work in a worker's queue: upstream's `Node*`, plus what
/// upstream reaches through the node's `_topology` and `_parent` pointers.
///
/// Upstream's queues hold a bare `Node*` because a `Node` knows its own
/// topology and its own parent; a node here is a slot in an arena that knows
/// nothing about the run it is taking part in, so the job carries both
/// instead. The four fields are a [`Task`] index, a [`GraphOwner`] for the
/// graph that index is into, a share of the run, and the [`Parent`] this
/// job's graph was scheduled under -- which is what the module docs'
/// arguments cost, and what buys a queue of jobs that cannot dangle.
///
/// `Clone` because resuming a preempted node means queueing the job it was
/// dequeued as a second time, and because the parent chain shares the job of
/// each parent with every child under it. Every field is a handle, so a clone
/// is an index and two or three atomic increments -- the third being the
/// parent's, for a job of a nested graph.
///
/// Five words, of which the owner is two: a tag and one pointer. It was four
/// before #74, when the only thing a job could name was a taskflow's storage
/// and one pointer said which; `BENCHMARKS.md` has what the fifth cost on
/// the two shapes built to measure the scheduler and nothing else.
#[derive(Clone)]
pub(crate) struct Job {
    /// Which task of the graph to run.
    task: Task,
    /// Who owns the graph `task` indexes, which is the whole of what makes
    /// that index resolvable: one of the five kinds the module docs set out.
    ///
    /// The job holds its own owner rather than reading the run's, and for a
    /// top-level node the two are the same `Arc<Flow>`: the run's share has
    /// to be *takeable* -- see [`Topology::release_flow`] -- and therefore
    /// lives behind a mutex, which is no place for a hot path to go.
    ///
    /// This is the field #74 widened, and what it widened from is where the
    /// shape came from: it was an `Arc<Flow>`, which already covered two of
    /// the kinds, and two more needed a path rather than a share.
    /// [`Job::graph`] is the single place that resolves whichever it is, and
    /// nothing above it has to know which it was.
    owner: GraphOwner,
    /// The run `task` is part of.
    topology: Arc<Topology>,
    /// What this job's graph was scheduled under: the run itself for a
    /// top-level node, or a [`Nested`] parent for a node of a nested graph or
    /// of a corun.
    parent: Parent,
}

/// Which graph a [`Job`]'s [`Task`] indexes, and what keeps that graph alive:
/// upstream's `Graph&`, as a choice of owner rather than a reference.
///
/// Four variants for the five kinds of graph the scheduler can run, because
/// what differs between the five is only who owns them, and on that question
/// the two nested-in-a-node kinds pair up. The module docs have the argument
/// in full, including why the subflow's is the one that cannot be read where
/// it lies and why a free async node's arena is nobody's but its own.
///
/// The run's own graph has a variant to itself even though a module's target
/// is a taskflow's storage too, and the ordering section of the module docs
/// is the whole of why: the shares of *that* graph are the ones a waiting
/// thread is released by.
///
/// `Clone` because every job of a graph holds an owner of its own, so
/// scheduling a graph of `n` sources clones this `n` times: one atomic
/// increment each, whichever variant it is.
#[derive(Clone)]
pub(crate) enum GraphOwner {
    /// The graph of the run itself, held through the run's [`Storage`] hold
    /// on the taskflow's storage rather than through a bare share of it --
    /// which is what makes the last of these shares to go the thing that
    /// releases the thread waiting on the run (#109).
    Run(Arc<Storage>),
    /// A taskflow's storage reached without such a hold, which is the target
    /// of a module task composed of an `Arc<Taskflow>` and the graph of a
    /// [`corun`](crate::Executor::corun).
    Flow(Arc<Flow>),
    /// A graph that one node of another graph owns, which is the next two:
    /// an adopted module's graph, and a subflow's subgraph.
    Inside(Arc<Inside>),
    /// The one-node arena an async task is, which belongs to no taskflow and
    /// to no node: `executor::async_node`, whose own docs say why a free node
    /// carries a graph at all.
    ///
    /// The share is upstream's reference count on the node, which the queued
    /// job holds for as long as it is queued -- so a caller who drops the
    /// last [`AsyncTask`](crate::AsyncTask) handle on a task that has not run
    /// yet does not take the node out from under the worker.
    Async(Arc<AsyncNode>),
}

/// A run's hold on the storage of the taskflow it is a run of: the share
/// every job of that run's own graph owns, and -- when the last of those
/// shares goes -- what releases the thread waiting on the run.
///
/// Upstream has nothing here, for the same reason it has no [`GraphOwner`]:
/// its `Topology` holds a `Taskflow&`, its queued nodes hold nothing at all,
/// and so there is no last share to be the last of. Here there is, and the
/// ordering section of the module docs is why the waiter hangs off it rather
/// than off the tear-down: a job copied out of a node that *preempted* cannot
/// be made to give its share up before the decrement that ends the run, so
/// the run is made to end after the share instead (#109).
///
/// One allocation per run, handed out by [`Topology::storage`] and cloned by
/// every job that run's graph is scheduled as -- so a job's hold on the graph
/// it walks still costs the one `Arc` it cost when this was a bare
/// `Arc<Flow>` (#40), with one pointer hop more to resolve it.
pub(crate) struct Storage {
    /// The storage itself: the graph and the queue of runs of it.
    ///
    /// An `Option` for the length of this type's [`Drop`] and no longer. The
    /// share has to be released *before* the waiter, and drop glue -- which
    /// runs after `Drop::drop` -- would get the two the wrong way round.
    flow: Option<Arc<Flow>>,
    /// The run whose waiter the last share of this hold releases, or an empty
    /// [`Weak`] for a hold that releases nobody.
    ///
    /// `Weak` rather than a share because the topology holds one of *these*
    /// until its tear-down: a share back would be a cycle, and the only thing
    /// that would break it is a run completing -- which a run abandoned by a
    /// leaked handle never does. Nothing is lost by it, since every job that
    /// holds this holds an `Arc<Topology>` beside it and the thread being
    /// released holds one too: the upgrade can only fail for a run nobody can
    /// still be waiting on.
    run: Weak<Topology>,
}

/// A graph owned by one node of another graph, named by that node.
///
/// Upstream needs nothing here: the arena hands out a `Graph&` and the node
/// it came out of is no longer part of the question. Here it is the whole of
/// the question, because that graph is *inside* the node's handle -- so the
/// only thing that names it is whoever owns the graph the node is in, plus
/// the node's own index. One allocation per nested graph scheduled, shared by
/// every job of it.
///
/// The chain is a chain of owners up to the run's own storage, and every link
/// of it is owning, which is the same statement the parent chain makes: a job
/// of a subflow of an adopted module keeps all three graphs alive because it
/// holds the innermost. It cannot be a cycle -- a graph a node owns cannot
/// contain the node that owns it -- so the recursion terminates at a
/// [`GraphOwner::Flow`] after as many links as the graph has nesting levels.
pub(crate) struct Inside {
    /// Who owns the graph the owning node lives in: the next link up.
    owner: GraphOwner,
    /// The owning node's index in that graph.
    node: Task,
    /// The graph itself, when it could not be left where it lies: a subflow's
    /// subgraph, moved out of the `Mutex` the body built it behind so that
    /// the jobs of it need no guard. `None` for an adopted module's graph,
    /// which the path above reads in place.
    ///
    /// The module docs say why a lock is not an option for a graph that is
    /// being run, and who may take this out of it.
    leased: Option<Graph>,
}

/// What the jobs of one graph count down to: upstream's `NodeBase* _parent`.
///
/// Two cases, where upstream has one pointer and a comparison against
/// `node->_topology` to tell them apart. Making them variants is what lets
/// that comparison be a `match` rather than a pointer identity -- and the
/// module docs say why the parent is here rather than on the node.
#[derive(Clone, Debug)]
pub(crate) enum Parent {
    /// The run itself, which is every top-level node's parent: upstream's
    /// `parent == node->_topology`. The topology is the one the job already
    /// holds, so the variant carries nothing.
    ///
    /// It is also what a free node with nothing above it carries, where the
    /// run it names is the sentinel [`Topology::free_standing`] -- upstream's
    /// *null* `_parent` over its null `_topology`. The two are told apart by
    /// the run rather than by a variant of their own, which is
    /// [`Topology::is_free_standing`], and the one place the difference is
    /// load-bearing is [`Job::record_panic`]: a chain rooted at a real run
    /// ends at an anchor, a chain rooted at the sentinel ends at nothing.
    Run,
    /// A graph nested inside the run: a node whose own graph is running, or
    /// the anchor a [`corun`](crate::Executor::corun) waits on.
    Nested(Arc<Nested>),
}

/// The parent of a nested graph, shared by every job of that graph.
///
/// Upstream's `NodeBase*` pointing at something that is not the topology,
/// which is a node whose own graph is running or the anchor a `corun` keeps on
/// its frame. This type is both, because the jobs under it treat the two
/// identically: they decrement a join counter, and the last one to drive it to
/// zero looks at what there is to do next.
#[derive(Debug)]
pub(crate) struct Nested {
    /// The state words when there is no node, which is upstream's local
    /// `NodeBase anchor` in `corun`: a counter of its own for the frame that
    /// is waiting to poll, the explicit-anchor bit that frame holds while it
    /// waits, and the payload slot a failure under it is delivered to.
    ///
    /// Unused when `resume` is set, because then the words that matter are the
    /// parent node's own -- its join counter is what a nested graph counts
    /// down, and its `nstate` is where `PREEMPTED` lives. Carrying both in one
    /// type rather than splitting them into two variants costs three words per
    /// nested run and buys a parent that the scheduler never has to case over.
    anchor: NodeBase,
    /// The job of the node this graph hangs off, which is both where the real
    /// state words are and what the last child to finish re-queues to resume
    /// it. `None` for a corun anchor, which has no node: the frame that
    /// scheduled the graph is waiting on the counter itself.
    resume: Option<Job>,
}

impl Parent {
    /// Accounts for `jobs` more jobs of this parent's graph becoming ready,
    /// before any of them is queued.
    ///
    /// Upstream's `parent->_join_counter.fetch_add(num_srcs)` in
    /// `_schedule_graph` and its `node->_parent->_join_counter.fetch_add(1)`
    /// in the two successor loops of `_invoke`, which are the same line over
    /// a different count.
    pub(crate) fn count_queued(&self, topology: &Topology, jobs: usize) {
        match self {
            Self::Run => topology.jobs_queued(jobs),
            Self::Nested(nested) => {
                nested
                    .base()
                    .join_counter()
                    .fetch_add(jobs, Ordering::Relaxed);
            }
        }
    }

    /// Whether any parent above this one has been cancelled or has failed.
    ///
    /// The ancestor half of upstream's `Node::_is_parent_cancelled`, over the
    /// whole chain where upstream reads the immediate parent only. Upstream
    /// can stop at one level because its `_process_exception` walks the chain
    /// *upwards* marking every ancestor as it records a failure, which
    /// [`Job::record_panic`] now does too -- so one level would be enough
    /// here as well, and reading every level is kept because it answers
    /// strictly more of the question for one relaxed load per level of
    /// nesting.
    ///
    /// What the extra levels catch is a failure in a *sibling* subtree. A
    /// walk marks the chain from the failing node up to its anchor, so a job
    /// whose own parent is unmarked may still have a marked grandparent --
    /// the anchor that failed -- and every job under that anchor is doomed
    /// whichever branch it is on. Upstream runs those jobs; skipping them is
    /// the same answer sooner.
    fn ancestor_cancelled(&self) -> bool {
        let mut parent = self;
        loop {
            let Self::Nested(nested) = parent else {
                return false;
            };

            // Upstream's `ESTATE::CANCELLED | ESTATE::EXCEPTION` on a
            // `NodeBase`, which is the pair `Cancellable::cancelled` reads on
            // the topology: the two mean the same thing to the scheduler.
            let estate = nested.base().estate().load(Ordering::Relaxed);
            if estate & (Cancellable::CANCELLED | Cancellable::EXCEPTION) != 0 {
                return true;
            }

            match nested.resume() {
                Some(job) => parent = job.parent(),
                None => return false,
            }
        }
    }
}

impl Nested {
    /// The parent a nested graph is to be scheduled under.
    ///
    /// `resume` is the job of the node whose graph it is -- the job a child
    /// re-queues to resume a preempted parent -- and `None` is the anchor
    /// case, a [`corun`](crate::Executor::corun) with no node to resume,
    /// whose frame waits on the counter itself.
    ///
    /// Shared from the start because every job of the nested graph holds a
    /// share of it, and because whoever schedules that graph may keep one of
    /// its own -- a corun frame does, to wait on the counter.
    pub(crate) fn new(resume: Option<Job>) -> Arc<Self> {
        Arc::new(Self {
            anchor: NodeBase::new(),
            resume,
        })
    }

    /// This parent as a [`Parent`], to schedule a graph under.
    pub(crate) fn as_parent(self: &Arc<Self>) -> Parent {
        Parent::Nested(Arc::clone(self))
    }

    /// The state words this parent's children count down: the parent node's
    /// own, or the anchor's when there is no node.
    pub(crate) fn base(&self) -> &NodeBase {
        match &self.resume {
            Some(job) => job.node().base(),
            None => &self.anchor,
        }
    }

    /// The job that resumes the parent node, if there is one.
    pub(crate) fn resume(&self) -> Option<&Job> {
        self.resume.as_ref()
    }

    /// How many jobs of this parent's graph are still outstanding, read
    /// without blocking.
    ///
    /// Upstream's `p->_join_counter.load(acquire) == 0`, the predicate its
    /// `_corun_graph` waits on. Zero means the nested run is over, in the
    /// sense that every job of it has finished its tear-down.
    pub(crate) fn outstanding(&self) -> usize {
        self.base().join_counter().load(Ordering::Acquire)
    }
}

impl GraphOwner {
    /// The graph of the run itself, held through that run's hold on the
    /// taskflow's storage.
    ///
    /// A share rather than the `&Arc<Storage>` the caller has, because every
    /// job built over it keeps one and the caller's own goes out of scope.
    pub(crate) fn run(storage: &Arc<Storage>) -> Self {
        Self::Run(Arc::clone(storage))
    }

    /// The graph of a taskflow's storage reached without a run's hold on it: a
    /// module task's target, or a corun's graph.
    ///
    /// A share rather than the `&Arc<Flow>` the caller has, for the reason
    /// [`GraphOwner::run`] gives.
    pub(crate) fn flow(flow: &Arc<Flow>) -> Self {
        Self::Flow(Arc::clone(flow))
    }

    /// The graph this owner names.
    ///
    /// Upstream's `Graph&`, resolved rather than passed: one branch for a
    /// taskflow's storage, and for a graph a node owns either the lease or a
    /// step up the path -- which is a node lookup and a match per level of
    /// nesting, and nothing at all for the top-level case every static graph
    /// is.
    ///
    /// Every borrow it hands out is derived from `&self`, which is what keeps
    /// the whole of this safe: the owner holds the graph, or holds the owner
    /// that holds the node that holds the graph, up to a share of a
    /// taskflow's storage.
    ///
    /// # Panics
    ///
    /// Panics if a node on the path is not a live node of its graph, which
    /// cannot happen for an owner the scheduler built -- see [`Job::node`],
    /// whose panic is the same one for the same reason.
    pub(crate) fn graph(&self) -> &Graph {
        match self {
            Self::Run(storage) => storage.graph(),
            Self::Flow(flow) => flow.graph(),
            Self::Inside(inside) => inside.graph(),
            Self::Async(node) => node.graph(),
        }
    }

    /// Whether the graph this names has a task a run could start at.
    ///
    /// `Flow::has_source_task` over any owner, which is the question every
    /// arm that runs a nested graph asks before scheduling one: a graph with
    /// nothing to start at queues no job, so a node preempted for it would
    /// never be resumed and the run would hang. It walks the graph, as that
    /// method does and for the reason it gives -- a walk is cheaper than the
    /// allocation collecting the sources would need.
    pub(crate) fn has_source_task(&self) -> bool {
        self.graph()
            .iter()
            .any(|(_, node)| node.num_predecessors() == 0)
    }

    /// Takes the leased graph back out of this owner, if this is the last
    /// share of it.
    ///
    /// `Subflow::join`'s way home: the joining frame wants the graph back in
    /// the node before it returns, so that the body can read what it built,
    /// and it is the one caller that *can* ask -- it owns a share and is not
    /// itself a job. `None` means some straggling job of a graph nested inside
    /// this one still holds a share, in which case the policy is applied by
    /// that share's own `Drop`; `executor::subflow` says why that case cannot
    /// be ruled out and what it costs.
    ///
    /// Also `None` for an owner that is not a lease at all, which no caller
    /// has reason to ask about: a taskflow's storage -- held with a run's hold
    /// on it or without -- an adopted module's graph and an async task's own
    /// arena are all read where they lie.
    pub(crate) fn take_lease(self) -> Option<Graph> {
        match self {
            Self::Run(_) | Self::Flow(_) | Self::Async(_) => None,
            // Taking the field rather than moving it out, because `Inside`
            // implements `Drop` -- and leaving `None` behind is exactly what
            // keeps that `Drop` from returning the graph a second time.
            Self::Inside(inside) => {
                let mut inside = Arc::into_inner(inside)?;
                inside.leased.take()
            }
        }
    }

    /// The address that identifies the graph this owner names, for the hash
    /// and the `Debug` a job needs.
    ///
    /// The allocation this owner holds a share of, which is one per graph
    /// either way: the taskflow's storage for a graph that is a taskflow's,
    /// and the path allocation for a graph a node owns. Every job of one
    /// scheduled graph therefore agrees on it, because they all hold clones
    /// of the one owner the scheduling path built.
    ///
    /// A pointer rather than the resolved graph's own address, because the
    /// caller is on the path that pushes a job to the shared buffers and a
    /// path walk is not what a hash is for.
    fn identity(&self) -> *const () {
        match self {
            Self::Run(storage) => Arc::as_ptr(storage).cast(),
            Self::Flow(flow) => Arc::as_ptr(flow).cast(),
            Self::Inside(inside) => Arc::as_ptr(inside).cast(),
            Self::Async(node) => Arc::as_ptr(node).cast(),
        }
    }
}

impl Storage {
    /// A hold on `flow` whose last share releases `run`'s waiter: what
    /// [`Topology::for_run`] gives a run somebody is waiting on.
    fn gating(flow: Arc<Flow>, run: &Weak<Topology>) -> Arc<Self> {
        Arc::new(Self {
            flow: Some(flow),
            run: run.clone(),
        })
    }

    /// A hold on `flow` that releases nobody: what a topology built by
    /// [`Topology::new`] gets.
    ///
    /// Two kinds of run reach that constructor, and neither is waited for
    /// through the completion slot this would release. A
    /// [`corun`](crate::Executor::corun) is waited for by the frame that
    /// started it, watching the anchor its graph is scheduled under; and the
    /// topologies this crate's own tests build by hand are driven step by step
    /// rather than run. Both end through [`Topology::release_flow`], which
    /// releases the waiter itself for a hold that cannot.
    fn ungated(flow: Arc<Flow>) -> Arc<Self> {
        Arc::new(Self {
            flow: Some(flow),
            run: Weak::new(),
        })
    }

    /// The storage this is a hold on.
    ///
    /// # Panics
    ///
    /// Panics once [`Drop`] has released it, which is a state no caller can
    /// observe: the only `None` is the one the destructor leaves behind.
    pub(crate) fn flow(&self) -> &Arc<Flow> {
        self.flow
            .as_ref()
            .expect("a run's hold on its storage is released only as it is dropped")
    }

    /// The graph, which is what the jobs holding this resolve their task
    /// index in.
    fn graph(&self) -> &Graph {
        self.flow().graph()
    }

    /// Whether dropping the last share of this hold is what releases the
    /// thread waiting on the run.
    ///
    /// `false` for an [`ungated`](Storage::ungated) hold, whose `run` is an
    /// empty [`Weak`] and whose strong count is therefore zero. Asked by
    /// [`Topology::release_flow`], which is called on a live topology, so a
    /// gating hold cannot answer `false` by having outlived its run.
    fn gates_completion(&self) -> bool {
        self.run.strong_count() != 0
    }
}

impl Drop for Storage {
    /// Releases the storage and then the thread waiting on the run, in that
    /// order, which is the whole of the guarantee
    /// [`Taskflow::graph_mut`](crate::Taskflow::graph_mut) makes.
    ///
    /// This runs when the last share of the hold goes, and which thread that
    /// is depends on how the run ended: the tear-down's own
    /// [`Topology::release_flow`] is the common case, and a worker still
    /// dropping a copy of a preempted node's job is the case issue #109 was.
    /// Either way the waiter is released after the last share rather than
    /// before it, so a caller whose `wait` has returned finds `Arc::get_mut`
    /// on its taskflow's storage uncontested. The module docs' ordering
    /// section has why those copies cannot be ordered instead.
    ///
    /// `Drop for Inside` is the same shape one level in -- the last share of a
    /// subflow's lease is what sends the subgraph home -- and rests on the
    /// same observation: no holder of a share can prove it is the last.
    fn drop(&mut self) {
        drop(self.flow.take());

        // `None` only for a run nobody can be waiting on: see the field.
        if let Some(run) = self.run.upgrade() {
            run.finish();
        }
    }
}

impl Inside {
    /// The graph this node owns.
    ///
    /// # Panics
    ///
    /// Panics if the owning node is not a live node of its graph, as
    /// [`GraphOwner::graph`] says.
    fn graph(&self) -> &Graph {
        // The lease first: a graph that had to come out of its lock to be run
        // is the one thing the path below cannot read in place.
        if let Some(leased) = &self.leased {
            return leased;
        }

        match self.owner.graph().node(self.node).handle() {
            Handle::AdoptedModule(graph) => graph,
            // Unreachable for an owner the scheduler built: it was made from
            // the very handle the dispatch arm was holding, and a handle can
            // only be replaced through `&mut Taskflow`, which no run allows.
            // A subflow's handle does not reach here either -- its graph is
            // the lease above.
            handle => unreachable!("a nested graph hangs off {handle:?}, which owns none"),
        }
    }
}

impl Drop for Inside {
    /// Sends a leased subgraph home, which is issue #50's half of the lease
    /// and the only reason this type has a destructor.
    ///
    /// The last share of a lease is the one that runs this, and the module
    /// docs' lease section plus `executor::subflow` are the argument for why
    /// it has to be a `Drop` at all: a job of a graph nested inside the
    /// leased one holds the lease *across* the decrement that can end the
    /// leased graph's own run -- the counter it decrements is in a node of
    /// that graph -- so no holder can prove it is the last, and a waiting
    /// frame's `Arc::into_inner` would fail at random.
    /// [`GraphOwner::take_lease`] is that attempt, for the one caller that has
    /// a use for it.
    ///
    /// What "home" means -- back into the node's `Mutex`, or dropped, which
    /// is upstream's `g.clear()` -- is the subflow's policy rather than this
    /// module's, so it lives with the rest of it in
    /// [`subflow::return_subgraph`](crate::executor::subflow::return_subgraph).
    /// Nothing happens for the lease-free owners: an adopted module's graph is
    /// read where it lies, and so is a taskflow's.
    ///
    /// # Panics
    ///
    /// Panics if the owning node is not a live subflow node of its graph,
    /// which cannot happen for a lease the scheduler built: the chain of
    /// owners above it is owning, so the graph the node lives in is alive
    /// until after this runs.
    fn drop(&mut self) {
        if let Some(graph) = self.leased.take() {
            subflow::return_subgraph(self.owner.graph().node(self.node), graph);
        }
    }
}

impl Job {
    /// Builds the job that runs `task` of `owner`'s graph as part of
    /// `topology`, under `parent`.
    fn new(task: Task, owner: &GraphOwner, topology: &Arc<Topology>, parent: &Parent) -> Self {
        Self {
            task,
            owner: owner.clone(),
            topology: Arc::clone(topology),
            parent: parent.clone(),
        }
    }

    /// The job that runs the free-standing node of `node`, which belongs to
    /// no run and to no graph the caller built.
    ///
    /// Upstream's `_schedule(Worker&, Node*)` over an async node, where the
    /// node it is handed carries `_topology == nullptr` and the parent it was
    /// given at construction. Here both travel with the job, as they do for
    /// every other node: the parent is whatever scheduled this task --
    /// [`Parent::Run`] over the sentinel for a task submitted to the
    /// executor, which is upstream's null parent, or a [`Parent::Nested`] for
    /// a task spawned by a runtime body or owned by a task group -- and the
    /// run is the sentinel [`Topology::free_standing`], which the module docs
    /// argue for at length.
    ///
    /// The parentless pair is upstream's `nullptr | nullptr` row of
    /// `_tear_down_async`'s table, and it is a pair rather than one fact in
    /// two places: the walk in [`Job::record_panic`] ends at the sentinel,
    /// because a chain that gets there has nothing above it either.
    ///
    /// The share of the node is what keeps it alive while the job is queued,
    /// which is upstream's reference count on the same node for the same
    /// reason: a caller who drops the last handle on a task that has not run
    /// yet must not free the node under the worker that is about to run it.
    pub(crate) fn free(node: &Arc<AsyncNode>, parent: &Parent) -> Self {
        Self {
            task: AsyncNode::TASK,
            owner: GraphOwner::Async(Arc::clone(node)),
            topology: Arc::clone(Topology::free_standing()),
            parent: parent.clone(),
        }
    }

    /// The jobs that start a graph: one per source task of it.
    ///
    /// Call it only after [`set_up_graph`] has returned a non-zero count for
    /// the same graph and `parent` has been told about those sources, since
    /// that count is what the parent's join counter was raised by.
    ///
    /// An associated function rather than a method on [`Topology`] because
    /// what it needs is the topology's *share*, and `self: &Arc<Self>` is not
    /// a receiver Rust has.
    pub(crate) fn sources<'a>(
        owner: &'a GraphOwner,
        topology: &'a Arc<Topology>,
        parent: &'a Parent,
    ) -> impl Iterator<Item = Self> + 'a {
        owner
            .graph()
            .iter()
            .filter(|(_, node)| node.num_predecessors() == 0)
            .map(move |(task, _)| Self::new(task, owner, topology, parent))
    }

    /// The graph this job's node lives in, whichever of the five kinds it is:
    /// [`GraphOwner::graph`] is the resolution and the `owner` field is why
    /// there is one.
    pub(crate) fn graph(&self) -> &Graph {
        self.owner.graph()
    }

    /// The owner of a graph this job's *own* node owns: the job's own owner
    /// plus its task index, which is the only thing that names an adopted
    /// module's graph or a subflow's subgraph.
    ///
    /// This is what a dispatch arm hands to
    /// `Shared::schedule_graph` for the two kinds whose graph is not a share
    /// of anything -- where the `Handle::Module` arm hands over the
    /// `Arc<Flow>` its own handle holds. `leased` is the graph itself for a
    /// subgraph that had to come out of its lock, and `None` for a graph the
    /// path can read where it lies; the module docs say which is which and
    /// why.
    pub(crate) fn graph_inside(&self, leased: Option<Graph>) -> GraphOwner {
        GraphOwner::Inside(Arc::new(Inside {
            owner: self.owner.clone(),
            node: self.task,
            leased,
        }))
    }

    /// The node to run.
    ///
    /// # Panics
    ///
    /// Panics if the job's task is not a live node of the graph, which cannot
    /// happen for a job built by the scheduler: a node can only be erased
    /// through `&mut Taskflow`, and `Taskflow::graph_mut` refuses while a
    /// share of the storage -- this job's own, for one -- is outstanding. The
    /// same holds of every node on the path to a nested graph, which this
    /// resolves through.
    pub(crate) fn node(&self) -> &Node {
        self.graph().node(self.task)
    }

    /// The free-standing node this job runs, or `None` for a job of a graph
    /// somebody built.
    ///
    /// Upstream's `std::get_if<Node::DependentAsync>(&node->_handle)`, which
    /// is how both of its async tear-downs reach the state that is a *free*
    /// node's rather than a node's: the `FINISHED` bit and the successor
    /// list. Here that state is on [`AsyncNode`] -- `executor::async_node`
    /// says why a free node is a one-node arena -- and this is the only way
    /// to it from a job, since [`Job::node`] resolves past the arena to the
    /// [`Node`] inside it.
    ///
    /// The one caller is the dependent-async tear-down in
    /// `executor::dependent_async`, which marks the task finished and drains
    /// what was waiting for it. The share rather than a borrow because a
    /// caller that needs to *keep* the node -- to name it as a dependency of
    /// something else -- needs the count, and handing back `&AsyncNode`
    /// would make that a second accessor for no gain: a borrow of the share
    /// is still a borrow of the node.
    pub(crate) fn async_node(&self) -> Option<&Arc<AsyncNode>> {
        match &self.owner {
            GraphOwner::Async(node) => Some(node),
            GraphOwner::Run(_) | GraphOwner::Flow(_) | GraphOwner::Inside(_) => None,
        }
    }

    /// What this job's graph was scheduled under.
    pub(crate) fn parent(&self) -> &Parent {
        &self.parent
    }

    /// The run this job is part of.
    pub(crate) fn topology(&self) -> &Topology {
        &self.topology
    }

    /// This job's *share* of the run, which is what scheduling a graph needs:
    /// every job of a nested graph holds one of its own, so
    /// [`Shared::schedule_graph`](crate::executor::Shared) takes the share
    /// rather than a reference.
    ///
    /// The three dispatch arms that run a nested graph reach the run through
    /// here -- they have a `&Job` and nothing else -- which is why this sits
    /// beside [`Job::topology`] rather than being folded into it.
    pub(crate) fn topology_share(&self) -> &Arc<Topology> {
        &self.topology
    }

    /// Records that the work of this job's node failed with `payload`,
    /// delivering it to whatever is waiting for the graph that node is part
    /// of: upstream's `Executor::_process_exception`.
    ///
    /// Three kinds of thing can be waiting, and the walk is how the right one
    /// is found. From the failing node upwards, every base on the parent
    /// chain is marked `EXCEPTION` -- which is what cancels the rest of each
    /// nested graph the failure is inside of, since that bit is half of what
    /// the early-out at the top of `WorkerThread::invoke` reads -- and the
    /// walk stops at the first base that is **explicitly anchored**: a frame
    /// in a `corun` or a `join`, which is waiting for exactly this graph and
    /// will re-raise the payload itself. Failing that, the innermost base
    /// seen carrying `IMPLICITLY_ANCHORED` gets it -- a node that preempted
    /// itself and means to look. Failing *that* -- no anchor anywhere on the
    /// chain, which takes a chain rooted at neither a run somebody started
    /// nor a frame that is waiting -- the payload stays on the failing node,
    /// where upstream also leaves it: an execution with nothing above it to
    /// report to.
    ///
    /// A chain rooted at a run **somebody started** ends at an anchor, which
    /// upstream says by constructing its topology `EXPLICITLY_ANCHORED` and
    /// this port says by [`Parent::Run`] over such a run being an anchor by
    /// definition. So a plain static graph behaves exactly as it did before
    /// anchors existed: the failing node is marked, the walk reaches the run,
    /// and the run's own claim keeps the first payload -- the same pair of
    /// steps [`Topology::record_panic`] takes for a failure that has no chain
    /// to walk at all.
    ///
    /// The sentinel [`Topology::free_standing`] is the one root that is
    /// **not** an anchor, and that is issue #105's: it is upstream's null
    /// `_topology` on a free node, a run nothing can wait for, nothing can
    /// cancel and nobody can read a payload back out of. Claiming it would
    /// mean claiming the one topology every free node in the process shares
    /// -- setting the `EXCEPTION` that [`Job::is_parent_cancelled`] reads, so
    /// that one failed async task skipped every async task submitted after it
    /// for the life of the process. A chain whose root is the sentinel
    /// therefore ends like upstream's null parent: with no anchor at all, so
    /// that the payload lands on the failing node and
    /// [`AsyncTask::take_panic`](crate::AsyncTask::take_panic) can read it.
    /// A free node *with* a parent is not that chain -- it walks its
    /// [`Parent::Nested`] links first, and only a root reached through them
    /// decides the case.
    ///
    /// Which run that root is, is the job's at the *root* rather than this
    /// job's own, which is why the walk carries one along. The two differ for
    /// exactly one shape: a free node spawned under a node of a real run
    /// (#92's runtime child) holds the sentinel on its own job while the
    /// chain above it roots at the run that is actually waiting.
    ///
    /// Whichever anchor is chosen, the `fetch_or` of `EXCEPTION | CAUGHT` on
    /// it decides whether this payload is the first one and therefore the one
    /// kept, which is #32's rule moved from the run to the anchor. A payload
    /// that loses that race is dropped, and that is the one deliberate
    /// departure from upstream here -- the comment on the delivery below says
    /// why.
    ///
    /// Nothing here can dangle and nothing here is a pointer walk. The chain
    /// is held by this job -- a [`Parent::Nested`] owns the job of the parent
    /// node, which owns its own parent, up to the root -- so every base the
    /// walk touches is alive because the walk's own job is alive. That the
    /// bases are alive is a stronger statement than it looks: a job of a
    /// nested graph has not yet decremented its parent's join counter, so no
    /// ancestor can be *finished* either, and no frame can have stopped
    /// waiting for one.
    pub(crate) fn record_panic(&self, payload: Box<dyn Any + Send>) {
        let node = self.node().base();
        let mut base = node;
        let mut parent = Some(&self.parent);
        // The run of the job whose parent `parent` is, which is the run a
        // `Parent::Run` at the end of the walk names. It travels with the
        // chain rather than being read off `self` because the two are not
        // always the same run -- see the docs above.
        let mut topology: &Topology = &self.topology;
        let mut implicit: Option<Anchor<'_>> = None;

        // Upstream's `while(ea && !(ea->_estate & EXPLICITLY_ANCHORED))`, with
        // its `ea = ea->_parent` spelled as a step along this job's chain and
        // its null pointer as the `None` two of the arms below produce.
        let explicit = loop {
            if base.is_explicitly_anchored() {
                break Some(Anchor::Base(base));
            }

            base.estate()
                .fetch_or(Cancellable::EXCEPTION, Ordering::Relaxed);
            // Only the innermost one, as upstream's comment says: the first
            // implicit anchor the walk meets is the closest to the failure.
            if implicit.is_none() && base.is_implicitly_anchored() {
                implicit = Some(Anchor::Base(base));
            }

            match parent {
                // Upstream's `ea == nullptr`: the chain is over, with no
                // frame on it waiting. A corun anchor whose guard is not held
                // ends a chain this way.
                None => break None,
                // Upstream's other null, `_topology == nullptr` on a free
                // node: the sentinel is a run nothing can wait for, so a
                // chain rooted at it ends with no anchor rather than with
                // one nobody can read (#105).
                Some(Parent::Run) if topology.is_free_standing() => break None,
                // The root, anchored for the length of the run.
                Some(Parent::Run) => break Some(Anchor::Run(topology)),
                Some(Parent::Nested(nested)) => {
                    base = nested.base();
                    match nested.resume() {
                        Some(job) => {
                            parent = Some(job.parent());
                            topology = job.topology();
                        }
                        None => parent = None,
                    }
                }
            }
        };

        // Upstream's `if(ea) ... else if(ia)`, as one choice rather than two
        // tests: an explicit anchor that has *already* caught a payload does
        // not hand this one on to an implicit anchor, which two `if let`s in
        // a row would lose.
        let anchor = match (explicit, implicit) {
            (Some(anchor), _) | (None, Some(anchor)) => anchor,
            // Upstream's last line, and the case the failing node's own slot
            // exists for: no anchor anywhere on the chain, so there is
            // nothing waiting -- an execution with no external control over
            // it, which is upstream's own description of it. A *silent* async
            // task submitted to the executor is exactly that and is what
            // reaches this: its chain is one link long and roots at the
            // sentinel, which the walk ends on rather than anchors at, so the
            // payload stays where `AsyncTask::take_panic` reads it -- #86's
            // half of upstream's `exception_ptr()`. A corun anchor whose
            // guard is not held is the other way in.
            (None, None) => Anchor::Base(node),
        };

        // The first failure under this anchor is the one it reports, which is
        // #32's rule applied where the failure is delivered rather than only
        // at the run. A later one is dropped, and that is the one place this
        // walk does not follow upstream: upstream parks the loser on the
        // failing node, where `tf::Task::exception_ptr()` -- an accessor this
        // port does not have -- can read it back. With nothing able to read
        // it, parking it would hold a caller's payload, destructor and all,
        // until the next set-up of that graph, where #32's thread-safety case
        // pins the opposite: of a thousand simultaneous failures exactly one
        // payload survives and the rest are dropped by the run.
        if anchor.claim_panic() {
            anchor.keep_panic(payload);
        }
    }

    /// Resolves this job into the node to run, the graph holding it and the
    /// run it is part of.
    ///
    /// Safe, and that is the point of the type: the job owns a share of the
    /// storage and of the run, so neither can have gone away, and the node
    /// comes out of the graph by index through ordinary safe code.
    ///
    /// # Panics
    ///
    /// Panics if the job's task is not a live node of the graph, as
    /// [`Job::node`] does and for the same reason.
    pub(crate) fn parts(&self) -> (&Node, &Graph, &Topology) {
        (self.node(), self.graph(), &self.topology)
    }

    /// Whether this job may be skipped because its run or some parent above it
    /// has been cancelled or failed: upstream's `Node::_is_parent_cancelled`.
    ///
    /// Both halves of upstream's condition, which is why a nested graph needs
    /// it at all: a job of a cancelled run is skipped by the first term, and a
    /// job whose *parent* failed -- a subflow whose body panicked, say -- by
    /// the second, even though the run as a whole is still going.
    ///
    /// A free async node answers through the same two terms and needs no case
    /// of its own: the first reads the sentinel run, which is never cancelled
    /// because nothing ever records anything on it -- [`Job::record_panic`]
    /// is what has to keep that true -- so a task with no parent is never
    /// skipped, and the second walks whatever chain is above it, which is how
    /// a task group cancels the children it owns. The module docs have the
    /// sentinel's side of it.
    pub(crate) fn is_parent_cancelled(&self) -> bool {
        self.topology.cancelled() || self.parent.ancestor_cancelled()
    }

    /// Whether `graph` is a graph this job is already running inside of: the
    /// one its own node lives in, or the one any node above it on the parent
    /// chain lives in.
    ///
    /// Upstream has nothing to compare: `_schedule_graph` takes a `Graph&`
    /// and `_set_up_graph` re-arms whatever it is handed. This is issue #82's
    /// question, and `executor::module` is the one caller -- a graph armed a
    /// second time while a run of it is in flight loses the state that run is
    /// keeping in its nodes, and that module's docs have what it costs.
    ///
    /// The **parent** chain rather than the owner chain, which is the one
    /// subtlety. A composed `Arc<Taskflow>`'s graph is owned by a [`Flow`]
    /// and names nothing above it, so two taskflows composing each other are
    /// a cycle no [`GraphOwner`] link records; what does record it is the
    /// chain of parents, each link of which is the job of the node whose
    /// graph is running -- exactly the graphs of this run that are still in
    /// flight above this job.
    ///
    /// One pointer comparison per level of nesting, no allocation, and
    /// nothing that touches a lock: a module task of a top-level graph pays
    /// exactly one comparison, because its chain ends at [`Parent::Run`]. A
    /// corun's anchor ends a chain without naming the job that started it
    /// ([`Nested::resume`] is `None` for one), so a graph enclosing a corun
    /// is not seen -- which is the limit of what this answers, and
    /// `Executor::corun` of a taskflow one of whose own tasks is running is
    /// already reported by [`crate::Work::invoke`]'s `try_lock` instead.
    pub(crate) fn runs_inside(&self, graph: &Graph) -> bool {
        let mut job = self;
        loop {
            if std::ptr::eq(job.graph(), graph) {
                return true;
            }

            // Upstream's `parent == node->_topology` as the end of the walk:
            // the run's own graph is the graph of the job the chain ends at,
            // so it has been compared by the time this returns.
            let Parent::Nested(nested) = job.parent() else {
                return false;
            };
            match nested.resume() {
                Some(parent) => job = parent,
                None => return false,
            }
        }
    }

    /// The job that runs `task` as part of the same run of the same graph,
    /// under the same parent.
    ///
    /// What a worker releasing a successor queues: a job's successors are jobs
    /// of its own graph, so every handle is cloned from the ones already in
    /// hand rather than looked up again.
    pub(crate) fn successor(&self, task: Task) -> Self {
        Self::new(task, &self.owner, self.topology_share(), &self.parent)
    }

    /// Spends this job, handing back what the worker still owes: the parent to
    /// count down and the run to account the job to.
    ///
    /// The job's own share of whatever owns its graph is released here, and
    /// *here* is the point: the caller's next move is to decrement the join
    /// counter that may end the run, so this keeps the window a share is
    /// outstanding for as narrow as it can be -- one job, from the moment it
    /// is counted to the moment it is spent. Consuming the job is what makes
    /// the compiler enforce it: the node and the graph a worker was holding
    /// are borrows of this value, so nothing can touch them afterwards.
    ///
    /// What it is *not* is the whole of the editable-taskflow guarantee, and
    /// the module docs' ordering section says why: a preempted node's job is
    /// copied into places no `finish` reaches, so the waiter is released by
    /// the last share of the run's [`Storage`] hold rather than by the
    /// counter this decrement drives. Narrowing the window is still worth
    /// doing -- a share is a share, and this is the hot path that holds most
    /// of them.
    ///
    /// For a job of a nested graph the owner released here is the path or the
    /// lease rather than a share of the taskflow -- and that is the same rule
    /// one level in: the last job of a subgraph gives the lease up before the
    /// counter that resumes the node the subgraph came out of. The share it
    /// keeps through its [`Parent`] goes later, which the module docs cover.
    pub(crate) fn finish(self) -> Finished {
        let Self {
            task: _,
            owner,
            topology,
            parent,
        } = self;

        // Before the caller's `job_finished`, which is the whole reason this
        // method exists rather than letting the job fall out of scope.
        drop(owner);
        Finished { parent, topology }
    }
}

/// Where a failure found by [`Job::record_panic`] is delivered.
///
/// Upstream needs no such type: every link of its parent chain is a
/// `NodeBase*`, its topology included, so the anchor it stops at is one
/// pointer whatever kind of thing it turned out to be. Here the chain's root
/// is a run rather than a base -- [`Parent::Run`] -- and a run keeps its
/// payload in the completion slot a `wait` reads rather than in a slot of its
/// own. That is the whole of the difference, so this names the two cases and
/// gives them the one pair of operations the walk needs.
enum Anchor<'a> {
    /// A run somebody started, which is anchored by definition for as long
    /// as it is running. Never the sentinel: nobody is waiting on that one,
    /// so [`Job::record_panic`] ends such a chain rather than building this.
    Run(&'a Topology),
    /// A node waiting for its own nested graph, or the anchor a `corun` keeps
    /// on its frame.
    Base(&'a NodeBase),
}

impl Anchor<'_> {
    /// Claims this anchor's one payload slot, reporting whether this caller
    /// was the first to do so.
    fn claim_panic(&self) -> bool {
        match self {
            Self::Run(topology) => topology.claim_panic(),
            Self::Base(base) => base.claim_panic(),
        }
    }

    /// Puts `payload` in the slot this caller has just claimed.
    fn keep_panic(&self, payload: Box<dyn Any + Send>) {
        match self {
            Self::Run(topology) => topology.keep_panic(payload),
            Self::Base(base) => base.keep_panic(payload),
        }
    }
}

/// What a job still owes once its work is done: upstream's two arguments to
/// `_tear_down_nonasync`, minus the node it no longer needs.
///
/// Built by [`Job::finish`] and spent by `WorkerThread::finish_job`. It is a
/// type rather than a pair so that the order [`Job::finish`] documents cannot
/// be got wrong by a caller holding the pieces: the only way to reach the
/// counters is to have given the graph up first.
pub(crate) struct Finished {
    /// The parent whose join counter this job decrements.
    parent: Parent,
    /// The run the job belonged to.
    topology: Arc<Topology>,
}

impl Finished {
    /// The parent to count down and the run to account the job to.
    pub(crate) fn into_parts(self) -> (Parent, Arc<Topology>) {
        (self.parent, self.topology)
    }
}

impl core::fmt::Debug for Job {
    /// Hand-written because the derived one would print the whole graph: a
    /// job's owner is an owning handle, not a field worth expanding. The two
    /// addresses are what identifies a job, as upstream's `Node*` identifies
    /// its own.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Job")
            .field("task", &self.task)
            .field("owner", &self.owner.identity())
            .field("topology", &Arc::as_ptr(&self.topology))
            .field("parent", &self.parent)
            .finish()
    }
}

impl std::hash::Hash for Job {
    /// Hand-written for the same reason [`Debug`](core::fmt::Debug) is, and it
    /// has one caller: `Buffers::push` routes a job to one of the shared
    /// overflow buffers by its hash, where upstream hashes the `Node*` it is
    /// about to push. The owner's allocation and the run's stand in for that
    /// pointer, and the task index is what separates two jobs of one run.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.task.hash(state);
        self.owner.identity().hash(state);
        Arc::as_ptr(&self.topology).hash(state);
    }
}

/// The part of a run a cancel token may touch: upstream's
/// `NodeBase::_estate`, in an allocation of its own.
///
/// Upstream's `tf::Future` *is* the cancel token -- it holds a
/// `std::weak_ptr<Topology>` and `cancel()` sets a bit through it -- and a
/// [`Canceller`](crate::Canceller) is the same move over a
/// `Weak<Cancellable>`. The indirection is what makes the token lifetime-free:
/// a [`RunHandle`](crate::RunHandle) carries the lifetimes of the executor and
/// the taskflow it borrows, and a token that a `'static` task body can capture
/// cannot carry either.
///
/// A separate allocation rather than a `Weak<Topology>`, for two reasons. A
/// token that could upgrade to an `Arc<Topology>` would hold a share of the
/// run for the length of the call, and a run holds a share of the taskflow's
/// storage -- so a stray `cancel` would briefly keep the graph of a run that
/// has already finished alive, which is exactly the thing
/// `Taskflow::graph_mut` refuses, and an edit racing that call would panic.
/// And it would hand a caller `join`, `finish` and the run policy along with
/// the one bit it asked for.
///
/// This type is also where the port keeps upstream's `ESTATE` vocabulary,
/// since it is the one estate word with a name of its own; the other is a
/// `NodeBase`'s, which reads the same constants. A *run's* word carries three
/// of the four: `CANCELLED`, which [`Cancellable::cancel`] sets, and the
/// `EXCEPTION | CAUGHT` pair that [`Cancellable::claim_panic`] sets together.
/// `CANCELLED` and `EXCEPTION` mean the same thing to the scheduler, which is
/// why [`Cancellable::cancelled`] is a mask over those two, as upstream's
/// `Topology::cancelled` is; `CAUGHT` is not part of that mask and says only
/// that the payload slot is taken. The fourth, `EXPLICITLY_ANCHORED`, is
/// never set on a run -- a run somebody started is anchored by definition
/// and the sentinel is not an anchor at all, which [`Job::record_panic`]
/// spells out.
pub(crate) struct Cancellable {
    /// Upstream's `_estate`, holding the three bits a topology uses.
    estate: AtomicU32,
    /// Whether the run is over, which is to say whether there is still
    /// anything to cancel.
    ///
    /// Upstream needs no equivalent: its token's `weak_ptr` is dead once the
    /// tear-down has popped the topology off the taskflow's queue, so the
    /// allocation going away *is* its answer to "has this run finished". Here
    /// a run's topology is shared with a handle that outlives the run -- a
    /// caller may hold it long after `wait` returned -- so the allocation is
    /// still there and the question needs its own answer. [`Topology::finish`]
    /// writes this before it releases a waiter, so a `cancel` after a `wait`
    /// always reports `false`.
    finished: AtomicBool,
}

impl Cancellable {
    /// Upstream's `ESTATE::CANCELLED`: this run has been cancelled.
    pub(crate) const CANCELLED: u32 = 0x4000_0000;
    /// Upstream's `ESTATE::EXCEPTION`: a task of this run failed, which to the
    /// scheduler means the same as `CANCELLED`.
    pub(crate) const EXCEPTION: u32 = 0x1000_0000;
    /// Upstream's `ESTATE::CAUGHT`: this run's one payload slot is taken, so
    /// a later failure has nowhere to go. Set together with `EXCEPTION` and
    /// read on its own, which is how upstream decides that a panic is the
    /// first one -- see [`Topology::record_panic`].
    pub(crate) const CAUGHT: u32 = 0x2000_0000;
    /// Upstream's `ESTATE::EXPLICITLY_ANCHORED`: somebody is waiting for the
    /// graph scheduled under this base, so a failure inside that graph is
    /// theirs to deal with and the panic walk stops here.
    ///
    /// It is in the estate vocabulary with the other three because that is
    /// the word it lives in, and it is the one of the four that is never set
    /// on a *run*: a topology is the chain's root and is treated as anchored
    /// by [`Job::record_panic`] without a bit of its own. What carries it is
    /// a `NodeBase`, through
    /// `NodeBase::anchor_explicitly`.
    pub(crate) const EXPLICITLY_ANCHORED: u32 = 0x8000_0000;

    /// A run that is neither cancelled nor finished.
    fn new() -> Self {
        Self {
            estate: AtomicU32::new(0),
            finished: AtomicBool::new(false),
        }
    }

    /// Sets `bits` in the estate word, reporting whether this call was the
    /// first to set them.
    ///
    /// Upstream's `_estate.fetch_or`, plus the answer
    /// [`Topology::record_panic`] needs: the task that first sets
    /// `EXCEPTION | CAUGHT` is the one whose panic the run reports, and every
    /// later one is a task whose failure the run already knows about.
    pub(crate) fn set(&self, bits: u32) -> bool {
        // Relaxed, as every upstream read and write of `_estate` is: the word
        // races with the scheduler by construction -- a task already running
        // finishes whether or not it sees the bit -- and what carries a run's
        // *results* between threads is the join counter and the completion
        // lock, not this.
        self.estate.fetch_or(bits, Ordering::Relaxed) & bits == 0
    }

    /// Claims the one payload slot of this run, reporting whether this caller
    /// was the first to do so.
    ///
    /// The run's half of `NodeBase::claim_panic`, and the same atomic:
    /// `EXCEPTION` beside `CAUGHT` so that claiming the slot is also what
    /// cancels the rest of the run, and `CAUGHT` masked on its own because
    /// that is the bit which says the slot is taken. On a run the two
    /// maskings happen to agree -- nothing sets `EXCEPTION` on a topology
    /// without claiming the slot in the same call -- and they are written the
    /// same way so that a reader has one rule to learn rather than two.
    pub(crate) fn claim_panic(&self) -> bool {
        let flag = Self::EXCEPTION | Self::CAUGHT;
        self.estate.fetch_or(flag, Ordering::Relaxed) & Self::CAUGHT == 0
    }

    /// Whether this run is cancelled: upstream's `Topology::cancelled`, which
    /// is either bit of the pair rather than `CANCELLED` alone.
    pub(crate) fn cancelled(&self) -> bool {
        self.estate.load(Ordering::Relaxed) & (Self::CANCELLED | Self::EXCEPTION) != 0
    }

    /// Cancels the run, reporting `false` if it had already finished.
    ///
    /// Upstream's `Future::cancel`, whose `false` is its `weak_ptr::lock()`
    /// failing. Cancelling twice reports `true` twice, as upstream's does: the
    /// answer is about the run, not about this caller.
    pub(crate) fn cancel(&self) -> bool {
        if self.finished.load(Ordering::Relaxed) {
            return false;
        }
        self.set(Self::CANCELLED);
        true
    }

    /// Records that the run is over, so that a later `cancel` reports `false`.
    fn mark_finished(&self) {
        self.finished.store(true, Ordering::Relaxed);
    }
}

/// One run of one [`Taskflow`](crate::Taskflow): upstream's `tf::Topology`.
///
/// One upstream member is not here: `_promise` is a [`Completion`] behind a
/// `Mutex`, because the payload a panicking task hands back is not a value
/// `std::promise` would carry. Upstream's `_estate` *is* here, as
/// [`Cancellable`], but in its own allocation rather than inline -- see that
/// type for why.
pub(crate) struct Topology {
    /// How many nodes of this run are outstanding: upstream's
    /// `NodeBase::_join_counter` on the topology. Raised by the source count
    /// when the graph is scheduled, by one for each successor that becomes
    /// ready, and lowered by one as each node finishes, so it reaches zero
    /// exactly once -- when the last node of the run is done.
    ///
    /// Only the jobs whose [`Parent`] is [`Parent::Run`] count here, which is
    /// every job of the run's own graph. A nested graph counts down the node
    /// it hangs off instead, and that node is itself one of these.
    join_counter: AtomicUsize,
    /// What decides whether the graph runs again, and what to call when it
    /// does not: upstream's `_predicate` and `_on_finish`.
    policy: Mutex<RunPolicy>,
    /// Everything a waiter needs, and the flag it waits on.
    completion: Mutex<Completion>,
    /// Signalled by [`Topology::finish`], under the lock.
    finished: Condvar,
    /// The cancellation and exception bits of this run, upstream's `_estate`,
    /// shared with every [`Canceller`](crate::Canceller) handed out for it.
    ///
    /// Owned here in the sense that upstream's `_estate` is a member of the
    /// topology; the extra allocation is what lets a token outlive the run
    /// handle's borrow. See [`Cancellable`].
    estate: Arc<Cancellable>,
    /// This run's share of its hold on the taskflow's storage, until the
    /// tear-down takes it back out.
    ///
    /// Upstream's `Topology::_taskflow`, which is a `Taskflow&` -- a reference
    /// that the taskflow outliving its runs is what keeps valid. A share
    /// instead, because this port makes no such assumption about the caller,
    /// and an `Option` behind a lock because the share has to be released at a
    /// particular moment rather than whenever the topology happens to be
    /// dropped: see [`Topology::release_flow`] and the module docs.
    ///
    /// A [`Storage`] rather than the `Arc<Flow>` it was before #109, because
    /// the share released here is the one the *waiter* hangs off: this is the
    /// share whose drop is usually the last, and `Storage`'s own `Drop` is
    /// what releases the thread waiting on the run when it is.
    ///
    /// `None` means there is no graph to run, which is a run that was over
    /// before it was handed out -- [`Topology::nothing_to_run`]. Those are
    /// never set up, never queued and never torn down, so nothing ever asks
    /// them for a graph.
    ///
    /// The lock is never held while either of the other two is, and never
    /// across anything but a clone or a take of the share -- never across the
    /// *drop* of one, which would run a destructor that takes the completion
    /// lock with this one held.
    storage: Mutex<Option<Arc<Storage>>>,
}

/// Upstream's `Topology::_predicate` and `_on_finish`, which is what makes a
/// topology able to run its graph more than once.
///
/// Behind a `Mutex` because both are called on whichever worker happens to
/// finish a run, and the predicate is `FnMut` -- upstream's is a mutable
/// lambda too, since `run_n`'s is a counter. The lock is uncontended by
/// construction: the only thread that ever evaluates a run's policy is the one
/// that drove its join counter to zero, and there is exactly one of those per
/// run. It is a lock rather than an atomic because the values behind it are
/// arbitrary closures.
///
/// Both are `'static`, as [`Work`](crate::Work) is and for the same reason:
/// the topology is `Arc`-shared with the taskflow's run queue, which has no
/// lifetime to spare. Upstream's callbacks capture by reference; a caller that
/// wants that here shares an `Arc` with the closure instead.
struct RunPolicy {
    /// Called once before the first run and once after each run finishes.
    /// `true` means "do not run the graph again".
    predicate: Box<dyn FnMut() -> bool + Send>,
    /// Called once, after the last run of the graph. An `Option` because it is
    /// `FnOnce` and therefore has to be moved out to be called.
    on_finish: Option<Box<dyn FnOnce() + Send>>,
}

/// The completion slot, in place of upstream's `std::promise<void>`.
///
/// Who gets woken is deliberately two fields rather than one: a thread
/// blocked in [`RunHandle::wait`](crate::RunHandle::wait) is woken through the
/// condvar, and a future polling a run is woken through the [`Waker`] left by
/// [`Topology::poll_join`]. A handle is waited for one way or the other, never
/// both, but keeping the two slots separate is what lets the blocking path
/// stay a condvar rather than becoming a hand-rolled futex over the waker.
struct Completion {
    /// Whether the run has finished. The condvar's predicate.
    finished: bool,
    /// What the first task to panic panicked with, in place of upstream's
    /// `std::exception_ptr`. Taken by whoever waits, so that it is reported
    /// once.
    panic: Option<Box<dyn Any + Send>>,
    /// A waker left by a future waiting on this run, woken instead of -- and
    /// as well as -- the condvar.
    waker: Option<Waker>,
}

impl Topology {
    /// A run of `flow`'s graph that has not started, over the policy
    /// `run_until` was given: `predicate` decides after each run whether to
    /// run the graph again, and `on_finish` is called once the answer is no.
    ///
    /// Upstream's `Topology(Taskflow&, P&&, C&&)`, whose `Taskflow&` this
    /// hold on the taskflow's storage is: upstream can hold a reference
    /// because its rule says the taskflow outlives its runs, and this port
    /// holds a share because it does not take the caller's word for that. The
    /// methods that walk the graph still take it as a parameter all the same,
    /// since every caller of them has it in hand already; what this share is
    /// for is the tear-down, which is reached from a job that has given its
    /// own share up. See the module docs.
    ///
    /// The hold this builds releases no waiter -- [`Storage::ungated`] has
    /// which runs want that and why it is the safe default. A run a caller
    /// gets a [`RunHandle`](crate::RunHandle) on goes through
    /// [`Topology::for_run`] instead.
    pub(crate) fn new<P, C>(predicate: P, on_finish: C, flow: Arc<Flow>) -> Self
    where
        P: FnMut() -> bool + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        Self::over(predicate, on_finish, Some(Storage::ungated(flow)))
    }

    /// The same run, for a caller that will hand a
    /// [`RunHandle`](crate::RunHandle) on it out: its hold on the storage
    /// names the run back, so that the last share of that hold is what
    /// releases whoever waits.
    ///
    /// Upstream has one constructor because it has nothing to name back --
    /// the `shared_ptr<Topology>` its `_fetch_enqueue` makes is the run's only
    /// owner and its `Topology` holds a `Taskflow&`. The naming back is
    /// #109's, and `Arc::new_cyclic` is what lets the hold be built with the
    /// run's own `Weak` in it rather than installed afterwards. The ordering
    /// section of the module docs says what it buys; [`Storage::ungated`] says
    /// which runs do without it and why that is not a trap.
    pub(crate) fn for_run<P, C>(predicate: P, on_finish: C, flow: Arc<Flow>) -> Arc<Self>
    where
        P: FnMut() -> bool + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        Arc::new_cyclic(|run| Self::over(predicate, on_finish, Some(Storage::gating(flow, run))))
    }

    /// The one run every free-standing async node claims to be part of: a
    /// sentinel that is never cancelled, never finished, never an anchor and
    /// holds no taskflow.
    ///
    /// Upstream needs nothing here -- its async node's `_topology` is null,
    /// and the tear-down written for such a node never reads it. The module
    /// docs have why this port answers with a sentinel instead of an
    /// `Option<Arc<Topology>>` on every job in the crate, and what the four
    /// properties above rest on.
    ///
    /// One per process rather than one per [`Executor`](crate::Executor),
    /// which is the stronger form of "shared": it is immutable in every way
    /// a run can be read -- its policy is never consulted, its completion
    /// slot is never written, its estate word is never set -- so there is
    /// nothing for two executors to disagree about, and nothing of either
    /// one's that it keeps alive.
    pub(crate) fn free_standing() -> &'static Arc<Self> {
        /// The sentinel itself, built on the first async task of the process
        /// and never dropped. One allocation, which is what keeps an async
        /// task at upstream's one-allocation cost.
        static FREE_STANDING: OnceLock<Arc<Topology>> = OnceLock::new();

        FREE_STANDING.get_or_init(|| {
            let topology = Self::over(|| true, || {}, None);
            // The phantom job that makes "never finishes" structural rather
            // than a rule somebody has to keep. Every job raises this counter
            // before it is queued and lowers it as it finishes -- which is
            // the scheduler's rule for every topology, not this one's -- so
            // the counter is one plus the number of free nodes in flight: it
            // can be driven down to one but never to zero, `job_finished`
            // never reports a last job, and the tear-down is unreachable. A
            // sentinel that *could* be torn down would evaluate a run policy
            // and release a waiter that does not exist.
            topology.join_counter.store(1, Ordering::Relaxed);
            Arc::new(topology)
        })
    }

    /// Whether this run is the sentinel [`Topology::free_standing`] rather
    /// than a run somebody started.
    ///
    /// Upstream's `node->_topology == nullptr`, which is a test it makes in
    /// two places: `_tear_down_async`, over the node, and
    /// `_process_exception`, over the anchor it would otherwise claim. The
    /// identity of the one allocation is what stands in for the null here,
    /// and [`Job::free`] is the only way onto a job, so this answers "is
    /// this a free node's job" as exactly as upstream's test does.
    ///
    /// The two callers that matter are [`Job::record_panic`], which ends a
    /// chain rooted here rather than anchoring it, and `executor::async`,
    /// which tells a free node's job from a [`Handle::Async`](crate::Handle)
    /// somebody built into a graph.
    pub(crate) fn is_free_standing(&self) -> bool {
        std::ptr::eq(self, &**Self::free_standing())
    }

    /// A run that is over before it begins and has no graph at all: what
    /// [`RunHandle::finished`](crate::RunHandle) is built on.
    ///
    /// Upstream hands back a ready `std::future` from a promise it fulfils on
    /// the spot and keeps no topology for it at all. There is one here so that
    /// every handle has the same shape, and it is finished before it is handed
    /// out: nothing can ask it to run again, so its policy is never consulted,
    /// and it holds no share of any taskflow, so a caller who leaks such a
    /// handle leaks nothing but the topology itself.
    pub(crate) fn nothing_to_run() -> Self {
        let topology = Self::over(|| true, || {}, None);
        topology.finish();
        topology
    }

    /// The one constructor, over a run that may or may not have a graph.
    fn over<P, C>(predicate: P, on_finish: C, storage: Option<Arc<Storage>>) -> Self
    where
        P: FnMut() -> bool + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        Self {
            join_counter: AtomicUsize::new(0),
            policy: Mutex::new(RunPolicy {
                predicate: Box::new(predicate),
                on_finish: Some(Box::new(on_finish)),
            }),
            completion: Mutex::new(Completion {
                finished: false,
                panic: None,
                waker: None,
            }),
            finished: Condvar::new(),
            estate: Arc::new(Cancellable::new()),
            storage: Mutex::new(storage),
        }
    }

    /// This run's share of its hold on the taskflow's storage, or `None` once
    /// the tear-down has released it.
    ///
    /// What the tear-down reaches the graph and the run queue through -- the
    /// job that got it there having released its own share, for the reason
    /// [`Job::finish`] gives -- and what [`Shared::set_up_topology`] schedules
    /// the graph under, so that every job of the run holds a share of the same
    /// hold.
    ///
    /// [`Shared::set_up_topology`]: crate::executor::Shared::set_up_topology
    pub(crate) fn storage(&self) -> Option<Arc<Storage>> {
        self.lock_storage().clone()
    }

    /// Releases this run's share of its hold on the taskflow's storage, and
    /// with it the thread waiting on the run.
    ///
    /// Called by the tear-down of the *final* run of a graph, as its last act
    /// and after everything that needed the graph. It is one act rather than
    /// two since #109: the share goes, and if it was the last share of the
    /// hold then [`Drop for Storage`](Storage) releases the waiter on the spot
    /// -- and if it was not, the worker still holding a copy of a preempted
    /// node's job releases the waiter when it drops that copy, a few
    /// instructions later. Either way the waiter wakes to a storage no run
    /// holds a share of, which is what [`Taskflow::graph_mut`] needs of it.
    ///
    /// [`Topology::finish`] is called here directly for a hold that names no
    /// run to release -- [`Storage::ungated`] -- which is also what makes this
    /// idempotent: a second call finds no share, releases the waiter again,
    /// and a waiter already released stays released.
    ///
    /// [`Taskflow::graph_mut`]: crate::Taskflow::graph_mut
    pub(crate) fn release_flow(&self) {
        // Taken out from under the lock and dropped outside it: the drop runs
        // a destructor that takes this topology's completion lock, and the
        // two are never held at once.
        let released = self.lock_storage().take();

        let releases_the_waiter = released
            .as_ref()
            .is_some_and(|storage| storage.gates_completion());
        drop(released);

        if !releases_the_waiter {
            self.finish();
        }
    }

    /// The share slot. Poisoning is taken rather than propagated, as the rest
    /// of this type's locks are: the slot holds one `Option` and nothing under
    /// the lock can panic.
    fn lock_storage(&self) -> MutexGuard<'_, Option<Arc<Storage>>> {
        self.storage.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether this run has been cancelled: upstream's `Topology::cancelled`.
    ///
    /// Read once per job, at the top of `WorkerThread::invoke`, which is
    /// upstream's `_is_parent_cancelled` early-out at the top of `_invoke`,
    /// and once more by [`Topology::run_again`].
    pub(crate) fn cancelled(&self) -> bool {
        self.estate.cancelled()
    }

    /// Cancels this run, reporting `false` if it has already finished:
    /// upstream's `Future::cancel`.
    pub(crate) fn cancel(&self) -> bool {
        self.estate.cancel()
    }

    /// The state a [`Canceller`](crate::Canceller) holds a `Weak` to.
    pub(crate) fn cancellable(&self) -> &Arc<Cancellable> {
        &self.estate
    }

    /// Accounts for `jobs` more nodes of this run becoming ready, before any
    /// of them is queued.
    ///
    /// Upstream's `node->_parent->_join_counter.fetch_add(..)`, where a
    /// top-level node's parent *is* the topology: see
    /// [`Parent::count_queued`], which is the only caller.
    fn jobs_queued(&self, jobs: usize) {
        self.join_counter.fetch_add(jobs, Ordering::Relaxed);
    }

    /// Accounts for one node of this run having finished, and reports whether
    /// it was the last.
    ///
    /// Upstream's `_tear_down_nonasync`, down to the ordering: the
    /// release half publishes everything the finished node wrote to whoever
    /// reads a zero, and the acquire half is what makes the zero reliable.
    pub(crate) fn job_finished(&self) -> bool {
        self.join_counter.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// Whether the run that has just finished should run the graph again:
    /// upstream's `!tpg->cancelled() && !tpg->_predicate()`, now both halves of
    /// it.
    ///
    /// This is the whole of the re-run decision, in one place and deliberately
    /// not inlined into the tear-down. The `&&` short-circuits, as upstream's
    /// does, which is the one thing worth noticing about the order: a cancelled
    /// run does not ask its predicate at all, so a `run_n` counter is not spent
    /// by a round that will never happen.
    ///
    /// A panicking task stops a `run_n` or `run_until` run too, and does it
    /// through [`Topology::cancelled`] rather than through a term of its own:
    /// the panic half upstream keeps commented out beside this condition is a
    /// task setting `EXCEPTION`, which is already one of the two bits that
    /// reads.
    pub(crate) fn run_again(&self) -> bool {
        !self.cancelled() && !self.stop_requested()
    }

    /// Evaluates the run predicate, reporting `true` for "stop".
    ///
    /// A panic out of the predicate is caught and recorded as the run's
    /// failure, and answers "stop". Upstream lets the exception propagate out
    /// of `_tear_down_topology`; here that would unwind past
    /// [`Topology::finish`] and leave the waiter blocked for good, so the
    /// failure is reported the way a panicking task's is.
    fn stop_requested(&self) -> bool {
        // The guard is dropped before the payload is recorded, so that the two
        // locks of a topology are never held at once.
        let evaluated = {
            let mut policy = self.policy();
            panic::catch_unwind(AssertUnwindSafe(|| (policy.predicate)()))
        };

        match evaluated {
            Ok(stop) => stop,
            Err(payload) => {
                self.record_panic(payload);
                true
            }
        }
    }

    /// Calls the on-finish callback, once. Upstream's `tpg->_on_finish()`.
    ///
    /// A panic out of the callback is caught and recorded, for the reason
    /// [`Topology::stop_requested`] gives.
    pub(crate) fn carry_out_callback(&self) {
        // Taken out from under the lock, so that the callback runs with no
        // lock of this topology held: it is user code, and the only thing it
        // may not do is reach back into the run that is finishing.
        let on_finish = self.policy().on_finish.take();

        if let Some(on_finish) = on_finish
            && let Err(payload) = panic::catch_unwind(AssertUnwindSafe(on_finish))
        {
            self.record_panic(payload);
        }
    }

    /// Records that this run failed, keeping the first payload and cancelling
    /// whatever the run had not started.
    ///
    /// The delivery half of upstream's `_process_exception` with the run as
    /// the anchor, which is where a failing *task* of a top-level graph ends
    /// up: [`Job::record_panic`] is the walk that decides that, and this is
    /// what it calls when the walk reaches the chain's root. The two other
    /// callers have no node and no chain to walk -- a run policy's predicate
    /// and its on-finish callback -- so they report their failure straight to
    /// the run that asked them.
    ///
    /// Two things come out of the one atomic. `EXCEPTION` is half of
    /// [`Topology::cancelled`], so setting it is the whole of "the rest of the
    /// run is cancelled": every job not yet dequeued is skipped by the
    /// early-out at the top of `WorkerThread::invoke`, and
    /// [`Topology::run_again`] answers no. And `CAUGHT` says the payload slot
    /// is taken, so the first panic is the one reported and later ones are
    /// dropped -- as upstream keeps only the first `exception_ptr`, for the
    /// same reason: with several tasks failing, the one the caller is told
    /// about should be the one that failed first, not the one that happened to
    /// finish unwinding last.
    ///
    /// A payload that arrives when the slot is already taken is dropped, and
    /// that is every anchor's answer rather than this one's: a `NodeBase`
    /// anchor drops its loser too, because the only way to write either slot
    /// is to have claimed it first -- [`Topology::claim_panic`] here,
    /// `NodeBase::claim_panic` there -- so dropping the loser is structural
    /// and not a choice made twice. Where that leaves upstream, which parks
    /// the loser somewhere this port cannot read it back from, is set out
    /// once in [`Job::record_panic`].
    pub(crate) fn record_panic(&self, payload: Box<dyn Any + Send>) {
        if self.claim_panic() {
            self.keep_panic(payload);
        }
    }

    /// Claims this run's one payload slot, reporting whether this caller was
    /// the first: the run's half of `NodeBase::claim_panic`.
    ///
    /// The atomic is the arbiter rather than the lock, so a later panic of a
    /// run that already has one is turned away without ever touching the
    /// completion slot -- which is what keeps a thousand simultaneously
    /// failing tasks off it.
    pub(crate) fn claim_panic(&self) -> bool {
        self.estate.claim_panic()
    }

    /// Puts `payload` in the slot this caller has just claimed, where a
    /// `wait` will find it.
    pub(crate) fn keep_panic(&self, payload: Box<dyn Any + Send>) {
        self.lock().panic = Some(payload);
    }

    /// Marks this run finished and releases whoever is waiting for it.
    /// Upstream's `_carry_out_promise`.
    ///
    /// **This is the last thing the caller may do with the run or with the
    /// graph.** The thread it releases is the one that can edit the taskflow
    /// again the moment it wakes, so a touch of the graph after this point
    /// would be a share of the storage outstanding during that edit -- which
    /// is a panic in the caller's `emplace` rather than anything a worker
    /// would notice. What makes the rule keepable is that the ordinary way
    /// here is through `Drop for Storage`, which releases the storage on its
    /// way to this call: [`Topology::release_flow`] is what starts that, and
    /// the module docs have the order in full. The two callers that reach
    /// this directly are a run whose hold releases nobody --
    /// [`Storage::ungated`] -- and [`Topology::nothing_to_run`], which has no
    /// storage and no run.
    ///
    /// The notification is sent *under* the lock rather than after it: a
    /// waiter cannot return from [`Condvar::wait`](std::sync::Condvar::wait)
    /// until it has the mutex back, so the unlock is this thread's last touch
    /// of the topology either way. That used to be the difference between a
    /// correct unlock and a use-after-free, since the released thread freed
    /// the topology; it no longer is -- the worker calling this holds a share
    /// of its own -- and it stays because it costs nothing and keeps the
    /// ordering above to one rule instead of two.
    pub(crate) fn finish(&self) {
        // Before the lock, so that it is ordered before the unlock that
        // releases a waiter: a `cancel` that follows a `wait` -- or follows the
        // `join` in `RunHandle::drop` -- has to report that there was nothing
        // left to cancel. A relaxed store is enough for exactly that reason;
        // the happens-before is the completion lock's, not this word's.
        self.estate.mark_finished();

        let mut completion = self.lock();
        completion.finished = true;
        let waker = completion.waker.take();
        self.finished.notify_all();
        drop(completion);

        // The waker was moved out above, so this touches nothing that the
        // released waiter may have freed.
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Blocks until this run has finished and returns what its first
    /// panicking task panicked with, if any.
    ///
    /// Idempotent, which is what lets [`RunHandle`](crate::RunHandle) call it
    /// from both `wait` and `Drop`: a second call finds the flag already set
    /// and the payload already taken.
    pub(crate) fn join(&self) -> Option<Box<dyn Any + Send>> {
        let mut completion = self.lock();
        while !completion.finished {
            completion = self
                .finished
                .wait(completion)
                .unwrap_or_else(PoisonError::into_inner);
        }
        completion.panic.take()
    }

    /// Reports whether this run has finished, leaving `waker` to be woken when
    /// it does if it has not, and takes the panic payload when it has.
    ///
    /// The non-blocking half of [`Topology::join`], and the whole of what
    /// `impl Future for RunHandle` needs. Registering the waker and reading the
    /// flag happen under one lock, which is what stops a run finishing between
    /// the two and leaving nobody to wake.
    pub(crate) fn poll_join(&self, waker: &Waker) -> Poll<Option<Box<dyn Any + Send>>> {
        let mut completion = self.lock();
        if completion.finished {
            return Poll::Ready(completion.panic.take());
        }

        // Replaced rather than kept, because a future may be polled by a
        // different task each time; `will_wake` is the cheap way to skip the
        // clone in the common case where it is not.
        if !completion
            .waker
            .as_ref()
            .is_some_and(|current| current.will_wake(waker))
        {
            completion.waker = Some(waker.clone());
        }
        Poll::Pending
    }

    /// The completion slot. A poisoned lock is taken rather than propagated,
    /// as the executor's other locks are: the panic that poisoned it is
    /// already recorded in the slot, and refusing to hand the slot back would
    /// turn a reported failure into a stuck `wait`.
    fn lock(&self) -> MutexGuard<'_, Completion> {
        self.completion
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The run policy. Poisoning is taken rather than propagated, as it is for
    /// the completion slot: a predicate or callback that panicked has already
    /// been recorded as this run's failure.
    fn policy(&self) -> MutexGuard<'_, RunPolicy> {
        self.policy.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Prepares every node of `graph` for a run and returns how many of them are
/// source tasks: upstream's `Executor::_set_up_graph`.
///
/// Applied to any graph, which is what makes a nested run possible at all --
/// upstream's own is called both from `_set_up_topology`, for a taskflow's own
/// graph, and from `_schedule_graph`, for a subgraph, a module's target or a
/// corun's. Three departures from upstream, and all three fall out of what
/// this port's nodes are:
///
/// - **No topology and no parent are written.** Upstream stores both on every
///   node here; they travel with the [`Job`] instead, for the reason the
///   module docs give. So this function needs neither argument, and is a free
///   function rather than a method on [`Topology`].
/// - **The sources are not moved to the front.** Upstream partitions its node
///   vector with `std::iter_swap` so that the sources are a prefix it can bulk
///   schedule; an arena cannot, since a shuffle would re-point every live
///   [`Task`] at another node. So the sources are found twice instead, here
///   and in [`Job::sources`], which costs a second walk over the graph and
///   saves the allocation that collecting them would need.
/// - **The state word is written through the mask.** Upstream assigns
///   `NSTATE::NONE` and then accumulates the count into the word with `+=`,
///   masking it back out when it arms the join counter. Here the count is
///   checked against [`Node::STRONG_DEPENDENCIES_MASK`] and stored, which is
///   the same word with the same bits and one impossible case ruled out: a
///   count can never be mistaken for a state bit.
///
/// Clearing the state bits is upstream's `node->_nstate = NSTATE::NONE`, and
/// it is what makes a re-run of a graph start clean: a node left `PREEMPTED`
/// or `JOINED_SUBFLOW` by a run that was cancelled -- or by one abandoned
/// with the executor dropped under it -- would otherwise have the next run
/// treat its first entry as a resumption and skip its body. Nothing a run
/// does can reach this through a node that is still carrying live state: the
/// nodes set up here are the ones about to start, and a parent whose nested
/// graph is being set up is never a node of that graph. That last clause
/// carries more weight than it did: the estate word now holds the
/// explicit-anchor bit as well, and arming a graph clears the word, so what
/// keeps a waiting frame's anchor from being wiped by the very call that
/// schedules the graph it waits for is that the anchor is not in it.
///
/// One shape did reach it, and it is issue #82's: a module task composed of a
/// taskflow the run is *already inside of* -- which a subflow body can build,
/// since the taskflow it belongs to can be alive behind an `Arc` -- would arm
/// that taskflow's graph a second time, clearing the `PREEMPTED` bit and the
/// join counter of the very node waiting for the graph being armed. Nothing
/// would then re-queue that node and the run would never finish.
/// `executor::module` refuses such a target before it is scheduled, through
/// [`Job::runs_inside`], and that refusal is what keeps the paragraph above
/// true rather than nearly true.
///
/// Emptying the payload slot is upstream's `node->_exception_ptr = nullptr`,
/// in the same loop and for the same reason: the slot is where an anchor's
/// failure waits, and a run must not report one the last run left. The one
/// thing this adds is the `CAUGHT` test that guards it. A payload reaches a
/// slot only through a claimant that set that bit in the estate word, so the
/// word the clearing is about to overwrite says whether there is anything
/// under the lock -- and a run in which nothing failed never takes a node's
/// lock at all.
///
/// # Panics
///
/// Panics if some task has more strong dependencies than the count field of
/// the state word can hold, which needs a quarter of a billion duplicate
/// edges into one node.
pub(crate) fn set_up_graph(graph: &Graph) -> usize {
    let mut num_sources = 0;

    for (task, node) in graph.iter() {
        let strong = TaskRef::new(graph, task).num_strong_dependencies();
        let counted = u32::try_from(strong)
            .ok()
            .filter(|count| count & !Node::STRONG_DEPENDENCIES_MASK == 0)
            .expect("a task has fewer strong dependencies than the state word can count");

        node.nstate().store(counted, Ordering::Relaxed);
        node.join_counter().store(strong, Ordering::Relaxed);

        // Upstream's `node->_exception_ptr = nullptr`: a payload left on a
        // node by a failure nobody came to collect must not be reported by
        // the next run of the same graph. The `swap` is what keeps a mutex
        // off this path for the runs that did not fail -- a payload is only
        // ever put in a slot by a claimant that set `CAUGHT` in the same
        // word, so a word without that bit cannot have one under it.
        if node.estate().swap(0, Ordering::Relaxed) & Cancellable::CAUGHT != 0 {
            node.base().clear_panic();
        }

        // A source is a node with no predecessors, which is not the same test
        // as "no strong dependencies" and is upstream's. The two part company
        // once conditioners exist: a task reached only by a conditioner has no
        // strong dependency, but it is still not a place a run may start.
        if node.num_predecessors() == 0 {
            num_sources += 1;
        }
    }

    num_sources
}

impl core::fmt::Debug for Topology {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let completion = self.lock();
        f.debug_struct("Topology")
            .field("outstanding", &self.join_counter.load(Ordering::Relaxed))
            .field("finished", &completion.finished)
            .field("panicked", &completion.panic.is_some())
            .field("cancelled", &self.cancelled())
            .finish()
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}

    // A job is built on the thread that schedules a task and dequeued by
    // whichever worker takes it, so it has to be `Send` -- which, now that it
    // owns its shares rather than pointing at them, it is derived rather than
    // asserted: `Arc<T>` is `Send` exactly when `T` is `Send + Sync`. The
    // three below are what that resolves to, and `Node` is there because a job
    // hands out a `&Node` on the worker that runs it. `Topology` is also
    // shared with the run handle, which a caller may move to another thread.
    //
    // A job has to be `Sync` too, because the parent chain shares the job of a
    // parent with every job under it: a `Nested` holds one, and `Arc<Nested>`
    // is `Send` only if what it holds is both.
    assert_send_sync::<Job>();
    // The owner is what a job hands a `&Graph` out of, and one of its two
    // variants owns a `Graph` outright -- a subflow's subgraph, leased for
    // the length of its run -- so the graph itself has to be shareable
    // across threads rather than merely reachable through storage that is.
    assert_send_sync::<GraphOwner>();
    assert_send_sync::<Graph>();
    assert_send_sync::<Nested>();
    // The free-standing node a `GraphOwner::Async` holds: shared with every
    // handle on the task and with the worker that runs it, so it is the same
    // requirement the two other owners make of what they hold.
    assert_send_sync::<AsyncNode>();
    assert_send_sync::<Topology>();
    assert_send_sync::<Flow>();
    assert_send_sync::<Node>();

    // A `Canceller` is a `Weak<Cancellable>` and has to be `Send + Sync +
    // 'static`, which it is exactly when `Cancellable` is `Send + Sync` --
    // it holds nothing but atomics, and nothing with a lifetime.
    assert_send_sync::<Cancellable>();
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Wake;

    use super::*;
    use crate::Executor;
    use crate::executor::queue::WorkerQueue;
    use crate::executor::worker::WorkerThread;
    use crate::graph::{AsyncWork, SubflowWork};
    use crate::prelude::*;

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

        let mut taskflow = Taskflow::new();
        taskflow.emplace(|| {});
        let taskflow = taskflow;
        let topology = Topology::for_run(|| true, || {}, Arc::clone(taskflow.flow()));
        let straggler = topology
            .storage()
            .expect("a run that has not been torn down holds its own share");

        assert!(topology.poll_join(&waker).is_pending());

        // The whole of the tear-down's last act, with the straggler still
        // holding a share: nothing is released, because the storage is not
        // free yet.
        topology.release_flow();
        assert!(
            topology.poll_join(&waker).is_pending(),
            "a run whose storage is still held is not over"
        );
        assert_eq!(woken.load(Ordering::Relaxed), 0);

        drop(straggler);
        assert!(
            topology.poll_join(&waker).is_ready(),
            "the last share of the hold releases the waiter"
        );
        assert_eq!(woken.load(Ordering::Relaxed), 1, "the waker was not woken");
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
    /// closest to the failure, not the outermost.
    ///
    /// The chain has to end without a run for this case to be reachable at
    /// all, because a run is an anchor by definition; a parent with nothing
    /// above it is a corun anchor's shape, which is what this builds.
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
