//! Translated from `taskflow/core/taskflow.hpp` (Taskflow v4.1.0).
//!
//! The four `dump`/`_dump` overloads there write a task graph as Graphviz DOT.
//! This port reproduces their output line for line, with one substitution
//! forced on it: upstream identifies a node by printing its address
//! (`p0x55f2a4...`) and a graph by printing the graph's address
//! (`cluster_p0x55f2b0...`). Neither is reproducible across runs, and neither
//! exists here -- a node is an index into its graph's arena, not a heap
//! allocation of its own.
//!
//! # Identifier substitution
//!
//! * A node is `p<arena index>`, so `p0`, `p1`, ... in creation order, which
//!   is what makes the output stable enough to assert on.
//! * A graph cluster is `cluster_p<graph id>`, where the graph id is the small
//!   integer upstream's own `Dumper` already assigns each graph it visits
//!   (`dumper.visited[top] = dumper.id++`). The top-level graph is therefore 0,
//!   and `cluster_p0` is the only cluster a static taskflow can produce.
//! * An unnamed taskflow's label, upstream's `Taskflow: p<this>`, names that
//!   same top-level graph: `Taskflow: p0`.
//! * An unnamed node falls back to its own id as its label, as upstream falls
//!   back to the address it is already printing, so a nameless first node
//!   reads `p0[label="p0" ];`.
//! * A node of a *nested* graph needs more than its arena index, because every
//!   graph numbers its own nodes from zero where upstream's addresses were
//!   unique across all of them. The ids of a nested graph therefore carry
//!   their cluster's name in front of the index: the nodes of module graph 1
//!   are `p1_0`, `p1_1`, ..., and the subgraph a node `p0` owns lives in
//!   `cluster_p0_sub` and holds `p0_sub_0`, `p0_sub_1`, ... The `_sub`
//!   component is what keeps those two families apart -- without it the
//!   children of the top graph's `p1` and the nodes of module graph 1 would
//!   both be `p1_0`, and `cluster_p1` would name two different clusters. The
//!   top-level graph keeps the bare `p<index>` form.
//!
//! Names are written through exactly as upstream writes them, with no DOT
//! escaping: upstream does not escape them either, and quietly diverging would
//! make this output differ from the format it claims to match.
//!
//! # Nested graphs
//!
//! The two kinds of nesting go in opposite directions, which is upstream's
//! distinction rather than this port's.
//!
//! A module task's target is dumped as a *sibling* cluster. Upstream numbers
//! the target's graph, pushes it on `Dumper::stack`, and leaves it to the
//! top-level loop, which writes it after the cluster it was found in has
//! closed. `Dumper::visited`, keyed by the address of the graph, is what makes
//! a target composed twice dump once -- two module tasks, one cluster, and the
//! second task reporting the first task's `m<id>`. It is also upstream's guard
//! against a taskflow that composes itself dumping for ever, and that guard is
//! reachable here rather than kept for upstream's sake alone. The top-level
//! builder cannot close the cycle: composing needs an `Arc<Taskflow>` of the
//! target and mutating needs `&mut`, and `Arc::get_mut` refuses once a clone
//! exists. A subflow body can, because a [`Subflow`](crate::Subflow) is a
//! [`FlowBuilder`](crate::FlowBuilder) over a subgraph and runs at a moment
//! when the taskflow it belongs to may already be alive behind an `Arc` of the
//! caller's own. The dump of one such graph terminates and names the cycle
//! rather than following it: the module task in the subgraph reports the
//! top-level graph's `m<id>`, exactly as the second of two module tasks on one
//! target does.
//!
//! A subflow's or an adopted module's graph is dumped *inside* the owning
//! node's cluster instead, by recursion, and cannot recurse for ever at all:
//! the node owns the graph, so no chain of them can close into a cycle.
//!
//! What the dumper pushes for a module is a clone of the `Arc<Taskflow>`
//! rather than a borrow of its graph. That costs one refcount per distinct
//! target and buys the case a borrow could not express: a module task sitting
//! in a subflow's subgraph, whose `Arc` is reachable only through a lock this
//! dump has let go of long before the stack is drained.
//!
//! # Dumping through the subflow lock
//!
//! [`Taskflow::dump`] takes `&self`, so a dump can run while the taskflow
//! does, and a subflow's subgraph lives behind a `Mutex` ([`Handle::Subflow`])
//! that the body building it holds for as long as that body runs. The dump
//! therefore reads it with `try_lock` and never waits:
//!
//! * A poisoned lock is taken with `into_inner`, as [`crate::subflow`] takes
//!   it. Poison means some body panicked, not that the nodes it had already
//!   built are unreadable, and a dump only reads them.
//! * A lock another thread holds -- a body still building that subgraph -- is
//!   left alone, and the node dumps without its cluster, exactly as a node
//!   whose subgraph is empty does. Waiting instead would stall the dumping
//!   thread behind a whole subflow body, and would deadlock outright when that
//!   body is the one asking for the dump; the subgraph is half-built at that
//!   moment anyway, so there is no instant worth waiting for.
//!
//! That is the advice upstream's own `dump` documentation gives from the other
//! side: run the taskflow first, and `retain(true)` the subflow, because a
//! subgraph is cleared when it is joined and an empty one puts no children in
//! the document.
//!
//! # Coverage
//!
//! Every [`Handle`] this crate has dumps, which is the whole of upstream's
//! four functions. The `match` arms below name every variant rather than
//! defaulting, so the next variant to arrive stops compiling until its format
//! is decided. The handles upstream has and this crate does not --
//! `NonpreemptiveRuntime` above all -- never sit in a taskflow's own graph,
//! so upstream's dump has nothing to say about them either.
//!
//! What is unchecked rather than unwritten is the syntax of the output: the
//! development container has no `graphviz`, so nothing here has been through
//! `dot`, and every golden in `tests/dump.rs` was derived by reading the C++
//! instead.

use core::fmt;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, TryLockError};

use crate::graph::{Graph, Handle, Node, Task};
use crate::taskflow::Taskflow;

/// The id of the top-level graph, upstream's `dumper.visited[top]`, which is
/// the first id `Dumper` hands out.
const TOP_GRAPH_ID: usize = 0;

/// The nesting level of the top-level graph's own `subgraph` line, which is
/// the depth upstream pushes it at: `dumper.stack.push({nullptr, top, 1})`.
const TOP_GRAPH_DEPTH: usize = 1;

/// The nesting level a top-level graph's nodes sit at: one for the `digraph`
/// body, one for the `subgraph cluster_p0` the taskflow wraps it in. Upstream
/// arrives at the same number by pushing the top graph at depth 1 and dumping
/// its nodes at `depth + 1`.
const TOP_GRAPH_LEVEL: usize = TOP_GRAPH_DEPTH + 1;

/// The prefix the top-level graph's node ids carry, which is nothing but the
/// `p` of `p0`: only a nested graph's ids grow a cluster name in front of the
/// arena index.
const TOP_NODE_PREFIX: &str = "p";

/// The component that tells a node-owned cluster's identifiers from a module
/// graph's. See the module docs: without it the subgraph of the top graph's
/// `p1` and module graph 1 would fight over `cluster_p1` and `p1_0`.
const SUBGRAPH_MARKER: &str = "_sub";

impl Graph {
    /// Writes the node and edge lines of this graph as Graphviz DOT.
    ///
    /// This is upstream's per-graph `_dump`, so it emits the *contents* of a
    /// cluster and not a whole document: one `p<id>[label="..." ];` line per
    /// node in creation order, each followed by its own `p<from> -> p<to>;`
    /// line per successor, all indented as upstream indents a top-level
    /// graph's nodes. For a document that `dot` will accept, dump the
    /// [`Taskflow`] that owns the graph.
    ///
    /// The nested clusters a subflow or an adopted module owns do appear here,
    /// because those are written inside the owning node's own lines. The
    /// sibling cluster a module task's target would get does not: that belongs
    /// to the document level, and only [`Taskflow::dump`] drains the stack
    /// those targets are pushed on. A module task still writes its own
    /// `box3d` line and the `[m<id>]` of the graph it names.
    ///
    /// # Errors
    ///
    /// Returns any error the writer returns.
    pub fn dump_into(&self, out: &mut impl fmt::Write) -> fmt::Result {
        let mut dumper = Dumper::default();
        // Numbering this graph first is what makes the `m<id>` a module task
        // inside it reports the same one it would report in a whole document:
        // ids go out in visit order, and the graph being dumped is visit one.
        let (top, _) = dumper.id_of(self);
        debug_assert_eq!(top, TOP_GRAPH_ID, "the graph being dumped is visit one");

        dump_graph(self, out, TOP_GRAPH_LEVEL, TOP_NODE_PREFIX, &mut dumper)
    }
}

impl Taskflow {
    /// Dumps the taskflow as a Graphviz DOT document.
    ///
    /// Upstream's `Taskflow::dump()`. See the [module docs](crate::dump) for
    /// the one way the output departs from upstream's: the node and graph
    /// addresses it prints are arena indices here.
    ///
    /// ```
    /// # use rustflow::{Handle, Taskflow};
    /// let mut taskflow = Taskflow::with_name("one");
    /// taskflow.graph_mut().add_named_node("a", Handle::Placeholder);
    /// assert_eq!(
    ///     taskflow.dump(),
    ///     concat!(
    ///         "digraph Taskflow {\n",
    ///         "  compound=true;\n",
    ///         "  subgraph cluster_p0 {\n",
    ///         "    label=\"Taskflow: one\";\n",
    ///         "    p0[label=\"a\" ];\n",
    ///         "  }\n",
    ///         "}\n",
    ///     )
    /// );
    /// ```
    ///
    /// A task that builds its graph while it runs is in the document only once
    /// it has run and only if it kept what it built, which is upstream's note
    /// on the same method: a subflow's subgraph is cleared when it is joined
    /// unless the body called `retain(true)`, and an empty subgraph writes no
    /// cluster at all. A module task is the exception, since its target is a
    /// graph somebody else built: it dumps before any run.
    ///
    /// # Panics
    ///
    /// Panics if the formatting machinery fails, which writing to a `String`
    /// cannot do.
    #[must_use]
    pub fn dump(&self) -> String {
        let mut dot = String::new();
        self.dump_into(&mut dot)
            .expect("writing a dump to a String cannot fail");
        dot
    }

    /// Dumps the taskflow as Graphviz DOT into an existing writer.
    ///
    /// Upstream's `Taskflow::dump(std::ostream&)` together with the
    /// module-stack `_dump` it calls.
    ///
    /// # Errors
    ///
    /// Returns any error the writer returns.
    pub fn dump_into(&self, out: &mut impl fmt::Write) -> fmt::Result {
        out.write_str("digraph Taskflow {\n")?;
        // Required upstream for the lhead/ltail cluster edges a subflow dump
        // emits, and kept unconditionally, as upstream keeps it.
        out.write_str("  compound=true;\n")?;
        self.dump_clusters(out)?;
        out.write_str("}\n")
    }

    /// Writes the `subgraph cluster_p0 { ... }` wrapper around the graph, then
    /// one sibling cluster per module graph reached from it.
    ///
    /// Upstream drives this from `Dumper::stack`, and so does this: the
    /// taskflow's own graph goes first, at depth 1 and labelled
    /// `Taskflow: <name>`, and every module task found anywhere under it --
    /// including inside another module's graph, which pushes in its turn --
    /// leaves its target on the stack for this loop to drain. The loop *pops*,
    /// so two module tasks in one graph write their clusters in the reverse of
    /// the order their ids were handed out; the ids themselves are visit
    /// order, which is what the `m<id>` labels report.
    fn dump_clusters(&self, out: &mut impl fmt::Write) -> fmt::Result {
        let mut dumper = Dumper::default();
        let (top, _) = dumper.id_of(self.graph());
        debug_assert_eq!(top, TOP_GRAPH_ID, "the top-level graph is visit one");

        write_cluster(
            out,
            self.graph(),
            top,
            TOP_GRAPH_DEPTH,
            ClusterLabel::Taskflow(self.name()),
            &mut dumper,
        )?;

        while let Some(pending) = dumper.stack.pop() {
            // Bumping the refcount is what keeps the borrow of the dumper's
            // own share of the target from overlapping the `&mut` the nodes
            // below need; it is free to drop afterwards, since the dumper
            // holds that share until the whole dump is over.
            let target = Arc::clone(&dumper.targets[pending.target]);
            write_cluster(
                out,
                target.graph(),
                pending.id,
                pending.depth,
                ClusterLabel::Module(&pending.name),
                &mut dumper,
            )?;
        }

        Ok(())
    }
}

impl fmt::Display for Taskflow {
    /// The same DOT document [`Taskflow::dump`] returns.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.dump_into(f)
    }
}

/// Upstream's `Taskflow::Dumper`: the graph-id counter, the graphs that
/// counter has already numbered, and the module graphs still owed a cluster.
#[derive(Debug, Default)]
struct Dumper {
    /// Upstream's `dumper.id`, the next graph id to hand out.
    next_id: usize,
    /// Upstream's `dumper.visited`, which keys a graph's id by the graph's
    /// address.
    visited: HashMap<*const Graph, usize>,
    /// A share of every module target numbered so far, held for the length of
    /// the dump. Keeping them alive is what stops an entry of `visited` from
    /// naming a graph that has been dropped, and so from matching a graph
    /// allocated later at the same address; the top-level graph needs no entry
    /// here, being borrowed by the dump itself.
    targets: Vec<Arc<Taskflow>>,
    /// Upstream's `dumper.stack` of module graphs waiting for a cluster.
    stack: Vec<PendingModule>,
}

impl Dumper {
    /// The id this dump has given `graph`, assigning the next one if it has
    /// none, together with whether the id is new.
    ///
    /// Upstream's `dumper.visited.find(mgraph) == end()` check and the
    /// `dumper.visited[mgraph] = dumper.id++` that follows it. Identity is the
    /// graph's address, as upstream's key is, so two `Arc`s cloned from one
    /// taskflow name one graph and get one cluster. The address is only ever
    /// compared and never dereferenced.
    fn id_of(&mut self, graph: &Graph) -> (usize, bool) {
        match self.visited.entry(std::ptr::from_ref(graph)) {
            Entry::Occupied(entry) => (*entry.get(), false),
            Entry::Vacant(entry) => {
                let id = self.next_id;
                self.next_id += 1;
                entry.insert(id);
                (id, true)
            }
        }
    }

    /// Takes a share of a module target and pushes its graph for
    /// [`Taskflow::dump_clusters`] to write as a sibling cluster.
    fn push_module(&mut self, name: &str, target: &Arc<Taskflow>, id: usize, depth: usize) {
        self.targets.push(Arc::clone(target));
        self.stack.push(PendingModule {
            name: name.to_owned(),
            target: self.targets.len() - 1,
            id,
            depth,
        });
    }
}

/// A module graph owed a cluster: one entry of upstream's `Dumper::stack`.
///
/// Upstream stores the module *node* and reads the label back off it. This
/// stores the two things that label needs instead, because the node may well
/// be behind a lock that is gone by the time the entry comes back off.
#[derive(Debug)]
struct PendingModule {
    /// The name of the module task that composed the graph, empty when that
    /// task has none.
    name: String,
    /// Which of [`Dumper::targets`] holds the taskflow this graph belongs to.
    target: usize,
    /// The id [`Dumper::id_of`] gave the graph, which names its cluster.
    id: usize,
    /// The nesting level to write the cluster at. Upstream pushes the level of
    /// the module task that found it, so a sibling cluster is indented as
    /// though it were nested inside the cluster it was composed from; the
    /// braces, not the indentation, are what say otherwise.
    depth: usize,
}

/// Which of the two labels a graph cluster gets.
///
/// Upstream decides by whether the stack entry carries a module node: the top
/// graph is pushed with a null one.
enum ClusterLabel<'a> {
    /// The top-level graph, upstream's null node: `Taskflow: <name>`, falling
    /// back to the graph's own id when the taskflow is unnamed.
    Taskflow(&'a str),
    /// A graph some module task composed: that task's name, falling back to
    /// `m<graph id>` when the task is unnamed.
    Module(&'a str),
}

/// Writes one `subgraph cluster_p<id> { ... }` and everything in it.
///
/// Upstream's top-level `_dump` loop body (`taskflow.hpp` lines 441-477): the
/// cluster line at `depth`, the label one level in, the graph's nodes one
/// level in, the closing brace back at `depth`.
fn write_cluster(
    out: &mut impl fmt::Write,
    graph: &Graph,
    id: usize,
    depth: usize,
    label: ClusterLabel<'_>,
    dumper: &mut Dumper,
) -> fmt::Result {
    let (label_prefix, name, fallback) = match label {
        ClusterLabel::Taskflow(name) => ("Taskflow: ", name, format!("p{TOP_GRAPH_ID}")),
        ClusterLabel::Module(name) => ("", name, format!("m{id}")),
    };

    open_cluster(
        out,
        depth,
        &format!("cluster_p{id}"),
        label_prefix,
        name,
        &fallback,
    )?;
    dump_graph(graph, out, depth + 1, &node_prefix(id), dumper)?;
    close_cluster(out, depth)
}

/// Writes the `subgraph <cluster> {` line at `level` and the `label` line one
/// level in: how both kinds of cluster begin, upstream's two `ind` writes.
fn open_cluster(
    out: &mut impl fmt::Write,
    level: usize,
    cluster: &str,
    label_prefix: &str,
    name: &str,
    fallback: &str,
) -> fmt::Result {
    indent(out, level)?;
    writeln!(out, "subgraph {cluster} {{")?;

    indent(out, level + 1)?;
    write!(out, "label=\"{label_prefix}")?;
    write_name(out, name, fallback)?;
    out.write_str("\";\n")
}

/// Writes the closing brace of a cluster opened at `level`.
fn close_cluster(out: &mut impl fmt::Write, level: usize) -> fmt::Result {
    indent(out, level)?;
    out.write_str("}\n")
}

/// Writes `name`, or `fallback` when there is no name: upstream falls back to
/// the address it is printing anyway, and this port to the id that replaced
/// it.
fn write_name(out: &mut impl fmt::Write, name: &str, fallback: &str) -> fmt::Result {
    out.write_str(if name.is_empty() { fallback } else { name })
}

/// Writes one `<from> -> <to>;` line per successor of `node`, each with
/// whatever [`edge_attributes`] gives an edge leaving that handle.
///
/// Upstream writes a module task's edges without the `_is_conditioner()`
/// question its per-node `_dump` asks, which comes to the same answer: a
/// module task is not a conditioner, so its edges carry no attributes.
fn write_edges(
    out: &mut impl fmt::Write,
    node: &Node,
    id: &str,
    level: usize,
    prefix: &str,
) -> fmt::Result {
    for (position, &successor) in node.successors().iter().enumerate() {
        indent(out, level)?;
        writeln!(
            out,
            "{id} -> {}{};",
            node_id(prefix, successor),
            edge_attributes(node.handle(), position)
        )?;
    }

    Ok(())
}

/// Dumps every node of `graph` at `level`, upstream's per-graph `_dump`.
///
/// Upstream splits the loop body in two, because a module task's line carries
/// its shape *before* its label and names the graph it composes as well as
/// itself; everything else goes through [`dump_node`]. `prefix` is what every
/// id written below starts with, and is the one argument upstream has no
/// equivalent for: its addresses need no help to stay unique.
fn dump_graph(
    graph: &Graph,
    out: &mut impl fmt::Write,
    level: usize,
    prefix: &str,
    dumper: &mut Dumper,
) -> fmt::Result {
    for (task, node) in graph.iter() {
        if let Handle::Module(target) = node.handle() {
            dump_module_node(node, task, target, out, level, prefix, dumper)?;
        } else {
            dump_node(node, task, out, level, prefix, dumper)?;
        }
    }
    Ok(())
}

/// Dumps one module task, upstream's `MODULE` branch of the per-graph `_dump`
/// (`taskflow.hpp` lines 569-588).
///
/// The target's graph is numbered here and pushed for the document-level loop
/// to write, but only on its first visit, which is what gives a graph composed
/// twice one cluster and makes a cycle of compositions terminate.
fn dump_module_node(
    node: &Node,
    task: Task,
    target: &Arc<Taskflow>,
    out: &mut impl fmt::Write,
    level: usize,
    prefix: &str,
    dumper: &mut Dumper,
) -> fmt::Result {
    let id = node_id(prefix, task);

    indent(out, level)?;
    write!(out, "{id}[shape=box3d, color=blue, label=\"")?;
    write_name(out, node.name(), &id)?;

    // Upstream numbers the target between writing the label and writing the
    // `[m<id>]` that reports the number, so an unnamed module task reports the
    // id this very visit handed out.
    let (graph_id, first_visit) = dumper.id_of(target.graph());
    if first_visit {
        dumper.push_module(node.name(), target, graph_id, level);
    }
    if node.name().is_empty() {
        write!(out, " [m{graph_id}]")?;
    }
    out.write_str("\"];\n")?;

    write_edges(out, node, &id, level, prefix)
}

/// Dumps one node and its outgoing edges, upstream's per-node `_dump`.
///
/// The space before the `]` is upstream's: it writes the label as `"\" "` and
/// then appends a shape only for the handles that have one, so a static task's
/// line ends `... ];`.
fn dump_node(
    node: &Node,
    task: Task,
    out: &mut impl fmt::Write,
    level: usize,
    prefix: &str,
    dumper: &mut Dumper,
) -> fmt::Result {
    let id = node_id(prefix, task);

    indent(out, level)?;
    write!(out, "{id}[label=\"")?;
    write_name(out, node.name(), &id)?;
    writeln!(out, "\" {}];", node_shape(node.handle()))?;

    write_edges(out, node, &id, level, prefix)?;

    // Upstream follows the edges with the nested cluster of the two handles
    // that own a graph of their own. A subflow's is the one this dump has to
    // take a lock to read; see the module docs for why it never waits for one,
    // and `src/subflow.rs` for who is holding it when it does not get it.
    match node.handle() {
        Handle::Subflow { subgraph, .. } => {
            let locked = match subgraph.try_lock() {
                Ok(subgraph) => Some(subgraph),
                Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
                Err(TryLockError::WouldBlock) => None,
            };
            if let Some(subgraph) = locked {
                dump_subgraph(&subgraph, out, level, &id, node.name(), "Subflow", dumper)?;
            }
        }
        Handle::AdoptedModule(subgraph) => {
            dump_subgraph(
                subgraph,
                out,
                level,
                &id,
                node.name(),
                "AdoptedModule",
                dumper,
            )?;
        }
        // A module task owns no graph either, and does not reach this function
        // at all: [`dump_graph`] sends it to [`dump_module_node`]. Neither
        // async handle owns a graph: the node a free async task is belongs to
        // the task rather than to the handle, and is in no graph -- and so in
        // no taskflow -- to be dumped. What reaches here is an async handle
        // somebody put in a graph through [`Graph::add_node`], and it is a
        // plain node like any other.
        Handle::Placeholder
        | Handle::Static(_)
        | Handle::Condition(_)
        | Handle::MultiCondition(_)
        | Handle::Runtime(_)
        | Handle::NonpreemptiveRuntime(_)
        | Handle::Module(_)
        | Handle::Async(_)
        | Handle::DependentAsync(_) => {}
    }

    Ok(())
}

/// Writes the nested cluster a subflow or an adopted module owns, upstream's
/// `SUBFLOW`/`ADOPTED_MODULE` branch of the per-node `_dump` (`taskflow.hpp`
/// lines 518-551), and the one join edge that follows it.
///
/// An empty subgraph writes nothing whatever, which is upstream's
/// `if(!g.empty())` and is what a subflow that has not run -- or that ran
/// without `retain(true)` -- looks like. `kind` is the word upstream puts in
/// the label, `Subflow` or `AdoptedModule`.
fn dump_subgraph(
    subgraph: &Graph,
    out: &mut impl fmt::Write,
    level: usize,
    parent_id: &str,
    parent_name: &str,
    kind: &str,
    dumper: &mut Dumper,
) -> fmt::Result {
    // Upstream's non-emptiness check and the `*g.begin()` of its join edge are
    // the same question asked twice; this asks it once.
    let Some((first, _)) = subgraph.iter().next() else {
        return Ok(());
    };

    let cluster = format!("cluster_{parent_id}{SUBGRAPH_MARKER}");
    let prefix = format!("{parent_id}{SUBGRAPH_MARKER}_");

    open_cluster(
        out,
        level,
        &cluster,
        &format!("{kind}: "),
        parent_name,
        parent_id,
    )?;

    indent(out, level + 1)?;
    out.write_str("color=blue;\n")?;

    dump_graph(subgraph, out, level + 1, &prefix, dumper)?;

    close_cluster(out, level)?;

    // Upstream's single cluster-level join edge: one dashed arrow from the
    // subgraph's first node to the node that owns it, clipped at the cluster
    // boundary by `ltail` so that `dot` draws the cluster as a whole feeding
    // the parent rather than one child of it doing so.
    indent(out, level)?;
    writeln!(
        out,
        "{} -> {parent_id} [ltail={cluster} style=dashed color=blue];",
        node_id(&prefix, first)
    )
}

/// The identifier one node is written as: its graph's prefix and its own arena
/// index. See the module docs for what the prefixes are and why.
fn node_id(prefix: &str, task: Task) -> String {
    format!("{prefix}{}", task.id())
}

/// The prefix every node id inside one graph cluster carries.
///
/// The top-level graph keeps the bare `p<index>`; any other graph with an id
/// is a module graph, whose nodes are `p<graph id>_<index>`.
fn node_prefix(graph_id: usize) -> String {
    if graph_id == TOP_GRAPH_ID {
        TOP_NODE_PREFIX.to_owned()
    } else {
        format!("p{graph_id}_")
    }
}

/// The shape attributes upstream gives a node, by handle.
///
/// Upstream's `switch` writes `shape=diamond color=black fillcolor=aquamarine
/// style=filled` for a condition or multi-condition task and nothing for
/// anything else. Everything else is a plain ellipse, so the string is empty --
/// including a module task, whose `shape=box3d, color=blue` has no `case`
/// here: it is written by the module branch of the per-graph `_dump`, before
/// the label rather than after it, and so by [`dump_module_node`].
fn node_shape(handle: &Handle) -> &'static str {
    match handle {
        Handle::Placeholder
        | Handle::Static(_)
        | Handle::Subflow { .. }
        | Handle::Runtime(_)
        | Handle::NonpreemptiveRuntime(_)
        | Handle::Module(_)
        | Handle::AdoptedModule(_)
        | Handle::Async(_)
        | Handle::DependentAsync(_) => "",
        Handle::Condition(_) | Handle::MultiCondition(_) => {
            "shape=diamond color=black fillcolor=aquamarine style=filled"
        }
    }
}

/// The attributes upstream appends to one outgoing edge.
///
/// An edge leaving a conditioner is weak -- which successor runs is decided at
/// runtime -- so upstream draws it dashed and labels it with its position in
/// the successor list: ` [style=dashed label="<position>"]`. Every other edge
/// is a plain dependency and carries no attributes. The position is the half of
/// that format that cannot be recovered from the node alone, which is why it is
/// threaded through from the call site.
fn edge_attributes(handle: &Handle, position: usize) -> String {
    match handle {
        Handle::Placeholder
        | Handle::Static(_)
        | Handle::Subflow { .. }
        | Handle::Runtime(_)
        | Handle::NonpreemptiveRuntime(_)
        | Handle::Module(_)
        | Handle::AdoptedModule(_)
        | Handle::Async(_)
        | Handle::DependentAsync(_) => String::new(),
        Handle::Condition(_) | Handle::MultiCondition(_) => {
            format!(" [style=dashed label=\"{position}\"]")
        }
    }
}

/// Writes upstream's `std::string ind(level * 2, ' ')`.
fn indent(out: &mut impl fmt::Write, level: usize) -> fmt::Result {
    write!(out, "{:width$}", "", width = level * 2)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::graph::{AsyncWork, SubflowWork, Work};

    /// A graph of `children` placeholders named `c0`, `c1`, ..., which is what
    /// a subflow body would have built had the executor run one yet.
    fn child_graph(children: usize) -> Graph {
        let mut graph = Graph::new();
        for child in 0..children {
            graph.add_named_node(format!("c{child}"), Handle::Placeholder);
        }
        graph
    }

    /// A subflow handle over a subgraph built by hand, the body being the one
    /// part of it a dump never reads.
    fn subflow_handle(subgraph: Graph) -> Handle {
        Handle::Subflow {
            work: SubflowWork::new(|_| {}),
            subgraph: Mutex::new(subgraph),
        }
    }

    /// A taskflow with one subflow task whose subgraph holds `children` nodes.
    fn with_subflow(children: usize) -> (Taskflow, Task) {
        let mut taskflow = Taskflow::with_name("dynamic");
        let spawner = taskflow
            .graph_mut()
            .add_named_node("spawner", subflow_handle(child_graph(children)));
        (taskflow, spawner)
    }

    /// The subgraph `Mutex` of a subflow task, for a test that wants to hold
    /// or poison it.
    fn subgraph_of(taskflow: &Taskflow, spawner: Task) -> &Mutex<Graph> {
        let Handle::Subflow { subgraph, .. } = taskflow.graph().node(spawner).handle() else {
            panic!("the spawner is a subflow task");
        };
        subgraph
    }

    /// The cluster body of a one-node taskflow, which is all
    /// [`Graph::dump_into`] emits.
    #[test]
    fn a_graph_dumps_its_cluster_body_only() {
        let mut graph = Graph::new();
        let a = graph.add_named_node("a", Handle::Placeholder);
        let b = graph.add_node(Handle::Placeholder);
        graph.add_edge(a, b);

        let mut dot = String::new();
        graph.dump_into(&mut dot).expect("String never fails");
        assert_eq!(
            dot,
            concat!(
                "    p0[label=\"a\" ];\n",
                "    p0 -> p1;\n",
                "    p1[label=\"p1\" ];\n",
            )
        );
    }

    /// Both async handles dump like a placeholder too: no shape of their own,
    /// no edge attributes and no nested cluster, which is upstream's
    /// `default` again. A free async node is in no taskflow to be dumped at
    /// all -- what this covers is an async handle somebody put in a graph
    /// through [`Graph::add_node`].
    #[test]
    fn the_async_handles_dump_like_a_placeholder() {
        let mut placeholder = Taskflow::with_name("t");
        placeholder
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        let expected = placeholder.dump();

        for handle in [
            Handle::Async(AsyncWork::plain(|| {})),
            Handle::DependentAsync(AsyncWork::plain(|| {})),
        ] {
            let mut taskflow = Taskflow::with_name("t");
            taskflow.graph_mut().add_named_node("a", handle);
            assert_eq!(taskflow.dump(), expected);
        }
    }

    /// A static task and a placeholder dump identically: both fall through
    /// upstream's shape `switch` to `default`.
    #[test]
    fn a_static_handle_dumps_like_a_placeholder() {
        let mut with_work = Taskflow::with_name("t");
        with_work
            .graph_mut()
            .add_named_node("a", Handle::Static(Work::new(|| {})));

        let mut placeholder = Taskflow::with_name("t");
        placeholder
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);

        assert_eq!(with_work.dump(), placeholder.dump());
    }

    /// The two conditioner handles dump identically: upstream's shape `switch`
    /// shares one case between `CONDITION` and `MULTI_CONDITION`, and its edge
    /// branch asks `_is_conditioner()`, which is true of both.
    #[test]
    fn a_multi_condition_handle_dumps_like_a_condition() {
        let mut one = Taskflow::with_name("t");
        let graph = one.graph_mut();
        let picker = graph.add_named_node("pick", Handle::Condition(Work::new(|| 0)));
        let target = graph.add_named_node("then", Handle::Placeholder);
        graph.add_edge(picker, target);

        let mut many = Taskflow::with_name("t");
        let graph = many.graph_mut();
        let picker = graph.add_named_node("pick", Handle::MultiCondition(Work::new(Vec::new)));
        let target = graph.add_named_node("then", Handle::Placeholder);
        graph.add_edge(picker, target);

        assert_eq!(one.dump(), many.dump());
        assert!(one.dump().contains("p0 -> p1 [style=dashed label=\"0\"];"));
    }

    #[test]
    fn display_and_dump_agree() {
        let mut taskflow = Taskflow::with_name("displayed");
        let a = taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        let b = taskflow
            .graph_mut()
            .add_named_node("b", Handle::Placeholder);
        taskflow.graph_mut().add_edge(a, b);

        assert_eq!(taskflow.to_string(), taskflow.dump());
        assert!(format!("{taskflow}").contains("label=\"Taskflow: displayed\";"));
    }

    /// `dump_into` appends, so a caller can wrap the document in its own text.
    #[test]
    fn dump_into_appends_to_the_writer() {
        let taskflow = Taskflow::new();
        let mut out = String::from("before\n");
        taskflow.dump_into(&mut out).expect("String never fails");
        assert!(out.starts_with("before\ndigraph Taskflow {\n"));
    }

    /// A duplicate edge is a second line, matching the second join-counter
    /// increment it causes.
    #[test]
    fn duplicate_edges_each_get_a_line() {
        let mut taskflow = Taskflow::with_name("dup");
        let a = taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        let b = taskflow
            .graph_mut()
            .add_named_node("b", Handle::Placeholder);
        taskflow.graph_mut().add_edge(a, b);
        taskflow.graph_mut().add_edge(a, b);

        assert_eq!(taskflow.dump().matches("p0 -> p1;").count(), 2);
    }

    #[test]
    fn indentation_is_two_spaces_per_level() {
        let mut out = String::new();
        indent(&mut out, 0).expect("String never fails");
        indent(&mut out, 3).expect("String never fails");
        assert_eq!(out, "      ");
    }

    /// The fragment [`Graph::dump_into`] writes keeps a module task's own line
    /// and drops the sibling cluster that only a document can hold -- and the
    /// `[m1]` it reports is the id the same target gets in a document, because
    /// the graph being dumped is numbered first either way.
    #[test]
    fn a_graph_fragment_numbers_a_module_but_writes_no_cluster() {
        let mut target = Taskflow::with_name("target");
        target.graph_mut().add_named_node("x", Handle::Placeholder);
        let target = Arc::new(target);

        let mut graph = Graph::new();
        graph.add_node(Handle::Module(Arc::clone(&target)));

        let mut dot = String::new();
        graph.dump_into(&mut dot).expect("String never fails");
        assert_eq!(dot, "    p0[shape=box3d, color=blue, label=\"p0 [m1]\"];\n");
    }

    /// A subgraph another thread is still building is left alone: the dump
    /// takes the lock with `try_lock`, so the node loses its cluster rather
    /// than the dumping thread losing the ability to finish.
    #[test]
    fn a_held_subgraph_dumps_without_its_cluster() {
        let (taskflow, spawner) = with_subflow(2);

        let held = subgraph_of(&taskflow, spawner)
            .lock()
            .expect("a fresh mutex is not poisoned");
        let dumped_while_held = taskflow.dump();
        drop(held);

        assert!(!dumped_while_held.contains("Subflow: spawner"));
        assert!(!dumped_while_held.contains("ltail="));
        // And the node itself is there all the same, so the document is still
        // the graph minus one cluster rather than minus a task.
        assert!(dumped_while_held.contains("p0[label=\"spawner\" ];"));
        assert!(taskflow.dump().contains("Subflow: spawner"));
    }

    /// A body that panicked poisons its node's subgraph, and what it built
    /// before panicking is exactly what a dump is for, so the lock is taken
    /// with `into_inner` rather than refused.
    #[test]
    fn a_poisoned_subgraph_still_dumps() {
        let (taskflow, spawner) = with_subflow(1);
        let subgraph = subgraph_of(&taskflow, spawner);

        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = subgraph.lock().expect("a fresh mutex is not poisoned");
            panic!("a subflow body panicking is what poisons the subgraph");
        }));
        assert!(panicked.is_err());
        assert!(subgraph.is_poisoned());

        assert!(taskflow.dump().contains("Subflow: spawner"));
        assert!(taskflow.dump().contains("p0_sub_0[label=\"c0\" ];"));
    }

    /// A node-owned cluster recurses, so a subgraph of a subgraph nests, and
    /// the ids keep growing a `_sub` component per level rather than
    /// repeating.
    #[test]
    fn a_subgraph_inside_a_subgraph_nests() {
        let mut inner = Graph::new();
        inner.add_named_node("spawner", subflow_handle(child_graph(1)));

        let mut outer = Taskflow::with_name("outer");
        outer
            .graph_mut()
            .add_named_node("spawner", subflow_handle(inner));

        let dot = outer.dump();
        assert!(dot.contains("subgraph cluster_p0_sub {"));
        assert!(dot.contains("subgraph cluster_p0_sub_0_sub {"));
        assert!(dot.contains("p0_sub_0_sub_0[label=\"c0\" ];"));
    }
}
