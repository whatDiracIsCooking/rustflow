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
//! * A node is `p<arena index>`, so `p0`, `p1`, ... in creation order. This is
//!   the substitution the issue asks for, and it is what makes the output
//!   stable enough to assert on.
//! * A graph cluster is `cluster_p<graph id>`, where the graph id is the small
//!   integer upstream's own `Dumper` already assigns each graph it visits
//!   (`dumper.visited[top] = dumper.id++`). The top-level graph is therefore 0,
//!   and `cluster_p0` is the only cluster a static taskflow can produce.
//! * An unnamed taskflow's label, upstream's `Taskflow: p<this>`, names that
//!   same top-level graph: `Taskflow: p0`.
//! * An unnamed node falls back to its own id as its label, as upstream falls
//!   back to the address it is already printing, so a nameless first node
//!   reads `p0[label="p0" ];`.
//!
//! Names are written through exactly as upstream writes them, with no DOT
//! escaping: upstream does not escape them either, and quietly diverging would
//! make this output differ from the format it claims to match.
//!
//! # What is not here yet
//!
//! Upstream's module-task stack and its nested subflow clusters key off handle
//! variants that do not exist before M4. The branches that would emit them are
//! marked below at the point where they belong, but nothing stands in for
//! them: an exhaustive `match` on [`Handle`] is a better reminder than a stub,
//! because adding the variant stops compiling until the branch is written.
//!
//! That is exactly what happened to the conditioner branches, and it is how
//! they were filled: the two handles landed first, in #28, leaving each arm a
//! `todo!()` naming #30 rather than a guess at the format, and #30 then
//! replaced both with the diamond shape and the dashed, position-labelled
//! edge upstream writes. So a graph holding a conditioner dumps like any
//! other now.
//!
//! The four dynamic-tasking handles are at the same stage as of #45: each arm
//! below is a `todo!()` naming #46, which owns the cluster format, so dumping
//! a graph that holds a subflow, runtime or module task panics until that
//! issue lands.

use core::fmt;

use crate::graph::{Graph, Handle, Node, Task};
use crate::taskflow::Taskflow;

/// The id of the top-level graph, upstream's `dumper.visited[top]`, which is
/// the first id `Dumper` hands out.
const TOP_GRAPH_ID: usize = 0;

/// The nesting level a top-level graph's nodes sit at: one for the `digraph`
/// body, one for the `subgraph cluster_p0` the taskflow wraps it in. Upstream
/// arrives at the same number by pushing the top graph at depth 1 and dumping
/// its nodes at `depth + 1`.
const TOP_GRAPH_LEVEL: usize = 2;

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
    /// # Errors
    ///
    /// Returns any error the writer returns.
    pub fn dump_into(&self, out: &mut impl fmt::Write) -> fmt::Result {
        dump_graph(self, out, TOP_GRAPH_LEVEL)
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

    /// Writes the `subgraph cluster_p0 { ... }` wrapper around the graph.
    ///
    /// Upstream drives this from a stack of graphs so that a module task can
    /// push the graph it refers to and have it dumped as a sibling cluster.
    /// Nothing can push, so the loop is one pass: the only graph is this
    /// taskflow's own, at depth 1, labelled `Taskflow: <name>`. The module
    /// branch -- which is also the only thing that makes `dumper.id` exceed
    /// [`TOP_GRAPH_ID`] -- arrives with module tasks.
    fn dump_clusters(&self, out: &mut impl fmt::Write) -> fmt::Result {
        let depth = 1;
        indent(out, depth)?;
        writeln!(out, "subgraph cluster_p{TOP_GRAPH_ID} {{")?;

        indent(out, depth + 1)?;
        out.write_str("label=\"Taskflow: ")?;
        if self.name().is_empty() {
            write!(out, "p{TOP_GRAPH_ID}")?;
        } else {
            out.write_str(self.name())?;
        }
        out.write_str("\";\n")?;

        dump_graph(self.graph(), out, depth + 1)?;

        indent(out, depth)?;
        out.write_str("}\n")
    }
}

impl fmt::Display for Taskflow {
    /// The same DOT document [`Taskflow::dump`] returns.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.dump_into(f)
    }
}

/// Dumps every node of `graph` at `level`, upstream's per-graph `_dump`.
///
/// Upstream splits the loop body in two, because a module task's line carries
/// a `shape=box3d` and the id of the graph it refers to rather than its own
/// label. Until that handle exists, every node goes through [`dump_node`].
fn dump_graph(graph: &Graph, out: &mut impl fmt::Write, level: usize) -> fmt::Result {
    for (task, node) in graph.iter() {
        dump_node(node, task, out, level)?;
    }
    Ok(())
}

/// Dumps one node and its outgoing edges, upstream's per-node `_dump`.
///
/// The space before the `]` is upstream's: it writes the label as `"\" "` and
/// then appends a shape only for the handles that have one, so a static task's
/// line ends `... ];`.
fn dump_node(node: &Node, task: Task, out: &mut impl fmt::Write, level: usize) -> fmt::Result {
    let id = task.id();

    indent(out, level)?;
    write!(out, "p{id}[label=\"")?;
    if node.name().is_empty() {
        write!(out, "p{id}")?;
    } else {
        out.write_str(node.name())?;
    }
    writeln!(out, "\" {}];", node_shape(node.handle()))?;

    for (position, successor) in node.successors().iter().enumerate() {
        indent(out, level)?;
        writeln!(
            out,
            "p{id} -> p{}{};",
            successor.id(),
            edge_attributes(node.handle(), position)
        )?;
    }

    // Upstream follows the edges with a nested `subgraph cluster_p<node> {
    // label="Subflow: ..."; color=blue; ... }` plus a dashed, `ltail`-clipped
    // join edge back to this node, for the two handles that own a graph of
    // their own (`Subflow`, `AdoptedModule`). Neither exists yet, so there is
    // nothing to recurse into; the cluster lands here when subflows do.

    Ok(())
}

/// The shape attributes upstream gives a node, by handle.
///
/// Upstream's `switch` writes `shape=diamond color=black fillcolor=aquamarine
/// style=filled` for a condition or multi-condition task and nothing for
/// anything else. Everything else is a plain ellipse, so the string is empty.
fn node_shape(handle: &Handle) -> &'static str {
    match handle {
        Handle::Placeholder | Handle::Static(_) => "",
        Handle::Condition(_) | Handle::MultiCondition(_) => {
            "shape=diamond color=black fillcolor=aquamarine style=filled"
        }
        Handle::Subflow { .. } | Handle::Runtime(_) => {
            todo!("the subflow and runtime node shapes are issue #46's")
        }
        Handle::Module(_) | Handle::AdoptedModule(_) => {
            todo!("the module node shape is issue #46's")
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
        Handle::Placeholder | Handle::Static(_) => String::new(),
        Handle::Condition(_) | Handle::MultiCondition(_) => {
            format!(" [style=dashed label=\"{position}\"]")
        }
        Handle::Subflow { .. }
        | Handle::Runtime(_)
        | Handle::Module(_)
        | Handle::AdoptedModule(_) => {
            todo!("issue #46 writes the edges leaving a dynamic task: {position}")
        }
    }
}

/// Writes upstream's `std::string ind(level * 2, ' ')`.
fn indent(out: &mut impl fmt::Write, level: usize) -> fmt::Result {
    write!(out, "{:width$}", "", width = level * 2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Work;

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
}
