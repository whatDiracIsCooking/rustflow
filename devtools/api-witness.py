#!/usr/bin/env python3
# Reports every public function that no test, example, benchmark or doc
# example witnesses, reading each call site against the *type* it reaches
# rather than against the bare function name.
#
#   devtools/api-witness.py                 # the summary, then what it flags
#   devtools/api-witness.py --all           # every declaration, witnessed too
#   devtools/api-witness.py <repo root>     # a tree other than this script's
#
# This is a REPORT, not a gate: it exits 0 whatever it finds, so an acceptance
# check or a PR body can quote its numbers without the script deciding what
# passes. Exit 2 means the measurement itself could not run. Nothing here is
# outside Python 3's standard library, so it runs in the container, on a host
# and in a worktree with no setup at all.
#
# WHAT IS MEASURED. Every `pub fn` declared above a file's test module in
# `src/**/*.rs`, each one counted against the type it is declared on -- the
# denominator the name-based measurement this replaces had, split per item:
# 265 names over 404 declarations, 65 of those names declared on more than
# one type. A method of a public trait is not a `pub fn` and is counted by
# neither. The corpus of witnesses is `tests/`, `examples/`, `benches/` and
# the ```-fenced blocks of the crate's own doc comments -- not the crate's own
# `#[cfg(test)]` modules, which are what a `pub fn` above them is *by*, and
# the question here is which of them a caller outside the crate reaches.
#
# WHAT IT ATTRIBUTES. A call site counts for a declaration when the script can
# see the type the call reaches, which it can for:
#
#   `Type::name(..)`   a qualified path, `<Type as Trait>::name(..)` included;
#   `name(..)`         a free function, path-qualified or not;
#   `recv.name(..)`    whenever `recv` resolves to a type.
#
# A receiver resolves from an identifier the chunk of text around it binds --
# a `let` with a type annotation or with a constructor call on the right
# (`let group = executor.task_group()?`), an annotated function or closure
# parameter, an identifier an enum pattern binds (`if let
# Handle::Static(work) = ..`), a `for` loop's own variable, `self` inside an
# `impl` -- or from a primary expression: a qualified call, a struct literal,
# a free call. On top of that root it follows a postfix chain of method calls,
# fields and indexing, resolved through the return types, struct fields, enum
# variants and `Fn` bounds that `src/` and the corpus themselves declare. So
# `taskflow.task_ref(task).hash_value()` reaches `TaskRef::hash_value`, and
# `executor.scoped_spawn(|scope| scope.spawn(..))` reaches `Scope::spawn`
# through the bound on `scoped_spawn`'s own parameter. A block expression in
# argument position is transparent to that last rule, a closure being so often
# built inside one.
#
# Four conveniences keep that honest rather than approximate. `Arc`, `Rc` and
# `Box` are seen through, since a caller calls through them. `Option` and
# `Result` are seen through an `unwrap`, an `expect` or a `?`, and a `Mutex`
# or `RwLock` through its guard. `Type::new`, `Type::default` and `Type::from`
# are taken to hand back a `Type` when no signature says otherwise, which is
# the only reading available for a *derived* `Default`. And a macro's
# metavariable is bound to the type of the matching argument at *every*
# invocation of that macro, because the body is expanded once per invocation
# and each of those calls really happens.
#
# WHAT IT CANNOT. There is no type inference here, so a receiver whose type
# only the compiler knows is invisible: a generic `T`, an unannotated closure
# parameter of a callee whose signature declares two `Fn` bounds, an element
# plucked out of a map, a binding whose type comes from a trait object. An
# identifier the surrounding chunk binds to two different types is dropped
# rather than guessed at, and a chunk is a whole `fn` rather than a block, so
# a name reused at two types inside one function is unresolvable by design.
# It reads text, not syntax: a call written inside a string literal reads as a
# call, though `//` comments are stripped from the corpus first, which is one
# of the two places this count differs from the name-based one -- a function
# named only by a *comment* in `tests/` was "named" there and is flagged here.
#
# Three limits are in the parsing rather than in the typing. A chunk is cut at
# every `fn` line, an indented one included, so a helper `fn` nested inside a
# test orphans each binding made above it. A `let`'s annotation is still read
# up to its first `;`, where an annotated parameter is read bracket-aware, so
# `let s: Arc<[Slot; 4]> = ..` loses its annotation and resolves through the
# right-hand side while `s: Arc<[Slot; 4]>` in a parameter list keeps it. And
# the brackets those scans count include `<`, so a `<` meant as a comparison
# rather than as a generic would unbalance one -- no signature or annotation
# in `src/` or the corpus writes one today.
#
# HOW IT FAILS SAFE. A call site it cannot attribute is credited to nothing.
# A name declared on more than one item is witnessed per declaration, so
# `hash_value` on three types needs three attributed call sites and is
# FLAGGED until it has them -- the whole point of being type-aware, and the
# reason a name the script cannot attribute reads as unwitnessed rather than
# as covered. The flagged list is therefore an upper bound on the gap and
# wants reading item by item; what it may not do is quietly call something
# covered.
#
# The one place it is deliberately looser is a name declared on exactly ONE
# item, where a bare-word match in the corpus is accepted as the witness: a
# hit can only have reached that one public function, give or take a private
# or foreign function of the same name, and keeping that reading is what makes
# this count comparable with the name-based one it replaces.

import re
import sys
from pathlib import Path

USAGE = "usage: devtools/api-witness.py [--all] [<repo root>]"

# A `pub fn` declaration, with the indentation that says whether it is a free
# function (column 0) or a method of the `impl` block above it.
PUB_FN = re.compile(
    r"^(\s*)pub\s+(?:const\s+|async\s+|unsafe\s+|extern\s+\"[^\"]*\"\s+)*fn\s+([a-z_][a-z0-9_]*)"
)
# Any `fn` declaration, for the return-type table a method chain is resolved
# through: a chain can pass through a crate-private step on its way to a
# public one.
ANY_FN = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:const\s+|async\s+|unsafe\s+|extern\s+\"[^\"]*\"\s+)*"
    r"fn\s+([a-z_][a-z0-9_]*)"
)
CFG_TEST = re.compile(r"^#\[cfg\(test\)\]")
DOC = re.compile(r"^\s*(?:///|//!)\s?(.*)$")
FENCE = re.compile(r"^\s*```")
IDENT = re.compile(r"[A-Za-z0-9_]")
TYPE_HEAD = re.compile(r"[A-Z][A-Za-z0-9_]*")

# Smart pointers a caller calls through, and the two tried-and-unwrapped
# enums: a chain step that does not resolve on the wrapper is retried on what
# it wraps.
DEREF = ("Arc", "Rc", "Box")
TRIED = ("Option", "Result")
# Chain steps that hand back the same type they were called on, so that a
# `.clone()` in the middle of a chain does not lose the receiver's type.
IDENTITY_STEPS = ("clone", "to_owned", "as_ref", "as_mut", "borrow", "borrow_mut")
# Locks, whose guard derefs to what it guards.
GUARDED = ("Mutex", "RwLock")
# Steps that hand back an iterator over the receiver's elements, and the
# adaptors whose closure is handed one element.
SOURCES = ("iter", "into_iter", "iter_mut", "drain")
ADAPTORS = (
    "map",
    "for_each",
    "filter",
    "filter_map",
    "find",
    "any",
    "all",
    "position",
    "inspect",
    "take_while",
    "skip_while",
    "retain",
)

# `Type::new(..)` and friends are taken to hand back a `Type` when no
# signature says otherwise, which is the convention this crate follows and the
# only reading available for a *derived* `Default`, where there is no `fn` to
# read a return type from.
CONSTRUCTORS = ("new", "default", "from")

FREE = ""  # the "type" a free function is declared on


def production_lines(path):
    """Lines of `path` above its column-0 `#[cfg(test)]`, if any."""
    out = []
    for line in path.read_text().splitlines():
        if CFG_TEST.match(line):
            break
        out.append(line)
    return out


def doc_examples(path):
    """Every ```-fenced block of the file's doc comments, one text each.

    One entry per block rather than one per file, so that an identifier two
    examples bind to two different types stays resolvable in each.
    """
    blocks, current, inside = [], [], False
    for line in path.read_text().splitlines():
        m = DOC.match(line)
        if not m:
            continue
        body = m.group(1)
        if FENCE.match(body):
            inside = not inside
            if not inside and current:
                blocks.append("\n".join(current))
                current = []
            continue
        if inside:
            current.append(body)
    return blocks


def strip_comments(text):
    """Blank out `//` comments, so a mention in prose is not read as a call."""
    return re.sub(r"//[^\n]*", "", text)


def bracket_delta(text, i):
    """How the character at `i` changes bracket depth.

    The `>` of a `->` or a `=>` closes nothing, which matters wherever a
    signature carries an inline `impl Fn(I) -> O` bound: counting that arrow
    as a closing angle bracket unbalanced every scan below it.
    """
    ch = text[i]
    if ch in "<([":
        return 1
    if ch in ">)]":
        if ch == ">" and i > 0 and text[i - 1] in "-=":
            return 0
        return -1
    return 0


def normalise(ty):
    """Drop references, parentheses and whitespace from a type expression."""
    ty = ty.strip()
    while True:
        stripped = re.sub(r"^(?:&(?:\s*'[a-z_]+)?\s*(?:mut\s+)?|\*(?:const|mut)\s+)", "", ty)
        stripped = stripped.strip()
        if stripped == ty:
            return ty
        ty = stripped


def head_of(ty):
    """The leading type name of a type expression, or None."""
    ty = normalise(ty)
    m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", ty)
    return m.group(1) if m else None


def type_arguments(ty):
    """The top-level generic arguments of a type expression."""
    ty = normalise(ty)
    start = ty.find("<")
    if start < 0 or not ty.endswith(">"):
        return []
    depth, arg, args = 0, "", []
    inner = ty[start + 1 : -1]
    for i, ch in enumerate(inner):
        depth += bracket_delta(inner, i)
        if ch == "," and depth == 0:
            args.append(arg)
            arg = ""
        else:
            arg += ch
    args.append(arg)
    return [a.strip() for a in args if a.strip() and not a.strip().startswith("'")]


def impl_self_type(header):
    """The type an `impl` header is for, or None when it names no plain type.

    `impl<R> Work<R>` is `Work<R>`, `impl Debug for Foo` is `Foo`.
    """
    rest = header[len("impl") :]
    if rest.startswith("<"):  # the impl's own generics
        depth = 0
        for i, ch in enumerate(rest):
            if ch == "<":
                depth += 1
            elif ch == ">":
                depth -= 1
                if depth == 0:
                    rest = rest[i + 1 :]
                    break
        else:
            return None
    depth = 0
    for m in re.finditer(r"<|>|\bfor\b", rest):
        if m.group(0) == "<":
            depth += 1
        elif m.group(0) == ">":
            depth -= 1
        elif depth == 0:  # `Trait for Type`
            rest = rest[m.end() :]
            break
    rest = rest.split(" where ")[0].strip()
    return rest if TYPE_HEAD.match(normalise(rest)) else None


def enclosing_header(lines, index):
    """The column-0 item header above `lines[index]`, joined to one line.

    Every `pub fn` in `src/` is written either at column 0 or at one level of
    indentation inside an `impl` or `trait` block, so the nearest column-0
    header that is not an attribute, a comment or a closing brace is the item
    the method belongs to.
    """
    j = index - 1
    while j >= 0:
        line = lines[j]
        if line and not line[0].isspace() and not line.startswith(("}", "#", "/", ")", ">")):
            break
        j -= 1
    if j < 0:
        return None
    # A rustfmt'ed header can run over several lines, with the `where` clause
    # and the opening brace below it; the continuation is indented.
    header = lines[j]
    k = j + 1
    while "{" not in header and k < len(lines) and lines[k][:1].isspace():
        header += " " + lines[k].strip()
        k += 1
    return header.split("{")[0].strip()


def header_owner(header):
    """The type or trait a header's methods hang off, or None."""
    header = re.sub(r"^(?:pub(?:\([^)]*\))?\s+|default\s+|unsafe\s+)+", "", header)
    if header.startswith("impl"):
        self_type = impl_self_type(header)
        return head_of(self_type) if self_type else None
    trait = re.match(r"trait\s+([A-Z][A-Za-z0-9_]*)", header)
    # A trait's own methods are keyed by the trait, and reached through the
    # list of traits each type implements.
    return trait.group(1) if trait else None


def return_type(lines, index):
    """The return type of the `fn` at `lines[index]`, or None for `-> ()`.

    The arrow looked for is the one outside every bracket: an `impl FnMut(I)
    -> O` bound written inline in the parameter list has an arrow of its own,
    and taking the first one read that bound as the return type.
    """
    sig = signature(lines, index)
    depth = 0
    arrow = -1
    for i, ch in enumerate(sig):
        if ch == "-" and depth == 0 and sig[i : i + 2] == "->":
            arrow = i
            break
        depth += bracket_delta(sig, i)
    if arrow < 0:
        return None
    ret = sig[arrow + 2 :].split(" where ")[0].strip()
    return ret or None


def signature_end(text):
    """Where a `fn` signature ends, or None where it has not ended yet.

    The `{` or `;` looked for is the one outside every bracket, because an
    array type in a parameter list carries a `;` of its own: `[IndexRange<T>;
    N]` read as the end of the signature, which both truncated the parameter
    list and hid the return type below it.
    """
    depth = 0
    for i, ch in enumerate(text):
        if depth == 0 and ch in "{;":
            return i
        depth += bracket_delta(text, i)
    return None


def signature(lines, index):
    """The `fn` at `lines[index]` up to its body, `where` clause included."""
    text = lines[index]
    k = index + 1
    end = signature_end(text)
    while end is None and k < len(lines):
        text += " " + lines[k].strip()
        end = signature_end(text)
        k += 1
    return text if end is None else text[:end]


def generic_parameters(sig):
    """The type parameters a `fn` declares for itself."""
    opening = re.match(r"\s*(?:pub(?:\([^)]*\))?\s+)?(?:[a-z]+\s+)*fn\s+[a-z_][a-z0-9_]*<", sig)
    if not opening:
        return set()
    rest = sig[opening.end() :]
    depth, text = 1, ""
    for ch in rest:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
            if depth == 0:
                break
        text += ch
    names = set()
    for piece in split_top_level(text):
        name = re.match(r"(?:const\s+)?([A-Z][A-Za-z0-9_]*)", piece)
        if name:
            names.add(name.group(1))
    return names


def closure_parameters(sig):
    """The parameters of the one `Fn` bound in a signature, or None.

    A signature with two of them -- `reduce`'s initialiser and reducer -- is
    left alone rather than guessed at positionally.
    """
    bounds = re.findall(r"\bFn(?:Mut|Once)?\(([^()]*)\)", sig)
    if len(bounds) != 1:
        return None
    return split_top_level(bounds[0])


def split_top_level(text, separator=","):
    """Split on a separator that is not inside brackets."""
    depth, piece, out = 0, "", []
    for i, ch in enumerate(text):
        depth += bracket_delta(text, i)
        if ch == separator and depth == 0:
            out.append(piece)
            piece = ""
        else:
            piece += ch
    out.append(piece)
    return [piece.strip() for piece in out if piece.strip()]


def annotated_type(text, start):
    """The type annotation beginning at `text[start]`, read bracket-aware.

    It ends at the first `,`, `)`, `|`, `=`, `;`, `{` or `}` outside every
    bracket -- a type expression holds none of those, braces included -- or at
    the bracket that closes the one the annotation sits in. The depth is what
    admits a generic carrying more than one argument, or any argument with a
    separator inside it: cutting `IndexRanges<i32, 2>` at its comma, or
    `OnceWork<dyn FnOnce() + Send>` at its parenthesis, gave the same
    identifier two type strings between this rule and the `let` rule above
    it, and an identifier at two types is dropped rather than guessed at.
    """
    depth = 0
    for i in range(start, len(text)):
        delta = bracket_delta(text, i)
        if depth == 0 and (text[i] in ",)|=;{}" or delta < 0):
            return text[start:i]
        depth += delta
    return text[start:]


class Types:
    """The tables a receiver expression is resolved through.

    `returns` maps (owner, method) to the method's declared return type, where
    an owner is a type, a trait, or `FREE` for a free function. `traits_of`
    maps a type to the traits it implements, which is how a call to one of the
    crate's extension-trait methods (`FlowBuilder::task_ref`, the algorithm
    traits) resolves on the type a test actually holds. `closures` maps
    (owner, method) to the parameters of the method's `Fn` bound, which is
    where a closure parameter's type comes from (`|subflow|` passed to
    `emplace_subflow`). `fields` maps (type, field) to the field's type.
    """

    def __init__(self):
        self.returns = {}
        self.traits_of = {}
        self.closures = {}
        self.fields = {}
        self.variants = {}
        self.generics = {}

    def absorb(self, lines):
        """Fill the tables from one file, yielding its `pub fn` declarations.

        Yields `(name, owner, line number)`, where an owner is a type head or
        `FREE` for a free function.
        """
        struct, enum, variant = None, None, None
        for i, line in enumerate(lines):
            if enum is not None:
                tuple_variant = re.match(r"\s{4}([A-Z][A-Za-z0-9_]*)\((.+)\),\s*$", line)
                named_variant = re.match(r"\s{4}([A-Z][A-Za-z0-9_]*)\s*\{\s*$", line)
                if tuple_variant:
                    self.variants.setdefault(
                        (enum, tuple_variant.group(1)), split_top_level(tuple_variant.group(2))
                    )
                elif named_variant:
                    variant = f"{enum}::{named_variant.group(1)}"
                elif variant is not None:
                    field = re.match(r"\s+([a-z_][a-z0-9_]*)\s*:\s*(.+?),?\s*$", line)
                    if field:
                        self.fields.setdefault((variant, field.group(1)), field.group(2))
                    elif line.startswith("    }"):
                        variant = None
                if line.startswith("}"):
                    enum = None
            opening = re.match(
                r"(?:pub(?:\([^)]*\))?\s+)?enum\s+([A-Z][A-Za-z0-9_]*)[^;]*\{\s*$", line
            )
            if opening:
                enum = opening.group(1)
            if struct is not None:
                field = re.match(
                    r"\s+(?:pub(?:\([^)]*\))?\s+)?([a-z_][a-z0-9_]*)\s*:\s*(.+?),?\s*$", line
                )
                if field:
                    self.fields.setdefault((struct, field.group(1)), field.group(2))
                if line.startswith("}"):
                    struct = None
            declaration = re.match(
                r"(?:pub(?:\([^)]*\))?\s+)?struct\s+([A-Z][A-Za-z0-9_]*)[^;]*\{\s*$", line
            )
            if declaration:
                struct = declaration.group(1)
            if line.startswith("impl"):
                header = enclosing_header(lines, i) or ""
                trait = re.match(r"impl(?:<[^{]*?>)?\s+([A-Za-z_][A-Za-z0-9_]*)", header)
                owner = header_owner(header)
                if owner and trait and " for " in header:
                    self.traits_of.setdefault(owner, set()).add(trait.group(1))
            any_fn = ANY_FN.match(line)
            if not any_fn:
                continue
            owner = FREE
            if line[:1].isspace():
                header = enclosing_header(lines, i)
                owner = header_owner(header) if header else None
                if owner is None:
                    continue  # a method of something this script cannot name
            name = any_fn.group(1)
            ret = return_type(lines, i)
            if ret is not None:
                if owner != FREE:
                    ret = re.sub(r"\bSelf\b", owner, ret)
                self.returns.setdefault((owner, name), ret)
            own_generics = generic_parameters(signature(lines, i))
            if own_generics:
                self.generics.setdefault((owner, name), own_generics)
            parameters = closure_parameters(signature(lines, i))
            if parameters is not None:
                self.closures.setdefault((owner, name), parameters)
            pub_fn = PUB_FN.match(line)
            if pub_fn:
                yield pub_fn.group(2), owner, i + 1

    def declared(self, owner, method):
        """(return type, own generics) of `owner::method`, through its traits."""
        for key in [(owner, method)] + [
            (trait, method) for trait in sorted(self.traits_of.get(owner, ()))
        ]:
            ret = self.returns.get(key)
            if ret is not None:
                return ret, self.generics.get(key, set())
        return None, set()

    def declared_return(self, owner, method):
        """The return type of `owner::method`, looking through its traits."""
        return self.declared(owner, method)[0]

    def field_type(self, ty, field):
        """The type of a field of `ty`, through a smart pointer if need be."""
        for head in deref_chain(ty):
            found = self.fields.get((head, field))
            if found is not None:
                return found
        return None

    def step(self, ty, method, arguments=None, bindings=None):
        """The type `ty.method(..)` hands back, or None.

        `arguments` is the call's own argument text, which is what a method
        whose return type is one of its type parameters is resolved through:
        `executor.make_observer(ChromeObserver::new())` is an
        `Arc<ChromeObserver>` only because of what it was handed.
        """
        if ty is None:
            return None
        if method in IDENTITY_STEPS:
            return ty
        head = head_of(ty)
        if head is None:
            return None
        if head in TRIED and method in ("unwrap", "expect"):
            return try_unwrap(ty)
        if method in SOURCES and element_of(ty) is not None:
            return f"Iterator<Item = {element_of(ty)}>"
        if head in GUARDED and method in ("lock", "read", "write"):
            # A guard derefs to what it guards, which is all this needs of it.
            args = type_arguments(ty)
            return f"Result<{args[0]}>" if args else None
        ret, own_generics = self.declared(head, method)
        if ret is None and head in DEREF:
            args = type_arguments(ty)
            return self.step(args[0], method, arguments, bindings) if args else None
        if ret is None:
            return None
        parameter = head_of(unwrap_deref(ret))
        if parameter in own_generics:
            if not arguments:
                return None
            handed = self.resolve(arguments[0], bindings or {})
            if handed is None:
                return None
            return re.sub(r"\b" + parameter + r"\b", head_of(handed), ret)
        return head if head_of(ret) == "Self" else ret

    def primary(self, expr, bindings):
        """The type of a primary expression, or None when it cannot be seen."""
        expr = normalise(expr).lstrip("(").strip()
        tried = expr.endswith("?")
        expr = expr.rstrip("?").strip()
        ty = self.primary_untried(expr, bindings)
        return try_unwrap(ty) if tried and ty else ty

    def primary_untried(self, expr, bindings):
        wrapper = re.match(r"^(?:Arc|Rc|Box)::(?:new|clone|from)\s*\((.*)\)\s*$", expr, re.S)
        if wrapper:  # a caller calls straight through these
            return unwrap_deref(self.resolve(wrapper.group(1), bindings))
        qualified = re.match(
            r"^(?:<\s*)?([A-Z][A-Za-z0-9_]*)(?:<[^<>]*>)?(?:\s+as\s+[^>]*>)?"
            r"(?:::<[^<>]*>)?::([a-z_][a-z0-9_]*)\s*\(",
            expr,
        )
        if qualified:
            owner, method = qualified.group(1), qualified.group(2)
            ret = self.declared_return(owner, method)
            if ret is None:
                return owner if method in CONSTRUCTORS else None
            return owner if head_of(ret) == "Self" else ret
        variant = re.match(r"^([A-Z][A-Za-z0-9_]*)::([A-Z][a-z0-9_][A-Za-z0-9_]*)\s*$", expr)
        if variant:  # a unit variant of an enum, `PipeType::Serial`
            return variant.group(1)
        literal = re.match(r"^([A-Z][A-Za-z0-9_]*)(?:<[^<>]*>)?\s*[({]", expr)
        if literal:  # a struct literal or a tuple-struct constructor
            return literal.group(1)
        free = re.match(
            r"^(?:[a-z_][a-z0-9_]*::)*([a-z_][a-z0-9_]*)(?:::<[^<>]*>)?\s*\(", expr
        )
        if free:
            return self.declared_return(FREE, free.group(1))
        ident = re.match(r"^(self|[a-z_][a-z0-9_]*)\s*$", expr)
        if ident:
            return bindings.get(ident.group(1))
        return None

    def resolve(self, expr, bindings):
        """The type an expression has, following its postfix chain."""
        primary, steps = split_postfix(expr)
        ty = self.primary(primary, bindings)
        for name, is_call, indexed, tried, arguments in steps:
            if name is None:
                return None
            if is_call:
                ty = self.step(ty, name, arguments, bindings)
            else:
                ty = self.field_type(ty, name)
            if tried:
                ty = try_unwrap(ty)
            if indexed:
                ty = element_of(ty)
            if ty is None:
                return None
        return ty

    def bindings(self, chunk, self_type=None):
        """Identifier -> type for one chunk of corpus text.

        An identifier the chunk gives two different types is dropped:
        resolving it to either would credit a declaration on a guess.
        """
        found = {}

        def record(name, ty):
            if ty is None or name.startswith("_"):
                return
            head = head_of(ty)
            if head is None or not head[0].isupper():
                return
            found.setdefault(name, set()).add(unwrap_deref(normalise(ty)))

        record("self", self_type)
        for m in re.finditer(
            r"\blet\s+(?:mut\s+)?([a-z_][a-z0-9_]*)\s*(?::\s*([^=;]+?))?\s*=\s*", chunk
        ):
            name, annotation = m.group(1), m.group(2)
            if annotation:
                record(name, annotation)
                continue
            value = tail_expression(balanced_expression(chunk, m.end()))
            record(name, self.resolve(value.strip(), found_so_far(found)))
        # What a tuple pattern binds, from a tuple return type.
        for m in re.finditer(r"\blet\s+\(([^()]*)\)\s*=\s*", chunk):
            names = split_top_level(m.group(1))
            ty = self.resolve(
                tail_expression(balanced_expression(chunk, m.end())), found_so_far(found)
            )
            if not (ty and ty.startswith("(") and ty.endswith(")")):
                continue
            parts = split_top_level(ty[1:-1])
            if len(names) != len(parts):
                continue
            for one, part in zip(names, parts):
                if re.fullmatch(r"(?:mut\s+)?[a-z_][a-z0-9_]*", one):
                    record(one.split()[-1], part)
        # What a `for` loop binds, from what it iterates over.
        for m in re.finditer(r"\bfor\s+([^\n]+?)\s+in\s+", chunk):
            pattern = m.group(1).strip()
            element = element_of(self.resolve(
                tail_expression(balanced_expression(chunk, m.end()).split("{")[0]),
                found_so_far(found),
            ))
            if element is None:
                continue
            if pattern.startswith("(") and pattern.endswith(")"):
                names = split_top_level(pattern[1:-1])
                parts = (
                    split_top_level(element[1:-1])
                    if element.startswith("(") and element.endswith(")")
                    else []
                )
                if len(names) == len(parts):
                    for one, part in zip(names, parts):
                        if re.fullmatch(r"(?:mut\s+)?[a-z_][a-z0-9_]*", one):
                            record(one.split()[-1], part)
            elif re.fullmatch(r"(?:mut\s+)?[a-z_][a-z0-9_]*", pattern):
                record(pattern.split()[-1], element)
        # Annotated parameters, of a function or of a closure. The type is
        # read from the end of the match rather than captured by it, so that
        # its own brackets bound it.
        for m in re.finditer(
            r"([a-z_][a-z0-9_]*)\s*:\s*(?=(?:&(?:\s*mut)?\s*)?[A-Z])", chunk
        ):
            record(m.group(1), annotated_type(chunk, m.end()))
        # Identifiers an enum pattern binds, typed by the variant's own fields:
        # `if let Handle::Static(work) = node.handle()`.
        for m in re.finditer(
            r"([A-Z][A-Za-z0-9_]*)::([A-Z][A-Za-z0-9_]*)\s*\(([^()]*)\)\s*(?:=>|=[^=])", chunk
        ):
            names = split_top_level(m.group(3))
            fields = self.variants.get((m.group(1), m.group(2)), [])
            if len(names) == len(fields):
                for name, ty in zip(names, fields):
                    if re.fullmatch(r"(?:mut\s+|ref\s+)*[a-z_][a-z0-9_]*", name):
                        record(name.split()[-1], ty)
        for m in re.finditer(
            r"([A-Z][A-Za-z0-9_]*)::([A-Z][A-Za-z0-9_]*)\s*\{([^{}]*)\}\s*(?:=>|=[^=])", chunk
        ):
            for piece in split_top_level(m.group(3)):
                name = piece.split(":")[0].strip()
                if re.fullmatch(r"[a-z_][a-z0-9_]*", name):
                    record(name, self.fields.get((f"{m.group(1)}::{m.group(2)}", name)))
        # Unannotated closure parameters, typed by the `Fn` bound of the method
        # the closure is passed to.
        for m in re.finditer(r"(?:move\s+)?\|([^|()]*)\|", chunk):
            method, opening = enclosing_call(chunk, m.start())
            if method is None:
                continue
            names = [one.strip() for one in split_top_level(m.group(1))]
            if method in ADAPTORS and len(names) == 1:
                # An iterator adaptor hands its closure one element.
                receiver = (
                    receiver_of(chunk, opening - len(method) - 1)
                    if opening - len(method) - 1 > 0
                    and chunk[opening - len(method) - 1] == "."
                    else None
                )
                element = element_of(self.resolve(receiver, found_so_far(found)))
                if element is not None and re.fullmatch(
                    r"(?:mut\s+)?[a-z_][a-z0-9_]*", names[0]
                ):
                    record(names[0].split()[-1], element)
                    continue
            candidates = {
                tuple(parameters)
                for (_, called), parameters in self.closures.items()
                if called == method
            }
            if len(candidates) != 1:
                continue
            parameters = next(iter(candidates))
            if len(names) != len(parameters):
                continue
            for name, parameter in zip(names, parameters):
                if re.fullmatch(r"(?:mut\s+)?[a-z_][a-z0-9_]*", name):
                    record(name.replace("mut ", "").strip(), parameter)
        return {name: next(iter(tys)) for name, tys in found.items() if len(tys) == 1}


def metavariable_types(text, scoped, types):
    """{metavariable: {type head, ...}} for a file's own `macro_rules`.

    A macro body is expanded once per invocation, so a call on `$receiver` is
    attributed to the type of the matching argument at *every* invocation --
    `tests/dependent_asyncs.rs` drives one body with an `Executor`, a
    `Runtime` and a `TaskGroup`, and all three calls really happen.
    """
    found = {}
    for m in re.finditer(r"macro_rules!\s+([a-z_][a-z0-9_]*)", text):
        matcher = re.search(r"\(([^)]*)\)\s*=>", text[m.end() :])
        if not matcher:
            continue
        metavariables = re.findall(r"\$([a-z_][a-z0-9_]*)\s*:\s*[a-z]+", matcher.group(1))
        if not metavariables:
            continue
        for chunk, bindings in scoped:
            for call in re.finditer(r"\b" + m.group(1) + r"!\s*\(", chunk):
                arguments = call_arguments(chunk[call.end() - 1 :])
                for name, argument in zip(metavariables, arguments):
                    ty = types.resolve(argument, bindings)
                    if ty is not None:
                        found.setdefault("$" + name, set()).add(head_of(ty))
    return found


def found_so_far(found):
    """The unambiguous part of a half-built binding table."""
    return {name: next(iter(tys)) for name, tys in found.items() if len(tys) == 1}


def unwrap_deref(ty):
    """A type with its `Arc`/`Rc`/`Box` layers taken off.

    A caller writes through those, so dropping them both resolves a chain and
    keeps `Executor` and `Arc<Executor>` from reading as two bindings of one
    identifier.
    """
    while ty and head_of(ty) in DEREF:
        args = type_arguments(ty)
        if not args:
            break
        ty = args[0]
    return ty


def tail_expression(expr):
    """The value of a block expression: the last statement inside it."""
    expr = expr.strip()
    if not (expr.startswith("{") and expr.endswith("}")):
        return expr
    inside = expr[1:-1]
    pieces = split_top_level(inside, ";")
    return tail_expression(pieces[-1].strip()) if pieces else expr


def balanced_expression(text, start):
    """The text from `start` to the top-level `;` or `,` that ends it."""
    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            if depth == 0:
                return text[start:i]
            depth -= 1
        elif ch == ";" and depth == 0:
            return text[start:i]
    return text[start:]


def try_unwrap(ty):
    """What an `Option` or a `Result` holds, or the type itself."""
    if ty and head_of(ty) in TRIED:
        args = type_arguments(ty)
        return args[0] if args else None
    return ty


def element_of(ty):
    """The element type of a sequence or an iterator, or None."""
    if ty is None:
        return None
    ty = normalise(ty).removeprefix("impl ").strip()
    item = re.search(r"\bIterator\s*<\s*Item\s*=\s*(.+?)\s*>\s*(?:\+.*)?$", ty)
    if item:
        return item.group(1).strip()
    slice_or_array = re.match(r"^\[(.+?)(?:;.*)?\]$", ty)
    if slice_or_array:
        return slice_or_array.group(1).strip()
    if head_of(ty) in ("Vec", "VecDeque"):
        args = type_arguments(ty)
        return args[0] if args else None
    return None


def argument_block(text, opening):
    """Whether the `{` at `opening` opens a block expression in an argument.

    Read from what precedes it: a `,` or a `(` is a block handed to a call,
    where an identifier before it is a struct literal, a `)` a function body
    and a `|` a closure body.
    """
    j = opening
    while j > 0 and text[j - 1] in " \t\r\n":
        j -= 1
    return j > 0 and text[j - 1] in ",("


def enclosing_call(text, position):
    """(name, index of its `(`) of the call whose arguments enclose `position`.

    A block expression written as an argument is walked straight through, so
    that a closure built inside one still belongs to the call it is handed to.
    That is the case that defeated the earlier reading: `tests/task_group.rs`
    writes every one of its cases as `in_a_group(&executor, route, { let ran =
    Arc::clone(&ran); move |group| .. })`, the block being where the `Arc`s a
    `'static` closure needs are cloned, and stopping at that `{` left `group`
    with no type -- so `TaskGroup::cancel`, whose only two call sites are in
    that file, read as unwitnessed while both of them assert its behaviour.
    """
    depth, i = 0, position
    while i > 0:
        ch = text[i - 1]
        if ch in ")]}":
            depth += 1
        elif ch in "([{":
            if depth == 0:
                if ch == "{" and argument_block(text, i - 1):
                    i -= 1  # a block in argument position, not the call
                    continue
                break
            depth -= 1
        i -= 1
    if i == 0 or text[i - 1] != "(":
        return None, 0
    j = i - 1
    while j > 0 and IDENT.match(text[j - 1]):
        j -= 1
    name = text[j : i - 1]
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
        return None, 0
    return name, i - 1


def call_arguments(piece):
    """The top-level arguments of a `name(..)` call, or an empty list."""
    opening = piece.find("(")
    if opening < 0:
        return []
    depth = 0
    for i in range(opening, len(piece)):
        if piece[i] in "([":
            depth += 1
        elif piece[i] in ")]":
            depth -= 1
            if depth == 0:
                return split_top_level(piece[opening + 1 : i])
    return []


def split_postfix(expr):
    """An expression as (primary, [(name, is_call, indexed, tried, args), ...])."""
    depth, cuts = 0, []
    for i, ch in enumerate(expr):
        if ch == "." and depth == 0 and expr[i : i + 2] != ".." and not expr[:i].endswith("."):
            cuts.append(i)
        depth += bracket_delta(expr, i)
    if not cuts:
        return expr, []
    primary = expr[: cuts[0]]
    steps = []
    for n, i in enumerate(cuts):
        end = cuts[n + 1] if n + 1 < len(cuts) else len(expr)
        piece = expr[i + 1 : end].strip()
        name = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", piece)
        if not name:
            return primary, [(None, False, False, False, [])]
        steps.append(
            (
                name.group(1),
                "(" in piece,
                piece.endswith("]"),
                "?" in piece,
                call_arguments(piece),
            )
        )
    return primary, steps


def receiver_of(text, dot):
    """The receiver expression ending at the `.` at index `dot`, or None.

    Walked backwards over the postfix tokens a receiver can be made of -- a
    call or index group, a `?`, an identifier, a `::` path, a `.` step --
    which crosses the line breaks a formatted method chain puts between them.
    """
    i = dot
    while True:
        while i > 0 and text[i - 1] in " \t\r\n":
            i -= 1
        if i == 0:
            return None
        ch = text[i - 1]
        if ch in ")]":
            opening = "(" if ch == ")" else "["
            depth = 0
            while i > 0:
                i -= 1
                if text[i] == ch:
                    depth += 1
                elif text[i] == opening:
                    depth -= 1
                    if depth == 0:
                        break
            else:
                return None
            continue
        if ch == "?":
            i -= 1
            continue
        if IDENT.match(ch):
            while i > 0 and IDENT.match(text[i - 1]):
                i -= 1
            if i > 0 and text[i - 1] == "$":  # a macro metavariable
                i -= 1
                break
            j = i
            while j > 0 and text[j - 1] in " \t\r\n":
                j -= 1
            if j > 1 and text[j - 1] == "." and text[j - 2] != ".":
                i = j - 1
                continue
            if j > 1 and text[j - 2 : j] == "::":
                i = j - 2
                continue
            break
        break
    expr = text[i:dot].strip()
    return expr or None


def deref_chain(ty):
    """A type and what it derefs to, outermost first."""
    chain = []
    while ty:
        head = head_of(ty)
        if head is None:
            break
        chain.append(head)
        if head not in DEREF:
            break
        args = type_arguments(ty)
        ty = args[0] if args else None
    return chain


def item_chunks(text):
    """One (self type, text) per column-0 `fn` item, the preamble first.

    Bindings are read per chunk rather than per file, because a test file
    binds `executor` or `work` in twenty functions and the types need not
    agree; a chunk is the nearest thing to a scope a line-based reading has.
    The self type is the `impl` block the chunk sits in, where there is one,
    so that `self.field` resolves inside a test's own observer.
    """
    starts = [
        m.start()
        for m in re.finditer(
            r"^[ \t]*(?:pub(?:\([^)]*\))?\s+)?"
            r"(?:const\s+|async\s+|unsafe\s+|extern\s+\"[^\"]*\"\s+)*fn\s",
            text,
            re.MULTILINE,
        )
    ]
    bounds = [0, *starts, len(text)]
    out = []
    for first, last in zip(bounds, bounds[1:]):
        chunk = text[first:last]
        if not chunk.strip():
            continue
        impls = re.findall(r"^impl[^\n]*", text[:last], re.MULTILINE)
        self_type = header_owner(impls[-1]) if impls else None
        out.append((self_type, chunk))
    return out


def witnesses(corpus, declared, types):
    """{(owner, name)} the corpus attributes, and the names it merely mentions."""
    attributed = set()
    bare = set()
    names = set(declared)
    for _, text in corpus:
        text = strip_comments(text)
        for name in names:
            if re.search(r"\b" + re.escape(name) + r"\b", text):
                bare.add(name)
        scoped = [(chunk, types.bindings(chunk, self_type)) for self_type, chunk in item_chunks(text)]
        metavariables = metavariable_types(text, scoped, types)
        for chunk, bindings in scoped:
            for name in names:
                quoted = re.escape(name)
                if name not in chunk:
                    continue
                # `Type::name(..)`, including `<Type as Trait>::name(..)`.
                for m in re.finditer(
                    r"(?:<\s*)?([A-Z][A-Za-z0-9_]*)(?:<[^<>]*>)?(?:\s+as\s+[^>]*)?>?"
                    r"(?:::<[^<>]*>)?::" + quoted + r"\s*\(",
                    chunk,
                ):
                    attributed.add((m.group(1), name))
                # `name(..)` or `path::name(..)`: a free function.
                if re.search(r"(?<![.\w])(?:[a-z_][a-z0-9_]*::)*" + quoted + r"\s*\(", chunk):
                    attributed.add((FREE, name))
                # `recv.name(..)`, attributed to whatever `recv` resolves to.
                for m in re.finditer(r"\.\s*" + quoted + r"\s*\(", chunk):
                    expr = receiver_of(chunk, m.start())
                    if expr is None:
                        continue
                    owners = declared.get(name, {})
                    if expr.startswith("$"):
                        for head in metavariables.get(expr, ()):
                            attributed.add((head, name))
                        continue
                    chain = deref_chain(types.resolve(expr, bindings))
                    if not chain:
                        continue
                    match = next((head for head in chain if head in owners), chain[0])
                    attributed.add((match, name))
    return attributed, bare


def header():
    """This script's own header comment, for `--help`."""
    lines = []
    for line in Path(__file__).read_text().splitlines()[1:]:
        if not line.startswith("#"):
            break
        lines.append(line.removeprefix("#").removeprefix(" "))
    return "\n".join(lines)


def main(argv):
    show_all = False
    root = None
    for arg in argv:
        if arg == "--all":
            show_all = True
        elif arg in ("-h", "--help"):
            print(USAGE)
            print()
            print(header())
            return 0
        elif arg.startswith("-"):
            print(f"api-witness: unknown argument '{arg}'\n{USAGE}", file=sys.stderr)
            return 2
        elif root is None:
            root = arg
        else:
            print(f"api-witness: too many arguments\n{USAGE}", file=sys.stderr)
            return 2
    # The repo root comes from this script's own location unless one is given,
    # so a run from a subdirectory measures the same tree.
    root = Path(root).resolve() if root else Path(__file__).resolve().parent.parent
    if not (root / "src").is_dir():
        print(f"api-witness: no src/ under {root}", file=sys.stderr)
        return 2

    src_files = sorted((root / "src").rglob("*.rs"))
    corpus_files = [
        path
        for directory in ("tests", "examples", "benches")
        for path in sorted((root / directory).rglob("*.rs"))
    ]

    types = Types()
    declared = {}
    for path in src_files:
        for name, owner, line in types.absorb(production_lines(path)):
            site = f"{path.relative_to(root)}:{line}"
            declared.setdefault(name, {}).setdefault(owner, []).append(site)
    # The corpus declares types and helper functions of its own, and a
    # receiver is often the return of one (`emplace(&mut taskflow, || {})`),
    # so their signatures go into the same tables. They add nothing to the
    # denominator: only `src/` is measured.
    for path in corpus_files:
        for _ in types.absorb(path.read_text().splitlines()):
            pass

    corpus = [(str(path.relative_to(root)), path.read_text()) for path in corpus_files]
    for path in src_files:
        for n, block in enumerate(doc_examples(path), 1):
            corpus.append((f"{path.relative_to(root)} (doc example {n})", block))

    attributed, bare = witnesses(corpus, declared, types)

    rows = []
    for name in sorted(declared):
        owners = declared[name]
        shared = len(owners) > 1
        for owner in sorted(owners):
            witnessed = (owner, name) in attributed or (not shared and name in bare)
            rows.append((name, owner, owners[owner], shared, witnessed))

    total = len(rows)
    shared_rows = [r for r in rows if r[3]]
    flagged = [r for r in rows if not r[4]]
    print(f"{total} public functions declared above the test modules, {len(declared)} names")
    print(
        f"{len(declared) - len({r[0] for r in shared_rows})} names on one item"
        f" (a bare-name match witnesses one), "
        f"{len({r[0] for r in shared_rows})} on several"
        f" ({len(shared_rows)} declarations, each needing its own attributed witness)"
    )
    print(f"{len(flagged)} unwitnessed under the type-aware reading")
    if not flagged and not show_all:
        return 0
    print()
    for name, owner, sites, shared, witnessed in rows:
        if witnessed and not show_all:
            continue
        item = f"{owner}::{name}" if owner else name
        mark = "    " if witnessed else "FLAG"
        note = " [name shared]" if shared else ""
        print(f"{mark} {item:52s} {', '.join(sites)}{note}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
