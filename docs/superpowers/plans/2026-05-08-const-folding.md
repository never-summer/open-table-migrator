# Intraprocedural Constant Folding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve `name → string_literal` constants at the file level so that detected I/O calls using `pd.read_parquet(MODULE_PATH)` produce `path_arg = "<resolved>"` instead of `None`. Three languages (Python/Java/Scala), two scope levels (module/class and function/method), `+` concatenation up to one level of indirection, true-const policy (any reassignment marks unresolvable).

**Architecture:** New `skills/open_table_migrator/scope.py` with `build_const_table(source, language, file)` returning `ConstTable`. Two-pass AST walk per file: pass 1 collects pure literal assignments, pass 2 resolves `+` concats whose operands are already-known literals. `ts_detector` builds the table once per file before the detection walk, and the call-site path-arg extractors look up identifiers via `const_table.resolve(name, scope_hint)`. `PatternMatch` gains an `attrs: dict[str, str]` field carrying `resolved_from` / `skipped_reason` for audit.

**Tech Stack:** Python 3.11+, `tree-sitter` (already used), `tree-sitter-python`, `tree-sitter-java`, `tree-sitter-scala`. No new deps.

**Spec:** [docs/superpowers/specs/2026-05-08-const-folding-design.md](../specs/2026-05-08-const-folding-design.md)

---

## File Structure

New file:
- `skills/open_table_migrator/scope.py` — `ConstBinding`, `ConstTable`, `build_const_table` dispatcher + per-language collectors

Modified:
- `skills/open_table_migrator/detector.py` — `PatternMatch` gains `attrs: dict[str, str] = field(default_factory=dict)`
- `skills/open_table_migrator/ts_detector.py` — build const-table per file; `_first_string_arg` and `_nth_positional_arg_string` gain optional `const_table` and `scope_hint` parameters, perform identifier lookup when the arg is not a string literal
- `skills/open_table_migrator/SKILL.md` — new "Constant folding" section

New tests:
- `tests/test_scope.py` — unit tests for `build_const_table`
- `tests/test_detector.py` — extended with const-folding end-to-end cases
- `tests/test_multi_table.py` — extended with mapping match through resolved path
- `tests/test_transformer_pandas.py` — extended with rewrite through resolved path

---

## Task 1: scope.py skeleton + dispatcher

**Files:**
- Create: `skills/open_table_migrator/scope.py`
- Create: `tests/test_scope.py`

- [ ] **Step 1: Failing tests for skeleton API**

`tests/test_scope.py`:
```python
from skills.open_table_migrator.scope import (
    ConstBinding, ConstTable, build_const_table,
)


def test_empty_python_returns_empty_table():
    table = build_const_table(b"", "python", "x.py")
    assert table.bindings == {}


def test_empty_java_returns_empty_table():
    table = build_const_table(b"", "java", "X.java")
    assert table.bindings == {}


def test_empty_scala_returns_empty_table():
    table = build_const_table(b"", "scala", "X.scala")
    assert table.bindings == {}


def test_resolve_missing_returns_none():
    table = ConstTable()
    assert table.resolve("FOO") is None


def test_resolve_module_scope_only():
    binding = ConstBinding(
        name="X", value="literal",
        file="x.py", line=1, scope="module", reason=None,
    )
    table = ConstTable(bindings={("module", "X"): binding})
    assert table.resolve("X") == binding
    assert table.resolve("X", scope_hint="some_func") == binding


def test_resolve_local_shadows_module():
    module = ConstBinding(
        name="X", value="module-val",
        file="x.py", line=1, scope="module", reason=None,
    )
    local = ConstBinding(
        name="X", value="local-val",
        file="x.py", line=5, scope="f", reason=None,
    )
    table = ConstTable(bindings={
        ("module", "X"): module,
        ("f", "X"): local,
    })
    assert table.resolve("X", scope_hint="f") == local
    assert table.resolve("X") == module
```

- [ ] **Step 2: Run, see fail**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement scope.py skeleton**

`skills/open_table_migrator/scope.py`:
```python
"""Intraprocedural constant folding for path-argument resolution.

Builds a per-file `ConstTable` keyed by (scope_key, name) so that the
detector can resolve `pd.read_parquet(MODULE_PATH)` to a literal value
when `MODULE_PATH = "s3://..."` is in scope.

Two-pass algorithm per language:
  Pass 1: collect assignments whose RHS is a single string literal.
  Pass 2: collect assignments whose RHS is `<literal_or_known> + <literal_or_known>`.

True-const policy: any reassignment in the same (scope, name) marks the
binding as unresolvable. Concat with 3+ operands or unsupported operators
is skipped with a reason.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .ts_parser import parse


@dataclass(frozen=True)
class ConstBinding:
    name: str
    value: str | None        # resolved literal, or None if unresolvable
    file: str
    line: int
    scope: str               # "module" or fully-qualified function/method name
    reason: str | None       # populated when value is None


@dataclass
class ConstTable:
    bindings: dict[tuple[str, str], ConstBinding] = field(default_factory=dict)

    def resolve(self, name: str, scope_hint: str | None = None) -> ConstBinding | None:
        if scope_hint is not None:
            binding = self.bindings.get((scope_hint, name))
            if binding is not None:
                return binding
        return self.bindings.get(("module", name))


def build_const_table(source: bytes, language: str, file: str) -> ConstTable:
    if not source:
        return ConstTable()
    tree = parse(source, language)
    collector = _COLLECTORS.get(language)
    if collector is None:
        return ConstTable()
    return collector(tree.root_node, source, file)


# Per-language collectors are populated in Tasks 2, 3, 4
_COLLECTORS: dict[str, callable] = {}
```

- [ ] **Step 4: Run, see pass**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/open_table_migrator/scope.py tests/test_scope.py
git commit -m "feat(scope): ConstBinding/ConstTable skeleton + dispatcher"
```

---

## Task 2: Python const collector

**Files:**
- Modify: `skills/open_table_migrator/scope.py`
- Modify: `tests/test_scope.py`

- [ ] **Step 1: Append failing tests for Python collector**

Append to `tests/test_scope.py`:
```python
from textwrap import dedent


def test_python_module_literal():
    src = dedent('''
        PATH = "s3://bucket/x"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("PATH")
    assert binding is not None
    assert binding.value == "s3://bucket/x"
    assert binding.scope == "module"


def test_python_module_literal_with_annotation():
    src = dedent('''
        PATH: str = "s3://bucket/x"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    assert table.resolve("PATH").value == "s3://bucket/x"


def test_python_module_concat_two_literals():
    src = dedent('''
        PATH = "s3://" + "bucket/x"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    assert table.resolve("PATH").value == "s3://bucket/x"


def test_python_module_concat_literal_plus_known_const():
    src = dedent('''
        BASE = "s3://bucket"
        PATH = BASE + "/events"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    assert table.resolve("PATH").value == "s3://bucket/events"


def test_python_module_concat_dependency_unresolved():
    src = dedent('''
        PATH = UNKNOWN + "/events"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("PATH")
    assert binding.value is None
    assert binding.reason == "dependency_unresolved"


def test_python_function_local_literal():
    src = dedent('''
        def job():
            p = "s3://bucket/x"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("p", scope_hint="job")
    assert binding is not None
    assert binding.value == "s3://bucket/x"
    assert binding.scope == "job"


def test_python_function_shadows_module():
    src = dedent('''
        PATH = "s3://module"
        def job():
            PATH = "s3://local"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    assert table.resolve("PATH").value == "s3://module"
    assert table.resolve("PATH", scope_hint="job").value == "s3://local"


def test_python_reassignment_marks_unresolvable():
    src = dedent('''
        PATH = "s3://a"
        PATH = "s3://b"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("PATH")
    assert binding.value is None
    assert binding.reason == "reassigned"


def test_python_fstring_skipped():
    src = dedent('''
        bucket = "x"
        PATH = f"s3://{bucket}/events"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("PATH")
    assert binding is None or binding.value is None


def test_python_non_literal_rhs_skipped():
    src = dedent('''
        import os
        PATH = os.getenv("X")
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("PATH")
    assert binding is None or binding.value is None
```

- [ ] **Step 2: Run, see fail**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v -k python`
Expected: most fail (collector returns empty).

- [ ] **Step 3: Implement Python collector**

Append to `skills/open_table_migrator/scope.py`:
```python
def _python_extract_string_literal(node, source: bytes) -> str | None:
    """Return the string value of a Python `string` node, or None."""
    if node.type != "string":
        return None
    # f-strings contain `interpolation` children — skip them
    for child in node.children:
        if child.type in ("interpolation",):
            return None
    for child in node.children:
        if child.type == "string_content":
            return source[child.start_byte:child.end_byte].decode()
    # Empty string: "" — children are just delimiters, no string_content
    return ""


def _python_assign_target_name(assignment_node, source: bytes) -> str | None:
    """If the LHS is a single identifier, return its name. Otherwise None."""
    left = assignment_node.child_by_field_name("left")
    if left is None:
        # fallback: first child before '=' that is identifier
        for child in assignment_node.children:
            if child.type == "identifier":
                return source[child.start_byte:child.end_byte].decode()
        return None
    if left.type == "identifier":
        return source[left.start_byte:left.end_byte].decode()
    return None


def _python_scope_key(node) -> str:
    """Walk up to the enclosing function_definition. Return its name or 'module'."""
    parent = node.parent
    while parent is not None:
        if parent.type == "function_definition":
            for child in parent.children:
                if child.type == "identifier":
                    return child.text.decode()
            return "anonymous"
        parent = parent.parent
    return "module"


def _python_iter_assignments(root):
    """Yield (assignment_node, scope_key) for every assignment in the file."""
    stack = [root]
    while stack:
        node = stack.pop()
        if node.type == "assignment":
            yield node
        for child in reversed(node.children):
            stack.append(child)


def _python_collector(root, source: bytes, file: str) -> ConstTable:
    """Two-pass Python collector."""
    table = ConstTable()
    seen: dict[tuple[str, str], int] = {}  # (scope, name) → assignment count

    def _add_or_reassign(scope: str, name: str, value: str | None,
                        line: int, reason: str | None) -> None:
        key = (scope, name)
        if key in seen:
            # Mark as reassigned regardless of new value
            table.bindings[key] = ConstBinding(
                name=name, value=None, file=file, line=line,
                scope=scope, reason="reassigned",
            )
            seen[key] += 1
            return
        seen[key] = 1
        table.bindings[key] = ConstBinding(
            name=name, value=value, file=file, line=line,
            scope=scope, reason=reason,
        )

    # Pass 1: pure literals
    for assign in _python_iter_assignments(root):
        name = _python_assign_target_name(assign, source)
        if name is None:
            continue
        right = assign.child_by_field_name("right")
        if right is None:
            continue
        literal = _python_extract_string_literal(right, source)
        if literal is None:
            continue
        scope = _python_scope_key(assign)
        line = assign.start_point[0] + 1
        _add_or_reassign(scope, name, literal, line, reason=None)

    # Pass 2: 1-level concat (`+`)
    for assign in _python_iter_assignments(root):
        name = _python_assign_target_name(assign, source)
        if name is None:
            continue
        right = assign.child_by_field_name("right")
        if right is None or right.type != "binary_operator":
            continue
        scope = _python_scope_key(assign)
        line = assign.start_point[0] + 1
        # Check operator is `+`
        op_node = None
        for child in right.children:
            if child.type == "+" or (child.is_named is False and source[child.start_byte:child.end_byte] == b"+"):
                op_node = child
                break
        if op_node is None:
            continue
        # Collect left/right operands
        left_n = right.child_by_field_name("left")
        right_n = right.child_by_field_name("right")
        if left_n is None or right_n is None:
            continue

        def _resolve_operand(operand) -> str | None:
            lit = _python_extract_string_literal(operand, source)
            if lit is not None:
                return lit
            if operand.type == "identifier":
                op_name = source[operand.start_byte:operand.end_byte].decode()
                # Try pass-1 bindings only — by checking that the binding's
                # value is not None and that it was added before pass-2 runs.
                # Pass 2 hasn't written this binding yet, so any existing
                # entry came from pass 1 OR an earlier pass-1 reassignment.
                # We only trust value-bearing bindings.
                lookup = table.resolve(op_name, scope_hint=scope)
                if lookup is not None and lookup.value is not None:
                    return lookup.value
            return None

        l_val = _resolve_operand(left_n)
        r_val = _resolve_operand(right_n)
        if l_val is None or r_val is None:
            _add_or_reassign(scope, name, value=None, line=line,
                             reason="dependency_unresolved")
            continue
        _add_or_reassign(scope, name, value=l_val + r_val, line=line,
                         reason=None)

    return table


_COLLECTORS["python"] = _python_collector
```

- [ ] **Step 4: Run, see pass**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v -k python`
Expected: 10 python tests pass. The 6 skeleton tests still pass.

- [ ] **Step 5: Run full scope suite**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v`
Expected: 16 passed (6 skeleton + 10 python).

- [ ] **Step 6: Commit**

```bash
git add skills/open_table_migrator/scope.py tests/test_scope.py
git commit -m "feat(scope): Python collector with literal + 1-level concat + reassignment"
```

---

## Task 3: Java const collector

**Files:**
- Modify: `skills/open_table_migrator/scope.py`
- Modify: `tests/test_scope.py`

- [ ] **Step 1: Append failing tests**

```python
def test_java_class_static_final():
    src = dedent('''
        public class Job {
            private static final String PATH = "s3://bucket/x";
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    binding = table.resolve("PATH")
    assert binding is not None
    assert binding.value == "s3://bucket/x"


def test_java_class_final_inline_no_static():
    src = dedent('''
        public class Job {
            private final String PATH = "s3://bucket/x";
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    assert table.resolve("PATH").value == "s3://bucket/x"


def test_java_non_final_skipped():
    src = dedent('''
        public class Job {
            private String PATH = "s3://bucket/x";
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    binding = table.resolve("PATH")
    assert binding is None or binding.value is None


def test_java_class_concat():
    src = dedent('''
        public class Job {
            private static final String BASE = "s3://bucket";
            private static final String PATH = BASE + "/events";
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    assert table.resolve("PATH").value == "s3://bucket/events"


def test_java_method_local_final():
    src = dedent('''
        public class Job {
            void run() {
                final String localPath = "s3://other";
            }
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    binding = table.resolve("localPath", scope_hint="run")
    assert binding is not None
    assert binding.value == "s3://other"


def test_java_method_local_non_final_skipped():
    src = dedent('''
        public class Job {
            void run() {
                String localPath = "s3://other";
            }
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    assert table.resolve("localPath", scope_hint="run") is None


def test_java_reassignment_in_method():
    src = dedent('''
        public class Job {
            void run() {
                final String p = "s3://a";
                final String p = "s3://b";
            }
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    binding = table.resolve("p", scope_hint="run")
    assert binding is not None
    assert binding.value is None
    assert binding.reason == "reassigned"
```

- [ ] **Step 2: Run, see fail**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v -k java`
Expected: 7 java tests fail.

- [ ] **Step 3: Implement Java collector**

Append to `skills/open_table_migrator/scope.py`:
```python
def _java_extract_string_literal(node, source: bytes) -> str | None:
    """Return the string value of a Java `string_literal` node, or None."""
    if node.type != "string_literal":
        return None
    for child in node.children:
        if child.type == "string_fragment":
            return source[child.start_byte:child.end_byte].decode()
    # Empty string literal: ""
    return ""


def _java_has_modifier(decl_node, modifier_name: str) -> bool:
    """Check if a field_declaration or local_variable_declaration has the modifier."""
    for child in decl_node.children:
        if child.type == "modifiers":
            for mod in child.children:
                if mod.type == modifier_name:
                    return True
    return False


def _java_scope_key(node, source: bytes) -> str:
    """Walk up. Method scope = enclosing method's name, else 'module'.

    For field_declaration we return 'module' (class-level).
    For local_variable_declaration we return the enclosing method name.
    """
    parent = node.parent
    while parent is not None:
        if parent.type == "method_declaration":
            name_n = parent.child_by_field_name("name")
            if name_n is not None:
                return source[name_n.start_byte:name_n.end_byte].decode()
            return "anonymous"
        parent = parent.parent
    return "module"


def _java_iter_declarations(root):
    """Yield every field_declaration and local_variable_declaration node."""
    stack = [root]
    while stack:
        node = stack.pop()
        if node.type in ("field_declaration", "local_variable_declaration"):
            yield node
        for child in reversed(node.children):
            stack.append(child)


def _java_collector(root, source: bytes, file: str) -> ConstTable:
    """Two-pass Java collector."""
    table = ConstTable()
    seen: dict[tuple[str, str], int] = {}

    def _add_or_reassign(scope: str, name: str, value: str | None,
                        line: int, reason: str | None) -> None:
        key = (scope, name)
        if key in seen:
            table.bindings[key] = ConstBinding(
                name=name, value=None, file=file, line=line,
                scope=scope, reason="reassigned",
            )
            seen[key] += 1
            return
        seen[key] = 1
        table.bindings[key] = ConstBinding(
            name=name, value=value, file=file, line=line,
            scope=scope, reason=reason,
        )

    def _is_eligible(decl) -> bool:
        # field_declaration with `final` (static optional) → eligible
        # local_variable_declaration with `final` → eligible
        # otherwise → skip
        return _java_has_modifier(decl, "final")

    def _iter_declarators(decl):
        for child in decl.children:
            if child.type == "variable_declarator":
                yield child

    # Pass 1: literals
    for decl in _java_iter_declarations(root):
        if not _is_eligible(decl):
            continue
        scope = _java_scope_key(decl, source)
        for declarator in _iter_declarators(decl):
            name_n = declarator.child_by_field_name("name")
            value_n = declarator.child_by_field_name("value")
            if name_n is None or value_n is None:
                continue
            name = source[name_n.start_byte:name_n.end_byte].decode()
            literal = _java_extract_string_literal(value_n, source)
            if literal is None:
                continue
            line = declarator.start_point[0] + 1
            _add_or_reassign(scope, name, literal, line, reason=None)

    # Pass 2: 1-level concat
    for decl in _java_iter_declarations(root):
        if not _is_eligible(decl):
            continue
        scope = _java_scope_key(decl, source)
        for declarator in _iter_declarators(decl):
            name_n = declarator.child_by_field_name("name")
            value_n = declarator.child_by_field_name("value")
            if name_n is None or value_n is None:
                continue
            if value_n.type != "binary_expression":
                continue
            name = source[name_n.start_byte:name_n.end_byte].decode()
            line = declarator.start_point[0] + 1
            # Check operator is `+`
            op_n = value_n.child_by_field_name("operator")
            if op_n is None or source[op_n.start_byte:op_n.end_byte] != b"+":
                continue
            left_n = value_n.child_by_field_name("left")
            right_n = value_n.child_by_field_name("right")
            if left_n is None or right_n is None:
                continue

            def _resolve_operand(operand) -> str | None:
                lit = _java_extract_string_literal(operand, source)
                if lit is not None:
                    return lit
                if operand.type == "identifier":
                    op_name = source[operand.start_byte:operand.end_byte].decode()
                    lookup = table.resolve(op_name, scope_hint=scope)
                    if lookup is not None and lookup.value is not None:
                        return lookup.value
                return None

            l_val = _resolve_operand(left_n)
            r_val = _resolve_operand(right_n)
            if l_val is None or r_val is None:
                _add_or_reassign(scope, name, value=None, line=line,
                                 reason="dependency_unresolved")
                continue
            _add_or_reassign(scope, name, value=l_val + r_val, line=line,
                             reason=None)

    return table


_COLLECTORS["java"] = _java_collector
```

- [ ] **Step 4: Run, see pass**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v -k java`
Expected: 7 java tests pass.

- [ ] **Step 5: Run full scope suite**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v`
Expected: 23 passed (6 + 10 python + 7 java).

- [ ] **Step 6: Commit**

```bash
git add skills/open_table_migrator/scope.py tests/test_scope.py
git commit -m "feat(scope): Java collector (static final + final + method-local)"
```

---

## Task 4: Scala const collector

**Files:**
- Modify: `skills/open_table_migrator/scope.py`
- Modify: `tests/test_scope.py`

- [ ] **Step 1: Append failing tests**

```python
def test_scala_object_val():
    src = dedent('''
        object Job {
            val PATH = "s3://bucket/x"
        }
    ''').encode()
    table = build_const_table(src, "scala", "Job.scala")
    binding = table.resolve("PATH")
    assert binding is not None
    assert binding.value == "s3://bucket/x"


def test_scala_var_skipped():
    src = dedent('''
        object Job {
            var PATH = "s3://bucket/x"
        }
    ''').encode()
    table = build_const_table(src, "scala", "Job.scala")
    binding = table.resolve("PATH")
    assert binding is None or binding.value is None


def test_scala_def_local_val():
    src = dedent('''
        object Job {
            def run(): Unit = {
                val p = "s3://other"
            }
        }
    ''').encode()
    table = build_const_table(src, "scala", "Job.scala")
    binding = table.resolve("p", scope_hint="run")
    assert binding is not None
    assert binding.value == "s3://other"


def test_scala_val_concat():
    src = dedent('''
        object Job {
            val BASE = "s3://bucket"
            val PATH = BASE + "/events"
        }
    ''').encode()
    table = build_const_table(src, "scala", "Job.scala")
    assert table.resolve("PATH").value == "s3://bucket/events"


def test_scala_reassignment_marks_unresolvable():
    src = dedent('''
        object Job {
            val p = "s3://a"
            val p = "s3://b"
        }
    ''').encode()
    table = build_const_table(src, "scala", "Job.scala")
    binding = table.resolve("p")
    assert binding is not None
    assert binding.value is None
    assert binding.reason == "reassigned"
```

- [ ] **Step 2: Run, see fail**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v -k scala`
Expected: 5 scala tests fail.

- [ ] **Step 3: Implement Scala collector**

Append to `skills/open_table_migrator/scope.py`:
```python
def _scala_extract_string_literal(node, source: bytes) -> str | None:
    """Return the string value of a Scala `string` node, or None."""
    if node.type != "string":
        return None
    text = source[node.start_byte:node.end_byte].decode()
    # tree-sitter-scala returns the full quoted string; strip quotes
    if text.startswith('"') and text.endswith('"'):
        # Skip interpolated/formatted strings (s"...", f"...", raw"...")
        return text[1:-1]
    return None


def _scala_scope_key(node, source: bytes) -> str:
    """Walk up to enclosing function_definition. Return its name or 'module'."""
    parent = node.parent
    while parent is not None:
        if parent.type == "function_definition":
            name_n = parent.child_by_field_name("name")
            if name_n is not None:
                return source[name_n.start_byte:name_n.end_byte].decode()
            return "anonymous"
        parent = parent.parent
    return "module"


def _scala_iter_val_definitions(root):
    """Yield (val_definition_node, is_mutable) for every val/var in the file."""
    stack = [root]
    while stack:
        node = stack.pop()
        if node.type == "val_definition":
            yield node, False
        elif node.type == "var_definition":
            yield node, True
        for child in reversed(node.children):
            stack.append(child)


def _scala_collector(root, source: bytes, file: str) -> ConstTable:
    """Two-pass Scala collector."""
    table = ConstTable()
    seen: dict[tuple[str, str], int] = {}

    def _add_or_reassign(scope: str, name: str, value: str | None,
                        line: int, reason: str | None) -> None:
        key = (scope, name)
        if key in seen:
            table.bindings[key] = ConstBinding(
                name=name, value=None, file=file, line=line,
                scope=scope, reason="reassigned",
            )
            seen[key] += 1
            return
        seen[key] = 1
        table.bindings[key] = ConstBinding(
            name=name, value=value, file=file, line=line,
            scope=scope, reason=reason,
        )

    def _name_of(defn) -> str | None:
        pat = defn.child_by_field_name("pattern")
        if pat is None:
            for child in defn.children:
                if child.type == "identifier":
                    return source[child.start_byte:child.end_byte].decode()
            return None
        if pat.type == "identifier":
            return source[pat.start_byte:pat.end_byte].decode()
        return None

    def _rhs_of(defn):
        return defn.child_by_field_name("value")

    # Pass 1: literals
    for defn, is_mutable in _scala_iter_val_definitions(root):
        if is_mutable:
            continue  # var skipped
        name = _name_of(defn)
        if name is None:
            continue
        rhs = _rhs_of(defn)
        if rhs is None:
            continue
        literal = _scala_extract_string_literal(rhs, source)
        if literal is None:
            continue
        scope = _scala_scope_key(defn, source)
        line = defn.start_point[0] + 1
        _add_or_reassign(scope, name, literal, line, reason=None)

    # Pass 2: 1-level concat (`infix_expression` with `+`)
    for defn, is_mutable in _scala_iter_val_definitions(root):
        if is_mutable:
            continue
        name = _name_of(defn)
        if name is None:
            continue
        rhs = _rhs_of(defn)
        if rhs is None or rhs.type != "infix_expression":
            continue
        # operator field
        op_n = rhs.child_by_field_name("operator")
        if op_n is None or source[op_n.start_byte:op_n.end_byte] != b"+":
            continue
        left_n = rhs.child_by_field_name("left")
        right_n = rhs.child_by_field_name("right")
        if left_n is None or right_n is None:
            continue
        scope = _scala_scope_key(defn, source)
        line = defn.start_point[0] + 1

        def _resolve_operand(operand) -> str | None:
            lit = _scala_extract_string_literal(operand, source)
            if lit is not None:
                return lit
            if operand.type == "identifier":
                op_name = source[operand.start_byte:operand.end_byte].decode()
                lookup = table.resolve(op_name, scope_hint=scope)
                if lookup is not None and lookup.value is not None:
                    return lookup.value
            return None

        l_val = _resolve_operand(left_n)
        r_val = _resolve_operand(right_n)
        if l_val is None or r_val is None:
            _add_or_reassign(scope, name, value=None, line=line,
                             reason="dependency_unresolved")
            continue
        _add_or_reassign(scope, name, value=l_val + r_val, line=line,
                         reason=None)

    return table


_COLLECTORS["scala"] = _scala_collector
```

- [ ] **Step 4: Run, see pass**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v -k scala`
Expected: 5 scala tests pass.

- [ ] **Step 5: Run full scope suite**

`PYTHONPATH=. python3 -m pytest tests/test_scope.py -v`
Expected: 28 passed (6 + 10 python + 7 java + 5 scala).

- [ ] **Step 6: Commit**

```bash
git add skills/open_table_migrator/scope.py tests/test_scope.py
git commit -m "feat(scope): Scala collector (val + concat + scope)"
```

---

## Task 5: ts_detector integration + PatternMatch.attrs

**Files:**
- Modify: `skills/open_table_migrator/detector.py`
- Modify: `skills/open_table_migrator/ts_detector.py`

The detector touches identifier arguments in several call-site emitters. The simplest threading is:

1. Add `attrs: dict[str, str]` field to `PatternMatch`.
2. In `ts_detect`, build `const_table` once per file before walking.
3. Replace `_first_string_arg(args, lang)` and `_nth_positional_arg_string(args, lang, n)` with variants that take `const_table` and `scope_hint` and return `(value, attrs_update)` tuple. When the arg is an identifier and resolves, return `(value, {"resolved_from": "..."})`. When the arg is an identifier that doesn't resolve (binding is None or value is None), return `(None, {"skipped_reason": "..."})`. When the arg is missing entirely, return `(None, {})`.

To keep the diff small, introduce a NEW helper `_first_string_arg_with_consts(args, lang, const_table, scope_hint) -> tuple[str | None, dict[str, str]]` and migrate call sites incrementally. Old helper stays for backward compat (existing tests).

- [ ] **Step 1: Failing tests**

Modify `tests/test_detector.py`. Find an existing test for `pd.read_parquet` and add new tests after it:

```python
from textwrap import dedent
from pathlib import Path

from skills.open_table_migrator.detector import detect_all_io


def test_python_module_const_resolves_in_read_parquet(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        import pandas as pd
        EVENTS_PATH = "s3://bucket/events"
        df = pd.read_parquet(EVENTS_PATH)
    '''))
    matches = detect_all_io(tmp_path)
    assert len(matches) == 1
    m = matches[0]
    assert m.path_arg == "s3://bucket/events"
    assert "resolved_from" in m.attrs
    assert "EVENTS_PATH" in m.attrs["resolved_from"]


def test_python_function_local_const_resolves(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        import pandas as pd
        def run():
            path = "s3://bucket/users"
            df = pd.read_parquet(path)
    '''))
    matches = detect_all_io(tmp_path)
    assert len(matches) == 1
    assert matches[0].path_arg == "s3://bucket/users"


def test_python_reassigned_skipped(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        import pandas as pd
        PATH = "s3://a"
        PATH = "s3://b"
        df = pd.read_parquet(PATH)
    '''))
    matches = detect_all_io(tmp_path)
    assert len(matches) == 1
    m = matches[0]
    assert m.path_arg is None
    assert m.attrs.get("skipped_reason") == "reassigned"


def test_java_static_final_resolves(tmp_path: Path):
    (tmp_path / "Job.java").write_text(dedent('''
        public class Job {
            private static final String EVENTS = "s3://bucket/events";
            void run() {
                spark.read().parquet(EVENTS);
            }
        }
    '''))
    matches = detect_all_io(tmp_path)
    assert any(m.path_arg == "s3://bucket/events" for m in matches)


def test_scala_val_resolves(tmp_path: Path):
    (tmp_path / "Job.scala").write_text(dedent('''
        object Job {
            val EVENTS = "s3://bucket/events"
            def run(): Unit = {
                spark.read.parquet(EVENTS)
            }
        }
    '''))
    matches = detect_all_io(tmp_path)
    assert any(m.path_arg == "s3://bucket/events" for m in matches)


def test_python_unresolved_identifier_keeps_none(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        import pandas as pd
        df = pd.read_parquet(SOME_UNDEFINED_PATH)
    '''))
    matches = detect_all_io(tmp_path)
    assert len(matches) == 1
    assert matches[0].path_arg is None
    # No binding at all — no skipped_reason set
    assert "skipped_reason" not in matches[0].attrs
```

- [ ] **Step 2: Run, see fail**

`PYTHONPATH=. python3 -m pytest tests/test_detector.py -v -k "const_resolves or reassigned_skipped or final_resolves or val_resolves or unresolved_identifier"`
Expected: 6 fail (some with AttributeError on `attrs`, some with path_arg=None where literal expected).

- [ ] **Step 3: Add `attrs` field to PatternMatch**

In `skills/open_table_migrator/detector.py`:

Replace:
```python
@dataclass
class PatternMatch:
    file: Path
    line: int
    pattern_type: str
    original_code: str
    path_arg: str | None = None
    end_line: int | None = None  # last physical line of the logical statement
    format: str | None = None  # NEW
```

With:
```python
from dataclasses import dataclass, field


@dataclass
class PatternMatch:
    file: Path
    line: int
    pattern_type: str
    original_code: str
    path_arg: str | None = None
    end_line: int | None = None  # last physical line of the logical statement
    format: str | None = None
    attrs: dict[str, str] = field(default_factory=dict)
```

- [ ] **Step 4: Wire const-table through ts_detector**

In `skills/open_table_migrator/ts_detector.py`, add at the top:

```python
from .scope import build_const_table, ConstTable
```

Find the `ts_detect` function (probably near the bottom of the file). It walks files and per-file calls into the detection logic. Add a `const_table` parameter through to the detection helpers. The cleanest way:

Inside `ts_detect`, in the loop over files, after `tree = parse(source, lang)`, add:
```python
const_table = build_const_table(source, lang, str(path.relative_to(project_root)))
```

Then propagate `const_table` through the call chain to wherever `_first_string_arg` and `_nth_positional_arg_string` are called.

Add a NEW helper near `_first_string_arg` (line 75):

```python
def _resolve_identifier_arg(
    args_node, lang: str, const_table: ConstTable, scope_hint: str | None,
) -> tuple[str | None, dict[str, str]]:
    """Resolve the first non-string identifier argument via const_table.

    Returns (value, attrs_update). If first arg is a string literal already,
    returns (literal, {}). If the first non-keyword positional arg is an
    identifier resolvable to a literal, returns (literal,
    {"resolved_from": "NAME@file:line"}). If identifier exists in const_table
    but value is None (reassigned / unresolvable), returns (None,
    {"skipped_reason": reason}). Otherwise (None, {}).
    """
    # First check if there's a string literal already
    val = _first_string_arg(args_node, lang)
    if val is not None:
        return val, {}

    # Find first non-keyword positional identifier
    for child in args_node.children:
        if child.type in ("(", ")", ",", "=", "keyword_argument"):
            continue
        if child.type == "identifier":
            name = child.text.decode()
            binding = const_table.resolve(name, scope_hint=scope_hint)
            if binding is None:
                return None, {}
            if binding.value is None:
                return None, {"skipped_reason": binding.reason or "unknown"}
            return binding.value, {
                "resolved_from": f"{name}@{binding.file}:{binding.line}",
            }
        # First positional that isn't an identifier or string: bail
        break
    return None, {}
```

Now find every call to `_first_string_arg(args, lang)` and the helper that builds `PatternMatch` instances. At every such site, ALSO compute scope_hint (the enclosing function/method) and call `_resolve_identifier_arg(args, lang, const_table, scope_hint)` when `_first_string_arg` returns None. Merge `attrs_update` into the eventual `PatternMatch.attrs`.

The cleanest minimal change is to add a helper that does both:

```python
def _first_arg_or_resolved(
    args_node, lang: str, const_table: ConstTable, scope_hint: str | None,
) -> tuple[str | None, dict[str, str]]:
    """Combined: try literal first, fall back to identifier resolution."""
    val = _first_string_arg(args_node, lang)
    if val is not None:
        return val, {}
    return _resolve_identifier_arg(args_node, lang, const_table, scope_hint)
```

Then at the existing call sites (search for `_first_string_arg(args, lang)`), replace each with:

```python
first_str, attrs_update = _first_arg_or_resolved(args, lang, const_table, scope_hint)
```

And when creating the PatternMatch:

```python
pm = PatternMatch(..., path_arg=first_str, ...)
pm.attrs.update(attrs_update)
matches.append(pm)
```

To compute `scope_hint`: walk up from the call_node to the nearest `function_definition` (Python) / `method_declaration` (Java) / `function_definition` (Scala) and extract its name. Add a helper:

```python
def _enclosing_function_name(call_node, lang: str, source: bytes) -> str | None:
    parent = call_node.parent
    while parent is not None:
        if lang == "python" and parent.type == "function_definition":
            for child in parent.children:
                if child.type == "identifier":
                    return source[child.start_byte:child.end_byte].decode()
        if lang == "java" and parent.type == "method_declaration":
            name_n = parent.child_by_field_name("name")
            if name_n is not None:
                return source[name_n.start_byte:name_n.end_byte].decode()
        if lang == "scala" and parent.type == "function_definition":
            name_n = parent.child_by_field_name("name")
            if name_n is not None:
                return source[name_n.start_byte:name_n.end_byte].decode()
        parent = parent.parent
    return None
```

NOTE: This task touches many call sites in `ts_detector.py` (over 20 emit-sites of `PatternMatch`). The implementer should:
1. Add `attrs` to `PatternMatch` first.
2. Add the new helpers (`_resolve_identifier_arg`, `_first_arg_or_resolved`, `_enclosing_function_name`).
3. Identify the central `_visit_call` / similar function (around line 575) and thread `const_table` through it.
4. Per emit site, compute scope_hint and call the new helper.

The detailed integration depends on the current ts_detector.py structure. The implementer should read ts_detector.py end-to-end before starting, then propose a minimal diff that:
- Builds const_table once per file inside `ts_detect`
- Passes it (or makes it available via closure) to the emit helpers
- Computes scope_hint at each emit site
- Updates `match.attrs` after emission

If the task is too large, ESCALATE and split into smaller pieces.

- [ ] **Step 5: Run, see pass**

`PYTHONPATH=. python3 -m pytest tests/test_detector.py -v -k "const_resolves or reassigned_skipped or final_resolves or val_resolves or unresolved_identifier"`
Expected: 6 passed.

- [ ] **Step 6: Run full migrator suite, ensure no regressions**

`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures -q`
Expected: 236 (existing) + 6 (new detector) + 28 (scope) = 270 passed.

- [ ] **Step 7: Commit**

```bash
git add skills/open_table_migrator/detector.py skills/open_table_migrator/ts_detector.py tests/test_detector.py
git commit -m "feat(detector): resolve identifier path args via scope.ConstTable"
```

---

## Task 6: Worklist + transformer integration tests

**Files:**
- Modify: `tests/test_multi_table.py`
- Modify: `tests/test_transformer_pandas.py`

These confirm the resolved path flows through the existing downstream code unchanged — mapping matcher and transformer.

- [ ] **Step 1: Failing test in test_multi_table.py**

Append to `tests/test_multi_table.py`:
```python
def test_mapping_matches_resolved_const_path(tmp_path):
    """Code uses a name → literal in const_table → mapping glob hits the resolved value."""
    from skills.open_table_migrator.detector import detect_all_io
    from skills.open_table_migrator.targets import (
        Mapping, MappingEntry, Target, build_resolver,
    )

    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'USERS_PATH = "s3://bucket/users/data.parquet"\n'
        'df = pd.read_parquet(USERS_PATH)\n'
    )
    matches = detect_all_io(tmp_path)
    assert len(matches) == 1
    assert matches[0].path_arg == "s3://bucket/users/data.parquet"

    mapping = Mapping(entries=[
        MappingEntry(
            path_glob="s3://bucket/users/*",
            target=Target(namespace="analytics", table="users"),
        ),
    ])
    # NOTE: targets.build_resolver may not accept project_root in the master
    # branch tip — this task may run before or after the uri-normalization
    # branch is merged. Use whatever signature exists at this point.
    try:
        resolver = build_resolver(mapping, fallback=None, project_root=tmp_path)
    except TypeError:
        resolver = build_resolver(mapping, fallback=None)
    decision = resolver(matches[0].path_arg)
    assert decision.migrate_to == Target(namespace="analytics", table="users")
```

- [ ] **Step 2: Run, see pass (with hopefully no extra wiring needed)**

`PYTHONPATH=. python3 -m pytest tests/test_multi_table.py::test_mapping_matches_resolved_const_path -v`

If this fails because `build_resolver` rejects `project_root` keyword (uri-normalization branch not yet merged): use the `except TypeError` fallback shown above — the test should still pass.

- [ ] **Step 3: Failing test in test_transformer_pandas.py**

Look at existing transformer tests for pattern. Append a similar test that:
- Creates a temp file with `EVENTS_PATH = "s3://bucket/events"` + `pd.read_parquet(EVENTS_PATH)`.
- Detects, builds worklist (using a mapping or a fallback target).
- Runs the pandas transformer.
- Asserts the rewritten file uses the resolved path correctly (e.g. `catalog.load_table("analytics.events").scan()` or similar — match the existing test's assertion style).

Read existing pandas transformer test first to find the assertion patterns. Test name: `test_pandas_transformer_uses_resolved_const_path`.

- [ ] **Step 4: Run, see pass**

`PYTHONPATH=. python3 -m pytest tests/test_transformer_pandas.py::test_pandas_transformer_uses_resolved_const_path -v`
Expected: pass.

- [ ] **Step 5: Full suite**

`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures -q`
Expected: 272 passed (270 + 2 new).

- [ ] **Step 6: Commit**

```bash
git add tests/test_multi_table.py tests/test_transformer_pandas.py
git commit -m "test(scope): mapping resolver + pandas transformer use resolved const paths"
```

---

## Task 7: SKILL.md "Constant folding" section + final verification

**Files:**
- Modify: `skills/open_table_migrator/SKILL.md`

- [ ] **Step 1: Locate insertion point**

Run: `grep -n "^##" skills/open_table_migrator/SKILL.md`
Insert the new section "## Constant folding" just before "## Known Limitations" (preserves the existing README anchor).

- [ ] **Step 2: Write the section**

Insert:
```markdown
## Constant folding

The detector resolves name-to-literal bindings at the file level so that I/O calls using a named constant become as informative as those using a literal directly.

```python
EVENTS_PATH = "s3://bucket/events"
df = pd.read_parquet(EVENTS_PATH)   # path_arg = "s3://bucket/events"
```

### What is resolved

- **Python:** module-level `X = "..."`, function-local `X = "..."`, one level of `+` concat (`X = BASE + "/events"` when `BASE` is already a known literal).
- **Java:** class `static final` and inline-initialised `final` fields, method-local `final` variables, one level of `+` concat across fields.
- **Scala:** object/class-level `val`, def-local `val`, one level of `+` concat.

### What is skipped

- **f-strings** (`f"..."`, `s"..."` Scala) — interpolation depends on runtime values.
- **`.format()`, `%`-format, multiplication** — only `+` concat is recognised.
- **Reassignment** — any reassigned name is marked unresolvable. `attrs.skipped_reason = "reassigned"` records the reason.
- **Constructor-only Java fields** — `private final String x;` initialised inside a constructor is not parsed.
- **Cross-file references** — `from config import X` is not followed; constants in other files are out of scope.
- **3+ operand concat** — `A + B + C` is not resolved; only single-`+` expressions.
- **Non-literal RHS** — `os.getenv(...)`, function calls, etc.

### Audit trail

When a match resolves a name, `match.attrs["resolved_from"] = "NAME@file:line"` is set so the worklist preserves the original source location. The detector emits the same `path_arg` value regardless of whether it came from a literal at the call site or a resolved binding.
```

- [ ] **Step 3: Run full suite one final time**

`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures -q`
Expected: 272 passed.

- [ ] **Step 4: Smoke run on existing fixtures**

```bash
PYTHONPATH=. python3 -m skills.open_table_migrator.cli tests/fixtures/python-pyspark --no-deps 2>&1 | tail -10
```
Expected: command runs, finds parquet operations (some of which may now resolve constants that previously were unresolved). Compare output to master if you have a copy — extra resolved paths are the win.

- [ ] **Step 5: Commit**

```bash
git add skills/open_table_migrator/SKILL.md
git commit -m "docs(scope): constant folding section in SKILL.md"
```

If the smoke run modified any fixture files (transformer side-effect), revert them — they're not part of this branch:
```bash
git checkout -- tests/fixtures/
rm -f lakehouse-worklist.json
```

---
