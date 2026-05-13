# Dynamic SQL Loading Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect Python/Java/Scala call-sites that load `.sql` files at runtime, parse those SQL files for INSERT/SELECT/JOIN/UPDATE/MERGE references, and produce cross-references between loader sites and parquet/orc tables (defined anywhere across SQL files).

**Architecture:** New module `skills/open_table_migrator/dynamic_sql.py` with 5 loader-pattern detectors via tree-sitter. Existing `sql_registry.py` extended with `TableReference` dataclass + non-DDL parsing. New `analyzer.cross_reference_dynamic_sql` joins loader sites to parquet/orc tables using full-path/basename file resolution. CLI wires the new pipeline into `convert_project`; worklist gains a `dynamic_sql_loaders` field.

**Tech Stack:** Python 3.11+, `tree-sitter` (already used) + `re` for SQL parsing. No new dependencies.

**Spec:** [docs/superpowers/specs/2026-05-08-dynamic-sql-loading-design.md](../specs/2026-05-08-dynamic-sql-loading-design.md)

---

## File Structure

New files:
- `skills/open_table_migrator/dynamic_sql.py` — `DynamicSqlLoader` + `detect_dynamic_sql_loaders` + per-language helpers
- `tests/test_dynamic_sql.py` — unit tests for the 5 loader patterns + const-folding integration

Modified files:
- `skills/open_table_migrator/sql_registry.py` — add `TableReference` + `scan_sql_table_references` + CTE filtering
- `skills/open_table_migrator/analyzer.py` — add `DynamicSqlCrossRef` + `cross_reference_dynamic_sql`
- `skills/open_table_migrator/cli.py` — wire `detect_dynamic_sql_loaders` + `cross_reference_dynamic_sql` into `convert_project`
- `skills/open_table_migrator/worklist.py` — add `dynamic_sql_loaders` to the worklist JSON
- `skills/open_table_migrator/SKILL.md` — new "Dynamic SQL loading" section
- `tests/test_sql_registry.py` — extend with INSERT/SELECT/JOIN/CTE cases
- `tests/test_analyzer.py` — extend with cross-ref tests

---

## Task 1: dynamic_sql.py skeleton + Python `open()` literal pattern

**Files:**
- Create: `skills/open_table_migrator/dynamic_sql.py`
- Create: `tests/test_dynamic_sql.py`

### Step 1: Failing test

`tests/test_dynamic_sql.py`:
```python
from pathlib import Path
from textwrap import dedent

from skills.open_table_migrator.dynamic_sql import (
    DynamicSqlLoader,
    detect_dynamic_sql_loaders,
)


def test_python_open_literal(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        sql = open("queries/events.sql").read()
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    loader = loaders[0]
    assert loader.pattern == "py_open"
    assert loader.sql_filename == "queries/events.sql"
    assert loader.confidence == "high"
    assert loader.line == 2


def test_python_open_non_sql_extension_skipped(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        data = open("data.csv").read()
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert loaders == []


def test_python_open_identifier_arg_skipped_without_const_table(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        SQL_FILE = "queries/x.sql"
        sql = open(SQL_FILE).read()
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    # Without const_table_for_file callback, identifier arguments are skipped.
    assert all(l.pattern != "py_open" or "queries/x.sql" not in l.sql_filename for l in loaders)
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_dynamic_sql.py -v`
Expected: `ModuleNotFoundError: No module named 'skills.open_table_migrator.dynamic_sql'`

### Step 3: Implement skeleton + `py_open`

`skills/open_table_migrator/dynamic_sql.py`:
```python
"""Dynamic SQL loading detection.

Finds call-sites in Python/Java/Scala that load .sql files at runtime, so
the migrator can cross-reference them with parquet/orc tables defined in
those files (or elsewhere in the SQL registry).

Patterns covered:
  - py_open: open("*.sql")
  - py_path_read_text: Path("*.sql").read_text() / .read_bytes()
  - py_pkgutil_get_data: pkgutil.get_data(<pkg>, "*.sql")
  - java_files_read: Files.readAllBytes/readString(Path.of/Paths.get("*.sql"))
  - java_resource_stream: any.getResourceAsStream("*.sql")
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .ts_parser import parse, language_for_file


_EXTS = {".py", ".java", ".scala"}
_IGNORED_DIRS = {".git", "__pycache__", "node_modules", "target", "build", ".gradle", "venv", ".venv"}
_SQL_SUFFIXES = (".sql", ".hql", ".ddl")


@dataclass(frozen=True)
class DynamicSqlLoader:
    file: Path
    line: int
    pattern: str
    sql_filename: str
    confidence: str  # "high" | "medium"


def detect_dynamic_sql_loaders(
    project_root: Path,
    *,
    const_table_for_file: Callable[[Path], object] | None = None,
) -> list[DynamicSqlLoader]:
    """Walk .py/.java/.scala in project_root, detect loader patterns."""
    out: list[DynamicSqlLoader] = []
    for src in project_root.rglob("*"):
        if not src.is_file() or src.suffix.lower() not in _EXTS:
            continue
        if any(part in _IGNORED_DIRS for part in src.parts):
            continue
        lang = language_for_file(src)
        if lang is None:
            continue
        source = src.read_bytes()
        const_table = const_table_for_file(src) if const_table_for_file else None
        if lang == "python":
            out.extend(_detect_python_loaders(source, src, const_table))
    return out


def _filename_is_sql(name: str) -> bool:
    return name.lower().endswith(_SQL_SUFFIXES)


def _extract_string_literal_python(node, source: bytes) -> str | None:
    """Return the string value of a Python `string` node, or None for f-strings."""
    if node.type != "string":
        return None
    for child in node.children:
        if child.type == "interpolation":
            return None
    for child in node.children:
        if child.type == "string_content":
            return source[child.start_byte:child.end_byte].decode()
    return ""


def _detect_python_loaders(
    source: bytes, src_path: Path, const_table,
) -> list[DynamicSqlLoader]:
    tree = parse(source, "python")
    out: list[DynamicSqlLoader] = []
    stack = [tree.root_node]
    while stack:
        node = stack.pop()
        if node.type == "call":
            loader = _try_py_open(node, source, src_path, const_table)
            if loader is not None:
                out.append(loader)
        for child in reversed(node.children):
            stack.append(child)
    return out


def _try_py_open(call_node, source: bytes, src_path: Path, const_table):
    """Detect `open("*.sql", ...)` calls."""
    func = call_node.child_by_field_name("function")
    if func is None or func.type != "identifier":
        return None
    func_name = source[func.start_byte:func.end_byte].decode()
    if func_name != "open":
        return None
    args = call_node.child_by_field_name("arguments")
    if args is None or args.named_child_count < 1:
        return None
    first_arg = args.named_children[0]
    filename = _extract_string_literal_python(first_arg, source)
    confidence = "high"
    if filename is None:
        # Identifier? — placeholder for Task 2 (const_table integration)
        return None
    if not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="py_open",
        sql_filename=filename,
        confidence=confidence,
    )
```

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_dynamic_sql.py -v`
Expected: 3 passed.

### Step 5: Commit

```bash
git add skills/open_table_migrator/dynamic_sql.py tests/test_dynamic_sql.py
git commit -m "feat(dynamic-sql): module skeleton + Python open() loader pattern"
```

---

## Task 2: Python `Path.read_text` + `pkgutil.get_data` + const-table resolution

**Files:**
- Modify: `skills/open_table_migrator/dynamic_sql.py`
- Modify: `tests/test_dynamic_sql.py`

### Step 1: Append failing tests

```python
def test_python_path_read_text(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        sql = Path("queries/x.sql").read_text()
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    assert loaders[0].pattern == "py_path_read_text"
    assert loaders[0].sql_filename == "queries/x.sql"


def test_python_path_read_bytes(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        raw = Path("queries/x.sql").read_bytes()
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    assert loaders[0].pattern == "py_path_read_text"


def test_python_pkgutil_get_data(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        sql = pkgutil.get_data(__name__, "queries/events.sql")
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    assert loaders[0].pattern == "py_pkgutil_get_data"
    assert loaders[0].sql_filename == "queries/events.sql"


def test_python_open_identifier_with_const_table(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        SQL_FILE = "queries/x.sql"
        sql = open(SQL_FILE).read()
    '''))
    from skills.open_table_migrator import scope
    loaders = detect_dynamic_sql_loaders(
        tmp_path,
        const_table_for_file=lambda p: scope.build_const_table(p.read_bytes(), "python", str(p)),
    )
    open_loaders = [l for l in loaders if l.pattern == "py_open"]
    assert len(open_loaders) == 1
    assert open_loaders[0].sql_filename == "queries/x.sql"
    assert open_loaders[0].confidence == "medium"
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_dynamic_sql.py -v -k "path_read or pkgutil or open_identifier_with_const"`
Expected: 4 fail.

### Step 3: Extend `_detect_python_loaders` and helpers

Modify `skills/open_table_migrator/dynamic_sql.py`. Replace `_detect_python_loaders` and add the new helpers:

```python
def _detect_python_loaders(
    source: bytes, src_path: Path, const_table,
) -> list[DynamicSqlLoader]:
    tree = parse(source, "python")
    out: list[DynamicSqlLoader] = []
    stack = [tree.root_node]
    while stack:
        node = stack.pop()
        if node.type == "call":
            for attempt in (_try_py_open, _try_py_path_read_text, _try_py_pkgutil_get_data):
                loader = attempt(node, source, src_path, const_table)
                if loader is not None:
                    out.append(loader)
                    break
        for child in reversed(node.children):
            stack.append(child)
    return out


def _resolve_string_or_identifier_python(
    node, source: bytes, const_table,
) -> tuple[str | None, str]:
    """Return (value, confidence). confidence in {"high", "medium", "skip"}."""
    literal = _extract_string_literal_python(node, source)
    if literal is not None:
        return literal, "high"
    if node.type == "identifier" and const_table is not None:
        name = source[node.start_byte:node.end_byte].decode()
        binding = const_table.resolve(name)
        if binding is not None and binding.value is not None:
            return binding.value, "medium"
    return None, "skip"


def _try_py_open(call_node, source: bytes, src_path: Path, const_table):
    func = call_node.child_by_field_name("function")
    if func is None or func.type != "identifier":
        return None
    func_name = source[func.start_byte:func.end_byte].decode()
    if func_name != "open":
        return None
    args = call_node.child_by_field_name("arguments")
    if args is None or args.named_child_count < 1:
        return None
    filename, confidence = _resolve_string_or_identifier_python(
        args.named_children[0], source, const_table,
    )
    if filename is None or not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="py_open",
        sql_filename=filename,
        confidence=confidence,
    )


def _try_py_path_read_text(call_node, source: bytes, src_path: Path, const_table):
    """Detect `Path("x.sql").read_text()` or `.read_bytes()`."""
    func = call_node.child_by_field_name("function")
    if func is None or func.type != "attribute":
        return None
    # method name
    method_name_node = None
    for child in func.children:
        if child.type == "identifier":
            method_name_node = child  # last identifier child is the method name
    if method_name_node is None:
        return None
    method_name = source[method_name_node.start_byte:method_name_node.end_byte].decode()
    if method_name not in ("read_text", "read_bytes"):
        return None
    # object should be a `Path("...sql")` call
    obj = func.child_by_field_name("object")
    if obj is None or obj.type != "call":
        return None
    inner_func = obj.child_by_field_name("function")
    if inner_func is None or inner_func.type != "identifier":
        return None
    if source[inner_func.start_byte:inner_func.end_byte].decode() != "Path":
        return None
    inner_args = obj.child_by_field_name("arguments")
    if inner_args is None or inner_args.named_child_count < 1:
        return None
    filename, confidence = _resolve_string_or_identifier_python(
        inner_args.named_children[0], source, const_table,
    )
    if filename is None or not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="py_path_read_text",
        sql_filename=filename,
        confidence=confidence,
    )


def _try_py_pkgutil_get_data(call_node, source: bytes, src_path: Path, const_table):
    """Detect `pkgutil.get_data(<pkg>, "x.sql")` — 2nd positional arg."""
    func = call_node.child_by_field_name("function")
    if func is None or func.type != "attribute":
        return None
    method_name_node = None
    for child in func.children:
        if child.type == "identifier":
            method_name_node = child
    if method_name_node is None:
        return None
    if source[method_name_node.start_byte:method_name_node.end_byte].decode() != "get_data":
        return None
    obj = func.child_by_field_name("object")
    if obj is None or obj.type != "identifier":
        return None
    if source[obj.start_byte:obj.end_byte].decode() != "pkgutil":
        return None
    args = call_node.child_by_field_name("arguments")
    if args is None or args.named_child_count < 2:
        return None
    second_arg = args.named_children[1]
    filename, confidence = _resolve_string_or_identifier_python(
        second_arg, source, const_table,
    )
    if filename is None or not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="py_pkgutil_get_data",
        sql_filename=filename,
        confidence=confidence,
    )
```

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_dynamic_sql.py -v`
Expected: 7 passed (3 existing + 4 new).

### Step 5: Commit

```bash
git add skills/open_table_migrator/dynamic_sql.py tests/test_dynamic_sql.py
git commit -m "feat(dynamic-sql): Path.read_text, pkgutil.get_data, const-table integration"
```

---

## Task 3: Java/Scala loaders — Files.read* + getResourceAsStream

**Files:**
- Modify: `skills/open_table_migrator/dynamic_sql.py`
- Modify: `tests/test_dynamic_sql.py`

### Step 1: Append failing tests

```python
def test_java_files_read_all_bytes(tmp_path):
    (tmp_path / "Job.java").write_text(dedent('''
        public class Job {
            void run() throws Exception {
                byte[] sql = Files.readAllBytes(Path.of("queries/events.sql"));
            }
        }
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    assert loaders[0].pattern == "java_files_read"
    assert loaders[0].sql_filename == "queries/events.sql"


def test_java_files_read_string_with_paths_get(tmp_path):
    (tmp_path / "Job.java").write_text(dedent('''
        public class Job {
            void run() throws Exception {
                String sql = Files.readString(Paths.get("x.sql"));
            }
        }
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    assert loaders[0].pattern == "java_files_read"
    assert loaders[0].sql_filename == "x.sql"


def test_java_get_resource_as_stream(tmp_path):
    (tmp_path / "Job.java").write_text(dedent('''
        public class Job {
            void run() {
                var stream = this.getClass().getResourceAsStream("/sql/events.sql");
            }
        }
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    assert loaders[0].pattern == "java_resource_stream"
    assert loaders[0].sql_filename == "/sql/events.sql"


def test_scala_get_resource_as_stream(tmp_path):
    (tmp_path / "Job.scala").write_text(dedent('''
        object Job {
            def run(): Unit = {
                val stream = getClass.getResourceAsStream("/sql/x.sql")
            }
        }
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    assert loaders[0].pattern == "java_resource_stream"
    assert loaders[0].sql_filename == "/sql/x.sql"


def test_loader_in_commented_line_not_detected(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        # sql = open("queries/x.sql").read()
        x = 1
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert loaders == []
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_dynamic_sql.py -v -k "java or scala or commented"`
Expected: 4 fail (java/scala detectors not implemented), 1 may pass already (commented line — AST already excludes comments).

### Step 3: Extend the dispatcher and add Java/Scala helpers

Modify `skills/open_table_migrator/dynamic_sql.py`. In `detect_dynamic_sql_loaders`, add a branch for Java and Scala:

```python
def detect_dynamic_sql_loaders(
    project_root: Path,
    *,
    const_table_for_file: Callable[[Path], object] | None = None,
) -> list[DynamicSqlLoader]:
    out: list[DynamicSqlLoader] = []
    for src in project_root.rglob("*"):
        if not src.is_file() or src.suffix.lower() not in _EXTS:
            continue
        if any(part in _IGNORED_DIRS for part in src.parts):
            continue
        lang = language_for_file(src)
        if lang is None:
            continue
        source = src.read_bytes()
        const_table = const_table_for_file(src) if const_table_for_file else None
        if lang == "python":
            out.extend(_detect_python_loaders(source, src, const_table))
        elif lang == "java":
            out.extend(_detect_java_loaders(source, src, const_table))
        elif lang == "scala":
            out.extend(_detect_scala_loaders(source, src, const_table))
    return out
```

Append the new helpers:

```python
def _extract_string_literal_java(node, source: bytes) -> str | None:
    if node.type != "string_literal":
        return None
    for child in node.children:
        if child.type == "string_fragment":
            return source[child.start_byte:child.end_byte].decode()
    return ""


def _extract_string_literal_scala(node, source: bytes) -> str | None:
    if node.type != "string":
        return None
    text = source[node.start_byte:node.end_byte].decode()
    if text.startswith('"') and text.endswith('"'):
        return text.strip('"')
    return None


def _detect_java_loaders(source: bytes, src_path: Path, const_table) -> list[DynamicSqlLoader]:
    tree = parse(source, "java")
    out: list[DynamicSqlLoader] = []
    stack = [tree.root_node]
    while stack:
        node = stack.pop()
        if node.type == "method_invocation":
            for attempt in (_try_java_files_read, _try_java_resource_stream):
                loader = attempt(node, source, src_path, "java")
                if loader is not None:
                    out.append(loader)
                    break
        for child in reversed(node.children):
            stack.append(child)
    return out


def _detect_scala_loaders(source: bytes, src_path: Path, const_table) -> list[DynamicSqlLoader]:
    tree = parse(source, "scala")
    out: list[DynamicSqlLoader] = []
    stack = [tree.root_node]
    while stack:
        node = stack.pop()
        if node.type == "call_expression":
            loader = _try_java_resource_stream(node, source, src_path, "scala")
            if loader is not None:
                out.append(loader)
        for child in reversed(node.children):
            stack.append(child)
    return out


def _try_java_files_read(call_node, source: bytes, src_path: Path, lang: str):
    """Detect `Files.readAllBytes(Path.of("x.sql"))` and similar."""
    name_n = call_node.child_by_field_name("name")
    if name_n is None:
        return None
    method_name = source[name_n.start_byte:name_n.end_byte].decode()
    if method_name not in ("readAllBytes", "readString"):
        return None
    obj_n = call_node.child_by_field_name("object")
    if obj_n is None:
        return None
    if source[obj_n.start_byte:obj_n.end_byte].decode() != "Files":
        return None
    args = call_node.child_by_field_name("arguments")
    if args is None or args.named_child_count < 1:
        return None
    first_arg = args.named_children[0]
    # first_arg should be a Path.of("x.sql") or Paths.get("x.sql") call
    if first_arg.type != "method_invocation":
        return None
    inner_obj = first_arg.child_by_field_name("object")
    inner_name = first_arg.child_by_field_name("name")
    inner_args = first_arg.child_by_field_name("arguments")
    if inner_obj is None or inner_name is None or inner_args is None:
        return None
    obj_text = source[inner_obj.start_byte:inner_obj.end_byte].decode()
    name_text = source[inner_name.start_byte:inner_name.end_byte].decode()
    if not ((obj_text == "Path" and name_text == "of") or (obj_text == "Paths" and name_text == "get")):
        return None
    if inner_args.named_child_count < 1:
        return None
    filename = _extract_string_literal_java(inner_args.named_children[0], source)
    if filename is None or not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="java_files_read",
        sql_filename=filename,
        confidence="high",
    )


def _try_java_resource_stream(call_node, source: bytes, src_path: Path, lang: str):
    """Detect `<anything>.getResourceAsStream("x.sql")`."""
    if lang == "java":
        name_n = call_node.child_by_field_name("name")
        args_n = call_node.child_by_field_name("arguments")
        if name_n is None or args_n is None:
            return None
        name_text = source[name_n.start_byte:name_n.end_byte].decode()
        if name_text != "getResourceAsStream":
            return None
        if args_n.named_child_count < 1:
            return None
        filename = _extract_string_literal_java(args_n.named_children[0], source)
    elif lang == "scala":
        # Scala call_expression: function (field_expression) + arguments
        func = call_node.child_by_field_name("function")
        args = call_node.child_by_field_name("arguments")
        if func is None or args is None:
            return None
        if func.type != "field_expression":
            return None
        # last identifier in field_expression is the method name
        method_name = None
        for child in reversed(func.children):
            if child.type in ("identifier", "property_identifier"):
                method_name = source[child.start_byte:child.end_byte].decode()
                break
        if method_name != "getResourceAsStream":
            return None
        if args.named_child_count < 1:
            return None
        filename = _extract_string_literal_scala(args.named_children[0], source)
    else:
        return None
    if filename is None or not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="java_resource_stream",
        sql_filename=filename,
        confidence="high",
    )
```

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_dynamic_sql.py -v`
Expected: 12 passed (7 existing + 5 new).

### Step 5: Commit

```bash
git add skills/open_table_migrator/dynamic_sql.py tests/test_dynamic_sql.py
git commit -m "feat(dynamic-sql): Java/Scala Files.read* and getResourceAsStream"
```

---

## Task 4: sql_registry.py — TableReference + INSERT/SELECT/JOIN parsing + CTE filter

**Files:**
- Modify: `skills/open_table_migrator/sql_registry.py`
- Modify: `tests/test_sql_registry.py`

### Step 1: Append failing tests

`tests/test_sql_registry.py` — append:
```python
def test_insert_into_emits_write_reference(tmp_path):
    (tmp_path / "x.sql").write_text("INSERT INTO events SELECT * FROM staging;")
    from skills.open_table_migrator.sql_registry import scan_sql_table_references
    refs = scan_sql_table_references(tmp_path)
    write_refs = [r for r in refs if r.role == "write"]
    assert any(r.table_name == "events" for r in write_refs)


def test_insert_overwrite_emits_write_reference(tmp_path):
    (tmp_path / "x.sql").write_text("INSERT OVERWRITE TABLE events SELECT 1;")
    from skills.open_table_migrator.sql_registry import scan_sql_table_references
    refs = scan_sql_table_references(tmp_path)
    assert any(r.table_name == "events" and r.role == "write" for r in refs)


def test_select_from_emits_read_reference(tmp_path):
    (tmp_path / "x.sql").write_text("SELECT * FROM events WHERE active = true;")
    from skills.open_table_migrator.sql_registry import scan_sql_table_references
    refs = scan_sql_table_references(tmp_path)
    assert any(r.table_name == "events" and r.role == "read" for r in refs)


def test_join_emits_read_reference(tmp_path):
    (tmp_path / "x.sql").write_text("SELECT * FROM users u JOIN devices d ON u.id = d.user_id;")
    from skills.open_table_migrator.sql_registry import scan_sql_table_references
    refs = scan_sql_table_references(tmp_path)
    assert any(r.table_name == "devices" and r.role == "read" for r in refs)


def test_update_emits_write_reference(tmp_path):
    (tmp_path / "x.sql").write_text("UPDATE events SET active = false WHERE id = 1;")
    from skills.open_table_migrator.sql_registry import scan_sql_table_references
    refs = scan_sql_table_references(tmp_path)
    assert any(r.table_name == "events" and r.role == "write" for r in refs)


def test_cte_name_not_registered_as_table(tmp_path):
    (tmp_path / "x.sql").write_text(
        "WITH staging AS (SELECT * FROM users) "
        "INSERT INTO events SELECT * FROM staging JOIN devices ON 1=1;"
    )
    from skills.open_table_migrator.sql_registry import scan_sql_table_references
    refs = scan_sql_table_references(tmp_path)
    # 'staging' is a CTE and must NOT appear as a table reference.
    assert not any(r.table_name == "staging" for r in refs)
    # Real tables (users, events, devices) should be present.
    names = {r.table_name for r in refs}
    assert "users" in names
    assert "events" in names
    assert "devices" in names
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_sql_registry.py -v -k "insert_into or insert_overwrite or select_from or join_emits or update_emits or cte_name"`
Expected: 6 fail (`scan_sql_table_references` doesn't exist).

### Step 3: Extend `sql_registry.py`

Add to `skills/open_table_migrator/sql_registry.py` near `TableDef`:

```python
@dataclass(frozen=True)
class TableReference:
    """Non-DDL reference to a table — INSERT / UPDATE / MERGE / FROM / JOIN."""
    table_name: str
    database: str | None
    role: str           # "write" or "read"
    sql_file: Path
    line: int


_NAME = r"`?\"?(?:(\w+)\.)?`?\"?(\w+)`?\"?"

_INSERT_INTO_RX = re.compile(rf"\bINSERT\s+INTO\s+(?:TABLE\s+)?{_NAME}", re.IGNORECASE)
_INSERT_OVERWRITE_RX = re.compile(rf"\bINSERT\s+OVERWRITE\s+(?:TABLE\s+)?{_NAME}", re.IGNORECASE)
_UPDATE_RX = re.compile(rf"\bUPDATE\s+{_NAME}\s+SET\b", re.IGNORECASE)
_MERGE_RX = re.compile(rf"\bMERGE\s+INTO\s+{_NAME}", re.IGNORECASE)
_FROM_RX = re.compile(rf"\bFROM\s+{_NAME}", re.IGNORECASE)
_JOIN_RX = re.compile(rf"\bJOIN\s+{_NAME}", re.IGNORECASE)
_WITH_CTE_RX = re.compile(r"\bWITH\s+(\w+)\s+AS\s*\(", re.IGNORECASE)


def _collect_cte_names(content: str) -> set[str]:
    return {m.group(1).lower() for m in _WITH_CTE_RX.finditer(content)}


def scan_sql_table_references(project_root: Path) -> list[TableReference]:
    """Scan .sql/.hql/.ddl for non-DDL table references."""
    refs: list[TableReference] = []
    for sql_file in sorted(project_root.rglob("*")):
        if not sql_file.is_file():
            continue
        if sql_file.suffix.lower() not in _SQL_EXTS:
            continue
        try:
            content = sql_file.read_text(errors="replace")
        except OSError:
            continue
        cte_names = _collect_cte_names(content)
        lines = content.split("\n")

        def _emit(role: str, rx) -> None:
            for m in rx.finditer(content):
                db = m.group(1)
                table = m.group(2)
                if table.lower() in cte_names:
                    continue
                line = content[:m.start()].count("\n") + 1
                refs.append(TableReference(
                    table_name=table, database=db, role=role,
                    sql_file=sql_file, line=line,
                ))

        _emit("write", _INSERT_INTO_RX)
        _emit("write", _INSERT_OVERWRITE_RX)
        _emit("write", _UPDATE_RX)
        _emit("write", _MERGE_RX)
        _emit("read",  _FROM_RX)
        _emit("read",  _JOIN_RX)
    return refs
```

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_sql_registry.py -v`
Expected: existing tests + 6 new pass.

### Step 5: Commit

```bash
git add skills/open_table_migrator/sql_registry.py tests/test_sql_registry.py
git commit -m "feat(sql-registry): TableReference + INSERT/SELECT/JOIN/UPDATE parsing + CTE filter"
```

---

## Task 5: analyzer.cross_reference_dynamic_sql + file resolution

**Files:**
- Modify: `skills/open_table_migrator/analyzer.py`
- Modify: `tests/test_analyzer.py`

### Step 1: Append failing tests

`tests/test_analyzer.py` — append:
```python
def test_cross_reference_dynamic_sql_with_create_in_same_file(tmp_path):
    (tmp_path / "queries").mkdir()
    schema_path = tmp_path / "queries" / "events.sql"
    schema_path.write_text("CREATE TABLE events (id INT) STORED AS PARQUET;")
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'sql = open("queries/events.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    assert len(cross) == 1
    assert cross[0].loader.sql_filename == "queries/events.sql"
    assert cross[0].match_kind == "exact_path"
    assert len(cross[0].tables) == 1
    assert cross[0].tables[0].table_name == "events"


def test_cross_reference_dynamic_sql_create_in_different_file(tmp_path):
    """schema.sql has CREATE; queries/events_update.sql has INSERT only.
    Loader of events_update.sql should still cross-ref to the events table."""
    (tmp_path / "schema.sql").write_text("CREATE TABLE events (id INT) STORED AS PARQUET;")
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "events_update.sql").write_text(
        "INSERT INTO events SELECT 1;"
    )
    (tmp_path / "job.py").write_text(
        'sql = open("queries/events_update.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    assert len(cross) == 1
    assert cross[0].tables[0].table_name == "events"
    assert cross[0].tables[0].format == "parquet"


def test_cross_reference_dynamic_sql_loader_with_no_registry_match(tmp_path):
    """Loader points at file not in registry → no cross-ref returned."""
    (tmp_path / "job.py").write_text(
        'sql = open("queries/missing.sql").read()\n'
    )
    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    assert cross == []
    # But the loader itself was detected:
    assert len(loaders) == 1


def test_cross_reference_dynamic_sql_basename_unique(tmp_path):
    """Loader uses basename only; project has unique file with that name."""
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "events.sql").write_text(
        "CREATE TABLE events (id INT) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'sql = open("events.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    assert len(cross) == 1
    assert cross[0].match_kind == "basename_unique"


def test_cross_reference_dynamic_sql_basename_ambiguous(tmp_path):
    """Two events.sql in different dirs — basename resolution returns both."""
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "events.sql").write_text(
        "CREATE TABLE events_a (id INT) STORED AS PARQUET;"
    )
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "events.sql").write_text(
        "CREATE TABLE events_b (id INT) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'sql = open("events.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    # Both files match by basename
    table_names = {t.table_name for c in cross for t in c.tables}
    assert "events_a" in table_names
    assert "events_b" in table_names
    assert all(c.match_kind == "basename_ambiguous" for c in cross)
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_analyzer.py -v -k cross_reference_dynamic_sql`
Expected: 5 fail (function doesn't exist).

### Step 3: Implement in `analyzer.py`

Append to `skills/open_table_migrator/analyzer.py`:

```python
from .dynamic_sql import DynamicSqlLoader
from .sql_registry import TableDef, TableReference


@dataclass(frozen=True)
class DynamicSqlCrossRef:
    loader: DynamicSqlLoader
    sql_file: Path
    tables: tuple[TableDef, ...]
    match_kind: str  # "exact_path" | "basename_unique" | "basename_ambiguous"


def _resolve_sql_file(
    loader: DynamicSqlLoader,
    sql_files: set[Path],
    project_root: Path,
) -> tuple[list[Path], str]:
    """Resolve loader.sql_filename to one or more registered SQL files.

    Tries in order:
      1. relative to loader's containing file's directory
      2. relative to project_root
      3. basename match across registered files
    """
    filename = loader.sql_filename.lstrip("/")
    candidates: list[Path] = []

    candidate = (loader.file.parent / filename).resolve()
    if candidate in sql_files:
        return [candidate], "exact_path"

    candidate = (project_root / filename).resolve()
    if candidate in sql_files:
        return [candidate], "exact_path"

    # Basename fallback
    basename = Path(filename).name
    basename_matches = [f for f in sql_files if f.name == basename]
    if len(basename_matches) == 1:
        return basename_matches, "basename_unique"
    if len(basename_matches) > 1:
        return basename_matches, "basename_ambiguous"
    return [], "none"


def cross_reference_dynamic_sql(
    loaders: list[DynamicSqlLoader],
    sql_defs: list[TableDef],
    sql_refs: list[TableReference],
    project_root: Path,
) -> list[DynamicSqlCrossRef]:
    """For each loader, find the SQL file it loads, collect mentioned tables,
    filter to parquet/orc CREATE TABLE defs (which may live in different files)."""
    sql_files = {d.file.resolve() for d in sql_defs} | {r.sql_file.resolve() for r in sql_refs}
    defs_by_name = {d.table_name.lower(): d for d in sql_defs if d.format in ("parquet", "orc")}

    out: list[DynamicSqlCrossRef] = []
    for loader in loaders:
        resolved_files, match_kind = _resolve_sql_file(loader, sql_files, project_root)
        if not resolved_files:
            continue
        for sql_file in resolved_files:
            mentioned: set[str] = set()
            for d in sql_defs:
                if d.file.resolve() == sql_file:
                    mentioned.add(d.table_name.lower())
            for r in sql_refs:
                if r.sql_file.resolve() == sql_file:
                    mentioned.add(r.table_name.lower())
            tables = tuple(defs_by_name[name] for name in mentioned if name in defs_by_name)
            if not tables:
                continue
            out.append(DynamicSqlCrossRef(
                loader=loader,
                sql_file=sql_file,
                tables=tables,
                match_kind=match_kind,
            ))
    return out
```

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_analyzer.py -v -k cross_reference_dynamic_sql`
Expected: 5 passed.

### Step 5: Full suite to confirm no regressions

`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: all tests pass (current 311 + 12 dynamic_sql + 6 sql_registry + 5 analyzer = 334).

### Step 6: Commit

```bash
git add skills/open_table_migrator/analyzer.py tests/test_analyzer.py
git commit -m "feat(analyzer): cross_reference_dynamic_sql with full-path and basename resolution"
```

---

## Task 6: CLI + worklist + SKILL.md + final verification

**Files:**
- Modify: `skills/open_table_migrator/cli.py`
- Modify: `skills/open_table_migrator/worklist.py`
- Modify: `skills/open_table_migrator/SKILL.md`

### Step 1: Find cli.py / worklist.py entry points

Run: `grep -n "detect_all_io\|build_worklist\|write_worklist\|scan_sql_files\|cross_reference_sql" skills/open_table_migrator/cli.py skills/open_table_migrator/worklist.py | head -20`

Identify where `scan_sql_files` and existing cross-ref are wired. The new logic goes alongside.

### Step 2: Wire detection + cross-ref in `cli.py`

In `cli.py`'s `convert_project` (or wherever the SQL registry is currently called), after `scan_sql_files(project_root)`, add:

```python
from .dynamic_sql import detect_dynamic_sql_loaders
from .sql_registry import scan_sql_table_references
from .analyzer import cross_reference_dynamic_sql
from .scope import build_const_table
from .ts_parser import language_for_file

def _build_const_for_file(p):
    lang = language_for_file(p)
    if lang is None:
        return None
    return build_const_table(p.read_bytes(), lang, str(p))

dyn_loaders = detect_dynamic_sql_loaders(
    project_root, const_table_for_file=_build_const_for_file,
)
sql_refs = scan_sql_table_references(project_root)
dyn_cross = cross_reference_dynamic_sql(dyn_loaders, sql_defs, sql_refs, project_root)
```

Then thread `dyn_loaders` and `dyn_cross` into the worklist builder call.

### Step 3: Add `dynamic_sql_loaders` to worklist

In `skills/open_table_migrator/worklist.py`, find `build_worklist` (the function that constructs the JSON). Add a parameter `dyn_cross: list[DynamicSqlCrossRef] | None = None`. Inside, before returning the worklist dict, add:

```python
if dyn_cross:
    worklist["dynamic_sql_loaders"] = [
        {
            "file": str(c.loader.file.relative_to(project_root) if project_root in c.loader.file.parents else c.loader.file),
            "line": c.loader.line,
            "pattern": c.loader.pattern,
            "sql_filename": c.loader.sql_filename,
            "confidence": c.loader.confidence,
            "resolved_to": str(c.sql_file.relative_to(project_root) if project_root in c.sql_file.parents else c.sql_file),
            "match_kind": c.match_kind,
            "tables": [
                {
                    "name": t.table_name,
                    "format": t.format,
                    "ddl_file": str(t.file.relative_to(project_root) if project_root in t.file.parents else t.file),
                    "ddl_line": t.line,
                }
                for t in c.tables
            ],
        }
        for c in dyn_cross
    ]
```

The exact location and signature need to match the existing function. Read `worklist.py` carefully and adapt.

### Step 4: Smoke run on existing fixture

Create a small fixture in `tests/fixtures/` (if not exists) with a Python loader and SQL files. Or just smoke against the synthetic-spring-app fixture (won't have Python loaders but verifies no crash):

```bash
PYTHONPATH=. python3 -m skills.open_table_migrator.cli tests/data_lineage/fixtures/synthetic-spring-app --no-deps 2>&1 | tail -10
```
Expected: command completes, no crash.

### Step 5: Add SKILL.md section

In `skills/open_table_migrator/SKILL.md`, find the existing "## Constant folding" section. Insert "## Dynamic SQL loading" BEFORE "## Known Limitations":

```markdown
## Dynamic SQL loading

The detector finds call-sites that load `.sql` files at runtime and cross-references them with parquet/orc tables defined in those (or related) files. This catches the common pattern where SQL is stored separately from code:

```python
sql = open("queries/events_update.sql").read()
spark.sql(sql)
```

### Detected patterns

| Language | Pattern | Example |
|---|---|---|
| Python | `py_open` | `open("x.sql")` |
| Python | `py_path_read_text` | `Path("x.sql").read_text()` |
| Python | `py_pkgutil_get_data` | `pkgutil.get_data(__name__, "x.sql")` |
| Java | `java_files_read` | `Files.readAllBytes(Path.of("x.sql"))` |
| Java/Scala | `java_resource_stream` | `getClass().getResourceAsStream("/sql/x.sql")` |

### What's parsed in the loaded SQL

Beyond `CREATE TABLE ... STORED AS PARQUET` (already handled), the SQL registry now also extracts non-DDL references:

- `INSERT INTO <table>` and `INSERT OVERWRITE TABLE <table>` → write reference
- `UPDATE <table> SET ...` → write reference
- `MERGE INTO <table>` → write reference
- `FROM <table>` / `JOIN <table>` → read reference

CTE names introduced by `WITH <name> AS (...)` are skipped from `FROM`/`JOIN` references.

### Cross-reference behavior

For each loader, the loader's `sql_filename` is resolved against the project tree in three steps:

1. Path relative to the loader's containing file's directory.
2. Path relative to project root.
3. Basename match across all registered `.sql` files (with `match_kind="basename_unique"` or `"basename_ambiguous"`).

Tables mentioned in the resolved SQL file (via CREATE / INSERT / FROM / JOIN / etc.) are joined against the registry of parquet/orc `CREATE TABLE` definitions across all `.sql` files. This handles the common pattern of `schema.sql` (with CREATE) and `queries/*.sql` (with INSERT only).

### Worklist output

Each cross-reference appears in `lakehouse-worklist.json` under `dynamic_sql_loaders`:

```json
{
  "file": "src/jobs/events.py",
  "line": 42,
  "pattern": "py_open",
  "sql_filename": "queries/events_update.sql",
  "confidence": "high",
  "resolved_to": "queries/events_update.sql",
  "match_kind": "exact_path",
  "tables": [
    {"name": "events", "format": "parquet", "ddl_file": "schema.sql", "ddl_line": 8}
  ]
}
```

### Limitations

- SQL templating (Jinja, `.format`, `${...}`) is not parsed.
- In-SQL `\i` / `SOURCE` / `!include` directives are not followed.
- ORM-generated SQL (SQLAlchemy `select(...)`, jOOQ DSL) is not detected.
- Dynamic table names within SQL (`INSERT INTO {schema}.events`) are not resolved.
```

### Step 6: Run full suite

`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: full suite passes (≥ 334).

### Step 7: Clean up smoke-run side effects

```bash
git checkout -- tests/data_lineage/fixtures/ tests/fixtures/ 2>/dev/null
rm -f lakehouse-worklist.json
git status -s
```
Expected: only `cli.py`, `worklist.py`, `SKILL.md` modified.

### Step 8: Commit

```bash
git add skills/open_table_migrator/cli.py skills/open_table_migrator/worklist.py skills/open_table_migrator/SKILL.md
git commit -m "feat(cli): wire dynamic SQL loaders into convert_project + worklist + docs"
```

---
