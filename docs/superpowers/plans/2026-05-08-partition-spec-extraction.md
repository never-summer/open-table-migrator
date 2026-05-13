# Partition Spec Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture `.partitionBy()` / `.bucketBy()` from Spark write chains and `PARTITIONED BY (...)` from Hive DDL as structured `PartitionTransform` data, propagate through `PatternMatch` / `TableDef` / worklist so the migration agent can render the correct Iceberg partition spec per target.

**Architecture:** New `PartitionTransform` frozen dataclass in `detector.py`. `PatternMatch` and `TableDef` gain `partition_spec: tuple[PartitionTransform, ...]` field. New helper `_extract_partition_spec` in `ts_detector.py` walks the method-invocation chain at every write emit-site. `sql_registry.py` extracts Hive `PARTITIONED BY` via regex. `cross_reference_sql` detects code↔DDL mismatches into `attrs.partition_mismatch`. Worklist serializes the field for write-direction entries only, omitting the key when empty.

**Tech Stack:** Python 3.11+, `tree-sitter` (already used) + `re`. No new dependencies.

**Spec:** [docs/superpowers/specs/2026-05-08-partition-spec-extraction-design.md](../specs/2026-05-08-partition-spec-extraction-design.md)

---

## File Structure

Modified files:
- `skills/open_table_migrator/detector.py` — add `PartitionTransform` dataclass + `partition_spec` field on `PatternMatch`
- `skills/open_table_migrator/ts_detector.py` — add `_extract_partition_spec` helper + thread it through write emit-sites
- `skills/open_table_migrator/sql_registry.py` — `_PARTITIONED_BY_RX` regex + `partition_spec` field on `TableDef` + populate during scan
- `skills/open_table_migrator/analyzer.py` — cross-ref mismatch detection populating `attrs["partition_mismatch"]`
- `skills/open_table_migrator/worklist.py` — serialize `partition_spec` for write entries (omitted when empty)
- `skills/open_table_migrator/SKILL.md` — new "Partition spec extraction" section

New tests:
- `tests/test_partition_spec.py` — detector unit tests (~12 cases)
- `tests/test_sql_registry.py` — extend with Hive `PARTITIONED BY` cases (~3 cases)
- `tests/test_analyzer.py` — extend with code↔DDL mismatch cases (~2 cases)
- `tests/test_worklist.py` (or extend `test_analyzer.py` if no separate worklist test file exists) — serialization (~2 cases)

---

## Task 1: PartitionTransform dataclass + PatternMatch field

**Files:**
- Modify: `skills/open_table_migrator/detector.py`
- Create or modify: `tests/test_partition_spec.py`

### Step 1: Failing test

`tests/test_partition_spec.py`:
```python
from skills.open_table_migrator.detector import PartitionTransform, PatternMatch
from pathlib import Path


def test_partition_transform_identity():
    t = PartitionTransform(kind="identity", column="region")
    assert t.column == "region"
    assert t.n is None


def test_partition_transform_bucket():
    t = PartitionTransform(kind="bucket", column="uid", n=8)
    assert t.n == 8


def test_pattern_match_has_empty_partition_spec_by_default():
    m = PatternMatch(
        file=Path("x.py"), line=1, pattern_type="spark_write_parquet",
        original_code="df.write.parquet('x')",
    )
    assert m.partition_spec == ()


def test_pattern_match_with_partition_spec():
    spec = (
        PartitionTransform(kind="identity", column="region"),
        PartitionTransform(kind="bucket", column="uid", n=8),
    )
    m = PatternMatch(
        file=Path("x.py"), line=1, pattern_type="spark_write_parquet",
        original_code="df.write.parquet('x')",
        partition_spec=spec,
    )
    assert len(m.partition_spec) == 2
    assert m.partition_spec[0].kind == "identity"
    assert m.partition_spec[1].n == 8
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_partition_spec.py -v`
Expected: `ImportError: cannot import name 'PartitionTransform'`.

### Step 3: Add `PartitionTransform` + extend `PatternMatch` in `skills/open_table_migrator/detector.py`

Find the existing `PatternMatch` definition. Replace the imports/preamble and class:

```python
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class PartitionTransform:
    kind: Literal["identity", "bucket"]
    column: str
    n: int | None = None      # only for bucket(N, col)


@dataclass
class PatternMatch:
    file: Path
    line: int
    pattern_type: str
    original_code: str
    path_arg: str | None = None
    end_line: int | None = None
    format: str | None = None
    attrs: dict[str, str] = field(default_factory=dict)
    partition_spec: tuple[PartitionTransform, ...] = field(default_factory=tuple)

    @property
    def direction(self) -> str:
        from .analyzer import direction_of
        return direction_of(self.pattern_type)
```

The `partition_spec` field defaults to an empty tuple so all existing call-sites that build PatternMatch without it continue to work.

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_partition_spec.py -v`
Expected: 4 passed.

### Step 5: Run full suite to confirm no regressions

`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: existing tests + 4 new = no regressions.

### Step 6: Commit

```bash
git add skills/open_table_migrator/detector.py tests/test_partition_spec.py
git commit -m "feat(detector): PartitionTransform dataclass + PatternMatch.partition_spec"
```

---

## Task 2: ts_detector — chain walker + integration with write emit-sites

**Files:**
- Modify: `skills/open_table_migrator/ts_detector.py`
- Modify: `tests/test_partition_spec.py`

### Step 1: Append failing tests for detection

```python
from pathlib import Path
from textwrap import dedent

from skills.open_table_migrator.detector import detect_all_io


def test_pyspark_partition_by_single_column(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy("region").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    assert len(write_matches) == 1
    spec = write_matches[0].partition_spec
    assert spec == (PartitionTransform(kind="identity", column="region"),)


def test_pyspark_partition_by_multiple_columns(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy("region", "date").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    spec = write_matches[0].partition_spec
    assert PartitionTransform(kind="identity", column="region") in spec
    assert PartitionTransform(kind="identity", column="date") in spec


def test_pyspark_bucket_by(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.bucketBy(8, "uid").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    spec = write_matches[0].partition_spec
    assert spec == (PartitionTransform(kind="bucket", column="uid", n=8),)


def test_pyspark_partition_and_bucket_combined(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy("region").bucketBy(8, "uid").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    spec = write_matches[0].partition_spec
    assert PartitionTransform(kind="identity", column="region") in spec
    assert PartitionTransform(kind="bucket", column="uid", n=8) in spec


def test_scala_partition_by(tmp_path: Path):
    (tmp_path / "Job.scala").write_text(dedent('''
        object Job {
            def run(): Unit = {
                df.write.partitionBy("region").saveAsTable("t")
            }
        }
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    spec = write_matches[0].partition_spec
    assert PartitionTransform(kind="identity", column="region") in spec


def test_java_partition_by(tmp_path: Path):
    (tmp_path / "Job.java").write_text(dedent('''
        public class Job {
            void run() {
                df.write().partitionBy("region").saveAsTable("t");
            }
        }
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    spec = write_matches[0].partition_spec
    assert PartitionTransform(kind="identity", column="region") in spec


def test_partition_by_empty_call(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy().saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    assert write_matches[0].partition_spec == ()


def test_partition_by_splat_skipped(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        cols = ["region"]
        df.write.partitionBy(*cols).saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    assert write_matches[0].partition_spec == ()


def test_partition_by_function_call_skipped(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy(year("ts")).saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    # year("ts") is a function call, not a literal — skip
    assert all(t.column != "ts" for t in write_matches[0].partition_spec)


def test_bucket_by_n_as_identifier_skipped(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        N = 8
        df.write.bucketBy(N, "uid").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    # N is an identifier, not an integer literal — skip the whole bucket call (MVP)
    assert all(t.kind != "bucket" for t in write_matches[0].partition_spec)


def test_partition_by_with_const_table_identifier(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        REGION = "region"
        df.write.partitionBy(REGION).saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    spec = write_matches[0].partition_spec
    # REGION resolves to "region" via const-folding (1.1)
    assert PartitionTransform(kind="identity", column="region") in spec
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_partition_spec.py -v -k "pyspark or scala or java or partition_by or bucket"`
Expected: all 11 new detection tests fail (chain walker not yet implemented).

### Step 3: Add `_extract_partition_spec` helper

In `skills/open_table_migrator/ts_detector.py`, near the existing helpers (around line 100, after `_get_args_node`), add:

```python
from .detector import PartitionTransform


def _integer_literal_value(node, source: bytes, lang: str) -> int | None:
    """Return the integer value of a tree-sitter literal node, or None."""
    if lang == "python" and node.type == "integer":
        try:
            return int(source[node.start_byte:node.end_byte].decode())
        except ValueError:
            return None
    if lang == "java" and node.type == "decimal_integer_literal":
        try:
            return int(source[node.start_byte:node.end_byte].decode())
        except ValueError:
            return None
    if lang == "scala" and node.type in ("integer_literal", "decimal_integer_literal"):
        try:
            return int(source[node.start_byte:node.end_byte].decode())
        except ValueError:
            return None
    return None


def _resolve_string_or_const(node, source: bytes, lang: str, const_table) -> str | None:
    """Try to resolve an argument node to a string literal value.

    Returns the string for literal args; resolves identifiers via const_table.
    Returns None for any other shape (splat, function call, list, etc.).
    """
    val = _extract_string(node, lang)
    if val is not None:
        return val
    if node.type == "identifier" and const_table is not None:
        name = source[node.start_byte:node.end_byte].decode()
        binding = const_table.resolve(name)
        if binding is not None and binding.value is not None:
            return binding.value
    return None


def _extract_partition_spec(
    call_node, source: bytes, lang: str, const_table,
) -> tuple[PartitionTransform, ...]:
    """Walk up the method-invocation chain from `call_node`, collect
    partitionBy / bucketBy transforms.

    Chain order: top of the chain comes first. For `df.write.partitionBy("region")
    .bucketBy(8, "uid").saveAsTable("t")` walked from `saveAsTable` upward,
    we visit `bucketBy` then `partitionBy`. Result preserves chain order
    so `partitionBy` transforms appear first in the result.
    """
    chain: list = []
    cur = call_node
    while cur is not None and cur.type in ("call", "method_invocation", "call_expression"):
        chain.append(cur)
        # Walk up via the receiver
        if lang == "python":
            func = cur.child_by_field_name("function")
            if func is None or func.type != "attribute":
                break
            cur = func.child_by_field_name("object")
            if cur is not None and cur.type == "call":
                continue
            else:
                break
        elif lang == "java":
            obj = cur.child_by_field_name("object")
            if obj is None or obj.type != "method_invocation":
                break
            cur = obj
        elif lang == "scala":
            func = cur.child_by_field_name("function")
            if func is None or func.type != "field_expression":
                break
            obj = func.child_by_field_name("value")
            if obj is None:
                obj = func.children[0] if func.children else None
            if obj is None or obj.type != "call_expression":
                break
            cur = obj
        else:
            break

    # Walk the chain from the TOP downwards (reverse the list since we appended
    # leaf-to-top): the top contains the first method called like `.partitionBy()`.
    transforms: list[PartitionTransform] = []
    for node in reversed(chain):
        method_name = _get_method_name(node, lang)
        if method_name == "partitionBy":
            args = _get_args_node(node, lang)
            if args is None:
                continue
            for arg in args.named_children:
                col = _resolve_string_or_const(arg, source, lang, const_table)
                if col is not None:
                    transforms.append(PartitionTransform(
                        kind="identity", column=col, n=None,
                    ))
        elif method_name == "bucketBy":
            args = _get_args_node(node, lang)
            if args is None or args.named_child_count < 2:
                continue
            n = _integer_literal_value(args.named_children[0], source, lang)
            if n is None:
                continue  # N must be int literal
            for arg in args.named_children[1:]:
                col = _resolve_string_or_const(arg, source, lang, const_table)
                if col is not None:
                    transforms.append(PartitionTransform(
                        kind="bucket", column=col, n=n,
                    ))
    return tuple(transforms)
```

### Step 4: Integrate into write emit-sites

In `_detect_calls_in_tree` (around line 599 in `ts_detector.py`), find each `PatternMatch(...)` construction that emits a **write**-direction pattern. For each one, before the construction, compute the partition spec and pass it to `PatternMatch`.

The cleanest approach: after the detector confirms a write pattern at `call_node`, call:
```python
partition_spec = _extract_partition_spec(call_node, source, lang, const_table)
```
Then pass `partition_spec=partition_spec` to the `PatternMatch(...)` constructor.

This change touches multiple emit sites. The simplest is to make `_extract_partition_spec` cheap when there's no chain (returns `()` quickly when the call has no methods named partitionBy/bucketBy upstream) and call it unconditionally for every PatternMatch construction whose pattern_type indicates "write":

```python
# In _detect_python_lib and _detect_spark_chain and _detect_calls_in_tree
# wherever a write-direction PatternMatch is built, add:
partition_spec = _extract_partition_spec(call_node, source, lang, const_table)

return PatternMatch(
    file=file_path, line=line, pattern_type=pattern_type,
    path_arg=first_str, end_line=end_line, format=fmt,
    original_code=code, attrs=attrs,
    partition_spec=partition_spec,
)
```

Read the current state of these functions, locate every write-direction emit-site (search for `pattern_type` strings containing `write`), add the partition_spec computation just before each.

### Step 5: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_partition_spec.py -v`
Expected: 15 passed (4 from Task 1 + 11 new detection tests).

### Step 6: Full suite

`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: pre-existing + 15 new = no regressions.

### Step 7: Commit

```bash
git add skills/open_table_migrator/ts_detector.py tests/test_partition_spec.py
git commit -m "feat(ts-detector): _extract_partition_spec walker + integration with write emit-sites"
```

---

## Task 3: sql_registry — Hive PARTITIONED BY + TableDef.partition_spec

**Files:**
- Modify: `skills/open_table_migrator/sql_registry.py`
- Modify: `tests/test_sql_registry.py`

### Step 1: Append failing tests

`tests/test_sql_registry.py`:
```python
def test_hive_partitioned_by_single_column(tmp_path):
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) "
        "PARTITIONED BY (region STRING) "
        "STORED AS PARQUET;"
    )
    from skills.open_table_migrator.sql_registry import scan_sql_files
    from skills.open_table_migrator.detector import PartitionTransform
    defs = scan_sql_files(tmp_path)
    assert len(defs) == 1
    assert defs[0].partition_spec == (
        PartitionTransform(kind="identity", column="region"),
    )


def test_hive_partitioned_by_multiple_columns(tmp_path):
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) "
        "PARTITIONED BY (region STRING, date_col DATE) "
        "STORED AS PARQUET;"
    )
    from skills.open_table_migrator.sql_registry import scan_sql_files
    from skills.open_table_migrator.detector import PartitionTransform
    defs = scan_sql_files(tmp_path)
    assert len(defs) == 1
    cols = [t.column for t in defs[0].partition_spec]
    assert "region" in cols
    assert "date_col" in cols
    assert all(t.kind == "identity" for t in defs[0].partition_spec)


def test_hive_no_partitioned_by(tmp_path):
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) STORED AS PARQUET;"
    )
    from skills.open_table_migrator.sql_registry import scan_sql_files
    defs = scan_sql_files(tmp_path)
    assert len(defs) == 1
    assert defs[0].partition_spec == ()
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_sql_registry.py -v -k partitioned_by`
Expected: 3 fail (TableDef has no partition_spec field).

### Step 3: Extend `TableDef` and parser in `skills/open_table_migrator/sql_registry.py`

Add import at top:
```python
from .detector import PartitionTransform
```

Modify `TableDef`:
```python
@dataclass
class TableDef:
    """A table definition extracted from a SQL file."""
    table_name: str
    database: str | None
    format: str
    file: Path
    line: int
    snippet: str
    partition_spec: tuple[PartitionTransform, ...] = ()
```

Add regex near the other patterns:
```python
_PARTITIONED_BY_RX = re.compile(
    r"\bPARTITIONED\s+BY\s*\(\s*([^)]+)\)",
    re.IGNORECASE,
)
```

Add helper:
```python
def _parse_partition_clause(content: str, ddl_start: int, ddl_end: int) -> tuple[PartitionTransform, ...]:
    """Look for PARTITIONED BY (...) inside the substring [ddl_start:ddl_end+] of content.

    Returns identity transforms for each column. Types are ignored.
    """
    # Search forward from ddl_start for the PARTITIONED BY clause that belongs
    # to this CREATE TABLE. Allow up to ~2000 chars window after ddl_end.
    window_end = min(len(content), ddl_end + 2000)
    window = content[ddl_start:window_end]
    m = _PARTITIONED_BY_RX.search(window)
    if not m:
        return ()
    cols_text = m.group(1)
    transforms: list[PartitionTransform] = []
    for part in cols_text.split(","):
        part = part.strip().strip("`").strip('"')
        if not part:
            continue
        # Take the first whitespace-separated token as the column name
        col_name = part.split()[0].strip("`").strip('"')
        if col_name:
            transforms.append(PartitionTransform(
                kind="identity", column=col_name, n=None,
            ))
    return tuple(transforms)
```

In `scan_sql_files`, in the existing loop where `TableDef` is appended, modify to compute partition_spec:

```python
for rx in (_CREATE_STORED_AS, _CREATE_USING, _CTAS_STORED_AS):
    for m in rx.finditer(content):
        db = m.group(1)
        table = m.group(2)
        fmt = m.group(3).lower()
        line = content[:m.start()].count("\n") + 1
        snippet = m.group(0).strip()[:200]
        partition_spec = _parse_partition_clause(content, m.start(), m.end())
        defs.append(TableDef(
            table_name=table,
            database=db,
            format=fmt,
            file=sql_file,
            line=line,
            snippet=snippet,
            partition_spec=partition_spec,
        ))
```

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_sql_registry.py -v -k partitioned_by`
Expected: 3 passed.

### Step 5: Full sql_registry suite

`PYTHONPATH=. python3 -m pytest tests/test_sql_registry.py -v`
Expected: all sql_registry tests pass.

### Step 6: Commit

```bash
git add skills/open_table_migrator/sql_registry.py tests/test_sql_registry.py
git commit -m "feat(sql-registry): PARTITIONED BY parsing into TableDef.partition_spec"
```

---

## Task 4: analyzer — code↔DDL partition mismatch detection

**Files:**
- Modify: `skills/open_table_migrator/analyzer.py`
- Modify: `tests/test_analyzer.py`

### Step 1: Append failing tests

`tests/test_analyzer.py`:
```python
def test_partition_mismatch_when_code_and_ddl_differ(tmp_path):
    """Code partitions by region, DDL partitions by date → mismatch flagged."""
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) PARTITIONED BY (date_col STRING) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'df.write.partitionBy("region").saveAsTable("events")\n'
    )
    from skills.open_table_migrator.detector import detect_all_io
    from skills.open_table_migrator.sql_registry import scan_sql_files
    from skills.open_table_migrator.analyzer import annotate_partition_mismatch

    matches = detect_all_io(tmp_path)
    defs = scan_sql_files(tmp_path)
    annotate_partition_mismatch(matches, defs)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    assert len(write_matches) == 1
    assert "partition_mismatch" in write_matches[0].attrs
    msg = write_matches[0].attrs["partition_mismatch"]
    assert "region" in msg
    assert "date_col" in msg


def test_no_partition_mismatch_when_code_and_ddl_agree(tmp_path):
    """Code and DDL both partition by region → no mismatch."""
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) PARTITIONED BY (region STRING) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'df.write.partitionBy("region").saveAsTable("events")\n'
    )
    from skills.open_table_migrator.detector import detect_all_io
    from skills.open_table_migrator.sql_registry import scan_sql_files
    from skills.open_table_migrator.analyzer import annotate_partition_mismatch

    matches = detect_all_io(tmp_path)
    defs = scan_sql_files(tmp_path)
    annotate_partition_mismatch(matches, defs)
    write_matches = [m for m in matches if "write" in m.pattern_type]
    assert "partition_mismatch" not in write_matches[0].attrs
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_analyzer.py -v -k partition_mismatch`
Expected: 2 fail (function doesn't exist).

### Step 3: Implement `annotate_partition_mismatch` in `skills/open_table_migrator/analyzer.py`

Append to the file:
```python
from .detector import PartitionTransform


def _format_transforms(transforms: tuple[PartitionTransform, ...]) -> str:
    """Render a transform tuple as a stable readable string."""
    parts = []
    for t in transforms:
        if t.kind == "identity":
            parts.append(f"identity({t.column})")
        elif t.kind == "bucket":
            parts.append(f"bucket({t.n}, {t.column})")
    return ", ".join(parts) if parts else "none"


def annotate_partition_mismatch(
    matches: list[PatternMatch],
    sql_defs: list[TableDef],
) -> None:
    """Compare each write-direction match's partition_spec with the SQL
    registry's partition_spec for the same table name. When they differ,
    populate match.attrs['partition_mismatch'] with a description.

    Operates in-place on the matches list. Skips matches that have no
    partition_spec on either side.
    """
    defs_by_name: dict[str, TableDef] = {}
    for d in sql_defs:
        defs_by_name[d.table_name.lower()] = d
        if d.database:
            defs_by_name[f"{d.database.lower()}.{d.table_name.lower()}"] = d

    for m in matches:
        if direction_of(m.pattern_type) != "write":
            continue
        if not m.path_arg:
            continue
        key = m.path_arg.strip("`").lower()
        d = defs_by_name.get(key)
        if d is None and "." in key:
            d = defs_by_name.get(key.rsplit(".", 1)[-1])
        if d is None:
            continue
        if not m.partition_spec and not d.partition_spec:
            continue
        if set(m.partition_spec) == set(d.partition_spec):
            continue
        m.attrs["partition_mismatch"] = (
            f"code: {_format_transforms(m.partition_spec)}; "
            f"ddl: {_format_transforms(d.partition_spec)}"
        )
```

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_analyzer.py -v -k partition_mismatch`
Expected: 2 passed.

### Step 5: Wire into CLI

In `skills/open_table_migrator/cli.py` (or wherever the matches/sql_defs are first available together), add a call after both have been computed:

Find the line in `convert_project` (or its callee) where both `matches` from detector and `sql_defs` from `scan_sql_files` exist. After both calls, add:

```python
from .analyzer import annotate_partition_mismatch
annotate_partition_mismatch(matches, sql_defs)
```

This mutates `matches` in-place so downstream worklist serialization sees the populated `attrs`. If `cli.py` already has analyzer imports, add `annotate_partition_mismatch` to the existing one.

### Step 6: Commit

```bash
git add skills/open_table_migrator/analyzer.py skills/open_table_migrator/cli.py tests/test_analyzer.py
git commit -m "feat(analyzer): annotate_partition_mismatch detects code↔DDL partition divergence"
```

---

## Task 5: worklist serialization + SKILL.md + final verification

**Files:**
- Modify: `skills/open_table_migrator/worklist.py`
- Modify: `skills/open_table_migrator/SKILL.md`
- Create: `tests/test_worklist.py` (or extend existing if it exists — check first)

### Step 1: Failing tests

Run `ls tests/test_worklist.py 2>&1` to check if the file exists. If yes, append; if no, create.

`tests/test_worklist.py`:
```python
from pathlib import Path
from textwrap import dedent

from skills.open_table_migrator.detector import (
    PartitionTransform, PatternMatch, detect_all_io,
)
from skills.open_table_migrator.targets import Target, constant_resolver
from skills.open_table_migrator.worklist import build_worklist


def test_write_entry_serializes_partition_spec(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy("region").bucketBy(8, "uid").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    resolver = constant_resolver(Target(namespace="analytics", table="t"))
    entries = build_worklist(matches, tmp_path, resolver)
    write_entries = [e for e in entries if e.direction == "write"]
    assert len(write_entries) >= 1
    e = write_entries[0]
    assert hasattr(e, "partition_spec")
    cols = {t["column"] for t in e.partition_spec}
    assert "region" in cols
    assert "uid" in cols
    kinds = {t["kind"] for t in e.partition_spec}
    assert "identity" in kinds
    assert "bucket" in kinds


def test_write_entry_without_partition_omits_field(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    resolver = constant_resolver(Target(namespace="analytics", table="t"))
    entries = build_worklist(matches, tmp_path, resolver)
    write_entries = [e for e in entries if e.direction == "write"]
    assert len(write_entries) >= 1
    e = write_entries[0]
    blob = e.to_dict()
    # Empty partition spec → field omitted from JSON
    assert "partition_spec" not in blob
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_worklist.py -v`
Expected: tests fail because `WorklistEntry` doesn't carry partition_spec yet.

### Step 3: Modify `WorklistEntry` and `build_worklist`

In `skills/open_table_migrator/worklist.py`:

Add to `WorklistEntry`:
```python
@dataclass
class WorklistEntry:
    # ...existing fields...
    hint: str

    # NEW: partition spec serialized as list of dicts. Empty list means
    # no partitioning (the field is then OMITTED from JSON via to_dict()).
    partition_spec: list = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        # Omit empty partition_spec for clean JSON diffs
        if not d.get("partition_spec"):
            d.pop("partition_spec", None)
        return d
```

Make sure `field` is imported: `from dataclasses import asdict, dataclass, field`.

In `build_worklist`, when constructing the `WorklistEntry`, add:
```python
ps = []
if direction == "write" and m.partition_spec:
    ps = [
        {"kind": t.kind, "column": t.column, **({"n": t.n} if t.n is not None else {})}
        for t in m.partition_spec
    ]

entries.append(WorklistEntry(
    # ...all existing fields...
    hint=_hint_for(m.pattern_type, direction, decision),
    partition_spec=ps,
))
```

The `partition_spec` list contains plain dicts so `to_dict()` (via `asdict`) serializes cleanly.

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_worklist.py -v`
Expected: 2 passed.

### Step 5: Full suite

`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: pre-existing + new tests pass, no regressions.

### Step 6: Add SKILL.md section

In `skills/open_table_migrator/SKILL.md`, insert a new section "## Partition spec extraction" **just before** "## Known Limitations" (or at the end if Known Limitations doesn't exist):

```markdown
## Partition spec extraction

When a Spark write site uses `.partitionBy(...)` or `.bucketBy(N, ...)`, the detector captures the partition specification structurally and propagates it into the worklist so the migration agent can generate the correct Iceberg `PartitionSpec` per target.

### Supported transforms (MVP)

| Spark API | Iceberg transform |
|---|---|
| `.partitionBy("col")` | `identity(col)` |
| `.partitionBy("col1", "col2", ...)` | one `identity(col)` per arg |
| `.bucketBy(N, "col1", "col2", ...)` | one `bucket(N, col)` per col arg |

Time-based transforms (`year`/`month`/`day`/`hour`) and `truncate(N, col)` are out of MVP scope — Spark uses these through `withColumn(...)` pre-pass + identity partitioning, which would require intra-method data flow.

### Detection scope

- **Python PySpark** — `df.write.partitionBy/.bucketBy`
- **JVM Spark** — Java/Scala DataFrame API
- **Hive DDL in `.sql` files** — `PARTITIONED BY (col1, col2)` (identity transforms only; Hive `PARTITIONED BY` has no `bucket(N)` form)

Constants used as args resolve via the const-folding module (1.1): `REGION = "region"; df.write.partitionBy(REGION)` resolves to `identity(region)`.

### Edge cases

- `partitionBy()` (no args) → no transforms
- `partitionBy(*cols)` (splat) → skipped silently
- `partitionBy(year("ts"))` (function call) → skipped silently (out of scope)
- `bucketBy(N_var, "col")` where N is an identifier → skipped (MVP requires int literal for N)
- Multiple `partitionBy` calls in one chain → merged in chain order

### Worklist output

Write-direction entries gain an optional `partition_spec` array:

```json
{
  "site": "src/jobs/users.py:54",
  "kind": "write",
  "target": "analytics.users",
  "partition_spec": [
    {"kind": "identity", "column": "region"},
    {"kind": "bucket", "column": "uid", "n": 8}
  ]
}
```

When no partitions are detected, the field is **omitted** from the JSON (not `[]`).

### Code↔DDL mismatch detection

If both `df.write.partitionBy(...).saveAsTable(t)` (code) and `CREATE TABLE t ... PARTITIONED BY (...)` (DDL in `.sql` file) exist for the same table, the analyzer compares both `partition_spec` values. If they diverge, the match's `attrs["partition_mismatch"]` is populated:

```
code: identity(region); ddl: identity(date_col)
```

The agent sees this warning and can flag it before applying the rewrite.
```

### Step 7: Smoke run

```bash
mkdir -p /tmp/partition-smoke
cat > /tmp/partition-smoke/job.py <<'EOF'
df.write.partitionBy("region").bucketBy(8, "uid").saveAsTable("analytics.users")
EOF
PYTHONPATH=. python3 -m skills.open_table_migrator.cli /tmp/partition-smoke --table users --namespace analytics --no-deps 2>&1 | tail -5
python3 -c "import json; b=json.load(open('/tmp/partition-smoke/lakehouse-worklist.json')); \
    write_entries = [e for e in b['entries'] if e['direction'] == 'write']; \
    print('partition_spec:', write_entries[0].get('partition_spec'))"
rm -rf /tmp/partition-smoke
```
Expected output contains:
```
partition_spec: [{'kind': 'identity', 'column': 'region'}, {'kind': 'bucket', 'column': 'uid', 'n': 8}]
```

### Step 8: Commit

```bash
git add skills/open_table_migrator/worklist.py skills/open_table_migrator/SKILL.md tests/test_worklist.py
git commit -m "feat(worklist): serialize partition_spec for write entries; SKILL.md docs"
```

---
