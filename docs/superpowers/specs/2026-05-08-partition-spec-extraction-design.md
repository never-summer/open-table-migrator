# Partition Spec Extraction — Design

**Date:** 2026-05-08
**Status:** Approved for implementation planning
**Branch:** `feat/partition-spec-extraction`
**Scope ref:** improvement 1.2 from the migrator roadmap

## Goal

When the detector finds a Spark write operation like `df.write.bucketBy(8, "uid").partitionBy("region").saveAsTable("t")`, structurally extract the partition transforms as data so the migration agent can generate the correct Iceberg `PartitionSpec` (PyIceberg builder, Spark `.partitionedBy(...)`, or DDL `PARTITIONED BY` clause). Today the regex-based `summarize_operation` already captures these for human-readable output but does NOT propagate them structurally — the worklist and downstream agent see nothing. This forces the agent to either guess or to re-parse the original source line, defeating the purpose of the detector.

## Non-goals

- Schema generation (`StructType([...])` → Iceberg schema). Different feature, separate spec.
- Time-based partition transforms (`year/month/day/hour`). Spark doesn't put them inside `.partitionBy()` — they're handled via `withColumn(...)` pre-pass, which would require intra-method dataflow.
- `truncate(N, col)` transform. Rare in Spark.
- Hive `CLUSTERED BY (col) INTO N BUCKETS`. Different mechanism than `bucketBy()` in DataFrame API.
- pandas / pyarrow `partition_cols=[...]` kwarg detection. Out of MVP — different API shape, low priority.
- Snippet generation (PyIceberg builder code, `.partitionedBy(...)` syntax, DDL clause). The worklist carries structured data only; agent renders per target context.

## Scope

Two Iceberg partition transforms (option D from brainstorm):

| Spark API | Iceberg transform |
|---|---|
| `.partitionBy("col")`, `.partitionBy("col1", "col2", ...)` | `identity(col)` per arg |
| `.bucketBy(N, "col1", "col2", ...)` | `bucket(N, col)` per col arg |

Detected runtimes:

| Runtime | Status |
|---|---|
| Python PySpark (`df.write.partitionBy/.bucketBy`) | ✓ |
| JVM Spark (Java/Scala DataFrame API) | ✓ |
| Hive DDL in `.sql` files (`PARTITIONED BY (col1, col2)`) | ✓ (identity transforms only — Hive `PARTITIONED BY` doesn't support `bucket`) |
| pandas `pq.write_table(df, path, partition_cols=[...])` | out of scope |
| pyarrow `pa.dataset.write_dataset(...)` with `partitioning=` | out of scope |
| jOOQ DSL | out of scope (no partition in jOOQ model) |

## Architecture

### Data model

New dataclass `PartitionTransform` (in `skills/open_table_migrator/detector.py`):

```python
from typing import Literal


@dataclass(frozen=True)
class PartitionTransform:
    kind: Literal["identity", "bucket"]
    column: str
    n: int | None = None      # only for bucket(N, col)
```

`PatternMatch` gains a new field:

```python
@dataclass
class PatternMatch:
    # ...existing fields...
    attrs: dict[str, str] = field(default_factory=dict)
    partition_spec: tuple[PartitionTransform, ...] = field(default_factory=tuple)
```

`TableDef` (in `sql_registry.py`) gains the same field:

```python
@dataclass(frozen=True)
class TableDef:
    # ...existing fields...
    partition_spec: tuple[PartitionTransform, ...] = ()
```

### Detection algorithm (ts_detector.py)

New helper `_extract_partition_spec(call_chain_root, source, lang, const_table) -> tuple[PartitionTransform, ...]`:

1. Walk the chain of `method_invocation` nodes from the root (the leaf call where the write was confirmed) **upward** to the chain head. In tree-sitter, each `method_invocation` has its receiver in `child_by_field_name("object")`, which is itself a `method_invocation` for chains.
2. At each node, check the method name. If `partitionBy`:
   - For each positional argument that's a string literal, emit `PartitionTransform("identity", col, None)`.
   - For each argument that's an identifier and resolves via `const_table` to a string, emit identity transform with the resolved value.
   - For each argument that's anything else (splat, function call, list, etc.), skip silently.
3. At each node, check if method is `bucketBy`:
   - The first argument must be an integer literal — this is `N`. If it's an identifier or expression, skip the entire `bucketBy` call.
   - For each subsequent string-literal or const-resolvable argument, emit `PartitionTransform("bucket", col, n=N)`.
4. Collect all transforms in chain order (top of chain first), return as tuple.

The helper is called by the existing emit-sites for write operations (saveAsTable, parquet, save, etc.) — at each PatternMatch emission for a write, run the helper and attach the result.

### Detection algorithm (sql_registry.py)

Existing `scan_sql_files` returns `list[TableDef]`. Each `TableDef.partition_spec` is populated by:

1. New regex `_PARTITIONED_BY_RX = re.compile(r"\bPARTITIONED\s+BY\s*\(\s*([^)]+)\)", re.IGNORECASE)`.
2. For each `CREATE TABLE ... PARTITIONED BY (...)` matched in the file, parse the contents of `(...)`:
   - Split by comma.
   - For each part, take the first word (alphanumeric + underscore) as the column name; ignore the type suffix (e.g. `region STRING` → `region`).
3. Build identity transforms for each column. Hive `PARTITIONED BY` cannot express `bucket` — those go through `CLUSTERED BY ... INTO N BUCKETS`, which is out of scope.

### Cross-reference (analyzer.py)

Extend existing `cross_reference_sql` (or its sibling that already joins `PatternMatch` to `TableDef`) so that when both sides have a `partition_spec`:

- If they match (same transforms in same order) — no change.
- If they differ — add to `match.attrs["partition_mismatch"]` a message like `"code: identity(region), bucket(8, uid); ddl: identity(region)"`.

This warning appears in the worklist `attrs` so the agent can flag it.

### Worklist serialization (worklist.py)

For each write-direction entry, if `partition_spec` is non-empty, serialize it as:

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

If `partition_spec` is empty, the field is **omitted entirely** from the JSON (not `[]`). This keeps the worklist diff-clean for write sites that have no partitioning.

For read sites the field is never emitted (partition info isn't actionable for reads).

## Edge cases

| Input | Behavior |
|---|---|
| `.partitionBy()` (no args) | empty list, contributes nothing |
| `.partitionBy(*cols)` (splat) | skip silently — variable list |
| `.partitionBy("a", *more, "b")` | take literal args (`a`, `b`), skip splat |
| `.partitionBy(year("ts"))` (function call) | skip silently — out of scope |
| `.bucketBy(8)` (no column) | skip silently — invalid Spark API |
| `.bucketBy(N_var, "col")` where N is a variable | skip — only integer literal N supported in MVP |
| `.partitionBy("region").partitionBy("date")` (double call in chain) | merge transforms in chain order |
| `.bucketBy(8, "a", "b")` | two bucket transforms, both with n=8 |
| Hive `PARTITIONED BY (region STRING, date DATE)` | two identity transforms (types ignored) |
| Cross-ref: code has `[identity(region)]`, DDL has `[identity(date)]` | mismatch → attrs warning |

## Worklist effect

Write-direction worklist entries gain an optional `partition_spec` field. Downstream agent uses it to generate target-specific syntax:

- PyIceberg builder: `PartitionSpec.builder_for(schema).identity("region").bucket("uid", 8).build()`
- Spark API: `.writeTo("ns.t").using("iceberg").partitionedBy("region", bucket(8, "uid"))`
- DDL: `PARTITIONED BY (region, bucket(8, uid))`

The migrator does **not** generate these snippets. That's the agent's job per target context.

## Testing strategy

### Level 1 — `tests/test_partition_spec.py` (new file, ~12 cases)

**PySpark (4):**
- `df.write.partitionBy("region").saveAsTable("t")` → `[(identity, region)]`
- `df.write.partitionBy("region", "date").saveAsTable("t")` → `[(identity, region), (identity, date)]`
- `df.write.bucketBy(8, "uid").saveAsTable("t")` → `[(bucket, uid, 8)]`
- `df.write.partitionBy("region").bucketBy(8, "uid").saveAsTable("t")` → both transforms

**JVM Spark (3):**
- Java: `df.write().partitionBy("region").saveAsTable("t")`
- Scala: `df.write.partitionBy("region").saveAsTable("t")`
- Scala: `df.write.bucketBy(8, "uid").saveAsTable("t")`

**Const folding (2):**
- Python: `REGION = "region"; df.write.partitionBy(REGION).saveAsTable("t")` → resolves
- Python: `df.write.bucketBy(N_VAR, "col").saveAsTable("t")` where N_VAR is a string → skip the bucket call (N must be int)

**Edge cases (3):**
- `.partitionBy()` → empty
- `.partitionBy(*cols)` → empty (splat skipped)
- `.partitionBy(year("ts"))` → empty (function call skipped)

### Level 2 — `tests/test_sql_registry.py` extensions (~3 cases)

- `CREATE TABLE x (...) PARTITIONED BY (region STRING) STORED AS PARQUET` → `partition_spec = (identity(region),)`
- `CREATE TABLE x (...) PARTITIONED BY (region, date_col) STORED AS PARQUET` → two identity transforms
- `CREATE TABLE x (...) STORED AS PARQUET` (no PARTITIONED BY clause) → empty tuple

### Level 3 — `tests/test_analyzer.py` extensions (~2 cases)

- Code and DDL agree on partitioning → no `partition_mismatch` in attrs
- Code says `[identity(region)]`, DDL says `[identity(date)]` → `attrs.partition_mismatch` populated

### Level 4 — `tests/test_worklist.py` (or extend existing) — 2 cases

- write entry with `partition_spec` → JSON contains `partition_spec` array
- write entry without partition → JSON has no `partition_spec` key (omitted, not empty array)

### Out of scope for tests

- pandas/pyarrow detection (we don't implement these)
- Time-based transforms (year/month/day/hour)
- Splat with `*args` resolution
- Performance benchmarks

## Risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Chain walking from leaf to head misses partition calls between non-write methods (e.g. `.option(...)` between `.partitionBy` and `.saveAsTable`) | low | Walk the full chain, check every node — `.option()` and other non-partition methods just don't match and are skipped |
| `bucketBy(N, col)` with N as an int literal vs `Integer.valueOf(8)` vs explicit cast | low | Only accept tree-sitter `integer_literal` (Python/Java) / `decimal_integer_literal` (Scala) for N. Other forms skip |
| DDL `PARTITIONED BY` parsing breaks on nested parens (CHAR(20) inside partition cols) | very low | Hive `PARTITIONED BY` doesn't allow parameterized types in the partition list (only `col TYPE` simple form). The regex `[^)]+` is fine |
| `partitionBy(col_str_const)` where const isn't in current file | low | Const-folding resolves only intra-file. Cross-module imports are out of scope (consistent with 1.1) |
| Two consecutive `partitionBy` calls in chain are unusual and may not match real Spark semantics | low | Spark actually keeps both — they merge. The transform list reflects this honestly |

## Estimated size

- Production: ~80 LOC in `ts_detector.py` (new helper `_extract_partition_spec` + integration with existing emit-sites) + ~30 LOC in `sql_registry.py` (regex + TableDef field) + ~30 LOC in `analyzer.py` (cross-ref mismatch detection) + ~20 LOC in `worklist.py` (serialization)
- Tests: ~150 LOC across `test_partition_spec.py`, `test_sql_registry.py`, `test_analyzer.py`, `test_worklist.py` (or test_analyzer for worklist tests if no separate file exists)
- Documentation: ~25 lines in SKILL.md
- Total: ~280 LOC. Two implementation sessions.

## Open questions deferred to implementation

- Exact name of the `attrs` key for mismatch: `partition_mismatch` (chosen) vs `partition_spec_mismatch`.
- Whether to dedupe transforms when the same column appears in two chained `partitionBy` calls — leave as-is (preserves user intent, no surprises).
- Whether mismatch detection happens in `cross_reference_sql` or in a new function — decide based on existing code organization at impl time.
- Handling of backticks/quotes in DDL partition column names (`PARTITIONED BY (\`region\`)`) — strip in impl, no test coverage needed in MVP.
