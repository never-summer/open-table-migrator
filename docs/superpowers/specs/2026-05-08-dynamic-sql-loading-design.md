# Dynamic SQL Loading Detection — Design

**Date:** 2026-05-08
**Status:** Approved for implementation planning
**Branch:** `feat/dynamic-sql-loading`
**Scope ref:** improvement 1.3 from the migrator roadmap

## Goal

Detect call-sites in Python/Java/Scala that load SQL files at runtime (`open("queries/events.sql").read()`, `Files.readAllBytes(Path.of("...sql"))`, `getResourceAsStream("...sql")`, etc.) and cross-reference them with parquet/orc tables defined or referenced in those SQL files. Today the migrator's `sql_registry` parses `.sql/.hql/.ddl` for `CREATE TABLE ... STORED AS PARQUET` but cannot link the runtime loader to the contents. The subagent prompt currently instructs users to "grep manually for `open(.*\.sql`" — this design folds that grep into the detector and produces actionable cross-references in the worklist.

## Non-goals

- No parsing of SQL templating (Jinja, `.format`, `${...}`).
- No following of in-SQL include directives (`\i`, `SOURCE`, `!include`) — those are a separate concern (potential follow-up 1.3.bis).
- No support for ORM-generated SQL (SQLAlchemy `select(...)`, jOOQ DSL, etc.) — different category.
- No support for dynamic table names embedded in SQL (`INSERT INTO {schema}.events`).
- No code rewriting. The output is signal, not transformations. Loader call-sites remain untouched; the agent decides what to do with the cross-references.

## Scope

Five loader patterns across three languages (B option from brainstorm):

| Language | Pattern key | API |
|---|---|---|
| Python | `py_open` | `open("*.sql")` (optionally `.read()` chained, optionally inside `with` block) |
| Python | `py_path_read_text` | `Path("*.sql").read_text()` and `Path("*.sql").read_bytes()` |
| Python | `py_pkgutil_get_data` | `pkgutil.get_data(<pkg>, "*.sql")` — 2nd positional arg |
| Java | `java_files_read` | `Files.readAllBytes(Path.of("*.sql"))`, `Files.readString(Path.of(...))`, `Files.readAllBytes(Paths.get("*.sql"))` |
| Java/Scala | `java_resource_stream` | `<receiver>.getResourceAsStream("*.sql")` — receiver can be `this.getClass()`, `getClass()`, `ClassLoader.getSystemResourceAsStream`, etc. |

Argument resolution:
- **String literal** → `confidence="high"`.
- **Identifier** + `scope.build_const_table` resolves to literal → `confidence="medium"`.
- **Identifier** without resolution → skipped.
- **Anything else** (expression, f-string, etc.) → skipped.

Only filenames ending in `.sql`, `.hql`, or `.ddl` (case-insensitive). Loaders that read other file types are out of scope.

## Architecture

### New module: `skills/open_table_migrator/dynamic_sql.py`

```python
@dataclass(frozen=True)
class DynamicSqlLoader:
    file: Path            # source file containing the call
    line: int
    pattern: str          # one of the five pattern keys above
    sql_filename: str     # the string argument passed to the loader (verbatim)
    confidence: str       # "high" | "medium"


def detect_dynamic_sql_loaders(
    project_root: Path,
    *,
    const_table_for_file: Callable[[Path], ConstTable] | None = None,
) -> list[DynamicSqlLoader]:
    """Walk .py/.java/.scala in project_root; return all detected loader sites.

    When const_table_for_file is provided, identifier arguments are resolved
    via the per-file const_table (from skills.open_table_migrator.scope).
    """
```

Per-language dispatch via separate helpers (`_detect_python_loaders`, `_detect_java_loaders`, `_detect_scala_loaders`), each walking the tree-sitter AST and recognising one or more of the five patterns. The dispatcher matches `tools_ts_detector.py`'s structure.

### Extensions to existing `skills/open_table_migrator/sql_registry.py`

Existing module parses `CREATE TABLE ... STORED AS PARQUET/ORC`. We extend it to also collect `INSERT`, `UPDATE`, `MERGE`, `FROM`, `JOIN` references.

```python
@dataclass(frozen=True)
class TableReference:
    """Non-DDL reference to a table — INSERT INTO / FROM / JOIN / UPDATE / MERGE."""
    table_name: str
    database: str | None
    role: str             # "write" (INSERT/UPDATE/MERGE) | "read" (FROM/JOIN)
    sql_file: Path
    line: int


def scan_sql_table_references(project_root: Path) -> list[TableReference]:
    """Scan .sql/.hql/.ddl for non-DDL table references (alongside existing scan_sql_files)."""
```

Regex patterns (case-insensitive, applied line by line for line-accurate locations):

| Statement | Pattern (simplified) | Role |
|---|---|---|
| INSERT INTO | `INSERT\s+INTO\s+(?:TABLE\s+)?<table>` | write |
| INSERT OVERWRITE | `INSERT\s+OVERWRITE\s+(?:TABLE\s+)?<table>` | write |
| UPDATE | `UPDATE\s+<table>\s+SET` | write |
| MERGE INTO | `MERGE\s+INTO\s+<table>` | write |
| FROM | `\bFROM\s+<table>\b` | read |
| JOIN | `\bJOIN\s+<table>\b` | read |

`<table>` is `[\w.`"]+` (allows backtick-, double-quote-, and dot-prefixed `db.table` forms). Trailing `;`, parens, and whitespace stripped.

**CTE filtering:** before recording `FROM`/`JOIN` references, scan for `WITH\s+<name>\s+AS\s*\(` definitions in the same file. References whose `table_name` matches a CTE name in the same file are skipped (CTEs are aliases, not tables).

### Extensions to `skills/open_table_migrator/analyzer.py`

```python
@dataclass(frozen=True)
class DynamicSqlCrossRef:
    loader: DynamicSqlLoader
    sql_file: Path                       # resolved file path
    tables: tuple[TableDef, ...]         # parquet/orc CREATE TABLE defs touched
    match_kind: str                      # "exact_path" | "basename_unique" | "basename_ambiguous"


def cross_reference_dynamic_sql(
    loaders: list[DynamicSqlLoader],
    sql_defs: list[TableDef],
    sql_refs: list[TableReference],
    project_root: Path,
) -> list[DynamicSqlCrossRef]:
    """For each loader, resolve sql_filename to a path, collect mentioned tables
    from that file, then filter to parquet/orc-defined ones."""
```

### File resolution strategy

For each `DynamicSqlLoader.sql_filename`, try in order:

1. **Containing-file relative:** `<dir(loader.file)>/<sql_filename>` — common when SQL lives next to code.
2. **Project-root relative:** `<project_root>/<sql_filename>` — when filename is project-relative.
3. **Basename fallback:** if neither path exists in the SQL registry, take `basename(sql_filename)` and look up by basename.
   - Exactly one match → `match_kind="basename_unique"`.
   - Multiple matches → `match_kind="basename_ambiguous"`, all matches returned.
   - Zero matches → loader is still emitted, but `cross_reference_dynamic_sql` returns no `DynamicSqlCrossRef` for it (the loader appears in the "all loaders" list, not the "cross-referenced" list).

### Table-resolution logic inside `cross_reference_dynamic_sql`

For each loader resolved to a `sql_file`:

```
sql_file_refs = [r for r in sql_refs if r.sql_file == sql_file]
sql_file_defs = [d for d in sql_defs if d.sql_file == sql_file]

mentioned_table_names = {r.table_name for r in sql_file_refs} | {d.table_name for d in sql_file_defs}

# Look up CREATE TABLE definitions across ALL sql files for these names,
# filter to parquet/orc formats.
parquet_defs = [
    d for d in sql_defs
    if d.table_name in mentioned_table_names
    and d.format in ("parquet", "orc")
]

if parquet_defs:
    emit DynamicSqlCrossRef(loader, sql_file, tuple(parquet_defs), match_kind)
```

This covers the "schema in one file, queries in another" case: `queries/events_update.sql` has `INSERT INTO events`, `schema.sql` has `CREATE TABLE events STORED AS PARQUET`. The cross-ref returned for the loader of `events_update.sql` includes the `events` table def even though the CREATE is in a different file.

## Integration points

| File | Change |
|---|---|
| `skills/open_table_migrator/dynamic_sql.py` | new file (~250 LOC) |
| `skills/open_table_migrator/sql_registry.py` | add `TableReference` dataclass + `scan_sql_table_references` function + CTE-name skip logic (~80 LOC) |
| `skills/open_table_migrator/analyzer.py` | add `DynamicSqlCrossRef` + `cross_reference_dynamic_sql` (~60 LOC) |
| `skills/open_table_migrator/cli.py` | wire `detect_dynamic_sql_loaders` and `cross_reference_dynamic_sql` into `convert_project`; pass `const_table_for_file=scope.build_const_table` |
| `skills/open_table_migrator/worklist.py` | add `dynamic_sql_loaders` field with serialized cross-refs |
| `skills/open_table_migrator/SKILL.md` | new "Dynamic SQL loading" section |

The detector pipeline (`ts_detect`) is NOT modified. Dynamic SQL loader detection is a separate pass run after `ts_detect` finishes — they operate on different inputs and produce different outputs.

## Worklist output

A new top-level key appears in `lakehouse-worklist.json`:

```json
{
  "version": 1,
  "count": 64,
  "rewrites": [...],
  "dynamic_sql_loaders": [
    {
      "file": "src/jobs/events.py",
      "line": 42,
      "pattern": "py_open",
      "sql_filename": "queries/events_update.sql",
      "confidence": "high",
      "resolved_to": "queries/events_update.sql",
      "match_kind": "exact_path",
      "tables": [
        {
          "name": "events",
          "format": "parquet",
          "ddl_file": "schema.sql",
          "ddl_line": 8
        }
      ]
    }
  ]
}
```

If a loader's `tables` array is empty (resolved file has no parquet/orc references), the entry is **still emitted** so the agent has visibility. Empty-`tables` entries serve as a "check manually" signal.

## Text report sections

The CLI's text report gains two new sections, immediately after the existing "SQL cross-references" section:

```
=== Dynamic SQL loader sites (12) ===
  src/jobs/events.py:42  py_open  → "queries/events_update.sql" (high)
  src/jobs/users.py:18   py_path_read_text  → "queries/users_select.sql" (high)
  src/lib/loader.java:55 java_files_read  → "/sql/devices.sql" (medium, resolved from SQL_PATH)
  ...

=== SQL files cross-referenced with parquet/orc tables (4) ===
  src/jobs/events.py:42 → events_update.sql  → table 'events' (parquet, schema.sql:8)
  src/jobs/users.py:18  → users_select.sql   → table 'users' (orc, schema.sql:14)
  ...
```

## Testing strategy

### Level 1 — `tests/test_dynamic_sql.py` (~12 cases)

Per-pattern detection (inline source via `dedent`, no filesystem):
- Python `open("queries/events.sql")` (literal arg)
- Python `open(SQL_FILE)` without const_table → not detected
- Python `open(SQL_FILE)` with const_table where `SQL_FILE = "x.sql"` → detected, confidence="medium"
- Python `Path("x.sql").read_text()` and `.read_bytes()`
- Python `pkgutil.get_data(__name__, "queries/x.sql")` — 2nd positional arg
- Java `Files.readAllBytes(Path.of("x.sql"))` and `Files.readString(Paths.get("x.sql"))`
- Java `getResourceAsStream("/sql/x.sql")` with various receivers
- Scala `getResourceAsStream("/sql/x.sql")`
- Non-`.sql` extension skipped (`open("data.csv")`)
- Multiple loaders in one file → all detected
- Loader in commented-out code → AST does not parse comments as calls → not detected

### Level 2 — `tests/test_sql_registry.py` extensions (~6 cases)

- `INSERT INTO events SELECT...` → `TableReference("events", role="write")`
- `INSERT OVERWRITE TABLE events ...` → write reference
- `SELECT * FROM events` → `TableReference("events", role="read")`
- `... JOIN devices ON ...` → read reference
- `UPDATE events SET ...` → write reference
- `WITH staging AS (...) SELECT FROM staging JOIN events` → only `events` registered as read; `staging` (CTE) skipped

### Level 3 — `tests/test_analyzer.py` extensions (~5 cases)

- Loader resolves to file with CREATE TABLE parquet → cross-ref `match_kind="exact_path"`
- Loader resolves to file with INSERT only, CREATE in another file → cross-ref via mentioned-tables join across all SQL files
- Loader resolves to file not in registry → loader emitted by `detect_dynamic_sql_loaders`, but `cross_reference_dynamic_sql` returns nothing for it
- Basename ambiguity (two `events.sql` in different dirs) → `match_kind="basename_ambiguous"`, all matches returned
- Loader filename is identifier resolved by const_table → cross-ref carries `confidence="medium"`

### Level 4 — `tests/test_integration.py` (or similar) — 1 case

End-to-end: synthetic fixture project with a Python loader, a schema.sql, a queries/*.sql → run `convert_project` → assert worklist contains expected `dynamic_sql_loaders` entry with correct tables.

### Out of scope for tests

- Performance benchmarks
- Fuzz-tests on regex variants
- Cross-language loader-chain tests (Python loader of SQL file referenced by Java code)

## Risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Regex-based INSERT/SELECT parsing misses edge cases (multi-line statements with comments, weird quoting) | medium | Per-line scan with explicit patterns; tests cover common forms. Unrecognised statements → skipped (no false positives) |
| CTE detection produces false negatives (recursive CTE, CTEs inside subqueries) | low | Conservative: any name introduced by `WITH ... AS` in the file is treated as CTE. Worst case: a real table with the same name as a CTE in same file isn't detected — acceptable, rare |
| Basename ambiguity becomes the common case in projects with `queries/{a,b}/events.sql` | low | `match_kind="basename_ambiguous"` returned with all candidates; agent decides |
| File resolution misses files outside the project (loaded from external dir) | medium | Loader still emitted; cross-ref empty. Agent sees the loader and can investigate manually |
| Performance on large projects (1000+ SQL files × 100s of loader sites) | low | Two passes total (one to build registry, one to detect loaders); both linear. No optimisation needed pre-shipping |
| Backtick-quoted table names in Hive/Spark SQL (`` `events` ``) confuse parsers | low | Pattern strips backticks; tested |
| Loader argument is a `Path()` constructor call rather than a string — argument extraction must descend into the call | medium | `_extract_string_literal_or_const` walks one level into `Path(...)` / `Paths.get(...)` to find the inner string |

## Estimated size

- Production: ~250 LOC `dynamic_sql.py` + ~80 LOC extensions in `sql_registry.py` + ~60 LOC extensions in `analyzer.py` + ~30 LOC wiring in `cli.py`/`worklist.py`
- Tests: ~200 LOC across `test_dynamic_sql.py` + `test_sql_registry.py` + `test_analyzer.py` + `test_integration.py`
- Documentation: ~30 lines in SKILL.md
- Total: ~650 LOC. Three implementation sessions.

## Open questions deferred to implementation

- Exact regex for `WITH ... AS ...` (CTE detection) — refine in impl when concrete edge cases appear (recursive CTE, multi-CTE chains).
- Quoting style normalisation (`` `events` `` vs `"events"` vs `events`) — strip in `_strip_table_quotes()` helper at registry layer.
- Multi-statement SQL files (`...;...;...`) — `sql_registry` already handles via line-wise scan; tests reuse existing fixtures.
- Whether `dynamic_sql_loaders` empty-tables entries should be sorted by file/line — choice during impl, not blocking design.
