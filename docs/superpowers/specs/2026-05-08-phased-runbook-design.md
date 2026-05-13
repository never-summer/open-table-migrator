# Phased Migration Runbook — Design

**Date:** 2026-05-08
**Status:** Approved for implementation planning
**Branch:** `feat/phased-runbook`
**Scope ref:** improvement 3.1 from the migrator roadmap

## Goal

Generate a phased migration runbook for banking change-review workflows. For each target Iceberg table found in the worklist, emit a per-table directory under `iceberg-runbook/` containing a Markdown migration plan and three SQL phase scripts (`phase1_add_files.sql`, `phase2_rewrite.sql`, `phase3_switchover.sql`). Currently change-review teams write this document by hand from the worklist — the migrator can produce a ready-to-paste artifact.

## Non-goals

- No Trino-specific syntax. Spark SQL only for phase 1 and phase 2; HMS metastore commands and Spark VIEW for phase 3.
- No ClickHouse, Snowflake, BigQuery, or other engine support.
- No automatic Hive schema extraction. Phase 1 emits a placeholder for the schema with a comment pointing the operator at `spark.read.parquet(...).printSchema()` or the source DDL.
- No data-size estimation, runtime prediction, or cost projection.
- No dependency graph (DAG) detection between tables. Tables are listed independently; the user coordinates execution order if needed.
- No notification, monitoring, or rollback orchestration. The runbook is a static artifact.

## Scope

For each `(namespace, table)` pair found in the worklist:

1. `iceberg-runbook/README.md` — top-level index with summary table and links.
2. `iceberg-runbook/<namespace>.<table>/migration-plan.md` — per-table plan with pre-flight checklist, phase descriptions, code-sites table, warnings.
3. `iceberg-runbook/<namespace>.<table>/phase1_add_files.sql` — Spark SQL for in-place Iceberg metadata creation via `system.add_files`.
4. `iceberg-runbook/<namespace>.<table>/phase2_rewrite.sql` — Spark SQL for `system.rewrite_data_files` compaction.
5. `iceberg-runbook/<namespace>.<table>/phase3_switchover.sql` — three OPTION blocks (Spark VIEW / HMS direct rename / Application-level rename), one chosen per stack.

The runbook directory is emitted alongside `lakehouse-worklist.json` on every `convert_project` run. `--dry-run` suppresses writing and prints the runbook as an additional preview section to stdout.

## Architecture

### New module: `skills/open_table_migrator/runbook.py`

```python
@dataclass(frozen=True)
class CodeSite:
    """One worklist entry condensed for the runbook."""
    file: str
    line: int
    direction: str          # "read" | "write"
    path_arg: str | None
    original_code: str      # one-line snippet, stripped


@dataclass(frozen=True)
class TableMigration:
    """Aggregated migration plan for one target Iceberg table."""
    namespace: str
    table: str
    source_format: str                              # "parquet" | "orc"
    source_path: str | None
    source_hive_table: str | None
    source_sql_file: str | None                     # path relative to project_root
    source_sql_line: int | None
    code_sites: tuple[CodeSite, ...]
    partition_spec: tuple[PartitionTransform, ...]  # may be empty
    partition_mismatch: str | None
    notes: tuple[str, ...]


def build_table_migrations(
    entries: list[WorklistEntry],
    sql_defs: list[TableDef],
    dyn_cross: list | None = None,
) -> list[TableMigration]:
    """Aggregate worklist entries by (namespace, table); enrich from sql_defs."""


def serialize_runbook(
    entries: list[WorklistEntry],
    sql_defs: list[TableDef],
    dyn_cross: list | None,
    project_root: Path,
) -> dict[Path, str]:
    """Build runbook contents as {relative_path: file_content}. No filesystem writes."""


def write_runbook(
    entries: list[WorklistEntry],
    sql_defs: list[TableDef],
    dyn_cross: list | None,
    project_root: Path,
) -> list[Path]:
    """Write all runbook files to disk. Returns paths actually written.

    Thin wrapper over serialize_runbook + write.
    """
```

### Aggregation algorithm

For each `WorklistEntry`:
- Skip if `resolved_namespace is None` or `resolved_table is None` (unresolved targets are not migratable).
- Group by `(resolved_namespace, resolved_table)`.

Per group:
- **`source_format`** — derived from any entry's `pattern_type`:
  - Contains `parquet` → `"parquet"`
  - Contains `orc` → `"orc"`
  - Else → `"parquet"` (default fallback)
- **`source_path`** — `path_arg` of any entry in the group (preferring write-direction).
- **`source_hive_table`** — match `path_arg` against `TableDef.table_name` (lowercase compare, optional `db.` prefix). If match found, use the matched name; else `None`.
- **`source_sql_file`, `source_sql_line`** — from the matched `TableDef` (relative path to `project_root` if under it, else absolute).
- **`partition_spec`** — `entry.partition_spec` from any write-direction entry (consistent within group; if multiple writes disagree, take the first).
- **`partition_mismatch`** — `entry.attrs.get("partition_mismatch")` from any entry in the group.
- **`code_sites`** — every entry in the group as a `CodeSite`, sorted by `(file, line)`.
- **`notes`** — collected from entries:
  - `attrs.get("skipped_reason")` per entry → `"Reassigned constant at file:line"`
  - Stream patterns (`*_stream_*` in pattern_type) → `"Structured Streaming detected — verify checkpoint location compatibility"`
  - PyArrow dataset patterns → `"PyArrow dataset API — manual partitioning may be needed"`
  - `partition_mismatch` populated → `"Code↔DDL partition spec mismatch — see migration-plan.md"`
  - Deduplicated.

`dyn_cross` is read but only contributes to `notes` for the affected tables. For each `DynamicSqlCrossRef` whose `tables[]` contains the table:
- Add note `"Dynamic SQL loader at {loader.file}:{loader.line} loads {loader.sql_filename} which references this table"`.

### File generators

Private functions in `runbook.py`, each takes `migrations: list[TableMigration]` or `m: TableMigration` and returns a string. Templates use string concatenation with f-strings for clarity (no templating library).

- `_render_readme(migrations, project_root) -> str`
- `_render_plan_md(m, project_root) -> str`
- `_render_phase1(m) -> str` — `system.add_files`
- `_render_phase2(m) -> str` — `system.rewrite_data_files`
- `_render_phase3(m) -> str` — three OPTION blocks

### Phase 3 template structure

`phase3_switchover.sql` has three labeled sections; user comments out two and keeps one:

```sql
-- ==========================================================================
-- OPTION A: Spark SQL CREATE VIEW (read-only consumers)
-- ==========================================================================
-- WORKS FOR: Spark, Hive Metastore consumers, downstream SELECT-only queries.
-- DOES NOT WORK FOR: clients doing INSERT INTO source — they will fail.

-- DROP TABLE source_hive_table;
-- CREATE VIEW source_hive_table AS SELECT * FROM <namespace>.<table>;

-- ==========================================================================
-- OPTION B: Hive Metastore direct rename
-- ==========================================================================
-- Atomic at metastore level. Requires HMS admin access.
--
-- ALTER TABLE prod.<table> RENAME TO prod.<table>_legacy_parquet;
-- -- Then register Iceberg under the original name via your catalog config.

-- ==========================================================================
-- OPTION C: Application-level rename (update code references)
-- ==========================================================================
-- Update each call site listed below to point at the new Iceberg name.
-- See lakehouse-worklist.json for the agent-driven rewrite plan.
--
-- Code sites (N):
--   <file>:<line>   <direction>   <original_code>
--   ...
```

When `source_hive_table` is `None` (no `.sql` DDL match), OPTION A and OPTION B blocks use the `source_path` as a comment marker; the user knows the rename isn't directly applicable and likely takes OPTION C.

### Partition mismatch handling

If `m.partition_mismatch` is populated, `phase1_add_files.sql` prepends a warning block:

```sql
-- ⚠ PARTITION MISMATCH DETECTED:
-- {partition_mismatch}
-- Pre-create the Iceberg table with the partition spec you actually want.
-- The PARTITIONED BY clause below reflects the CODE side. If DDL is right,
-- replace it.
```

### CLI integration

`convert_project` calls `write_runbook(entries, sql_defs, dyn_cross, project_root)` after `_run_hybrid` (which writes the worklist). Order in normal flow:

1. detect_parquet_usage → matches
2. scan_sql_files → sql_defs
3. annotate_partition_mismatch
4. detect_dynamic_sql_loaders + cross_reference_dynamic_sql → dyn_cross
5. build_resolver
6. (dry_run branch — see below)
7. _run_hybrid → writes `lakehouse-worklist.json`
8. write_runbook → writes `iceberg-runbook/`
9. update_dependencies, etc.

### Dry-run integration

In `_run_dry` (added in 5.1), a new 5th section is appended after the build-file diff:

```python
def _run_dry(...) -> int:
    # ... existing code ...
    runbook_files = serialize_runbook(entries, sql_defs, dyn_cross, project_root)

    print("=== DRY RUN — no files will be modified ===\n")
    _print_dry_summary(...)
    _print_dry_worklist(worklist_json)
    _print_dry_prepass(prepass_plans)
    _print_dry_build(build_plans)
    _print_dry_runbook(runbook_files)
    return 0


def _print_dry_runbook(runbook_files: dict[Path, str]):
    if not runbook_files:
        return
    print("--- Runbook preview (iceberg-runbook/) ---")
    for rel_path, content in sorted(runbook_files.items()):
        print(f"=== {rel_path} ===")
        print(content)
        print()
```

Summary line in `_print_dry_summary` also gains:
```python
if runbook_files:
    print(f"Would write: iceberg-runbook/ ({len(runbook_files)} files for {n_tables} table(s))")
```

## Testing strategy

### `tests/test_runbook.py` — module unit tests (~12 cases)

**Aggregation (6 cases):**

- `build_table_migrations` with one worklist entry → one `TableMigration`
- Multiple entries on the same target → one `TableMigration` with `code_sites` of correct length
- Entry with `resolved_table=None` is skipped (unresolved)
- `TableMigration.source_format` derives from `pattern_type` (parquet, orc)
- `TableMigration.partition_spec` inherited from write-direction entry
- `TableMigration.partition_mismatch` picked up from `attrs["partition_mismatch"]`

**Serialization (4 cases):**

- `serialize_runbook` with one migration produces 4 files (README + plan.md + 3 phase scripts)
- `serialize_runbook` with empty migrations returns empty dict
- README contains a summary table listing all migrations
- Per-table directory naming: `iceberg-runbook/<namespace>.<table>/`

**Phase content (2 cases):**

- `phase1_add_files.sql` contains `CALL system.add_files` with the correct target/source table names
- `phase3_switchover.sql` has three OPTION blocks (Spark VIEW / HMS / Code-side) and the code-side block lists worklist sites

### `tests/test_cli.py` extension (~2 cases)

- `convert_project()` (normal run) writes `iceberg-runbook/` at project root
- `convert_project(dry_run=True)` does NOT create `iceberg-runbook/` but stdout includes the `--- Runbook preview ---` section

### Out of scope for tests

- Multi-engine SQL output (we don't generate it)
- Schema extraction
- Performance / latency benchmarks

## Risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Phase 1 SQL fails because schema placeholder isn't filled in | high | Explicit comment + recommended command (`spark.read.parquet().printSchema()`). Banking ops always paste schemas manually before running |
| HMS direct rename is destructive and irreversible | low | OPTION B is commented out by default; the operator must uncomment and run manually |
| `partition_mismatch` info isn't enough — operator needs to decide which side is right | medium | Warning explicitly says "the PARTITIONED BY below reflects CODE side; replace if DDL is right" |
| Per-table directory name collisions (e.g., two tables both named `analytics.events`) | very low | Worklist guarantees unique `(namespace, table)`; duplicates would have been a worklist bug |
| Runbook becomes stale once code is rewritten | medium | Same as worklist staleness — re-run `convert_project` to regenerate. Both artifacts evolve together |
| Output grows large for projects with 100+ tables | low | Each per-table dir is small (~4 KB total); 100 tables = ~400 KB total. Filesystem-friendly |

## Estimated size

- Production: ~250 LOC `runbook.py` (data model, aggregation, 5 generators)
- Integration: ~80 LOC in `cli.py` (call site + `_print_dry_runbook` helper + summary line)
- Tests: ~200 LOC across `test_runbook.py` + 2 cases in `test_cli.py`
- Documentation: ~30 lines in SKILL.md
- Total: ~560 LOC. Two implementation sessions.

## Open questions deferred to implementation

- Exact wording inside phase 3 OPTION block comments (cosmetic).
- Whether the runbook README's summary table should include `partition_mismatch` flag as a separate column or be combined into the partition_spec column (current spec: separate column with ⚠ marker on row).
- Whether to also emit a top-level `iceberg-runbook/lakehouse-worklist.json` symlink for one-click access to worklist from inside the runbook directory (likely no — adds OS-specific behavior).
- Sorting order of per-table directories in README — alphabetic by `<namespace>.<table>`.
