# Phased Migration Runbook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate per-table `iceberg-runbook/<namespace>.<table>/` directories with `migration-plan.md` + 3 phase SQL scripts, plus top-level `iceberg-runbook/README.md`, automatically on every `convert_project` run. `--dry-run` suppresses write and prints contents to stdout as a 5th preview section.

**Architecture:** New `skills/open_table_migrator/runbook.py` with `TableMigration` / `CodeSite` dataclasses and three layers: `build_table_migrations` (pure aggregation from worklist + sql_defs + dyn_cross), `serialize_runbook` (pure renderer returning `{relative_path: content}` dict), `write_runbook` (thin wrapper that writes serialized content). CLI calls `write_runbook` after `_run_hybrid` in the normal flow; `_run_dry` calls `serialize_runbook` and prints output.

**Tech Stack:** Python 3.11+, stdlib only. No new dependencies.

**Spec:** [docs/superpowers/specs/2026-05-08-phased-runbook-design.md](../specs/2026-05-08-phased-runbook-design.md)

---

## File Structure

New files:
- `skills/open_table_migrator/runbook.py` — data model + aggregation + serialization + writer
- `tests/test_runbook.py` — unit tests for the module

Modified files:
- `skills/open_table_migrator/cli.py` — call `write_runbook` in normal flow; add `_print_dry_runbook` helper and call it from `_run_dry`; update summary line in `_print_dry_summary`
- `skills/open_table_migrator/SKILL.md` — new "Phased migration runbook" section
- `tests/test_cli.py` — 2 end-to-end cases (normal-run writes dir; dry-run does not write but prints section)

---

## Task 1: runbook.py skeleton — dataclasses + build_table_migrations

**Files:**
- Create: `skills/open_table_migrator/runbook.py`
- Create: `tests/test_runbook.py`

### Step 1: Failing tests

`tests/test_runbook.py`:
```python
"""Tests for runbook module."""
from pathlib import Path

from skills.open_table_migrator.detector import PartitionTransform
from skills.open_table_migrator.runbook import (
    CodeSite, TableMigration, build_table_migrations,
)
from skills.open_table_migrator.worklist import WorklistEntry


def _entry(
    *,
    file="src/job.py",
    start_line=1,
    pattern_type="pandas_read_parquet",
    direction="read",
    path_arg=None,
    namespace="analytics",
    table="events",
    original_code='df = pd.read_parquet("s3://prod/events")',
    attrs=None,
    partition_spec=None,
):
    return WorklistEntry(
        file=file,
        start_line=start_line,
        end_line=start_line,
        pattern_type=pattern_type,
        direction=direction,
        language="python",
        path_arg=path_arg,
        original_code=original_code,
        surrounding="",
        resolved_namespace=namespace,
        resolved_table=table,
        needs_manual_target=False,
        hint="",
        attrs=attrs or {},
        partition_spec=partition_spec or [],
    )


def test_one_entry_produces_one_migration():
    migrations = build_table_migrations([_entry()], sql_defs=[], dyn_cross=None)
    assert len(migrations) == 1
    m = migrations[0]
    assert m.namespace == "analytics"
    assert m.table == "events"
    assert m.source_format == "parquet"
    assert len(m.code_sites) == 1


def test_multiple_entries_same_table_aggregated():
    entries = [
        _entry(file="a.py", start_line=10),
        _entry(file="b.py", start_line=20, direction="write"),
        _entry(file="c.py", start_line=30),
    ]
    migrations = build_table_migrations(entries, sql_defs=[], dyn_cross=None)
    assert len(migrations) == 1
    assert len(migrations[0].code_sites) == 3


def test_entry_with_unresolved_target_skipped():
    entries = [_entry(namespace=None, table=None)]
    # WorklistEntry doesn't accept None for resolved_*; build by reusing fixture
    # with empty strings — but production check is on `resolved_table is None`.
    # So instead, construct an entry where resolved_table is None explicitly.
    entry = WorklistEntry(
        file="x.py", start_line=1, end_line=1,
        pattern_type="pandas_read_parquet", direction="read",
        language="python", path_arg="s3://bucket/x",
        original_code='df = pd.read_parquet("s3://bucket/x")',
        surrounding="",
        resolved_namespace=None,
        resolved_table=None,
        needs_manual_target=True,
        hint="",
    )
    migrations = build_table_migrations([entry], sql_defs=[], dyn_cross=None)
    assert migrations == []


def test_source_format_from_pattern_type():
    parquet_m = build_table_migrations(
        [_entry(pattern_type="pandas_read_parquet")], sql_defs=[],
    )
    orc_m = build_table_migrations(
        [_entry(pattern_type="pandas_read_orc", table="t2")], sql_defs=[],
    )
    assert parquet_m[0].source_format == "parquet"
    assert orc_m[0].source_format == "orc"


def test_partition_spec_inherited_from_write_entry():
    entries = [
        _entry(direction="read"),  # read doesn't carry partition_spec
        _entry(
            direction="write",
            partition_spec=[
                {"kind": "identity", "column": "region"},
                {"kind": "bucket", "column": "uid", "n": 8},
            ],
        ),
    ]
    migrations = build_table_migrations(entries, sql_defs=[])
    assert len(migrations) == 1
    spec = migrations[0].partition_spec
    assert PartitionTransform(kind="identity", column="region") in spec
    assert PartitionTransform(kind="bucket", column="uid", n=8) in spec


def test_partition_mismatch_picked_up_from_attrs():
    entries = [
        _entry(attrs={"partition_mismatch": "code: identity(region); ddl: identity(date_col)"}),
    ]
    migrations = build_table_migrations(entries, sql_defs=[])
    assert migrations[0].partition_mismatch == "code: identity(region); ddl: identity(date_col)"
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_runbook.py -v`
Expected: `ModuleNotFoundError: No module named 'skills.open_table_migrator.runbook'`

### Step 3: Implement `runbook.py` data model + aggregation

`skills/open_table_migrator/runbook.py`:
```python
"""Phased migration runbook generator.

Aggregates the worklist by (namespace, table), enriches with SQL registry
data, and renders per-table runbook directories with:
  - migration-plan.md
  - phase1_add_files.sql
  - phase2_rewrite.sql
  - phase3_switchover.sql
plus a top-level README.md.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .detector import PartitionTransform
from .sql_registry import TableDef
from .worklist import WorklistEntry


@dataclass(frozen=True)
class CodeSite:
    """One worklist entry condensed for the runbook."""
    file: str
    line: int
    direction: str
    path_arg: str | None
    original_code: str


@dataclass(frozen=True)
class TableMigration:
    """Aggregated migration plan for one target Iceberg table."""
    namespace: str
    table: str
    source_format: str
    source_path: str | None
    source_hive_table: str | None
    source_sql_file: str | None
    source_sql_line: int | None
    code_sites: tuple[CodeSite, ...]
    partition_spec: tuple[PartitionTransform, ...]
    partition_mismatch: str | None
    notes: tuple[str, ...]


def _source_format(pattern_type: str) -> str:
    pt = pattern_type.lower()
    if "orc" in pt:
        return "orc"
    return "parquet"


def _is_write(direction: str) -> bool:
    return direction == "write"


def _match_hive_table(
    path_arg: str | None,
    sql_defs: list[TableDef],
) -> TableDef | None:
    """Match path_arg against a TableDef.table_name. Returns first match or None.

    Tries exact match (case-insensitive), then basename of path-style args.
    """
    if not path_arg:
        return None
    key = path_arg.strip("`").lower()
    # Strip database prefix if present (e.g. "db.events" -> "events")
    short = key.rsplit(".", 1)[-1]
    for d in sql_defs:
        if d.table_name.lower() == key or d.table_name.lower() == short:
            return d
    return None


def _spec_from_dicts(raw: list) -> tuple[PartitionTransform, ...]:
    """Convert worklist.WorklistEntry.partition_spec (list of dicts) to tuple of dataclass."""
    out = []
    for entry in raw:
        if isinstance(entry, PartitionTransform):
            out.append(entry)
            continue
        out.append(PartitionTransform(
            kind=entry["kind"],
            column=entry["column"],
            n=entry.get("n"),
        ))
    return tuple(out)


def build_table_migrations(
    entries: list[WorklistEntry],
    sql_defs: list[TableDef],
    dyn_cross: list | None = None,
) -> list[TableMigration]:
    """Group worklist entries by (namespace, table); enrich from sql_defs."""
    groups: dict[tuple[str, str], list[WorklistEntry]] = {}
    for e in entries:
        if e.resolved_namespace is None or e.resolved_table is None:
            continue
        groups.setdefault((e.resolved_namespace, e.resolved_table), []).append(e)

    migrations: list[TableMigration] = []
    for (ns, tbl), group in sorted(groups.items()):
        # source_format: derive from first entry's pattern_type
        source_format = _source_format(group[0].pattern_type)

        # source_path: prefer write entry's path_arg
        write_entries = [e for e in group if _is_write(e.direction)]
        source_path = None
        for e in (write_entries + group):
            if e.path_arg:
                source_path = e.path_arg
                break

        # source_hive_table + sql_file + sql_line via sql_defs match
        hive_td = _match_hive_table(source_path, sql_defs)
        source_hive_table = hive_td.table_name if hive_td else None
        source_sql_file = str(hive_td.file) if hive_td else None
        source_sql_line = hive_td.line if hive_td else None

        # partition_spec: from any write entry
        partition_spec: tuple[PartitionTransform, ...] = ()
        for e in write_entries:
            if e.partition_spec:
                partition_spec = _spec_from_dicts(e.partition_spec)
                break

        # partition_mismatch: from any entry's attrs
        partition_mismatch: str | None = None
        for e in group:
            if "partition_mismatch" in e.attrs:
                partition_mismatch = e.attrs["partition_mismatch"]
                break

        # code_sites: sort by (file, line)
        code_sites = tuple(sorted([
            CodeSite(
                file=e.file,
                line=e.start_line,
                direction=e.direction,
                path_arg=e.path_arg,
                original_code=e.original_code,
            )
            for e in group
        ], key=lambda c: (c.file, c.line)))

        # notes: aggregate warnings
        notes_set: set[str] = set()
        for e in group:
            reason = e.attrs.get("skipped_reason")
            if reason:
                notes_set.add(f"Skipped at {e.file}:{e.start_line}: {reason}")
            if "stream" in e.pattern_type:
                notes_set.add("Structured Streaming detected — verify checkpoint location compatibility")
            if "pyarrow_dataset" in e.pattern_type or "pyarrow_parquet_file" in e.pattern_type:
                notes_set.add("PyArrow dataset API — manual partitioning may be needed")
        if partition_mismatch:
            notes_set.add("Code↔DDL partition spec mismatch — see migration-plan.md")
        if dyn_cross:
            for c in dyn_cross:
                for t in c.tables:
                    if t.table_name.lower() == tbl.lower():
                        notes_set.add(
                            f"Dynamic SQL loader at {c.loader.file}:{c.loader.line} "
                            f"loads {c.loader.sql_filename} which references this table"
                        )

        migrations.append(TableMigration(
            namespace=ns,
            table=tbl,
            source_format=source_format,
            source_path=source_path,
            source_hive_table=source_hive_table,
            source_sql_file=source_sql_file,
            source_sql_line=source_sql_line,
            code_sites=code_sites,
            partition_spec=partition_spec,
            partition_mismatch=partition_mismatch,
            notes=tuple(sorted(notes_set)),
        ))
    return migrations
```

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_runbook.py -v`
Expected: 6 passed.

Run full suite:
`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: no regressions.

### Step 5: Commit

```bash
git add skills/open_table_migrator/runbook.py tests/test_runbook.py
git commit -m "feat(runbook): data model + build_table_migrations aggregation"
```

---

## Task 2: serialize_runbook + 5 renderer helpers

**Files:**
- Modify: `skills/open_table_migrator/runbook.py`
- Modify: `tests/test_runbook.py`

### Step 1: Failing tests

Append to `tests/test_runbook.py`:
```python
from skills.open_table_migrator.runbook import serialize_runbook


def test_serialize_runbook_one_migration_produces_4_files(tmp_path):
    entries = [_entry()]
    files = serialize_runbook(entries, sql_defs=[], dyn_cross=None, project_root=tmp_path)

    rel_paths = {str(p) for p in files.keys()}
    assert "iceberg-runbook/README.md" in rel_paths
    assert "iceberg-runbook/analytics.events/migration-plan.md" in rel_paths
    assert "iceberg-runbook/analytics.events/phase1_add_files.sql" in rel_paths
    assert "iceberg-runbook/analytics.events/phase2_rewrite.sql" in rel_paths
    assert "iceberg-runbook/analytics.events/phase3_switchover.sql" in rel_paths


def test_serialize_runbook_empty_entries_returns_empty(tmp_path):
    files = serialize_runbook([], sql_defs=[], dyn_cross=None, project_root=tmp_path)
    assert files == {}


def test_readme_contains_summary_table_listing_migrations(tmp_path):
    entries = [
        _entry(table="events"),
        _entry(file="x.py", start_line=2, table="users"),
    ]
    files = serialize_runbook(entries, sql_defs=[], dyn_cross=None, project_root=tmp_path)
    readme = files[Path("iceberg-runbook/README.md")]
    assert "analytics.events" in readme
    assert "analytics.users" in readme


def test_per_table_directory_naming(tmp_path):
    entries = [_entry(namespace="warehouse", table="orders")]
    files = serialize_runbook(entries, sql_defs=[], dyn_cross=None, project_root=tmp_path)
    rel_paths = {str(p) for p in files.keys()}
    assert "iceberg-runbook/warehouse.orders/migration-plan.md" in rel_paths


def test_phase1_contains_add_files_call(tmp_path):
    entries = [_entry()]
    files = serialize_runbook(entries, sql_defs=[], dyn_cross=None, project_root=tmp_path)
    phase1 = files[Path("iceberg-runbook/analytics.events/phase1_add_files.sql")]
    assert "system.add_files" in phase1
    assert "analytics.events" in phase1


def test_phase3_has_three_option_blocks(tmp_path):
    entries = [_entry()]
    files = serialize_runbook(entries, sql_defs=[], dyn_cross=None, project_root=tmp_path)
    phase3 = files[Path("iceberg-runbook/analytics.events/phase3_switchover.sql")]
    assert "OPTION A" in phase3
    assert "OPTION B" in phase3
    assert "OPTION C" in phase3
    # Option C lists code sites
    assert "src/job.py" in phase3
```

### Step 2: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_runbook.py -v -k serialize`
Expected: ImportError on `serialize_runbook`.

### Step 3: Append renderers + serializer to `runbook.py`

Append to `skills/open_table_migrator/runbook.py`:
```python
def _format_transforms_inline(transforms: tuple[PartitionTransform, ...]) -> str:
    if not transforms:
        return "(none)"
    parts = []
    for t in transforms:
        if t.kind == "identity":
            parts.append(f"identity({t.column})")
        elif t.kind == "bucket":
            parts.append(f"bucket({t.n}, {t.column})")
    return ", ".join(parts)


def _format_partitioned_by_clause(transforms: tuple[PartitionTransform, ...]) -> str:
    if not transforms:
        return ""
    parts = []
    for t in transforms:
        if t.kind == "identity":
            parts.append(t.column)
        elif t.kind == "bucket":
            parts.append(f"bucket({t.n}, {t.column})")
    return "PARTITIONED BY (" + ", ".join(parts) + ")"


def _render_readme(migrations: list[TableMigration]) -> str:
    lines = [
        "# Iceberg Migration Runbook",
        "",
        "Generated by open_table_migrator. **Review carefully before execution.**",
        "",
        f"## Tables to migrate ({len(migrations)})",
        "",
        "| # | Source | Target | Source format | Code sites | Partition spec |",
        "|---|---|---|---|---|---|",
    ]
    for i, m in enumerate(migrations, start=1):
        source = m.source_path or m.source_hive_table or "(unknown)"
        target = f"`{m.namespace}.{m.table}`"
        ps = _format_transforms_inline(m.partition_spec)
        if m.partition_mismatch:
            ps += " ⚠ MISMATCH"
        lines.append(
            f"| {i} | `{source}` | {target} | {m.source_format} | "
            f"{len(m.code_sites)} | {ps} |"
        )

    if any(m.partition_mismatch for m in migrations):
        lines += [
            "",
            "⚠ Tables flagged with MISMATCH have code↔DDL partition divergence. See per-table runbook.",
        ]

    lines += [
        "",
        "## Execution order",
        "",
        "Phases must run in order **per table**. Tables themselves can run in parallel.",
        "Recommended sequence per table:",
        "- Phase 1 (add_files) — minutes, in-place, low-risk",
        "- Validation pause (verify table count matches source)",
        "- Phase 2 (rewrite) — hours, resource-heavy, can be deferred",
        "- Phase 3 (switchover) — minutes, coordinated cutover",
        "",
        "## Per-table runbooks",
        "",
    ]
    for i, m in enumerate(migrations, start=1):
        flag = " ⚠ partition mismatch" if m.partition_mismatch else ""
        lines.append(
            f"{i}. [{m.namespace}.{m.table}](./{m.namespace}.{m.table}/migration-plan.md){flag}"
        )

    return "\n".join(lines) + "\n"


def _render_plan_md(m: TableMigration) -> str:
    fq = f"`{m.namespace}.{m.table}`"
    source = m.source_path or m.source_hive_table or "(unknown)"
    ps_inline = _format_transforms_inline(m.partition_spec)
    sql_ref = (
        f"`{m.source_sql_file}:{m.source_sql_line}`"
        if m.source_sql_file and m.source_sql_line
        else "(none found)"
    )
    n_read = sum(1 for c in m.code_sites if c.direction == "read")
    n_write = sum(1 for c in m.code_sites if c.direction == "write")

    lines = [
        f"# Migration plan: {fq}",
        "",
        f"**Source:** `{source}` ({m.source_format})",
        f"**Target:** {fq} (Iceberg)",
        f"**Source DDL:** {sql_ref}",
        f"**Partition spec:** {ps_inline}",
        f"**Code sites:** {len(m.code_sites)} ({n_read} reads, {n_write} writes)",
        "",
        "## Pre-flight checklist",
        "",
        f"- [ ] Iceberg catalog `{m.namespace.split('.')[0]}` is registered in Spark / HMS",
        f"- [ ] Target schema `{m.namespace}` exists",
        f"- [ ] Source table is in Hive Metastore",
        "- [ ] Backup of source DDL captured",
        "- [ ] Maintenance window scheduled (if Phase 3 affects production)",
        "",
        "## Phase 1: add_files (in-place, ~minutes)",
        "",
        "Run `phase1_add_files.sql`. Brings existing data files under Iceberg metadata "
        "without rewriting data. Reversible by dropping the target table.",
        "",
        f"**Validation after:** `SELECT COUNT(*) FROM {m.namespace}.{m.table};` should equal "
        f"the source table count.",
        "",
        "## Phase 2: rewrite_data_files (compaction, ~hours)",
        "",
        "Run `phase2_rewrite.sql`. Compacts small files; applies sort order; rewrites "
        "partition layout. Resource-heavy — schedule outside peak hours.",
        "",
        "**Skip Phase 2 if:** source files are already well-sized (>128MB each) and you do NOT "
        "need the new partition spec.",
        "",
        "## Phase 3: switchover (coordinated cutover)",
        "",
        "Run `phase3_switchover.sql`. Three options listed inside — pick ONE per your stack.",
        "",
        "## Code sites (from `lakehouse-worklist.json`)",
        "",
        "| File | Line | Direction | Original |",
        "|---|---|---|---|",
    ]
    for c in m.code_sites:
        snippet = c.original_code.replace("|", "\\|").splitlines()[0][:80]
        lines.append(f"| `{c.file}` | {c.line} | {c.direction} | `{snippet}` |")

    if m.notes:
        lines += ["", "## Warnings", ""]
        for n in m.notes:
            lines.append(f"- {n}")

    return "\n".join(lines) + "\n"


def _render_phase1(m: TableMigration) -> str:
    fq = f"{m.namespace}.{m.table}"
    partitioned = _format_partitioned_by_clause(m.partition_spec)
    source_label = m.source_hive_table or m.source_path or "<source_table>"

    lines: list[str] = []
    if m.partition_mismatch:
        lines += [
            "-- ⚠ PARTITION MISMATCH DETECTED:",
            f"-- {m.partition_mismatch}",
            "-- Pre-create the Iceberg table with the partition spec you actually want.",
            "-- The PARTITIONED BY clause below reflects the CODE side. If DDL is right,",
            "-- replace it.",
            "",
        ]
    lines += [
        f"-- PHASE 1: Add existing {m.source_format.capitalize()} files to Iceberg metadata (in-place)",
        f"-- Estimated runtime: ~minutes for typical tables. No data rewrite.",
        f"-- Reversible: DROP TABLE {fq};",
        "",
        f"CREATE TABLE {fq} (",
        "  -- TODO: copy schema from source. Generate via:",
        f"  --   spark.read.{m.source_format}('{m.source_path or '<source_path>'}').printSchema()",
    ]
    if m.source_sql_file:
        lines.append(f"  -- Or capture from source DDL: {m.source_sql_file}:{m.source_sql_line}")
    lines += [
        ")",
        "USING iceberg",
    ]
    if partitioned:
        lines.append(partitioned)
    lines += [
        "TBLPROPERTIES (",
        "  'write.parquet.compression-codec' = 'zstd',",
        "  'format-version' = '2'",
        ");",
        "",
        f"CALL system.add_files(",
        f"  table => '{fq}',",
        f"  source_table => '`{source_label}`'",
        f");",
        "",
        "-- Validation:",
        f"-- SELECT COUNT(*) FROM {fq};",
        f"-- SELECT COUNT(*) FROM `{source_label}`;",
        "-- The counts MUST match before proceeding to Phase 2.",
    ]
    return "\n".join(lines) + "\n"


def _render_phase2(m: TableMigration) -> str:
    fq = f"{m.namespace}.{m.table}"
    lines = [
        "-- PHASE 2: Compact and rewrite data files",
        "-- Estimated runtime: minutes to hours depending on data size. Resource-heavy.",
        "-- Can be deferred — Phase 1 result is already queryable.",
        "",
        "CALL system.rewrite_data_files(",
        f"  table => '{fq}',",
        "  strategy => 'binpack',",
        "  options => map('min-input-files', '5', 'target-file-size-bytes', '536870912')",
        ");",
        "",
        "-- If you changed partition spec in Phase 1 (vs original Hive PARTITIONED BY):",
        "-- CALL system.rewrite_data_files(",
        f"--   table => '{fq}',",
        "--   options => map('partial-progress.enabled', 'true')",
        "-- );",
        "",
        "-- Validation: no row count change, file count should decrease significantly.",
        f"-- SELECT data_file_count, total_data_file_size_in_bytes FROM {fq}.snapshots;",
    ]
    return "\n".join(lines) + "\n"


def _render_phase3(m: TableMigration) -> str:
    fq = f"{m.namespace}.{m.table}"
    hive_ref = m.source_hive_table or "<source_hive_table>"
    lines = [
        "-- PHASE 3: Switchover (coordinated cutover)",
        "--",
        "-- Choose ONE option below based on your stack. Comment out the others.",
        "--",
        "-- ==========================================================================",
        "-- OPTION A: Spark SQL CREATE VIEW (read-only consumers)",
        "-- ==========================================================================",
        "-- Replaces the old Hive table with a view pointing at Iceberg.",
        "-- WORKS FOR: Spark, Hive Metastore consumers, downstream SELECT-only queries.",
        f"-- DOES NOT WORK FOR: clients doing INSERT INTO `{hive_ref}` — they will fail.",
        "",
        f"-- DROP TABLE `{hive_ref}`;",
        f"-- CREATE VIEW `{hive_ref}` AS SELECT * FROM {fq};",
        "",
        "-- ==========================================================================",
        "-- OPTION B: Hive Metastore direct rename",
        "-- ==========================================================================",
        "-- Atomic at metastore level. Requires HMS admin access.",
        "-- Source table is renamed; Iceberg takes its place.",
        "--",
        f"-- ALTER TABLE `{hive_ref}` RENAME TO `{hive_ref}_legacy_{m.source_format}`;",
        "-- -- Then register Iceberg under the original name via your catalog config.",
        "",
        "-- ==========================================================================",
        "-- OPTION C: Application-level rename (update code references)",
        "-- ==========================================================================",
        "-- Update each call site listed below to point at the new Iceberg name.",
        "-- See lakehouse-worklist.json for full details + agent-driven rewrite plan.",
        "--",
        f"-- Code sites ({len(m.code_sites)}):",
    ]
    for c in m.code_sites:
        snippet = c.original_code.splitlines()[0][:80]
        lines.append(f"--   {c.file}:{c.line}   {c.direction}   {snippet}")
    return "\n".join(lines) + "\n"


def serialize_runbook(
    entries: list[WorklistEntry],
    sql_defs: list[TableDef],
    dyn_cross: list | None,
    project_root: Path,
) -> dict[Path, str]:
    """Build runbook contents as {relative_path: file_content}. No filesystem writes."""
    migrations = build_table_migrations(entries, sql_defs, dyn_cross)
    if not migrations:
        return {}

    root = Path("iceberg-runbook")
    files: dict[Path, str] = {}
    files[root / "README.md"] = _render_readme(migrations)

    for m in migrations:
        table_dir = root / f"{m.namespace}.{m.table}"
        files[table_dir / "migration-plan.md"] = _render_plan_md(m)
        files[table_dir / "phase1_add_files.sql"] = _render_phase1(m)
        files[table_dir / "phase2_rewrite.sql"] = _render_phase2(m)
        files[table_dir / "phase3_switchover.sql"] = _render_phase3(m)

    return files
```

### Step 4: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_runbook.py -v`
Expected: 12 passed (6 aggregation + 6 serialization).

### Step 5: Commit

```bash
git add skills/open_table_migrator/runbook.py tests/test_runbook.py
git commit -m "feat(runbook): serialize_runbook + 5 renderer helpers (no I/O)"
```

---

## Task 3: write_runbook + CLI integration (normal + dry-run)

**Files:**
- Modify: `skills/open_table_migrator/runbook.py`
- Modify: `skills/open_table_migrator/cli.py`
- Modify: `tests/test_cli.py`

### Step 1: Add `write_runbook` to runbook.py

Append to `skills/open_table_migrator/runbook.py`:
```python
def write_runbook(
    entries: list[WorklistEntry],
    sql_defs: list[TableDef],
    dyn_cross: list | None,
    project_root: Path,
) -> list[Path]:
    """Write all runbook files to disk. Returns paths actually written.

    Thin wrapper over serialize_runbook + write.
    """
    written: list[Path] = []
    for rel_path, content in serialize_runbook(entries, sql_defs, dyn_cross, project_root).items():
        abs_path = project_root / rel_path
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        abs_path.write_text(content)
        written.append(abs_path)
    return written
```

### Step 2: Failing CLI tests

Append to `tests/test_cli.py`:
```python
def test_convert_project_normal_run_writes_runbook(tmp_path):
    """convert_project (no dry-run) writes iceberg-runbook/ directory."""
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/events.parquet")\n'
    )
    rc = convert_project(
        tmp_path,
        table_name="events", namespace="analytics",
        mapping=None,
        update_deps=False,
        dry_run=False,
    )
    assert rc == 0
    runbook_dir = tmp_path / "iceberg-runbook"
    assert runbook_dir.is_dir()
    assert (runbook_dir / "README.md").exists()
    assert (runbook_dir / "analytics.events").is_dir()
    assert (runbook_dir / "analytics.events" / "phase1_add_files.sql").exists()


def test_convert_project_dry_run_does_not_write_runbook(tmp_path, capsys):
    """convert_project(dry_run=True) does NOT create iceberg-runbook/."""
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/events.parquet")\n'
    )
    convert_project(
        tmp_path,
        table_name="events", namespace="analytics",
        mapping=None,
        update_deps=False,
        dry_run=True,
    )
    assert not (tmp_path / "iceberg-runbook").exists()
    out = capsys.readouterr().out
    assert "--- Runbook preview" in out
```

### Step 3: Run, see fail

`PYTHONPATH=. python3 -m pytest tests/test_cli.py -v -k runbook`
Expected: 2 fail (no `write_runbook` call in cli, no `--- Runbook preview` in dry-run output).

### Step 4: Wire `write_runbook` into normal flow

In `skills/open_table_migrator/cli.py`, find the `_run_hybrid(project_root, matches, resolver, dyn_cross=dyn_cross)` call in `convert_project`. After that line, add:

```python
    from .runbook import write_runbook
    from .sql_registry import scan_sql_files
    # sql_defs is already in scope from earlier in convert_project
    # entries is built inside _run_hybrid — we need to extract it.
```

Actually, `entries` is built inside `_run_hybrid` and isn't returned. We need to refactor.

Read `_run_hybrid` at line 147 of cli.py. Find where `entries = build_worklist(matches, project_root, resolver)` is. Change `_run_hybrid` to return the entries, OR build them outside and pass in.

Cleaner approach: build `entries` in `convert_project` and pass to `_run_hybrid`. Modify:

In `convert_project`, BEFORE `_run_hybrid` call:
```python
    from .worklist import build_worklist
    entries = build_worklist(matches, project_root, resolver)
```

Then change `_run_hybrid` signature to accept `entries: list[WorklistEntry]` directly. Find `_run_hybrid` at line 147 and change:
```python
def _run_hybrid(
    project_root: Path,
    entries: list,
    *,
    dyn_cross: list | None = None,
) -> None:
    """Run the hybrid (worklist + prepass) mode.

    `entries` is pre-built by the caller so that runbook generation can
    share the same data without re-running build_worklist.
    """
    # ... existing body, replacing `entries = build_worklist(matches, project_root, resolver)` line with nothing
    # ... and removing the `matches, resolver` params from signature
```

The simplest minimum-disruption variant: keep `_run_hybrid` signature unchanged, and add a SECOND call to `build_worklist` outside for runbook. Worklist building is cheap and idempotent — duplication is fine. Pick THIS approach:

In `convert_project`, AFTER the `_run_hybrid(project_root, matches, resolver, dyn_cross=dyn_cross)` call, add:

```python
    from .runbook import write_runbook
    from .worklist import build_worklist
    entries_for_runbook = build_worklist(matches, project_root, resolver)
    write_runbook(entries_for_runbook, sql_defs, dyn_cross, project_root)
```

`sql_defs` is already in scope (set earlier in `convert_project`).

### Step 5: Wire runbook preview into `_run_dry`

In `skills/open_table_migrator/cli.py`, find `_run_dry` (line ~174). Currently builds entries + prepass_plans + build_plans + worklist_json. Add a 5th step:

```python
def _run_dry(
    project_root: Path,
    matches: list,
    resolver,
    *,
    dyn_cross: list | None,
    update_deps_flag: bool,
) -> int:
    """Render the dry-run preview to stdout. No file I/O for writes."""
    from .deps import plan_dependencies_update
    from .prepass import plan_prepass
    from .runbook import serialize_runbook
    from .sql_registry import scan_sql_files
    from .worklist import build_worklist, serialize_worklist

    entries = build_worklist(matches, project_root, resolver)
    prepass_plans = plan_prepass(matches, resolver)
    build_plans = plan_dependencies_update(project_root) if update_deps_flag else []
    worklist_json = serialize_worklist(entries, project_root=project_root, dyn_cross=dyn_cross)

    # sql_defs is needed for runbook — recompute (cheap)
    sql_defs = scan_sql_files(project_root)
    runbook_files = serialize_runbook(entries, sql_defs, dyn_cross, project_root)

    print("=== DRY RUN — no files will be modified ===\n")
    _print_dry_summary(entries, prepass_plans, build_plans, dyn_cross, runbook_files)
    _print_dry_worklist(worklist_json)
    _print_dry_prepass(prepass_plans)
    _print_dry_build(build_plans)
    _print_dry_runbook(runbook_files)
    return 0
```

Add `_print_dry_runbook` near the other `_print_dry_*` helpers:
```python
def _print_dry_runbook(runbook_files: dict):
    if not runbook_files:
        return
    print("--- Runbook preview (iceberg-runbook/) ---")
    for rel_path, content in sorted(runbook_files.items(), key=lambda kv: str(kv[0])):
        print(f"=== {rel_path} ===")
        print(content)
        print()
```

Update `_print_dry_summary` signature to accept `runbook_files`:
```python
def _print_dry_summary(entries, prepass_plans, build_plans, dyn_cross, runbook_files):
    print("--- Summary ---")
    print(f"Detected {len(entries)} I/O operation(s).")
    print(f"Would write: lakehouse-worklist.json ({len(entries)} entries)")
    if dyn_cross:
        print(f"  with {len(dyn_cross)} dynamic SQL loader cross-references")
    total_markers = sum(p.marker_count for p in prepass_plans)
    pyspark_files = sum(1 for p in prepass_plans if p.pyspark_conf_added)
    if prepass_plans:
        msg = f"Would prepass: {len(prepass_plans)} file(s) with {total_markers} marker(s)"
        if pyspark_files:
            msg += f", pyspark conf added in {pyspark_files} file(s)"
        print(msg)
    if build_plans:
        names = ", ".join(p.file.name for p in build_plans)
        print(f"Would update: {names}")
    if runbook_files:
        n_tables = sum(1 for p in runbook_files if str(p).endswith("migration-plan.md"))
        print(f"Would write: iceberg-runbook/ ({len(runbook_files)} files for {n_tables} table(s))")
    print()
```

### Step 6: Run, see pass

`PYTHONPATH=. python3 -m pytest tests/test_cli.py -v -k runbook`
Expected: 2 passed.

Run full suite:
`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: no regressions.

### Step 7: Commit

```bash
git add skills/open_table_migrator/runbook.py skills/open_table_migrator/cli.py tests/test_cli.py
git commit -m "feat(cli): write_runbook in normal flow + Runbook preview in --dry-run"
```

---

## Task 4: SKILL.md + final smoke run

**Files:**
- Modify: `skills/open_table_migrator/SKILL.md`

### Step 1: Find insertion point

Run: `grep -n "^##" skills/open_table_migrator/SKILL.md`
Insert "## Phased migration runbook" right before "## Known Limitations" (or at the end if missing).

### Step 2: Add section

```markdown
## Phased migration runbook

For each target Iceberg table found in the worklist, the migrator emits a per-table directory under `iceberg-runbook/` containing:

- `migration-plan.md` — phase descriptions, pre-flight checklist, code-sites table, warnings
- `phase1_add_files.sql` — Spark SQL for `system.add_files` (in-place metadata creation)
- `phase2_rewrite.sql` — Spark SQL for `system.rewrite_data_files` (compaction)
- `phase3_switchover.sql` — three OPTION blocks: Spark VIEW, HMS direct rename, Application-level rename per worklist

Plus `iceberg-runbook/README.md` as a top-level index with a summary table of all migrations.

### Phases

| Phase | Estimated runtime | Risk |
|---|---|---|
| 1: add_files | minutes | Low — reversible by dropping target table |
| 2: rewrite_data_files | hours | Medium — resource-heavy, can be deferred |
| 3: switchover | minutes | Coordinated cutover — requires consumer alignment |

### Phase 3 options

Phase 3 has three switchover patterns. The user picks ONE per stack and comments out the others:

- **OPTION A (Spark VIEW)** — replace old Hive table with a view pointing at Iceberg. Works for SELECT-only consumers; breaks `INSERT INTO` clients.
- **OPTION B (HMS direct rename)** — atomic at metastore level. Requires admin access.
- **OPTION C (Application-level rename)** — update each call site in the code per `lakehouse-worklist.json`. Listed in the SQL file as comments.

### Automation

Runbook generation runs alongside worklist generation on every `convert_project`. The `--dry-run` flag suppresses the directory write and prints the runbook contents as a 5th preview section.

### Limitations

- Spark SQL syntax only (no Trino, ClickHouse, Snowflake).
- Schema in `phase1_add_files.sql` is a placeholder — the operator must run `spark.read.parquet(...).printSchema()` and paste the result.
- No data-size estimation or runtime prediction.
- No DAG between tables (each migration is independent).
- `partition_mismatch` warning is included in the runbook, but the operator must decide which side (code or DDL) is correct.
```

### Step 3: Smoke run

```bash
mkdir -p /tmp/runbook-smoke/queries
cat > /tmp/runbook-smoke/queries/events.sql <<'EOF'
CREATE TABLE events (id INT, region STRING)
PARTITIONED BY (region STRING)
STORED AS PARQUET;
EOF
cat > /tmp/runbook-smoke/job.py <<'EOF'
import pandas as pd
df = pd.read_parquet("s3://bucket/events.parquet")
df.write.partitionBy("region").saveAsTable("events")
EOF

PYTHONPATH=. python3 -m skills.open_table_migrator.cli /tmp/runbook-smoke \
    --table events --namespace analytics --no-deps 2>&1 | tail -10
echo "---"
echo "Runbook structure:"
find /tmp/runbook-smoke/iceberg-runbook -type f 2>/dev/null
echo "---"
echo "phase1 preview:"
head -20 /tmp/runbook-smoke/iceberg-runbook/analytics.events/phase1_add_files.sql 2>&1 | head -20
```

Expected:
- Runbook dir created with `README.md` + `analytics.events/` + 4 files inside
- phase1 SQL contains `CALL system.add_files` and references `analytics.events`

Clean up:
```bash
rm -rf /tmp/runbook-smoke
```

### Step 4: Full suite

`PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: no regressions, all new tests pass.

### Step 5: Clean up any smoke side effects

```bash
git checkout -- tests/data_lineage/fixtures/ tests/fixtures/ 2>/dev/null
rm -rf lakehouse-worklist.json iceberg-runbook/ 2>/dev/null
git status -s
```
Expected: only `SKILL.md` modified.

### Step 6: Commit

```bash
git add skills/open_table_migrator/SKILL.md
git commit -m "docs(runbook): phased migration runbook section in SKILL.md"
```

---
