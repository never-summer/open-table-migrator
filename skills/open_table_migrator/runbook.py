"""Phased migration runbook generator."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .detector import PartitionTransform
from .sql_registry import TableDef
from .worklist import WorklistEntry


@dataclass(frozen=True)
class CodeSite:
    file: str
    line: int
    direction: str
    path_arg: str | None
    original_code: str


@dataclass(frozen=True)
class TableMigration:
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
    return "orc" if "orc" in pattern_type.lower() else "parquet"


def _match_hive_table(path_arg, sql_defs):
    if not path_arg:
        return None
    key = path_arg.strip("`").lower()
    short = key.rsplit(".", 1)[-1]
    for d in sql_defs:
        if d.table_name.lower() == key or d.table_name.lower() == short:
            return d
    return None


def _spec_from_dicts(raw):
    out = []
    for entry in raw:
        if isinstance(entry, PartitionTransform):
            out.append(entry)
            continue
        out.append(PartitionTransform(kind=entry["kind"], column=entry["column"], n=entry.get("n")))
    return tuple(out)


def build_table_migrations(entries, sql_defs, dyn_cross=None):
    groups = {}
    for e in entries:
        if e.resolved_namespace is None or e.resolved_table is None:
            continue
        groups.setdefault((e.resolved_namespace, e.resolved_table), []).append(e)

    migrations = []
    for (ns, tbl), group in sorted(groups.items()):
        source_format = _source_format(group[0].pattern_type)
        write_entries = [e for e in group if e.direction == "write"]

        source_path = None
        for e in (write_entries + group):
            if e.path_arg:
                source_path = e.path_arg
                break

        hive_td = _match_hive_table(source_path, sql_defs)
        source_hive_table = hive_td.table_name if hive_td else None
        source_sql_file = str(hive_td.file) if hive_td else None
        source_sql_line = hive_td.line if hive_td else None

        partition_spec = ()
        for e in write_entries:
            if e.partition_spec:
                partition_spec = _spec_from_dicts(e.partition_spec)
                break

        partition_mismatch = None
        for e in group:
            if "partition_mismatch" in e.attrs:
                partition_mismatch = e.attrs["partition_mismatch"]
                break

        code_sites = tuple(sorted([
            CodeSite(file=e.file, line=e.start_line, direction=e.direction,
                     path_arg=e.path_arg, original_code=e.original_code)
            for e in group
        ], key=lambda c: (c.file, c.line)))

        notes_set = set()
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
            namespace=ns, table=tbl, source_format=source_format,
            source_path=source_path, source_hive_table=source_hive_table,
            source_sql_file=source_sql_file, source_sql_line=source_sql_line,
            code_sites=code_sites, partition_spec=partition_spec,
            partition_mismatch=partition_mismatch, notes=tuple(sorted(notes_set)),
        ))
    return migrations


def _format_transforms_inline(transforms):
    if not transforms:
        return "(none)"
    parts = []
    for t in transforms:
        if t.kind == "identity":
            parts.append(f"identity({t.column})")
        elif t.kind == "bucket":
            parts.append(f"bucket({t.n}, {t.column})")
    return ", ".join(parts)


def _format_partitioned_by_clause(transforms):
    if not transforms:
        return ""
    parts = []
    for t in transforms:
        if t.kind == "identity":
            parts.append(t.column)
        elif t.kind == "bucket":
            parts.append(f"bucket({t.n}, {t.column})")
    return "PARTITIONED BY (" + ", ".join(parts) + ")"


def _render_readme(migrations):
    lines = [
        "# Iceberg Migration Runbook", "",
        "Generated by open_table_migrator. **Review carefully before execution.**", "",
        f"## Tables to migrate ({len(migrations)})", "",
        "| # | Source | Target | Source format | Code sites | Partition spec |",
        "|---|---|---|---|---|---|",
    ]
    for i, m in enumerate(migrations, start=1):
        source = m.source_path or m.source_hive_table or "(unknown)"
        ps = _format_transforms_inline(m.partition_spec)
        if m.partition_mismatch:
            ps += " ⚠ MISMATCH"
        lines.append(
            f"| {i} | `{source}` | `{m.namespace}.{m.table}` | {m.source_format} | "
            f"{len(m.code_sites)} | {ps} |"
        )
    if any(m.partition_mismatch for m in migrations):
        lines += ["", "⚠ Tables flagged with MISMATCH have code↔DDL partition divergence. See per-table runbook."]
    lines += [
        "", "## Execution order", "",
        "Phases must run in order **per table**. Tables themselves can run in parallel.",
        "Recommended sequence per table:",
        "- Phase 1 (add_files) — minutes, in-place, low-risk",
        "- Validation pause (verify table count matches source)",
        "- Phase 2 (rewrite) — hours, resource-heavy, can be deferred",
        "- Phase 3 (switchover) — minutes, coordinated cutover",
        "", "## Per-table runbooks", "",
    ]
    for i, m in enumerate(migrations, start=1):
        flag = " ⚠ partition mismatch" if m.partition_mismatch else ""
        lines.append(f"{i}. [{m.namespace}.{m.table}](./{m.namespace}.{m.table}/migration-plan.md){flag}")
    return "\n".join(lines) + "\n"


def _render_plan_md(m):
    fq = f"`{m.namespace}.{m.table}`"
    source = m.source_path or m.source_hive_table or "(unknown)"
    ps_inline = _format_transforms_inline(m.partition_spec)
    sql_ref = f"`{m.source_sql_file}:{m.source_sql_line}`" if m.source_sql_file and m.source_sql_line else "(none found)"
    n_read = sum(1 for c in m.code_sites if c.direction == "read")
    n_write = sum(1 for c in m.code_sites if c.direction == "write")
    lines = [
        f"# Migration plan: {fq}", "",
        f"**Source:** `{source}` ({m.source_format})",
        f"**Target:** {fq} (Iceberg)",
        f"**Source DDL:** {sql_ref}",
        f"**Partition spec:** {ps_inline}",
        f"**Code sites:** {len(m.code_sites)} ({n_read} reads, {n_write} writes)",
        "", "## Pre-flight checklist", "",
        f"- [ ] Iceberg catalog `{m.namespace.split('.')[0]}` is registered in Spark / HMS",
        f"- [ ] Target schema `{m.namespace}` exists",
        f"- [ ] Source table is in Hive Metastore",
        "- [ ] Backup of source DDL captured",
        "- [ ] Maintenance window scheduled (if Phase 3 affects production)",
        "", "## Phase 1: add_files (in-place, ~minutes)", "",
        "Run `phase1_add_files.sql`. Brings existing data files under Iceberg metadata "
        "without rewriting data. Reversible by dropping the target table.",
        "",
        f"**Validation after:** `SELECT COUNT(*) FROM {m.namespace}.{m.table};` should equal "
        f"the source table count.",
        "", "## Phase 2: rewrite_data_files (compaction, ~hours)", "",
        "Run `phase2_rewrite.sql`. Compacts small files; applies sort order; rewrites "
        "partition layout. Resource-heavy — schedule outside peak hours.",
        "",
        "**Skip Phase 2 if:** source files are already well-sized (>128MB each) and you do NOT "
        "need the new partition spec.",
        "", "## Phase 3: switchover (coordinated cutover)", "",
        "Run `phase3_switchover.sql`. Three options listed inside — pick ONE per your stack.",
        "", "## Code sites (from `lakehouse-worklist.json`)", "",
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


def _render_phase1(m):
    fq = f"{m.namespace}.{m.table}"
    partitioned = _format_partitioned_by_clause(m.partition_spec)
    source_label = m.source_hive_table or m.source_path or "<source_table>"
    lines = []
    if m.partition_mismatch:
        lines += [
            "-- ⚠ PARTITION MISMATCH DETECTED:",
            f"-- {m.partition_mismatch}",
            "-- Pre-create the Iceberg table with the partition spec you actually want.",
            "-- The PARTITIONED BY clause below reflects the CODE side. If DDL is right,",
            "-- replace it.", "",
        ]
    lines += [
        f"-- PHASE 1: Add existing {m.source_format.capitalize()} files to Iceberg metadata (in-place)",
        f"-- Estimated runtime: ~minutes for typical tables. No data rewrite.",
        f"-- Reversible: DROP TABLE {fq};", "",
        f"CREATE TABLE {fq} (",
        "  -- TODO: copy schema from source. Generate via:",
        f"  --   spark.read.{m.source_format}('{m.source_path or '<source_path>'}').printSchema()",
    ]
    if m.source_sql_file:
        lines.append(f"  -- Or capture from source DDL: {m.source_sql_file}:{m.source_sql_line}")
    lines += [")", "USING iceberg"]
    if partitioned:
        lines.append(partitioned)
    lines += [
        "TBLPROPERTIES (",
        "  'write.parquet.compression-codec' = 'zstd',",
        "  'format-version' = '2'",
        ");", "",
        f"CALL system.add_files(",
        f"  table => '{fq}',",
        f"  source_table => '`{source_label}`'",
        f");", "",
        "-- Validation:",
        f"-- SELECT COUNT(*) FROM {fq};",
        f"-- SELECT COUNT(*) FROM `{source_label}`;",
        "-- The counts MUST match before proceeding to Phase 2.",
    ]
    return "\n".join(lines) + "\n"


def _render_phase2(m):
    fq = f"{m.namespace}.{m.table}"
    lines = [
        "-- PHASE 2: Compact and rewrite data files",
        "-- Estimated runtime: minutes to hours depending on data size. Resource-heavy.",
        "-- Can be deferred — Phase 1 result is already queryable.", "",
        "CALL system.rewrite_data_files(",
        f"  table => '{fq}',",
        "  strategy => 'binpack',",
        "  options => map('min-input-files', '5', 'target-file-size-bytes', '536870912')",
        ");", "",
        "-- If you changed partition spec in Phase 1 (vs original Hive PARTITIONED BY):",
        "-- CALL system.rewrite_data_files(",
        f"--   table => '{fq}',",
        "--   options => map('partial-progress.enabled', 'true')",
        "-- );", "",
        "-- Validation: no row count change, file count should decrease significantly.",
        f"-- SELECT data_file_count, total_data_file_size_in_bytes FROM {fq}.snapshots;",
    ]
    return "\n".join(lines) + "\n"


def _render_phase3(m):
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
        f"-- DOES NOT WORK FOR: clients doing INSERT INTO `{hive_ref}` — they will fail.", "",
        f"-- DROP TABLE `{hive_ref}`;",
        f"-- CREATE VIEW `{hive_ref}` AS SELECT * FROM {fq};", "",
        "-- ==========================================================================",
        "-- OPTION B: Hive Metastore direct rename",
        "-- ==========================================================================",
        "-- Atomic at metastore level. Requires HMS admin access.",
        "-- Source table is renamed; Iceberg takes its place.",
        "--",
        f"-- ALTER TABLE `{hive_ref}` RENAME TO `{hive_ref}_legacy_{m.source_format}`;",
        "-- -- Then register Iceberg under the original name via your catalog config.", "",
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


def serialize_runbook(entries, sql_defs, dyn_cross, project_root):
    """Build runbook contents as {relative_path: file_content}. No filesystem writes."""
    migrations = build_table_migrations(entries, sql_defs, dyn_cross)
    if not migrations:
        return {}
    root = Path("iceberg-runbook")
    files = {root / "README.md": _render_readme(migrations)}
    for m in migrations:
        table_dir = root / f"{m.namespace}.{m.table}"
        files[table_dir / "migration-plan.md"] = _render_plan_md(m)
        files[table_dir / "phase1_add_files.sql"] = _render_phase1(m)
        files[table_dir / "phase2_rewrite.sql"] = _render_phase2(m)
        files[table_dir / "phase3_switchover.sql"] = _render_phase3(m)
    return files
