"""Tests for runbook module."""
from pathlib import Path

from skills.open_table_migrator.scripts.detector import PartitionTransform
from skills.open_table_migrator.scripts.runbook import (
    CodeSite, TableMigration, build_table_migrations,
)
from skills.open_table_migrator.scripts.worklist import WorklistEntry


def _entry(*, file="src/job.py", start_line=1, pattern_type="pandas_read_parquet",
           direction="read", path_arg=None, namespace="analytics", table="events",
           original_code='df = pd.read_parquet("s3://prod/events")',
           attrs=None, partition_spec=None):
    return WorklistEntry(
        file=file, start_line=start_line, end_line=start_line,
        pattern_type=pattern_type, direction=direction, language="python",
        path_arg=path_arg, original_code=original_code, surrounding="",
        resolved_namespace=namespace, resolved_table=table,
        needs_manual_target=False, hint="",
        attrs=attrs or {}, partition_spec=partition_spec or [],
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
    entry = WorklistEntry(
        file="x.py", start_line=1, end_line=1,
        pattern_type="pandas_read_parquet", direction="read",
        language="python", path_arg="s3://bucket/x",
        original_code='df = pd.read_parquet("s3://bucket/x")', surrounding="",
        resolved_namespace=None, resolved_table=None,
        needs_manual_target=True, hint="",
    )
    migrations = build_table_migrations([entry], sql_defs=[], dyn_cross=None)
    assert migrations == []


def test_source_format_from_pattern_type():
    parquet_m = build_table_migrations([_entry(pattern_type="pandas_read_parquet")], sql_defs=[])
    orc_m = build_table_migrations([_entry(pattern_type="pandas_read_orc", table="t2")], sql_defs=[])
    assert parquet_m[0].source_format == "parquet"
    assert orc_m[0].source_format == "orc"


def test_partition_spec_inherited_from_write_entry():
    entries = [
        _entry(direction="read"),
        _entry(direction="write", partition_spec=[
            {"kind": "identity", "column": "region"},
            {"kind": "bucket", "column": "uid", "n": 8},
        ]),
    ]
    migrations = build_table_migrations(entries, sql_defs=[])
    spec = migrations[0].partition_spec
    assert PartitionTransform(kind="identity", column="region") in spec
    assert PartitionTransform(kind="bucket", column="uid", n=8) in spec


def test_partition_mismatch_picked_up_from_attrs():
    entries = [_entry(attrs={"partition_mismatch": "code: identity(region); ddl: identity(date_col)"})]
    migrations = build_table_migrations(entries, sql_defs=[])
    assert migrations[0].partition_mismatch == "code: identity(region); ddl: identity(date_col)"


from skills.open_table_migrator.scripts.runbook import serialize_runbook


def test_serialize_runbook_one_migration_produces_5_files(tmp_path):
    files = serialize_runbook([_entry()], sql_defs=[], dyn_cross=None, project_root=tmp_path)
    rel_paths = {str(p) for p in files.keys()}
    assert "iceberg-runbook/README.md" in rel_paths
    assert "iceberg-runbook/analytics.events/migration-plan.md" in rel_paths
    assert "iceberg-runbook/analytics.events/phase1_add_files.sql" in rel_paths
    assert "iceberg-runbook/analytics.events/phase2_rewrite.sql" in rel_paths
    assert "iceberg-runbook/analytics.events/phase3_switchover.sql" in rel_paths


def test_serialize_runbook_empty_entries_returns_empty(tmp_path):
    assert serialize_runbook([], sql_defs=[], dyn_cross=None, project_root=tmp_path) == {}


def test_readme_contains_summary_table_listing_migrations(tmp_path):
    entries = [_entry(table="events"), _entry(file="x.py", start_line=2, table="users")]
    files = serialize_runbook(entries, sql_defs=[], dyn_cross=None, project_root=tmp_path)
    readme = files[Path("iceberg-runbook/README.md")]
    assert "analytics.events" in readme
    assert "analytics.users" in readme


def test_per_table_directory_naming(tmp_path):
    entries = [_entry(namespace="warehouse", table="orders")]
    files = serialize_runbook(entries, sql_defs=[], dyn_cross=None, project_root=tmp_path)
    assert "iceberg-runbook/warehouse.orders/migration-plan.md" in {str(p) for p in files.keys()}


def test_phase1_contains_add_files_call(tmp_path):
    files = serialize_runbook([_entry()], sql_defs=[], dyn_cross=None, project_root=tmp_path)
    phase1 = files[Path("iceberg-runbook/analytics.events/phase1_add_files.sql")]
    assert "system.add_files" in phase1
    assert "analytics.events" in phase1


def test_phase3_has_three_option_blocks(tmp_path):
    files = serialize_runbook([_entry()], sql_defs=[], dyn_cross=None, project_root=tmp_path)
    phase3 = files[Path("iceberg-runbook/analytics.events/phase3_switchover.sql")]
    assert "OPTION A" in phase3 and "OPTION B" in phase3 and "OPTION C" in phase3
    assert "src/job.py" in phase3


def test_phase1_renders_partition_mismatch_warning(tmp_path):
    entries = [_entry(attrs={"partition_mismatch": "code: identity(region); ddl: identity(date_col)"})]
    files = serialize_runbook(entries, sql_defs=[], dyn_cross=None, project_root=tmp_path)
    phase1 = files[Path("iceberg-runbook/analytics.events/phase1_add_files.sql")]
    assert "PARTITION MISMATCH DETECTED" in phase1
    assert "code: identity(region); ddl: identity(date_col)" in phase1


def test_migration_plan_includes_dyn_cross_note(tmp_path):
    from types import SimpleNamespace
    loader = SimpleNamespace(file="src/loader.py", line=42, sql_filename="queries/events.sql")
    table_ref = SimpleNamespace(table_name="events")
    cross = SimpleNamespace(loader=loader, tables=[table_ref])
    files = serialize_runbook([_entry()], sql_defs=[], dyn_cross=[cross], project_root=tmp_path)
    plan = files[Path("iceberg-runbook/analytics.events/migration-plan.md")]
    assert "Dynamic SQL loader at src/loader.py:42" in plan
    assert "queries/events.sql" in plan
