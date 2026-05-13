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
    assert "partition_spec" not in blob


def test_worklist_entry_carries_partition_mismatch_attr(tmp_path):
    """When code and DDL disagree on partitions, the worklist JSON must carry
    the partition_mismatch warning in attrs (it's mutated onto matches by
    annotate_partition_mismatch in the CLI flow)."""
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) PARTITIONED BY (date_col STRING) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'df.write.partitionBy("region").saveAsTable("events")\n'
    )

    from skills.open_table_migrator.detector import detect_all_io
    from skills.open_table_migrator.sql_registry import scan_sql_files
    from skills.open_table_migrator.analyzer import annotate_partition_mismatch
    from skills.open_table_migrator.targets import Target, constant_resolver
    from skills.open_table_migrator.worklist import build_worklist

    matches = detect_all_io(tmp_path)
    sql_defs = scan_sql_files(tmp_path)
    annotate_partition_mismatch(matches, sql_defs)

    resolver = constant_resolver(Target(namespace="analytics", table="events"))
    entries = build_worklist(matches, tmp_path, resolver)
    write_entries = [e for e in entries if e.direction == "write"]
    assert len(write_entries) >= 1
    # Find the entry for `events` table
    matched = [e for e in write_entries if e.path_arg and "events" in e.path_arg]
    assert len(matched) >= 1
    e = matched[0]
    blob = e.to_dict()
    assert "attrs" in blob, f"WorklistEntry.to_dict() must include attrs; got keys {list(blob)}"
    assert "partition_mismatch" in blob["attrs"], (
        f"partition_mismatch missing; attrs={blob['attrs']}"
    )
    msg = blob["attrs"]["partition_mismatch"]
    assert "region" in msg
    assert "date_col" in msg
