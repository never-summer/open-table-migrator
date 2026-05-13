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
