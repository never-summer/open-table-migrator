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
