from skills.open_table_migrator.detector import PartitionTransform, PatternMatch, detect_all_io
from pathlib import Path
from textwrap import dedent


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


def test_pyspark_partition_by_single_column(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy("region").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if m.direction == "write"]
    assert len(write_matches) == 1
    spec = write_matches[0].partition_spec
    assert spec == (PartitionTransform(kind="identity", column="region"),)


def test_pyspark_partition_by_multiple_columns(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy("region", "date").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if m.direction == "write"]
    spec = write_matches[0].partition_spec
    assert PartitionTransform(kind="identity", column="region") in spec
    assert PartitionTransform(kind="identity", column="date") in spec


def test_pyspark_bucket_by(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.bucketBy(8, "uid").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if m.direction == "write"]
    spec = write_matches[0].partition_spec
    assert spec == (PartitionTransform(kind="bucket", column="uid", n=8),)


def test_pyspark_partition_and_bucket_combined(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy("region").bucketBy(8, "uid").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if m.direction == "write"]
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
    write_matches = [m for m in matches if m.direction == "write"]
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
    write_matches = [m for m in matches if m.direction == "write"]
    spec = write_matches[0].partition_spec
    assert PartitionTransform(kind="identity", column="region") in spec


def test_partition_by_empty_call(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy().saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if m.direction == "write"]
    assert write_matches[0].partition_spec == ()


def test_partition_by_splat_skipped(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        cols = ["region"]
        df.write.partitionBy(*cols).saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if m.direction == "write"]
    assert write_matches[0].partition_spec == ()


def test_partition_by_function_call_skipped(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        df.write.partitionBy(year("ts")).saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if m.direction == "write"]
    assert all(t.column != "ts" for t in write_matches[0].partition_spec)


def test_bucket_by_n_as_identifier_skipped(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        N = 8
        df.write.bucketBy(N, "uid").saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if m.direction == "write"]
    assert all(t.kind != "bucket" for t in write_matches[0].partition_spec)


def test_partition_by_with_const_table_identifier(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        REGION = "region"
        df.write.partitionBy(REGION).saveAsTable("t")
    '''))
    matches = detect_all_io(tmp_path)
    write_matches = [m for m in matches if m.direction == "write"]
    spec = write_matches[0].partition_spec
    assert PartitionTransform(kind="identity", column="region") in spec
