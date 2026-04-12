"""Extended detector coverage: ORC, generic format(), USING SQL, streaming, pyarrow dataset."""
import textwrap
from pathlib import Path
from skills.open_table_migrator.detector import detect_parquet_usage


def write(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(content))
    return p


# ─── ORC patterns ─────────────────────────────────────────────────────

def test_detects_pandas_orc_read(tmp_path):
    write(tmp_path, "etl.py", 'import pandas as pd\ndf = pd.read_orc("data.orc")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pandas_orc_read" in types


def test_detects_pandas_orc_write(tmp_path):
    write(tmp_path, "etl.py", 'df.to_orc("out.orc")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pandas_orc_write" in types


def test_detects_pyspark_orc_read(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.read.orc("s3://bucket/events/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyspark_orc_read" in types


def test_detects_pyspark_orc_write(tmp_path):
    write(tmp_path, "jobs.py", 'df.write.mode("overwrite").orc("out/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyspark_orc_write" in types


def test_detects_java_spark_orc_read(tmp_path):
    write(tmp_path, "src/Job.java", 'Dataset<Row> df = spark.read().orc("data/");\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "java_spark_orc_read" in types


def test_detects_java_spark_orc_write(tmp_path):
    write(tmp_path, "src/Job.java", 'df.write().mode("overwrite").orc("out/");\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "java_spark_orc_write" in types


def test_detects_scala_spark_orc_read(tmp_path):
    write(tmp_path, "src/Job.scala", 'val df = spark.read.orc("data/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "scala_spark_orc_read" in types


def test_detects_scala_spark_orc_write(tmp_path):
    write(tmp_path, "src/Job.scala", 'df.write.mode("overwrite").orc("out/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "scala_spark_orc_write" in types


def test_detects_hive_stored_as_orc(tmp_path):
    write(tmp_path, "src/Job.java", 'spark.sql("CREATE TABLE t (id BIGINT) STORED AS ORC");\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "hive_create_orc" in types


# ─── Generic .format("parquet"/"orc") patterns ────────────────────────

def test_detects_pyspark_format_parquet_read(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.read.format("parquet").load("data/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyspark_read_fmt" in types


def test_detects_pyspark_format_orc_read(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.read.format("orc").load("data/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyspark_read_fmt" in types


def test_detects_pyspark_format_write(tmp_path):
    write(tmp_path, "jobs.py", 'df.write.format("parquet").save("out/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyspark_write_fmt" in types


def test_detects_java_format_parquet_read(tmp_path):
    write(tmp_path, "src/Job.java", 'Dataset<Row> df = spark.read().format("parquet").load("data/");\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "java_spark_read_fmt" in types


def test_detects_scala_format_parquet_write(tmp_path):
    write(tmp_path, "src/Job.scala", 'df.write.format("parquet").save("out/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "scala_spark_write_fmt" in types


def test_detects_java_format_parquet_read_with_options(tmp_path):
    write(tmp_path, "src/Job.java",
          'Dataset<Row> df = spark.read().option("mergeSchema", true).format("parquet").load("data/");\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "java_spark_read_fmt" in types


# ─── SQL USING parquet/orc ────────────────────────────────────────────

def test_detects_using_parquet(tmp_path):
    write(tmp_path, "src/Etl.java", 'spark.sql("CREATE TABLE t (id BIGINT) USING parquet");\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "sql_using_parquet" in types


def test_detects_using_orc(tmp_path):
    write(tmp_path, "src/Etl.java", 'spark.sql("CREATE TABLE t (id BIGINT) USING orc");\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "sql_using_orc" in types


def test_detects_using_parquet_in_python(tmp_path):
    write(tmp_path, "jobs.py", 'spark.sql("CREATE TABLE events (id BIGINT) USING parquet")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "sql_using_parquet" in types


# ─── INSERT INTO TABLE ────────────────────────────────────────────────

def test_detects_insert_into_table(tmp_path):
    write(tmp_path, "src/Etl.java", 'spark.sql("INSERT INTO TABLE events SELECT * FROM staging");\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "hive_insert_into" in types


def test_detects_insert_into_without_table_keyword(tmp_path):
    write(tmp_path, "src/Etl.java", 'spark.sql("INSERT INTO events SELECT * FROM staging");\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "hive_insert_into" in types


# ─── Structured Streaming ─────────────────────────────────────────────

def test_detects_pyspark_stream_read_parquet(tmp_path):
    write(tmp_path, "stream.py", 'df = spark.readStream.parquet("s3://stream/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyspark_stream_read" in types


def test_detects_pyspark_stream_read_format(tmp_path):
    write(tmp_path, "stream.py",
          'df = spark.readStream.format("parquet").option("path", "in/").load()\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyspark_stream_read_fmt" in types


def test_detects_pyspark_stream_write_parquet(tmp_path):
    write(tmp_path, "stream.py",
          'df.writeStream.format("parquet").option("path", "out/").start()\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyspark_stream_write_fmt" in types


def test_detects_java_stream_parquet(tmp_path):
    write(tmp_path, "src/Stream.java",
          'Dataset<Row> df = spark.readStream().format("parquet").load("in/");\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "java_spark_stream_read_fmt" in types


def test_detects_scala_stream_parquet(tmp_path):
    write(tmp_path, "src/Stream.scala",
          'val df = spark.readStream.format("parquet").load("in/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "scala_spark_stream_read_fmt" in types


# ─── PyArrow dataset / ParquetFile / ParquetDataset ───────────────────

def test_detects_pyarrow_parquet_file(tmp_path):
    write(tmp_path, "store.py", 'import pyarrow.parquet as pq\nf = pq.ParquetFile("data.parquet")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyarrow_parquet_file" in types


def test_detects_pyarrow_parquet_dataset(tmp_path):
    write(tmp_path, "store.py", 'import pyarrow.parquet as pq\nds = pq.ParquetDataset("data/")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyarrow_parquet_dataset" in types


def test_detects_pyarrow_dataset_read(tmp_path):
    write(tmp_path, "store.py",
          'import pyarrow as pa\nds = pa.dataset.dataset("data/", format="parquet")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyarrow_dataset_read" in types


def test_detects_pyarrow_dataset_write(tmp_path):
    write(tmp_path, "store.py",
          'import pyarrow as pa\npa.dataset.write_dataset(tbl, "out/", format="parquet")\n')
    types = {m.pattern_type for m in detect_parquet_usage(tmp_path)}
    assert "pyarrow_dataset_write" in types


# ─── Regression: no false positives for readStream vs read ────────────

def test_readstream_does_not_match_batch_read(tmp_path):
    write(tmp_path, "stream.py",
          'df = spark.readStream.format("parquet").load("in/")\n')
    matches = detect_parquet_usage(tmp_path)
    types = {m.pattern_type for m in matches}
    # Should flag as stream, not as batch pyspark_read_fmt
    assert "pyspark_stream_read_fmt" in types
    assert "pyspark_read_fmt" not in types
