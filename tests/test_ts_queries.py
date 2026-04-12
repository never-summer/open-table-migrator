"""Comprehensive tests for the tree-sitter AST detector."""
import textwrap
from pathlib import Path

from skills.open_table_migrator.ts_detector import ts_detect


def write(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(content))
    return p


# ── pandas ────────────────────────────────────────────────────────────────

def test_pandas_read_parquet(tmp_path):
    write(tmp_path, "etl.py", 'import pandas as pd\ndf = pd.read_parquet("data.parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_read_parquet" and m.format == "parquet" for m in matches)


def test_pandas_write_parquet(tmp_path):
    write(tmp_path, "etl.py", 'df.to_parquet("out.parquet", index=False)\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_write_parquet" for m in matches)


def test_pandas_read_csv(tmp_path):
    write(tmp_path, "etl.py", 'import pandas as pd\ndf = pd.read_csv("data.csv")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_read_csv" for m in matches)


def test_pandas_write_json(tmp_path):
    write(tmp_path, "etl.py", 'df.to_json("out.json")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_write_json" for m in matches)


def test_pandas_read_orc(tmp_path):
    write(tmp_path, "etl.py", 'import pandas as pd\ndf = pd.read_orc("data.orc")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_read_orc" for m in matches)


def test_pandas_no_false_positive_in_comment(tmp_path):
    write(tmp_path, "etl.py", '# df = pd.read_parquet("data.parquet")\nx = 1\n')
    matches = ts_detect(tmp_path)
    assert not any("pandas" in m.pattern_type for m in matches)


def test_pandas_no_false_positive_in_string(tmp_path):
    write(tmp_path, "etl.py", 'msg = "use pd.read_parquet to load data"\n')
    matches = ts_detect(tmp_path)
    assert not any("pandas" in m.pattern_type for m in matches)


def test_pandas_dynamic_format(tmp_path):
    write(tmp_path, "etl.py", 'import pandas as pd\ndf = pd.read_feather("data.feather")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_read_feather" and m.format == "feather" for m in matches)


# ── Spark Python ──────────────────────────────────────────────────────────

def test_spark_read_parquet_py(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.read.parquet("s3://bucket/events/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_parquet" for m in matches)


def test_spark_write_parquet_py(tmp_path):
    write(tmp_path, "jobs.py", 'df.write.mode("overwrite").parquet("output/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_write_parquet" for m in matches)


def test_spark_read_format_orc_py(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.read.format("orc").load("data/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_orc" and m.format == "orc" for m in matches)


def test_spark_write_format_csv_py(tmp_path):
    write(tmp_path, "jobs.py", 'df.write.format("csv").save("out/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_write_csv" for m in matches)


def test_spark_read_csv_py(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.read.csv("data.csv")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_csv" for m in matches)


def test_spark_table_read(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.table("events")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_table" and m.path_arg == "events" for m in matches)


# ── Spark Java ────────────────────────────────────────────────────────────

def test_spark_read_parquet_java(tmp_path):
    write(tmp_path, "src/Job.java", 'Dataset<Row> df = spark.read().parquet("data/events/");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_parquet" for m in matches)


def test_spark_write_parquet_java(tmp_path):
    write(tmp_path, "src/Job.java", 'df.write().mode("overwrite").parquet("output/");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_write_parquet" for m in matches)


def test_spark_format_parquet_java(tmp_path):
    write(tmp_path, "src/Job.java", 'Dataset<Row> df = spark.read().format("parquet").load("data/");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_parquet" for m in matches)


# ── Spark Scala ───────────────────────────────────────────────────────────

def test_spark_read_parquet_scala(tmp_path):
    write(tmp_path, "src/Job.scala", 'val df = spark.read.parquet("data/events/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_parquet" for m in matches)


def test_spark_write_parquet_scala(tmp_path):
    write(tmp_path, "src/Job.scala", 'df.write.mode("overwrite").parquet("output/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_write_parquet" for m in matches)


def test_spark_multiline_chain_scala(tmp_path):
    write(tmp_path, "src/Job.scala", textwrap.dedent("""\
        usersDF.write
          .format("parquet")
          .bucketBy(8, "uid")
          .mode(SaveMode.OverWrite)
          .saveAsTable("UsersTbl")
    """))
    matches = ts_detect(tmp_path)
    assert any(m.path_arg == "UsersTbl" for m in matches)


# ── Streaming ─────────────────────────────────────────────────────────────

def test_spark_stream_read_py(tmp_path):
    write(tmp_path, "s.py", 'df = spark.readStream.parquet("s3://stream/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_stream_read_parquet" for m in matches)


def test_spark_stream_write_format_py(tmp_path):
    write(tmp_path, "s.py", 'df.writeStream.format("parquet").option("path", "out/").start()\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_stream_write_parquet" for m in matches)


# ── pyarrow ───────────────────────────────────────────────────────────────

def test_pyarrow_read_parquet(tmp_path):
    write(tmp_path, "s.py", 'import pyarrow.parquet as pq\nt = pq.read_table("data.parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pyarrow_read_parquet" for m in matches)


def test_pyarrow_write_parquet(tmp_path):
    write(tmp_path, "s.py", 'import pyarrow.parquet as pq\npq.write_table(table, "data.parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pyarrow_write_parquet" for m in matches)


def test_pyarrow_read_orc(tmp_path):
    write(tmp_path, "s.py", 'import pyarrow.orc as orc\nt = orc.read_table("data.orc")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pyarrow_read_orc" for m in matches)


def test_pyarrow_parquet_file(tmp_path):
    write(tmp_path, "s.py", 'import pyarrow.parquet as pq\nf = pq.ParquetFile("data.parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pyarrow_read_dataset" for m in matches)


def test_pyarrow_dataset_write(tmp_path):
    write(tmp_path, "s.py", 'import pyarrow as pa\npa.dataset.write_dataset(tbl, "out/", format="parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pyarrow_write_dataset" for m in matches)


# ── Hive SQL ──────────────────────────────────────────────────────────────

def test_hive_stored_as_parquet_java(tmp_path):
    write(tmp_path, "src/H.java", 'spark.sql("CREATE TABLE events (id BIGINT) STORED AS PARQUET");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "hive_create_parquet" for m in matches)


def test_hive_stored_as_orc_scala(tmp_path):
    write(tmp_path, "src/H.scala", 'spark.sql("CREATE TABLE events (id BIGINT) STORED AS ORC")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "hive_create_orc" for m in matches)


def test_hive_using_parquet_py(tmp_path):
    write(tmp_path, "j.py", 'spark.sql("CREATE TABLE events (id BIGINT) USING parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "hive_create_parquet" for m in matches)


def test_hive_insert_overwrite(tmp_path):
    write(tmp_path, "src/H.java", 'spark.sql("INSERT OVERWRITE TABLE events SELECT * FROM staging");\n')
    matches = ts_detect(tmp_path)
    assert any("hive_insert" in m.pattern_type for m in matches)


def test_hive_save_as_table(tmp_path):
    write(tmp_path, "src/H.java", 'df.write().mode("overwrite").saveAsTable("events");\n')
    matches = ts_detect(tmp_path)
    assert any(m.path_arg == "events" for m in matches)


# ── stdlib ────────────────────────────────────────────────────────────────

def test_stdlib_csv_writer(tmp_path):
    write(tmp_path, "e.py", 'import csv\nw = csv.writer(open("out.csv", "w"))\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "stdlib_write_csv" for m in matches)


def test_stdlib_csv_reader(tmp_path):
    write(tmp_path, "l.py", 'import csv\nr = csv.reader(open("data.csv"))\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "stdlib_read_csv" for m in matches)


def test_java_file_writer(tmp_path):
    write(tmp_path, "src/E.java", 'FileWriter fw = new FileWriter("out.txt");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "stdlib_write_file" for m in matches)


# ── Multiline + extraction ────────────────────────────────────────────────

def test_end_line_multiline(tmp_path):
    write(tmp_path, "src/J.scala", textwrap.dedent("""\
        usersDF.write
          .format("parquet")
          .bucketBy(8, "uid")
          .saveAsTable("UsersTbl")
    """))
    matches = ts_detect(tmp_path)
    m = next(m for m in matches if m.path_arg == "UsersTbl")
    assert m.end_line >= m.line + 2


def test_path_arg_from_pandas(tmp_path):
    write(tmp_path, "e.py", 'import pandas as pd\ndf = pd.read_parquet("data/events.parquet")\n')
    matches = ts_detect(tmp_path)
    m = next(m for m in matches if "pandas" in m.pattern_type)
    assert m.path_arg == "data/events.parquet"


def test_path_arg_from_write_table(tmp_path):
    write(tmp_path, "s.py", 'import pyarrow.parquet as pq\npq.write_table(table, "data.parquet")\n')
    matches = ts_detect(tmp_path)
    m = next(m for m in matches if "pyarrow" in m.pattern_type)
    assert m.path_arg == "data.parquet"
