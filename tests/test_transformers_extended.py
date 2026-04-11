"""Transformer coverage for ORC, format(), USING SQL, streaming, pyarrow dataset."""
from skills.parquet_to_iceberg.transformers.pandas import transform_pandas_file
from skills.parquet_to_iceberg.transformers.pyspark import transform_pyspark_file
from skills.parquet_to_iceberg.transformers.pyarrow import transform_pyarrow_file
from skills.parquet_to_iceberg.transformers.jvm import transform_jvm_file


# ─── Pandas ORC ───────────────────────────────────────────────────────

def test_pandas_orc_read_rewritten():
    src = 'import pandas as pd\ndf = pd.read_orc("data.orc")\n'
    out = transform_pandas_file(src, table_name="events", namespace="default")
    assert "pd.read_orc" not in out
    assert "tbl.scan().to_pandas()" in out


def test_pandas_orc_write_rewritten():
    src = 'import pandas as pd\ndf.to_orc("out.orc")\n'
    out = transform_pandas_file(src, table_name="events", namespace="default")
    assert ".to_orc(" not in out
    assert "tbl.overwrite(df)" in out


# ─── PySpark ORC and format() ─────────────────────────────────────────

def test_pyspark_orc_read_rewritten():
    src = 'df = spark.read.orc("s3://bucket/")\n'
    out = transform_pyspark_file(src, table_name="events", namespace="default")
    assert "spark.read.orc" not in out
    assert 'spark.table("default.events")' in out


def test_pyspark_format_parquet_rewritten():
    src = 'df = spark.read.format("parquet").load("data/")\n'
    out = transform_pyspark_file(src, table_name="events", namespace="default")
    assert '.format("parquet")' not in out
    assert 'spark.table("default.events")' in out


def test_pyspark_format_orc_write_rewritten():
    src = 'df.write.format("orc").save("out/")\n'
    out = transform_pyspark_file(src, table_name="events", namespace="default")
    assert '.format("orc")' not in out
    assert '.writeTo("default.events").overwritePartitions()' in out


def test_pyspark_streaming_kept_with_warning():
    src = 'df = spark.readStream.format("parquet").load("in/")\n'
    out = transform_pyspark_file(src, table_name="events", namespace="default")
    assert "TODO(iceberg)" in out
    assert "readStream" in out  # original line preserved


# ─── PyArrow ORC + dataset warn ───────────────────────────────────────

def test_pyarrow_orc_read_rewritten():
    src = 'from pyarrow import orc\nt = orc.read_table("data.orc")\n'
    out = transform_pyarrow_file(src, table_name="events", namespace="default")
    assert "orc.read_table" not in out
    assert "tbl.scan().to_arrow()" in out


def test_pyarrow_parquet_file_warned():
    src = 'import pyarrow.parquet as pq\nf = pq.ParquetFile("data.parquet")\n'
    out = transform_pyarrow_file(src, table_name="events", namespace="default")
    assert "TODO(iceberg)" in out
    assert "ParquetFile" in out  # original preserved


def test_pyarrow_dataset_write_warned():
    src = 'import pyarrow as pa\npa.dataset.write_dataset(tbl, "out/", format="parquet")\n'
    out = transform_pyarrow_file(src, table_name="events", namespace="default")
    assert "TODO(iceberg)" in out


# ─── JVM: ORC, format(), USING SQL, streaming ─────────────────────────

def test_java_orc_read_rewritten():
    src = 'Dataset<Row> df = spark.read().orc("data/");\n'
    out = transform_jvm_file(src, language="java", table_name="events", namespace="default")
    assert ".orc(" not in out
    assert '.read().format("iceberg").load("default.events")' in out


def test_java_orc_write_rewritten():
    src = 'df.write().mode("overwrite").orc("out/");\n'
    out = transform_jvm_file(src, language="java", table_name="events", namespace="default")
    assert ".orc(" not in out
    assert '.writeTo("default.events").overwritePartitions()' in out


def test_java_format_parquet_read_rewritten():
    src = 'Dataset<Row> df = spark.read().format("parquet").load("data/");\n'
    out = transform_jvm_file(src, language="java", table_name="events", namespace="default")
    assert '.format("parquet")' not in out
    assert '.read().format("iceberg").load("default.events")' in out


def test_scala_format_parquet_write_rewritten():
    src = 'df.write.format("parquet").save("out/")\n'
    out = transform_jvm_file(src, language="scala", table_name="events", namespace="default")
    assert '.format("parquet")' not in out
    assert '.writeTo("default.events").overwritePartitions()' in out


def test_sql_stored_as_orc_rewritten():
    src = 'spark.sql("CREATE TABLE t (id BIGINT) STORED AS ORC");\n'
    out = transform_jvm_file(src, language="java", table_name="events", namespace="default")
    assert "STORED AS ORC" not in out
    assert "USING iceberg" in out


def test_sql_using_parquet_rewritten():
    src = 'spark.sql("CREATE TABLE t (id BIGINT) USING parquet");\n'
    out = transform_jvm_file(src, language="java", table_name="events", namespace="default")
    assert "USING parquet" not in out
    assert "USING iceberg" in out


def test_sql_using_orc_rewritten():
    src = 'spark.sql("CREATE TABLE t (id BIGINT) USING orc");\n'
    out = transform_jvm_file(src, language="java", table_name="events", namespace="default")
    assert "USING orc" not in out
    assert "USING iceberg" in out


def test_jvm_streaming_warned_not_rewritten():
    src = 'Dataset<Row> df = spark.readStream().format("parquet").load("in/");\n'
    out = transform_jvm_file(src, language="java", table_name="events", namespace="default")
    assert "TODO(iceberg)" in out
    assert "readStream" in out  # original preserved
