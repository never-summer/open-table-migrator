import textwrap
from pathlib import Path
from skills.parquet_to_iceberg.detector import detect_parquet_usage, PatternMatch


def write_file(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(content))
    return p


def test_detects_pandas_read(tmp_path):
    write_file(tmp_path, "etl.py", """
        import pandas as pd
        df = pd.read_parquet("data/events.parquet")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert len(matches) == 1
    assert matches[0].pattern_type == "pandas_read"
    assert matches[0].file.name == "etl.py"


def test_detects_pandas_write(tmp_path):
    write_file(tmp_path, "etl.py", """
        import pandas as pd
        df.to_parquet("out.parquet", index=False)
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pandas_write" for m in matches)


def test_detects_pyspark_read(tmp_path):
    write_file(tmp_path, "jobs.py", """
        df = spark.read.parquet("s3://bucket/events/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pyspark_read" for m in matches)


def test_detects_pyspark_write(tmp_path):
    write_file(tmp_path, "jobs.py", """
        df.write.mode("overwrite").parquet("output/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pyspark_write" for m in matches)


def test_detects_pyarrow_read(tmp_path):
    write_file(tmp_path, "store.py", """
        import pyarrow.parquet as pq
        t = pq.read_table("data.parquet")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pyarrow_read" for m in matches)


def test_detects_pyarrow_write(tmp_path):
    write_file(tmp_path, "store.py", """
        import pyarrow.parquet as pq
        pq.write_table(table, "data.parquet")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pyarrow_write" for m in matches)


def test_skips_non_source_files(tmp_path):
    write_file(tmp_path, "README.md", "pd.read_parquet is useful")
    matches = detect_parquet_usage(tmp_path)
    assert len(matches) == 0


def test_no_matches_in_clean_project(tmp_path):
    write_file(tmp_path, "app.py", "x = 1 + 1")
    assert detect_parquet_usage(tmp_path) == []


# ─── Java/Scala Spark patterns ─────────────────────────────────────────

def test_detects_java_spark_read(tmp_path):
    write_file(tmp_path, "src/Job.java", """
        import org.apache.spark.sql.Dataset;
        public class Job {
            Dataset<Row> df = spark.read().parquet("data/events/");
        }
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "java_spark_read" for m in matches)


def test_detects_java_spark_write(tmp_path):
    write_file(tmp_path, "src/Job.java", """
        df.write().mode("overwrite").parquet("output/");
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "java_spark_write" for m in matches)


def test_detects_scala_spark_read(tmp_path):
    write_file(tmp_path, "src/Job.scala", """
        val df = spark.read.parquet("data/events/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "scala_spark_read" for m in matches)


def test_detects_scala_spark_write(tmp_path):
    write_file(tmp_path, "src/Job.scala", """
        df.write.mode("overwrite").parquet("output/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "scala_spark_write" for m in matches)


# ─── Hive / SparkSQL patterns ──────────────────────────────────────────

def test_detects_hive_stored_as_parquet(tmp_path):
    write_file(tmp_path, "src/Hive.java", '''
        spark.sql("CREATE TABLE events (id BIGINT) STORED AS PARQUET");
    ''')
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "hive_create_parquet" for m in matches)


def test_detects_hive_save_as_table(tmp_path):
    write_file(tmp_path, "src/Hive.java", """
        df.write().mode("overwrite").saveAsTable("events");
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "hive_save_as_table" for m in matches)


def test_detects_hive_insert_into(tmp_path):
    write_file(tmp_path, "src/Hive.java", '''
        spark.sql("INSERT OVERWRITE TABLE events SELECT * FROM staging");
    ''')
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "hive_insert_overwrite" for m in matches)
