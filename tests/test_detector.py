import textwrap
from pathlib import Path
from skills.open_table_migrator.scripts.detector import detect_parquet_usage, PatternMatch


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
    assert matches[0].pattern_type == "pandas_read_parquet"
    assert matches[0].file.name == "etl.py"


def test_detects_pandas_write(tmp_path):
    write_file(tmp_path, "etl.py", """
        import pandas as pd
        df.to_parquet("out.parquet", index=False)
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pandas_write_parquet" for m in matches)


def test_detects_pyspark_read(tmp_path):
    write_file(tmp_path, "jobs.py", """
        df = spark.read.parquet("s3://bucket/events/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "spark_read_parquet" for m in matches)


def test_detects_pyspark_write(tmp_path):
    write_file(tmp_path, "jobs.py", """
        df.write.mode("overwrite").parquet("output/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "spark_write_parquet" for m in matches)


def test_detects_pyarrow_read(tmp_path):
    write_file(tmp_path, "store.py", """
        import pyarrow.parquet as pq
        t = pq.read_table("data.parquet")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pyarrow_read_parquet" for m in matches)


def test_detects_pyarrow_write(tmp_path):
    write_file(tmp_path, "store.py", """
        import pyarrow.parquet as pq
        pq.write_table(table, "data.parquet")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pyarrow_write_parquet" for m in matches)


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
    assert any(m.pattern_type == "spark_read_parquet" for m in matches)


def test_detects_java_spark_write(tmp_path):
    write_file(tmp_path, "src/Job.java", """
        df.write().mode("overwrite").parquet("output/");
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "spark_write_parquet" for m in matches)


def test_detects_scala_spark_read(tmp_path):
    write_file(tmp_path, "src/Job.scala", """
        val df = spark.read.parquet("data/events/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "spark_read_parquet" for m in matches)


def test_detects_scala_spark_write(tmp_path):
    write_file(tmp_path, "src/Job.scala", """
        df.write.mode("overwrite").parquet("output/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "spark_write_parquet" for m in matches)


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
    assert any(m.pattern_type == "hive_save_table" for m in matches)


def test_detects_hive_insert_into(tmp_path):
    write_file(tmp_path, "src/Hive.java", '''
        spark.sql("INSERT OVERWRITE TABLE events SELECT * FROM staging");
    ''')
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type.startswith("hive_insert_") for m in matches)


# ─── Const-folding / identifier resolution ────────────────────────────

from textwrap import dedent
from pathlib import Path


def test_python_module_const_resolves_in_read_parquet(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        import pandas as pd
        EVENTS_PATH = "s3://bucket/events"
        df = pd.read_parquet(EVENTS_PATH)
    '''))
    from skills.open_table_migrator.scripts.detector import detect_all_io
    matches = detect_all_io(tmp_path)
    assert len(matches) >= 1
    pq_match = [m for m in matches if "parquet" in m.pattern_type]
    assert len(pq_match) == 1
    m = pq_match[0]
    assert m.path_arg == "s3://bucket/events"
    assert "resolved_from" in m.attrs
    assert "EVENTS_PATH" in m.attrs["resolved_from"]


def test_python_function_local_const_resolves(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        import pandas as pd
        def run():
            path = "s3://bucket/users"
            df = pd.read_parquet(path)
    '''))
    from skills.open_table_migrator.scripts.detector import detect_all_io
    matches = detect_all_io(tmp_path)
    pq_match = [m for m in matches if "parquet" in m.pattern_type]
    assert len(pq_match) == 1
    assert pq_match[0].path_arg == "s3://bucket/users"


def test_python_reassigned_skipped(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        import pandas as pd
        PATH = "s3://a"
        PATH = "s3://b"
        df = pd.read_parquet(PATH)
    '''))
    from skills.open_table_migrator.scripts.detector import detect_all_io
    matches = detect_all_io(tmp_path)
    pq_match = [m for m in matches if "parquet" in m.pattern_type]
    assert len(pq_match) == 1
    m = pq_match[0]
    assert m.path_arg is None
    assert m.attrs.get("skipped_reason") == "reassigned"


def test_java_static_final_resolves(tmp_path: Path):
    (tmp_path / "Job.java").write_text(dedent('''
        public class Job {
            private static final String EVENTS = "s3://bucket/events";
            void run() {
                spark.read().parquet(EVENTS);
            }
        }
    '''))
    from skills.open_table_migrator.scripts.detector import detect_all_io
    matches = detect_all_io(tmp_path)
    assert any(m.path_arg == "s3://bucket/events" for m in matches)


def test_scala_val_resolves(tmp_path: Path):
    (tmp_path / "Job.scala").write_text(dedent('''
        object Job {
            val EVENTS = "s3://bucket/events"
            def run(): Unit = {
                spark.read.parquet(EVENTS)
            }
        }
    '''))
    from skills.open_table_migrator.scripts.detector import detect_all_io
    matches = detect_all_io(tmp_path)
    assert any(m.path_arg == "s3://bucket/events" for m in matches)


def test_python_unresolved_identifier_keeps_none(tmp_path: Path):
    (tmp_path / "job.py").write_text(dedent('''
        import pandas as pd
        df = pd.read_parquet(SOME_UNDEFINED_PATH)
    '''))
    from skills.open_table_migrator.scripts.detector import detect_all_io
    matches = detect_all_io(tmp_path)
    pq_match = [m for m in matches if "parquet" in m.pattern_type]
    assert len(pq_match) == 1
    assert pq_match[0].path_arg is None
    assert "skipped_reason" not in pq_match[0].attrs
