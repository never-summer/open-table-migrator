from pathlib import Path
from skills.open_table_migrator.analyzer import build_report, format_report, direction_of, is_migration_candidate
from skills.open_table_migrator.detector import PatternMatch


def _match(file: str, pattern: str, line: int = 1) -> PatternMatch:
    return PatternMatch(file=Path(file), line=line, pattern_type=pattern, original_code="...")


def test_direction_of_read():
    assert direction_of("pandas_read") == "read"
    assert direction_of("java_spark_read") == "read"
    assert direction_of("pyarrow_read") == "read"


def test_direction_of_write():
    assert direction_of("pandas_write") == "write"
    assert direction_of("hive_save_as_table") == "write"
    assert direction_of("hive_insert_overwrite") == "write"


def test_direction_of_schema():
    assert direction_of("hive_create_parquet") == "schema"


def test_build_report_counts():
    matches = [
        _match("a.py", "pandas_read"),
        _match("a.py", "pandas_write"),
        _match("b.java", "java_spark_read"),
        _match("b.java", "hive_create_parquet"),
    ]
    report = build_report(matches)
    assert report.total == 4
    assert len(report.by_file[Path("a.py")]) == 2
    assert len(report.by_direction["read"]) == 2
    assert len(report.by_direction["write"]) == 1
    assert len(report.by_direction["schema"]) == 1
    assert "pandas_read" in report.by_pattern_type
    assert len(report.by_pattern_type["pandas_read"]) == 1


def test_build_report_empty():
    report = build_report([])
    assert report.total == 0
    assert report.by_file == {}
    assert report.by_direction == {"read": [], "write": [], "schema": []}


def test_format_report_human_readable(tmp_path):
    matches = [
        _match(str(tmp_path / "a.py"), "pandas_read", line=10),
        _match(str(tmp_path / "a.py"), "pandas_write", line=20),
        _match(str(tmp_path / "b.java"), "java_spark_read", line=5),
    ]
    report = build_report(matches)
    text = format_report(report, project_root=tmp_path)
    assert "3 Parquet operation" in text or "Found 3" in text
    assert "a.py" in text
    assert "b.java" in text
    assert "read" in text.lower()
    assert "write" in text.lower()
    # Shows line numbers
    assert ":10" in text or "line 10" in text


def test_direction_of_new_taxonomy():
    assert direction_of("spark_read_parquet") == "read"
    assert direction_of("spark_write_csv") == "write"
    assert direction_of("spark_stream_read_orc") == "read"
    assert direction_of("spark_stream_write_parquet") == "write"
    assert direction_of("pandas_read_json") == "read"
    assert direction_of("pandas_write_excel") == "write"
    assert direction_of("pyarrow_read_parquet") == "read"
    assert direction_of("pyarrow_write_orc") == "write"
    assert direction_of("hive_create_parquet") == "schema"
    assert direction_of("hive_insert_orc") == "write"
    assert direction_of("hive_save_parquet") == "write"
    assert direction_of("stdlib_read_csv") == "read"
    assert direction_of("stdlib_write_file") == "write"


def test_direction_of_old_taxonomy_still_works():
    assert direction_of("pandas_read") == "read"
    assert direction_of("pyspark_write") == "write"
    assert direction_of("hive_create_parquet") == "schema"
    assert direction_of("hive_save_as_table") == "write"
    assert direction_of("java_spark_read") == "read"
    assert direction_of("scala_spark_stream_write_fmt") == "write"
    assert direction_of("pyarrow_dataset_write") == "write"


def test_is_migration_candidate_new_taxonomy():
    assert is_migration_candidate("spark_read_parquet") is True
    assert is_migration_candidate("spark_write_orc") is True
    assert is_migration_candidate("spark_read_csv") is False
    assert is_migration_candidate("pandas_write_json") is False
    assert is_migration_candidate("hive_create_parquet") is True
    assert is_migration_candidate("hive_create_orc") is True


def test_partition_mismatch_when_code_and_ddl_differ(tmp_path):
    """Code partitions by region, DDL partitions by date → mismatch flagged."""
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) PARTITIONED BY (date_col STRING) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'df.write.partitionBy("region").saveAsTable("events")\n'
    )
    from skills.open_table_migrator.detector import detect_all_io
    from skills.open_table_migrator.sql_registry import scan_sql_files
    from skills.open_table_migrator.analyzer import annotate_partition_mismatch

    matches = detect_all_io(tmp_path)
    defs = scan_sql_files(tmp_path)
    annotate_partition_mismatch(matches, defs)
    write_matches = [m for m in matches if m.direction == "write"]
    assert len(write_matches) >= 1
    flagged = [m for m in write_matches if "partition_mismatch" in m.attrs]
    assert len(flagged) >= 1
    msg = flagged[0].attrs["partition_mismatch"]
    assert "region" in msg
    assert "date_col" in msg


def test_no_partition_mismatch_when_code_and_ddl_agree(tmp_path):
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) PARTITIONED BY (region STRING) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'df.write.partitionBy("region").saveAsTable("events")\n'
    )
    from skills.open_table_migrator.detector import detect_all_io
    from skills.open_table_migrator.sql_registry import scan_sql_files
    from skills.open_table_migrator.analyzer import annotate_partition_mismatch

    matches = detect_all_io(tmp_path)
    defs = scan_sql_files(tmp_path)
    annotate_partition_mismatch(matches, defs)
    write_matches = [m for m in matches if m.direction == "write"]
    for m in write_matches:
        assert "partition_mismatch" not in m.attrs


def test_partition_mismatch_when_order_differs(tmp_path):
    """Spec requires order-sensitive comparison. Code: [region, date],
    DDL: [date, region] — both columns present, but in different order →
    should be flagged as mismatch.
    """
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) "
        "PARTITIONED BY (date_col STRING, region STRING) "
        "STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'df.write.partitionBy("region", "date_col").saveAsTable("events")\n'
    )
    from skills.open_table_migrator.detector import detect_all_io
    from skills.open_table_migrator.sql_registry import scan_sql_files
    from skills.open_table_migrator.analyzer import annotate_partition_mismatch

    matches = detect_all_io(tmp_path)
    defs = scan_sql_files(tmp_path)
    annotate_partition_mismatch(matches, defs)
    write_matches = [m for m in matches if m.direction == "write"]
    flagged = [m for m in write_matches if "partition_mismatch" in m.attrs]
    assert len(flagged) >= 1, (
        "Order difference must be flagged as mismatch per spec"
    )
