from pathlib import Path
from skills.parquet_to_iceberg.analyzer import build_report, format_report, direction_of
from skills.parquet_to_iceberg.detector import PatternMatch


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
