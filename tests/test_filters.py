from pathlib import Path
from skills.parquet_to_iceberg.detector import PatternMatch
from skills.parquet_to_iceberg.filters import filter_matches


def _m(file: str, pattern: str) -> PatternMatch:
    return PatternMatch(file=Path(file), line=1, pattern_type=pattern, original_code="...")


def test_filter_by_direction_read():
    matches = [
        _m("a.py", "pandas_read"),
        _m("a.py", "pandas_write"),
        _m("b.java", "java_spark_read"),
    ]
    result = filter_matches(matches, directions={"read"})
    assert len(result) == 2
    assert all(m.pattern_type.endswith("_read") for m in result)


def test_filter_by_direction_write():
    matches = [
        _m("a.py", "pandas_read"),
        _m("a.py", "pandas_write"),
        _m("b.java", "hive_save_as_table"),
    ]
    result = filter_matches(matches, directions={"write"})
    assert len(result) == 2
    assert all(m.pattern_type in {"pandas_write", "hive_save_as_table"} for m in result)


def test_filter_by_direction_schema():
    matches = [
        _m("a.java", "hive_create_parquet"),
        _m("b.py", "pandas_read"),
    ]
    result = filter_matches(matches, directions={"schema"})
    assert len(result) == 1
    assert result[0].pattern_type == "hive_create_parquet"


def test_filter_by_pattern_type():
    matches = [
        _m("a.py", "pandas_read"),
        _m("b.py", "pyspark_read"),
        _m("c.java", "java_spark_read"),
    ]
    result = filter_matches(matches, pattern_types={"pandas_read", "java_spark_read"})
    assert len(result) == 2
    files = {m.file.name for m in result}
    assert files == {"a.py", "c.java"}


def test_filter_by_include_glob():
    matches = [
        _m("src/etl/pipeline.py", "pandas_read"),
        _m("src/api/handler.py", "pandas_read"),
        _m("tests/test_etl.py", "pandas_read"),
    ]
    result = filter_matches(matches, include_files=["src/etl/*"])
    assert len(result) == 1
    assert "pipeline.py" in str(result[0].file)


def test_filter_by_exclude_glob():
    matches = [
        _m("src/etl.py", "pandas_read"),
        _m("tests/test_etl.py", "pandas_read"),
    ]
    result = filter_matches(matches, exclude_files=["tests/*"])
    assert len(result) == 1
    assert "src/etl.py" in str(result[0].file)


def test_filter_combined():
    matches = [
        _m("src/etl.py", "pandas_read"),
        _m("src/etl.py", "pandas_write"),
        _m("src/api.py", "pandas_read"),
        _m("tests/test_etl.py", "pandas_read"),
    ]
    result = filter_matches(
        matches,
        directions={"read"},
        include_files=["src/*"],
        exclude_files=["src/api.py"],
    )
    assert len(result) == 1
    assert "src/etl.py" in str(result[0].file)
    assert result[0].pattern_type == "pandas_read"


def test_filter_no_filters_returns_all():
    matches = [_m("a.py", "pandas_read"), _m("b.py", "pandas_write")]
    result = filter_matches(matches)
    assert len(result) == 2
