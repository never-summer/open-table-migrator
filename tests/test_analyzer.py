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


def test_cross_reference_dynamic_sql_with_create_in_same_file(tmp_path):
    (tmp_path / "queries").mkdir()
    schema_path = tmp_path / "queries" / "events.sql"
    schema_path.write_text("CREATE TABLE events (id INT) STORED AS PARQUET;")
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'sql = open("queries/events.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    assert len(cross) == 1
    assert cross[0].loader.sql_filename == "queries/events.sql"
    assert cross[0].match_kind == "exact_path"
    assert len(cross[0].tables) == 1
    assert cross[0].tables[0].table_name == "events"


def test_cross_reference_dynamic_sql_create_in_different_file(tmp_path):
    """schema.sql has CREATE; queries/events_update.sql has INSERT only.
    Loader of events_update.sql should still cross-ref to the events table."""
    (tmp_path / "schema.sql").write_text("CREATE TABLE events (id INT) STORED AS PARQUET;")
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "events_update.sql").write_text(
        "INSERT INTO events SELECT 1;"
    )
    (tmp_path / "job.py").write_text(
        'sql = open("queries/events_update.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    assert len(cross) == 1
    assert cross[0].tables[0].table_name == "events"
    assert cross[0].tables[0].format == "parquet"


def test_cross_reference_dynamic_sql_loader_with_no_registry_match(tmp_path):
    """Loader points at file not in registry → no cross-ref returned."""
    (tmp_path / "job.py").write_text(
        'sql = open("queries/missing.sql").read()\n'
    )
    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    assert cross == []
    # But the loader itself was detected:
    assert len(loaders) == 1


def test_cross_reference_dynamic_sql_basename_unique(tmp_path):
    """Loader uses basename only; project has unique file with that name."""
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "events.sql").write_text(
        "CREATE TABLE events (id INT) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'sql = open("events.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    assert len(cross) == 1
    assert cross[0].match_kind == "basename_unique"


def test_cross_reference_dynamic_sql_resolved_to_is_relative_on_macos(tmp_path):
    """The resolved_to field should be relative to project_root even when
    tmp_path is on a symlinked filesystem like macOS /tmp → /private/tmp.
    """
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "events.sql").write_text(
        "CREATE TABLE events (id INT) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'sql = open("queries/events.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    assert len(cross) == 1
    # The sql_file in the cross-ref must be relativizable against tmp_path
    # (i.e., both resolved or both unresolved — consistent).
    sql_file = cross[0].sql_file
    try:
        rel = sql_file.relative_to(tmp_path)
        assert str(rel) == "queries/events.sql"
    except ValueError:
        # Try with resolved project_root — should work
        rel = sql_file.relative_to(tmp_path.resolve())
        assert str(rel) == "queries/events.sql"


def test_worklist_resolved_to_is_relative_on_symlinked_tmp(tmp_path):
    """write_worklist resolved_to must be relative even when project_root
    is accessed via a symlink (as on macOS where /tmp → /private/tmp).

    Creates a real symlink so the test is portable: project_root is the
    symlink path while _resolve_sql_file internally uses .resolve().
    """
    import os
    from pathlib import Path

    real_root = tmp_path / "real_project"
    real_root.mkdir()
    sym_root = tmp_path / "sym_project"
    os.symlink(real_root, sym_root)  # sym_root → real_root

    (real_root / "queries").mkdir()
    (real_root / "queries" / "events.sql").write_text(
        "CREATE TABLE events (id INT) STORED AS PARQUET;"
    )
    (real_root / "job.py").write_text(
        'sql = open("queries/events.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )
    from skills.open_table_migrator.worklist import write_worklist

    # Pass the SYMLINK path as project_root — this is the macOS scenario
    # where the user references /tmp/... but .resolve() returns /private/tmp/...
    loaders = detect_dynamic_sql_loaders(sym_root)
    defs = scan_sql_files(sym_root)
    refs = scan_sql_table_references(sym_root)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, sym_root)

    import json
    worklist_path = write_worklist([], sym_root, dyn_cross=cross)
    blob = json.loads(worklist_path.read_text())
    dyn = blob.get("dynamic_sql_loaders") or []
    assert len(dyn) == 1, f"Expected 1 dyn entry, got: {dyn}"
    # resolved_to must NOT be an absolute path
    assert not dyn[0]["resolved_to"].startswith("/"), (
        f"resolved_to leaked absolute path: {dyn[0]['resolved_to']}"
    )
    # file must also not be absolute
    assert not dyn[0]["file"].startswith("/"), (
        f"file leaked absolute path: {dyn[0]['file']}"
    )


def test_cross_reference_dynamic_sql_emits_empty_tables_when_no_parquet(tmp_path):
    """Per spec: loaders pointing at SQL files with no parquet/orc tables
    should STILL be emitted (with tables=()) for visibility."""
    (tmp_path / "queries").mkdir()
    # SQL file with SELECT but no CREATE TABLE in registry (table not registered as parquet)
    (tmp_path / "queries" / "csv_export.sql").write_text(
        "SELECT * FROM raw_data;"  # raw_data not in registry as parquet
    )
    (tmp_path / "job.py").write_text(
        'sql = open("queries/csv_export.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    # Loader resolved to a registered SQL file but no parquet/orc tables.
    # Should still emit ONE cross-ref with tables=()
    assert len(cross) == 1
    assert cross[0].tables == ()
    assert cross[0].loader.sql_filename == "queries/csv_export.sql"


def test_cross_reference_dynamic_sql_basename_ambiguous(tmp_path):
    """Two events.sql in different dirs — basename resolution returns both."""
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "events.sql").write_text(
        "CREATE TABLE events_a (id INT) STORED AS PARQUET;"
    )
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "events.sql").write_text(
        "CREATE TABLE events_b (id INT) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'sql = open("events.sql").read()\n'
    )

    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )

    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)
    table_names = {t.table_name for c in cross for t in c.tables}
    assert "events_a" in table_names
    assert "events_b" in table_names
    assert all(c.match_kind == "basename_ambiguous" for c in cross)
