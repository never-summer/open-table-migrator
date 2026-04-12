"""Tests for SQL file registry scanning and cross-referencing with code operations."""
import textwrap
from pathlib import Path

from skills.open_table_migrator.analyzer import cross_reference_sql, dedup_matches
from skills.open_table_migrator.detector import detect_parquet_usage
from skills.open_table_migrator.sql_registry import (
    TableDef,
    build_format_map,
    scan_sql_files,
)


# ─── scan_sql_files ─────────────────────────────────────────────────

def test_scan_finds_stored_as_parquet(tmp_path: Path):
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT, ts TIMESTAMP) STORED AS PARQUET;\n"
    )
    defs = scan_sql_files(tmp_path)
    assert len(defs) == 1
    assert defs[0].table_name == "events"
    assert defs[0].format == "parquet"
    assert defs[0].database is None


def test_scan_finds_stored_as_orc(tmp_path: Path):
    (tmp_path / "schema.sql").write_text(
        "CREATE EXTERNAL TABLE logs (msg STRING) STORED AS ORC;\n"
    )
    defs = scan_sql_files(tmp_path)
    assert len(defs) == 1
    assert defs[0].table_name == "logs"
    assert defs[0].format == "orc"


def test_scan_finds_using_parquet(tmp_path: Path):
    (tmp_path / "ddl.sql").write_text(
        "CREATE TABLE IF NOT EXISTS analytics.clicks (url STRING) USING parquet;\n"
    )
    defs = scan_sql_files(tmp_path)
    assert len(defs) == 1
    assert defs[0].table_name == "clicks"
    assert defs[0].database == "analytics"
    assert defs[0].format == "parquet"


def test_scan_finds_ctas_stored_as(tmp_path: Path):
    (tmp_path / "init.hql").write_text(textwrap.dedent("""\
        CREATE TABLE summary (cnt BIGINT)
        STORED AS PARQUET
        AS SELECT count(*) AS cnt FROM raw;
    """))
    defs = scan_sql_files(tmp_path)
    assert len(defs) == 1
    assert defs[0].table_name == "summary"


def test_scan_deduplicates_by_table_name(tmp_path: Path):
    (tmp_path / "a.sql").write_text(
        "CREATE TABLE events (id INT) STORED AS PARQUET;\n"
    )
    (tmp_path / "b.sql").write_text(
        "CREATE TABLE events (id INT) USING parquet;\n"
    )
    defs = scan_sql_files(tmp_path)
    assert len(defs) == 1


def test_scan_ignores_non_sql_files(tmp_path: Path):
    (tmp_path / "readme.md").write_text("CREATE TABLE x STORED AS PARQUET;\n")
    (tmp_path / "code.py").write_text("CREATE TABLE x STORED AS PARQUET;\n")
    defs = scan_sql_files(tmp_path)
    assert len(defs) == 0


def test_scan_handles_qualified_table_name(tmp_path: Path):
    (tmp_path / "schema.ddl").write_text(
        "CREATE TABLE `mydb`.`events` (id INT) STORED AS PARQUET;\n"
    )
    defs = scan_sql_files(tmp_path)
    assert len(defs) == 1
    assert defs[0].database == "mydb"
    assert defs[0].table_name == "events"


# ─── build_format_map ───────────────────────────────────────────────

def test_format_map_bare_and_qualified():
    defs = [
        TableDef("events", "analytics", "parquet", Path("a.sql"), 1, "..."),
        TableDef("logs", None, "orc", Path("b.sql"), 5, "..."),
    ]
    fm = build_format_map(defs)
    assert fm["events"] == "parquet"
    assert fm["analytics.events"] == "parquet"
    assert fm["logs"] == "orc"
    assert "none.logs" not in fm


# ─── cross_reference_sql ────────────────────────────────────────────

def test_cross_ref_matches_save_as_table_to_sql_def(tmp_path: Path):
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT, ts TIMESTAMP) STORED AS PARQUET;\n"
    )
    (tmp_path / "Job.scala").write_text(textwrap.dedent("""\
        object Job {
          val df = spark.read.json("input.json")
          df.write.saveAsTable("events")
        }
    """))

    matches = detect_parquet_usage(tmp_path)
    sql_defs = scan_sql_files(tmp_path)
    fmt_map = build_format_map(sql_defs)
    sites = dedup_matches(matches)
    xrefs = cross_reference_sql(sites, fmt_map, sql_defs)

    assert len(xrefs) == 1
    assert xrefs[0].sql_table == "events"
    assert xrefs[0].sql_format == "parquet"
    assert xrefs[0].sql_file.name == "schema.sql"


def test_cross_ref_matches_qualified_table_name(tmp_path: Path):
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE analytics.events (id INT) STORED AS PARQUET;\n"
    )
    (tmp_path / "Job.scala").write_text(textwrap.dedent("""\
        object Job {
          val df = spark.read.json("input.json")
          df.write.saveAsTable("analytics.events")
        }
    """))

    matches = detect_parquet_usage(tmp_path)
    sql_defs = scan_sql_files(tmp_path)
    fmt_map = build_format_map(sql_defs)
    sites = dedup_matches(matches)
    xrefs = cross_reference_sql(sites, fmt_map, sql_defs)

    assert len(xrefs) == 1
    assert xrefs[0].sql_format == "parquet"


def test_cross_ref_ignores_file_paths(tmp_path: Path):
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) STORED AS PARQUET;\n"
    )
    (tmp_path / "etl.py").write_text(
        'df = pd.read_parquet("s3://bucket/events/data.parquet")\n'
    )

    matches = detect_parquet_usage(tmp_path)
    sql_defs = scan_sql_files(tmp_path)
    fmt_map = build_format_map(sql_defs)
    sites = dedup_matches(matches)
    xrefs = cross_reference_sql(sites, fmt_map, sql_defs)

    assert len(xrefs) == 0


def test_cross_ref_empty_when_no_sql_files(tmp_path: Path):
    (tmp_path / "etl.py").write_text(
        'df.write.saveAsTable("events")\n'
    )

    matches = detect_parquet_usage(tmp_path)
    sql_defs = scan_sql_files(tmp_path)
    fmt_map = build_format_map(sql_defs)
    sites = dedup_matches(matches)
    xrefs = cross_reference_sql(sites, fmt_map, sql_defs)

    assert len(xrefs) == 0


def test_cross_ref_no_match_when_table_not_parquet(tmp_path: Path):
    (tmp_path / "schema.sql").write_text(
        "CREATE TABLE events (id INT) STORED AS TEXTFILE;\n"
    )
    (tmp_path / "Job.scala").write_text(textwrap.dedent("""\
        object Job {
          df.write.saveAsTable("events")
        }
    """))

    matches = detect_parquet_usage(tmp_path)
    sql_defs = scan_sql_files(tmp_path)
    fmt_map = build_format_map(sql_defs)
    sites = dedup_matches(matches)
    xrefs = cross_reference_sql(sites, fmt_map, sql_defs)

    assert len(xrefs) == 0
