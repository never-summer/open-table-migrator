"""Regression tests for bugs documented in docs/bugs-2026-04-11-learningsparkv2.md."""
import json
import textwrap
from pathlib import Path

from skills.open_table_migrator.analyzer import find_ddl_references
from skills.open_table_migrator.cli import convert_project
from skills.open_table_migrator.deps import update_dependencies
from skills.open_table_migrator.detector import detect_parquet_usage


# ─── A2 / A3: multi-line JVM chain folding ───────────────────────────

def test_detector_folds_multi_line_scala_write_chain(tmp_path: Path):
    (tmp_path / "Job.scala").write_text(textwrap.dedent("""
        object Job {
          usersDF.write.format("parquet")
            .bucketBy(8, "uid")
            .mode(SaveMode.OverWrite)
            .saveAsTable("UsersTbl")
        }
    """).lstrip())
    matches = detect_parquet_usage(tmp_path)
    # Detector should find the chain and extract "UsersTbl" as path_arg
    assert len(matches) >= 1
    paths = {m.path_arg for m in matches}
    assert "UsersTbl" in paths
    # start_line points at the first physical line of the chain
    target = next(m for m in matches if m.path_arg == "UsersTbl")
    assert target.end_line is not None and target.end_line > target.line


# ─── A1 / A8: CLI honest reporting ───────────────────────────────────

def test_cli_reports_worklist_for_detected_file(tmp_path: Path, capsys):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "bad.scala").write_text(
        'object Bad { df.write.format("parquet").save("s3://unresolved/") }\n'
    )
    rc = convert_project(proj, table_name="fallback", namespace="default")
    out = capsys.readouterr().out
    assert rc == 0
    assert "rewrite task" in out
    assert (proj / "lakehouse-worklist.json").exists()


def test_cli_reports_no_build_files_found(tmp_path: Path, capsys):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "etl.py").write_text('import pandas as pd\ndf = pd.read_parquet("x")\n')
    convert_project(proj, table_name="t", namespace="ns")
    out = capsys.readouterr().out
    assert "No build files updated" in out


def test_cli_reports_updated_build_files(tmp_path: Path, capsys):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "etl.py").write_text('import pandas as pd\ndf = pd.read_parquet("x")\n')
    (proj / "requirements.txt").write_text("pandas\n")
    convert_project(proj, table_name="t", namespace="ns")
    out = capsys.readouterr().out
    assert "Updated build file" in out
    assert "requirements.txt" in out


# ─── A4: sbt + nested build files ────────────────────────────────────

def test_update_dependencies_adds_sbt(tmp_path: Path):
    sbt = tmp_path / "build.sbt"
    sbt.write_text('name := "myproj"\nscalaVersion := "2.12.18"\n')
    updated = update_dependencies(tmp_path)
    assert sbt in updated
    content = sbt.read_text()
    assert '"org.apache.iceberg"' in content
    assert 'iceberg-spark-runtime-3.5' in content
    assert '%%' in content


def test_update_dependencies_does_not_duplicate_sbt(tmp_path: Path):
    sbt = tmp_path / "build.sbt"
    sbt.write_text(
        'libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.5.0"\n'
    )
    updated = update_dependencies(tmp_path)
    assert sbt not in updated
    assert sbt.read_text().count("iceberg-spark-runtime") == 1


def test_update_dependencies_scans_nested_modules(tmp_path: Path):
    (tmp_path / "mod-a").mkdir()
    (tmp_path / "mod-b").mkdir()
    pom_a = tmp_path / "mod-a" / "pom.xml"
    pom_a.write_text('<project><dependencies></dependencies></project>\n')
    gradle_b = tmp_path / "mod-b" / "build.gradle"
    gradle_b.write_text('dependencies {\n}\n')
    updated = update_dependencies(tmp_path)
    assert pom_a in updated
    assert gradle_b in updated
    assert "iceberg-spark-runtime" in pom_a.read_text()
    assert "iceberg-spark-runtime" in gradle_b.read_text()


def test_update_dependencies_returns_empty_when_nothing_found(tmp_path: Path):
    updated = update_dependencies(tmp_path)
    assert updated == []


# ─── A9: PatternMatch.direction property ─────────────────────────────

def test_pattern_match_exposes_direction_attribute(tmp_path: Path):
    (tmp_path / "etl.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("in.parquet")\n'
        'df.to_parquet("out.parquet")\n'
    )
    matches = detect_parquet_usage(tmp_path)
    dirs = {m.pattern_type: m.direction for m in matches}
    assert dirs["pandas_read_parquet"] == "read"
    assert dirs["pandas_write_parquet"] == "write"


def test_pattern_match_direction_schema_for_hive_ddl(tmp_path: Path):
    (tmp_path / "Ddl.scala").write_text(
        'object Ddl { spark.sql("CREATE TABLE t (id INT) STORED AS PARQUET") }\n'
    )
    matches = detect_parquet_usage(tmp_path)
    schemas = [m for m in matches if m.pattern_type == "hive_create_parquet"]
    assert schemas
    assert schemas[0].direction == "schema"


# ─── A10: SQL DDL references to mapped tables ────────────────────────

def test_find_ddl_references_flags_drop_cache_on_mapped_table(tmp_path: Path):
    (tmp_path / "Job.scala").write_text(textwrap.dedent("""
        object Job {
          spark.sql("DROP TABLE IF EXISTS UsersTbl")
          usersDF.write.format("parquet").saveAsTable("UsersTbl")
          spark.sql("CACHE TABLE UsersTbl")
          spark.sql("UNCACHE TABLE UsersTbl")
        }
    """).lstrip())
    matches = detect_parquet_usage(tmp_path)
    refs = find_ddl_references(matches, tmp_path)
    cmds = {(r.command, r.table_name) for r in refs}
    assert ("DROP TABLE", "UsersTbl") in cmds
    assert ("CACHE TABLE", "UsersTbl") in cmds
    assert ("UNCACHE TABLE", "UsersTbl") in cmds


def test_find_ddl_references_ignores_unmapped_tables(tmp_path: Path):
    (tmp_path / "Job.scala").write_text(textwrap.dedent("""
        object Job {
          spark.sql("DROP TABLE SomeOtherTable")
          usersDF.write.format("parquet").saveAsTable("UsersTbl")
        }
    """).lstrip())
    matches = detect_parquet_usage(tmp_path)
    refs = find_ddl_references(matches, tmp_path)
    names = {r.table_name for r in refs}
    assert "SomeOtherTable" not in names


def test_find_ddl_references_empty_when_no_matches(tmp_path: Path):
    refs = find_ddl_references([], tmp_path)
    assert refs == []


def test_find_ddl_references_skips_file_paths(tmp_path: Path):
    (tmp_path / "etl.py").write_text(textwrap.dedent("""
        import pandas as pd
        df = pd.read_parquet("s3://bucket/data")
        # S3 paths are not SQL identifiers — no DDL cross-ref possible
    """).lstrip())
    matches = detect_parquet_usage(tmp_path)
    refs = find_ddl_references(matches, tmp_path)
    assert refs == []
