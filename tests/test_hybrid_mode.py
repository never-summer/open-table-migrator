"""Tests for the hybrid CLI mode: prepass + worklist emission."""
import json
import textwrap
from pathlib import Path

from skills.open_table_migrator.cli import convert_project
from skills.open_table_migrator.detector import detect_parquet_usage
from skills.open_table_migrator.prepass import run_prepass
from skills.open_table_migrator.targets import (
    Mapping,
    MappingEntry,
    Target,
    build_resolver,
)
from skills.open_table_migrator.worklist import build_worklist, write_worklist


# ─── Prepass: skip markers and pyspark conf comment ──────────────────

def test_prepass_injects_skip_marker_above_skipped_line(tmp_path: Path):
    src_file = tmp_path / "etl.py"
    src_file.write_text(textwrap.dedent("""
        import pandas as pd
        keep = pd.read_parquet("s3://bucket/events/x")
        legacy = pd.read_parquet("s3://legacy/old.parquet")
    """).lstrip())
    matches = detect_parquet_usage(tmp_path)
    mapping = Mapping(entries=[
        MappingEntry("s3://bucket/events/*", Target("analytics", "events")),
        MappingEntry("s3://legacy/*", target=None, skip=True),
    ])
    resolver = build_resolver(mapping, fallback=None)
    run_prepass(matches, resolver)
    out = src_file.read_text()
    # Legacy line preserved verbatim, marker inserted above it
    assert 'pd.read_parquet("s3://legacy/old.parquet")' in out
    assert "iceberg: skipped by mapping" in out
    # Kept line untouched (no rewrite in prepass)
    assert 'pd.read_parquet("s3://bucket/events/x")' in out


def test_prepass_injects_pyspark_conf_comment_once(tmp_path: Path):
    src_file = tmp_path / "job.py"
    src_file.write_text(textwrap.dedent("""
        def load():
            df = spark.read.parquet("data/a")
            df2 = spark.read.parquet("data/b")
            return df, df2
    """).lstrip())
    matches = detect_parquet_usage(tmp_path)
    resolver = build_resolver(None, fallback=Target("default", "t"))
    run_prepass(matches, resolver)
    out = src_file.read_text()
    # Conf comment injected exactly once, with proper 4-space indent
    assert out.count("IcebergSparkSessionExtensions") == 1
    for line in out.splitlines():
        if "Iceberg:" in line:
            assert line.startswith("    ")
    # Original read ops untouched
    assert out.count('spark.read.parquet') == 2


def test_prepass_idempotent(tmp_path: Path):
    src_file = tmp_path / "job.py"
    src_file.write_text(textwrap.dedent("""
        df = spark.read.parquet("x")
    """).lstrip())
    matches = detect_parquet_usage(tmp_path)
    resolver = build_resolver(None, fallback=Target("default", "t"))
    run_prepass(matches, resolver)
    first = src_file.read_text()
    # Re-detect + rerun prepass; conf comment should NOT be duplicated
    matches2 = detect_parquet_usage(tmp_path)
    run_prepass(matches2, resolver)
    second = src_file.read_text()
    assert first == second
    assert second.count("IcebergSparkSessionExtensions") == 1


def test_prepass_skips_non_pyspark_file(tmp_path: Path):
    src_file = tmp_path / "etl.py"
    src_file.write_text('import pandas as pd\ndf = pd.read_parquet("x")\n')
    matches = detect_parquet_usage(tmp_path)
    resolver = build_resolver(None, fallback=Target("default", "t"))
    run_prepass(matches, resolver)
    out = src_file.read_text()
    # No pyspark patterns → no conf comment
    assert "IcebergSparkSessionExtensions" not in out


# ─── Worklist builder ────────────────────────────────────────────────

def test_worklist_has_entry_per_non_skip_match(tmp_path: Path):
    (tmp_path / "etl.py").write_text(textwrap.dedent("""
        import pandas as pd
        a = pd.read_parquet("s3://bucket/events/2024")
        b = pd.read_parquet("s3://legacy/old")
    """).lstrip())
    matches = detect_parquet_usage(tmp_path)
    mapping = Mapping(entries=[
        MappingEntry("s3://bucket/events/*", Target("analytics", "events")),
        MappingEntry("s3://legacy/*", target=None, skip=True),
    ])
    resolver = build_resolver(mapping, fallback=None)
    entries = build_worklist(matches, tmp_path, resolver)
    # One entry for the mapped read; legacy is skip → not in worklist
    assert len(entries) == 1
    e = entries[0]
    assert e.resolved_namespace == "analytics"
    assert e.resolved_table == "events"
    assert e.needs_manual_target is False
    assert e.direction == "read"
    assert e.language == "python"
    assert "pd.read_parquet" in e.original_code
    assert "surrounding" and e.surrounding  # non-empty context


def test_worklist_marks_unresolved_targets(tmp_path: Path):
    (tmp_path / "etl.py").write_text(textwrap.dedent("""
        import pandas as pd
        df = pd.read_parquet("s3://unknown/x")
    """).lstrip())
    matches = detect_parquet_usage(tmp_path)
    resolver = build_resolver(None, fallback=None)
    entries = build_worklist(matches, tmp_path, resolver)
    assert len(entries) == 1
    assert entries[0].needs_manual_target is True
    assert entries[0].resolved_namespace is None
    assert "unresolved" in entries[0].hint


def test_worklist_folds_multi_line_scala_chain(tmp_path: Path):
    (tmp_path / "Job.scala").write_text(textwrap.dedent("""
        object Job {
          usersDF.write.format("parquet")
            .bucketBy(8, "uid")
            .saveAsTable("UsersTbl")
        }
    """).lstrip())
    matches = detect_parquet_usage(tmp_path)
    resolver = build_resolver(None, fallback=Target("analytics", "users"))
    entries = build_worklist(matches, tmp_path, resolver)
    # end_line > start_line (logical span covers the chain)
    write_entries = [e for e in entries if e.direction == "write"]
    assert write_entries
    e = write_entries[0]
    assert e.end_line > e.start_line
    assert "bucketBy" in e.original_code
    assert "saveAsTable" in e.original_code


def test_worklist_file_is_written_with_relative_paths(tmp_path: Path):
    (tmp_path / "etl.py").write_text(
        'import pandas as pd\ndf = pd.read_parquet("x.parquet")\n'
    )
    matches = detect_parquet_usage(tmp_path)
    resolver = build_resolver(None, fallback=Target("default", "events"))
    entries = build_worklist(matches, tmp_path, resolver)
    path = write_worklist(entries, tmp_path)
    assert path == tmp_path / "iceberg-worklist.json"
    data = json.loads(path.read_text())
    assert data["version"] == 1
    assert data["count"] == 1
    assert data["entries"][0]["file"] == "etl.py"  # relative, not absolute


# ─── CLI hybrid mode ─────────────────────────────────────────────────

def test_cli_hybrid_mode_writes_worklist_and_leaves_reads_untouched(
    tmp_path: Path, capsys
):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "etl.py").write_text(
        'import pandas as pd\ndf = pd.read_parquet("x.parquet")\n'
    )
    rc = convert_project(proj, table_name="events", namespace="default", mode="hybrid")
    assert rc == 0
    out = capsys.readouterr().out
    assert "Hybrid mode" in out
    assert "iceberg-worklist.json" in out
    # Worklist exists
    wl = proj / "iceberg-worklist.json"
    assert wl.exists()
    data = json.loads(wl.read_text())
    assert data["count"] == 1
    # Source file was NOT rewritten (read op still there)
    assert 'pd.read_parquet("x.parquet")' in (proj / "etl.py").read_text()


def test_cli_hybrid_mode_runs_prepass_for_skip_markers(tmp_path: Path):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "etl.py").write_text(textwrap.dedent("""
        import pandas as pd
        df = pd.read_parquet("s3://legacy/old.parquet")
    """).lstrip())
    mapping_path = proj / "mapping.json"
    mapping_path.write_text(json.dumps({
        "tables": [{"path_glob": "s3://legacy/*", "skip": True}],
    }))
    from skills.open_table_migrator.targets import load_mapping
    mapping = load_mapping(mapping_path)
    convert_project(proj, mapping=mapping, mode="hybrid")
    out = (proj / "etl.py").read_text()
    # Skip marker injected; original line preserved
    assert "iceberg: skipped by mapping" in out
    assert 'pd.read_parquet("s3://legacy/old.parquet")' in out
    # Worklist has zero entries (the only match was skipped)
    data = json.loads((proj / "iceberg-worklist.json").read_text())
    assert data["count"] == 0


def test_cli_hybrid_mode_injects_pyspark_conf_comment(tmp_path: Path):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "job.py").write_text(textwrap.dedent("""
        def load():
            df = spark.read.parquet("data/")
            return df
    """).lstrip())
    convert_project(proj, table_name="events", namespace="default", mode="hybrid")
    out = (proj / "job.py").read_text()
    assert "IcebergSparkSessionExtensions" in out
    # Source read op untouched (hybrid does not rewrite)
    assert 'spark.read.parquet("data/")' in out


def test_cli_no_deps_flag_skips_dependency_update(tmp_path: Path, capsys):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "etl.py").write_text('import pandas as pd\ndf = pd.read_parquet("x")\n')
    (proj / "requirements.txt").write_text("pandas\n")
    convert_project(
        proj, table_name="t", namespace="ns", mode="hybrid", update_deps=False
    )
    out = capsys.readouterr().out
    assert "Deps updater skipped" in out
    # requirements.txt untouched
    assert (proj / "requirements.txt").read_text() == "pandas\n"


def test_cli_deterministic_mode_preserves_old_behavior(tmp_path: Path, capsys):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "etl.py").write_text('import pandas as pd\ndf = pd.read_parquet("x")\n')
    convert_project(proj, table_name="events", namespace="default", mode="deterministic")
    out = capsys.readouterr().out
    assert "Converted" in out
    # No worklist file in deterministic mode
    assert not (proj / "iceberg-worklist.json").exists()
    # Actual rewrite happened
    rewritten = (proj / "etl.py").read_text()
    assert "pd.read_parquet" not in rewritten
    assert "tbl.scan().to_pandas()" in rewritten
