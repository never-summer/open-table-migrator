"""Tests for prepass module."""
from pathlib import Path
from textwrap import dedent

from skills.open_table_migrator.scripts.detector import PatternMatch
from skills.open_table_migrator.scripts.prepass import PrepassPlan, plan_prepass
from skills.open_table_migrator.scripts.targets import Target


def test_plan_prepass_for_skip_match_does_not_write_disk(tmp_path):
    """plan_prepass must NOT modify files. It returns a plan only."""
    src = tmp_path / "job.py"
    src.write_text(dedent('''
        import pandas as pd
        df = pd.read_parquet("s3://legacy/x.parquet")
    '''))
    original_content = src.read_text()

    from skills.open_table_migrator.scripts.targets import Mapping, MappingEntry, build_resolver
    mapping = Mapping(entries=[
        MappingEntry(path_glob="s3://legacy/*", skip=True, target=None),
    ])
    resolver = build_resolver(mapping, fallback=None, project_root=tmp_path)

    m = PatternMatch(
        file=src,
        line=3,
        pattern_type="pandas_read_parquet",
        original_code='df = pd.read_parquet("s3://legacy/x.parquet")',
        path_arg="s3://legacy/x.parquet",
    )
    plans = plan_prepass([m], resolver)

    # File on disk MUST NOT change
    assert src.read_text() == original_content
    # Plan reflects what would be added
    assert len(plans) == 1
    plan = plans[0]
    assert plan.file == src
    assert plan.original == original_content
    assert "iceberg: skipped by mapping" in plan.modified
    assert plan.marker_count == 1


def test_plan_prepass_for_pyspark_match_adds_conf_block(tmp_path):
    """A pyspark match (non-skip) should result in a plan that adds the
    Iceberg conf comment block."""
    src = tmp_path / "job.py"
    src.write_text(dedent('''
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.parquet("s3://prod/users")
    '''))
    original = src.read_text()

    from skills.open_table_migrator.scripts.targets import Mapping, build_resolver
    fallback = Target(namespace="ns", table="t")
    resolver = build_resolver(Mapping(), fallback=fallback, project_root=tmp_path)

    m = PatternMatch(
        file=src,
        line=4,
        pattern_type="pyspark_read_parquet",
        original_code='df = spark.read.parquet("s3://prod/users")',
        path_arg="s3://prod/users",
    )
    plans = plan_prepass([m], resolver)

    # Still must not write
    assert src.read_text() == original
    assert len(plans) == 1
    plan = plans[0]
    assert plan.pyspark_conf_added is True
    # The conf block content varies — accept either marker pattern
    assert (
        "iceberg: see SparkSession conf" in plan.modified
        or "spark.sql.catalog" in plan.modified
    )


def test_plan_prepass_for_file_with_no_changes_returns_no_plan(tmp_path):
    """When a match's resolver decision is neither skip nor pyspark-conf-eligible,
    plan_prepass should not return a plan (or return one where modified == original)."""
    src = tmp_path / "job.py"
    src.write_text("# already migrated\n")
    original = src.read_text()

    from skills.open_table_migrator.scripts.targets import Mapping, build_resolver
    fallback = Target(namespace="ns", table="t")
    resolver = build_resolver(Mapping(), fallback=fallback, project_root=tmp_path)

    m = PatternMatch(
        file=src,
        line=1,
        pattern_type="pandas_read_csv",  # non-skip, non-pyspark
        original_code='df = pd.read_csv("foo.csv")',
        path_arg="foo.csv",
    )
    plans = plan_prepass([m], resolver)

    # Either no plan returned, or plan where modified == original
    assert src.read_text() == original
    for plan in plans:
        if plan.file == src:
            assert plan.modified == plan.original
