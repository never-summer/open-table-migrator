"""Multi-table routing: path extraction, resolver, mapping loading, transformer integration."""
import json
import textwrap
from pathlib import Path

from skills.parquet_to_iceberg.detector import detect_parquet_usage
from skills.parquet_to_iceberg.extract import extract_path_arg
from skills.parquet_to_iceberg.targets import (
    Mapping,
    MappingEntry,
    Target,
    build_resolver,
    load_mapping,
)
from skills.parquet_to_iceberg.transformers.pandas import transform_pandas_file
from skills.parquet_to_iceberg.transformers.pyspark import transform_pyspark_file
from skills.parquet_to_iceberg.transformers.jvm import transform_jvm_file
from skills.parquet_to_iceberg.cli import convert_project


# ─── extract_path_arg ─────────────────────────────────────────────────

def test_extract_pandas_read_literal():
    assert extract_path_arg('df = pd.read_parquet("data/events.parquet")') == "data/events.parquet"


def test_extract_pandas_read_orc():
    assert extract_path_arg('df = pd.read_orc("data.orc")') == "data.orc"


def test_extract_pyspark_read_parquet():
    assert extract_path_arg('df = spark.read.parquet("s3://bucket/events/")') == "s3://bucket/events/"


def test_extract_pyspark_format_load():
    assert extract_path_arg('df = spark.read.format("parquet").load("s3://bucket/users/")') == "s3://bucket/users/"


def test_extract_java_spark_read():
    assert extract_path_arg('Dataset<Row> df = spark.read().parquet("s3://bucket/orders/");') == "s3://bucket/orders/"


def test_extract_variable_returns_none():
    assert extract_path_arg('df = spark.read.parquet(path)') is None


def test_extract_pq_write_second_arg():
    assert extract_path_arg('pq.write_table(table, "out.parquet")') == "out.parquet"


def test_extract_save_as_table():
    assert extract_path_arg('df.write().saveAsTable("warehouse.orders");') == "warehouse.orders"


def test_extract_create_table_sql():
    assert extract_path_arg('spark.sql("CREATE TABLE events (id BIGINT) STORED AS PARQUET")') == "events"


def test_extract_insert_overwrite_table():
    assert extract_path_arg('spark.sql("INSERT OVERWRITE TABLE orders SELECT * FROM staging")') == "orders"


# ─── Mapping loading + resolver ───────────────────────────────────────

def test_load_mapping_from_json(tmp_path: Path):
    mapping_file = tmp_path / "mapping.json"
    mapping_file.write_text(json.dumps({
        "default": {"namespace": "default", "table": "unknown"},
        "tables": [
            {"path_glob": "s3://b/events/*", "namespace": "analytics", "table": "events"},
            {"path_glob": "*users*", "namespace": "analytics", "table": "users"},
        ],
    }))
    m = load_mapping(mapping_file)
    assert m.default == Target("default", "unknown")
    assert len(m.entries) == 2
    assert m.entries[0].path_glob == "s3://b/events/*"
    assert m.entries[0].target == Target("analytics", "events")


def test_resolver_routes_by_glob():
    m = Mapping(
        entries=[
            MappingEntry("s3://b/events/*", Target("analytics", "events")),
            MappingEntry("*users*", Target("analytics", "users")),
        ],
    )
    resolve = build_resolver(m, fallback=None)
    assert resolve("s3://b/events/2024/") == Target("analytics", "events")
    assert resolve("s3://other/users/list") == Target("analytics", "users")


def test_resolver_returns_fallback_when_no_match():
    m = Mapping(entries=[MappingEntry("foo/*", Target("ns", "foo"))])
    fallback = Target("default", "catchall")
    resolve = build_resolver(m, fallback=fallback)
    assert resolve("bar/baz") == fallback


def test_resolver_returns_none_when_unresolvable():
    m = Mapping(entries=[MappingEntry("foo/*", Target("ns", "foo"))])
    resolve = build_resolver(m, fallback=None)
    assert resolve("bar/baz") is None
    assert resolve(None) is None


def test_resolver_mapping_default_beats_fallback():
    m = Mapping(entries=[], default=Target("mapped", "default"))
    resolve = build_resolver(m, fallback=Target("cli", "fallback"))
    assert resolve("anything") == Target("mapped", "default")


# ─── Detector exposes path_arg ────────────────────────────────────────

def test_detector_captures_path_arg(tmp_path: Path):
    (tmp_path / "etl.py").write_text('df = pd.read_parquet("s3://bucket/events/2024")\n')
    matches = detect_parquet_usage(tmp_path)
    assert len(matches) == 1
    assert matches[0].path_arg == "s3://bucket/events/2024"


def test_detector_captures_none_for_variable_arg(tmp_path: Path):
    (tmp_path / "etl.py").write_text('df = pd.read_parquet(path)\n')
    matches = detect_parquet_usage(tmp_path)
    assert len(matches) == 1
    assert matches[0].path_arg is None


# ─── Transformer with mapping: multi-table in one file ───────────────

def test_pandas_multi_table_routing():
    src = textwrap.dedent("""
        import pandas as pd
        events = pd.read_parquet("s3://bucket/events/2024")
        users = pd.read_parquet("s3://bucket/users/list")
        users.to_parquet("s3://bucket/users/backup")
    """).lstrip()
    mapping = Mapping(entries=[
        MappingEntry("s3://bucket/events/*", Target("analytics", "events")),
        MappingEntry("s3://bucket/users/*", Target("analytics", "users")),
    ])
    out = transform_pandas_file(src, mapping=mapping)
    assert "pd.read_parquet" not in out
    assert ".to_parquet(" not in out
    assert "tbl_analytics_events" in out
    assert "tbl_analytics_users" in out
    assert 'load_table(("analytics", "events"))' in out
    assert 'load_table(("analytics", "users"))' in out


def test_pyspark_multi_table_routing():
    src = textwrap.dedent("""
        events = spark.read.parquet("s3://bucket/events/")
        users = spark.read.parquet("s3://bucket/users/")
        users.write.mode("overwrite").parquet("s3://bucket/users/out")
    """).lstrip()
    mapping = Mapping(entries=[
        MappingEntry("s3://bucket/events/*", Target("analytics", "events")),
        MappingEntry("s3://bucket/users/*", Target("analytics", "users")),
    ])
    out = transform_pyspark_file(src, mapping=mapping)
    assert 'spark.table("analytics.events")' in out
    assert 'spark.table("analytics.users")' in out
    assert 'writeTo("analytics.users")' in out
    assert ".read.parquet" not in out


def test_jvm_multi_table_routing_java():
    src = textwrap.dedent("""
        Dataset<Row> events = spark.read().parquet("s3://bucket/events/");
        Dataset<Row> users = spark.read().parquet("s3://bucket/users/");
        users.write().mode("overwrite").parquet("s3://bucket/users/out");
    """).lstrip()
    mapping = Mapping(entries=[
        MappingEntry("s3://bucket/events/*", Target("analytics", "events")),
        MappingEntry("s3://bucket/users/*", Target("analytics", "users")),
    ])
    out = transform_jvm_file(src, language="java", mapping=mapping)
    assert 'load("analytics.events")' in out
    assert 'load("analytics.users")' in out
    assert 'writeTo("analytics.users")' in out
    assert ".parquet(" not in out


def test_unresolvable_path_emits_todo():
    src = 'import pandas as pd\ndf = pd.read_parquet(dynamic_path)\n'
    mapping = Mapping(entries=[MappingEntry("s3://*", Target("ns", "t"))])
    out = transform_pandas_file(src, mapping=mapping)
    assert "TODO(iceberg): could not resolve target" in out
    assert "pd.read_parquet(dynamic_path)" in out  # original line preserved


def test_fallback_used_when_mapping_misses():
    src = 'import pandas as pd\ndf = pd.read_parquet("local.parquet")\n'
    mapping = Mapping(entries=[MappingEntry("s3://*", Target("cloud", "remote"))])
    out = transform_pandas_file(src, mapping=mapping, namespace="default", table_name="local_fallback")
    assert 'load_table(("default", "local_fallback"))' in out


# ─── CLI end-to-end multi-table ───────────────────────────────────────

def test_cli_multi_table_with_mapping(tmp_path: Path):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "etl.py").write_text(textwrap.dedent("""
        import pandas as pd
        events = pd.read_parquet("s3://bucket/events/2024")
        users = pd.read_parquet("s3://bucket/users/list")
    """).lstrip())

    mapping_file = tmp_path / "mapping.json"
    mapping_file.write_text(json.dumps({
        "tables": [
            {"path_glob": "s3://bucket/events/*", "namespace": "analytics", "table": "events"},
            {"path_glob": "s3://bucket/users/*", "namespace": "analytics", "table": "users"},
        ],
    }))

    mapping = load_mapping(mapping_file)
    rc = convert_project(proj, mapping=mapping)
    assert rc == 0
    rewritten = (proj / "etl.py").read_text()
    assert 'load_table(("analytics", "events"))' in rewritten
    assert 'load_table(("analytics", "users"))' in rewritten
    assert "pd.read_parquet" not in rewritten


def test_cli_rejects_missing_config(tmp_path: Path):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "etl.py").write_text('import pandas as pd\ndf = pd.read_parquet("x.parquet")\n')
    rc = convert_project(proj)  # no table/namespace, no mapping
    assert rc == 2


# ─── Single-table back-compat (no mapping) still works ───────────────

def test_single_table_still_uses_plain_tbl():
    src = 'import pandas as pd\ndf = pd.read_parquet("x.parquet")\n'
    out = transform_pandas_file(src, table_name="events", namespace="default")
    assert "tbl.scan().to_pandas()" in out
    assert "tbl_default_events" not in out
