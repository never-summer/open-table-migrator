"""Multi-table routing: path extraction, resolver, mapping loading."""
import json
from pathlib import Path

from skills.open_table_migrator.detector import detect_parquet_usage
from skills.open_table_migrator.extract import extract_path_arg
from skills.open_table_migrator.targets import (
    Mapping,
    MappingEntry,
    Target,
    build_resolver,
    load_mapping,
)


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
    assert resolve("s3://b/events/2024/", "read").migrate_to == Target("analytics", "events")
    assert resolve("s3://other/users/list", "read").migrate_to == Target("analytics", "users")


def test_resolver_returns_fallback_when_no_match():
    m = Mapping(entries=[MappingEntry("foo/*", Target("ns", "foo"))])
    fallback = Target("default", "catchall")
    resolve = build_resolver(m, fallback=fallback)
    assert resolve("bar/baz", "read").migrate_to == fallback


def test_resolver_returns_unresolved():
    m = Mapping(entries=[MappingEntry("foo/*", Target("ns", "foo"))])
    resolve = build_resolver(m, fallback=None)
    d1 = resolve("bar/baz", "read")
    assert d1.migrate_to is None and not d1.skip
    d2 = resolve(None, "read")
    assert d2.migrate_to is None and not d2.skip


def test_resolver_mapping_default_beats_fallback():
    m = Mapping(entries=[], default=Target("mapped", "default"))
    resolve = build_resolver(m, fallback=Target("cli", "fallback"))
    assert resolve("anything", "read").migrate_to == Target("mapped", "default")


def test_resolver_skip_entry():
    m = Mapping(entries=[MappingEntry("s3://legacy/*", target=None, skip=True)])
    resolve = build_resolver(m, fallback=Target("default", "x"))
    assert resolve("s3://legacy/events", "read").skip is True
    # Fallback still applies to non-matching paths
    assert resolve("s3://other/", "read").migrate_to == Target("default", "x")


def test_resolver_direction_scoped_entry():
    m = Mapping(entries=[
        MappingEntry("s3://b/*", target=Target("ns", "t"), direction="write"),
    ])
    resolve = build_resolver(m, fallback=None)
    # Only write matches; read should be unresolved
    assert resolve("s3://b/x", "write").migrate_to == Target("ns", "t")
    read_decision = resolve("s3://b/x", "read")
    assert read_decision.migrate_to is None and not read_decision.skip


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


def test_load_mapping_with_skip_and_direction(tmp_path: Path):
    mapping_file = tmp_path / "mapping.json"
    mapping_file.write_text(json.dumps({
        "tables": [
            {"path_glob": "s3://x/*", "skip": True},
            {"path_glob": "s3://y/*", "direction": "write", "namespace": "ns", "table": "y"},
        ],
    }))
    m = load_mapping(mapping_file)
    assert m.entries[0].skip is True
    assert m.entries[0].target is None
    assert m.entries[1].direction == "write"
    assert m.entries[1].target == Target("ns", "y")
