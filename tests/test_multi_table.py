"""Multi-table routing: path extraction, resolver, mapping loading, transformer integration."""
import json
import textwrap
from pathlib import Path

from skills.parquet_to_iceberg.detector import detect_parquet_usage
from skills.parquet_to_iceberg.extract import extract_path_arg
from skills.parquet_to_iceberg.targets import (
    Decision,
    Mapping,
    MappingEntry,
    Target,
    build_resolver,
    load_mapping,
)
from skills.parquet_to_iceberg.transformers.pandas import transform_pandas_file
from skills.parquet_to_iceberg.transformers.pyspark import transform_pyspark_file
from skills.parquet_to_iceberg.transformers.pyarrow import transform_pyarrow_file
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
    rc = convert_project(proj, mapping=mapping, mode="deterministic")
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


# ─── Skip + direction in transformers ────────────────────────────────

def test_pandas_skip_leaves_line_untouched():
    src = textwrap.dedent("""
        import pandas as pd
        keep = pd.read_parquet("s3://bucket/events/2024")
        legacy = pd.read_parquet("s3://legacy/old.parquet")
    """).lstrip()
    mapping = Mapping(entries=[
        MappingEntry("s3://bucket/events/*", Target("analytics", "events")),
        MappingEntry("s3://legacy/*", target=None, skip=True),
    ])
    out = transform_pandas_file(src, mapping=mapping)
    # The skipped line is preserved verbatim
    assert 'pd.read_parquet("s3://legacy/old.parquet")' in out
    assert "iceberg: skipped by mapping" in out
    # The kept one is rewritten
    assert "tbl.scan().to_pandas()" in out or 'load_table(("analytics", "events"))' in out


def test_pyspark_direction_scoped_write_only():
    src = textwrap.dedent("""
        events = spark.read.parquet("s3://bucket/events/")
        events.write.mode("overwrite").parquet("s3://bucket/events/")
    """).lstrip()
    # Migrate writes only; reads stay as parquet.
    mapping = Mapping(entries=[
        MappingEntry("s3://bucket/events/*", target=Target("analytics", "events"), direction="write"),
        MappingEntry("s3://bucket/events/*", target=None, skip=True, direction="read"),
    ])
    out = transform_pyspark_file(src, mapping=mapping)
    assert 'writeTo("analytics.events")' in out
    # Read kept as parquet
    assert 'spark.read.parquet("s3://bucket/events/")' in out
    assert "iceberg: skipped by mapping" in out


def test_jvm_skip_keeps_original():
    src = textwrap.dedent("""
        Dataset<Row> df = spark.read().parquet("s3://legacy/raw/");
        df.write().mode("overwrite").parquet("s3://new/out/");
    """).lstrip()
    mapping = Mapping(entries=[
        MappingEntry("s3://legacy/*", target=None, skip=True),
        MappingEntry("s3://new/*", target=Target("analytics", "out")),
    ])
    out = transform_jvm_file(src, language="java", mapping=mapping)
    assert 'spark.read().parquet("s3://legacy/raw/")' in out
    assert "// iceberg: skipped by mapping" in out
    assert 'writeTo("analytics.out")' in out


def test_pyarrow_skip_keeps_original():
    src = 'import pyarrow.parquet as pq\ntbl = pq.read_table("data/old.parquet")\n'
    mapping = Mapping(entries=[MappingEntry("data/*", target=None, skip=True)])
    out = transform_pyarrow_file(src, mapping=mapping)
    assert 'pq.read_table("data/old.parquet")' in out
    assert "iceberg: skipped by mapping" in out
    # No catalog header should be emitted since nothing gets migrated
    assert "load_catalog" not in out


def test_cli_mapping_with_skip_entries(tmp_path: Path):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "etl.py").write_text(textwrap.dedent("""
        import pandas as pd
        keep = pd.read_parquet("s3://bucket/events/2024")
        legacy = pd.read_parquet("s3://legacy/old.parquet")
    """).lstrip())

    mapping_file = tmp_path / "mapping.json"
    mapping_file.write_text(json.dumps({
        "tables": [
            {"path_glob": "s3://bucket/events/*", "namespace": "analytics", "table": "events"},
            {"path_glob": "s3://legacy/*", "skip": True},
        ],
    }))
    mapping = load_mapping(mapping_file)
    rc = convert_project(proj, mapping=mapping, mode="deterministic")
    assert rc == 0
    rewritten = (proj / "etl.py").read_text()
    assert 'load_table(("analytics", "events"))' in rewritten
    assert 'pd.read_parquet("s3://legacy/old.parquet")' in rewritten
    assert "iceberg: skipped by mapping" in rewritten


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
