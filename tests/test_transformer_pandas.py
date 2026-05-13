from skills.open_table_migrator.transformers.pandas import transform_pandas_file


def test_transforms_read_parquet():
    source = '''import pandas as pd

df = pd.read_parquet("data/events.parquet")
print(df.head())
'''
    result = transform_pandas_file(
        source,
        table_name="events",
        namespace="default",
    )
    assert "pd.read_parquet" not in result
    assert "catalog.load_table" in result
    assert 'import pyiceberg' in result or 'from pyiceberg' in result


def test_transforms_write_parquet():
    source = '''import pandas as pd

df.to_parquet("data/events.parquet", index=False)
'''
    result = transform_pandas_file(
        source,
        table_name="events",
        namespace="default",
    )
    assert "to_parquet" not in result
    assert "overwrite" in result or "append" in result


def test_adds_catalog_import_once():
    source = '''import pandas as pd
df1 = pd.read_parquet("a.parquet")
df2 = pd.read_parquet("b.parquet")
'''
    result = transform_pandas_file(source, table_name="events", namespace="default")
    assert result.count("load_catalog") >= 1  # at least catalog setup
    # Import added exactly once
    assert result.count("from pyiceberg.catalog") == 1


def test_preserves_non_parquet_lines():
    source = '''import pandas as pd

x = 42
df = pd.read_parquet("data.parquet")
print(x)
'''
    result = transform_pandas_file(source, table_name="events", namespace="default")
    assert "x = 42" in result
    assert "print(x)" in result


def test_pandas_transformer_uses_resolved_const_path(tmp_path):
    """Const-folded path_arg propagates through detect → worklist → rewrite hint.

    The transformer (transform_pandas_file) works on raw source text and uses
    extract_path_arg, which handles only literal strings.  The resolver is
    given the *detected* path_arg — which the tree-sitter detector has already
    const-folded.  This test exercises the full detect → build_worklist →
    rewrite-hint pipeline and verifies that ``analytics.events`` appears in
    the worklist hint produced for the resolved constant reference.
    """
    from skills.open_table_migrator.detector import detect_parquet_usage
    from skills.open_table_migrator.targets import (
        Mapping, MappingEntry, Target, build_resolver,
    )
    from skills.open_table_migrator.worklist import build_worklist

    (tmp_path / "job.py").write_text(
        "import pandas as pd\n"
        'EVENTS_PATH = "s3://bucket/events"\n'
        "df = pd.read_parquet(EVENTS_PATH)\n"
    )

    # Step 1: const-folding detector resolves EVENTS_PATH → literal
    matches = detect_parquet_usage(tmp_path)
    pq_matches = [m for m in matches if "parquet" in m.pattern_type]
    assert len(pq_matches) == 1
    assert pq_matches[0].path_arg == "s3://bucket/events", (
        f"expected const-folded path, got {pq_matches[0].path_arg!r}"
    )

    # Step 2: mapping resolver picks the right target from the resolved path
    mapping = Mapping(entries=[
        MappingEntry(
            path_glob="s3://bucket/events*",
            target=Target(namespace="analytics", table="events"),
        ),
    ])
    resolver = build_resolver(mapping, fallback=None)

    # Step 3: worklist entry carries the resolved target (what the agent rewrites to)
    entries = build_worklist(pq_matches, tmp_path, resolver)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.resolved_namespace == "analytics"
    assert entry.resolved_table == "events"
    assert "analytics.events" in entry.hint
