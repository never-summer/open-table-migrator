from skills.parquet_to_iceberg.transformers.pandas import transform_pandas_file


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
