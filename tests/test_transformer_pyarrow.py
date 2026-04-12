from skills.open_table_migrator.transformers.pyarrow import transform_pyarrow_file


def test_transforms_pq_read_table():
    source = '''import pyarrow.parquet as pq
t = pq.read_table("data/events.parquet")
'''
    result = transform_pyarrow_file(source, table_name="events", namespace="default")
    assert "pq.read_table" not in result
    assert "catalog" in result
    assert "pyiceberg" in result


def test_transforms_pq_write_table():
    source = '''import pyarrow.parquet as pq
pq.write_table(table, "data/events.parquet")
'''
    result = transform_pyarrow_file(source, table_name="events", namespace="default")
    assert "pq.write_table" not in result
    assert "overwrite" in result or "append" in result


def test_preserves_schema_definition():
    source = '''import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([pa.field("id", pa.int64())])
t = pq.read_table("data.parquet")
'''
    result = transform_pyarrow_file(source, table_name="events", namespace="default")
    assert "SCHEMA = pa.schema" in result
