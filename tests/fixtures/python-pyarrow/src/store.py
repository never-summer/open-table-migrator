import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path


SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("status", pa.string()),
    pa.field("value", pa.float64()),
])


def write_table(table: pa.Table, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def read_table(path: str) -> pa.Table:
    return pq.read_table(path)


def make_sample_table() -> pa.Table:
    return pa.table(
        {"id": [1, 2, 3], "status": ["active", "active", "done"], "value": [1.0, 2.0, 3.0]},
        schema=SCHEMA,
    )
