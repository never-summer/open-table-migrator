"""Pass 3 — sqlglot parses each SqlUnit, emits column-level edges."""
from skills.data_lineage.model import Edge

from .sql_extract import SqlUnit


def run(units: list[SqlUnit]) -> list[Edge]:
    return []
