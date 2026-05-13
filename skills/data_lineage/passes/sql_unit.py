"""SqlUnit dataclass — lives here to avoid circular imports between
sql_extract.py (which imports extractors) and the extractors (which need SqlUnit)."""
from dataclasses import dataclass


@dataclass(frozen=True)
class SqlUnit:
    file: str
    line: int
    kind: str            # "jdbc_query", "jpa_query", "jooq_select", ...
    sql: str | None      # raw SQL string, None for jOOQ DSL
    jooq_columns: tuple[str, ...] = ()
    jooq_tables: tuple[str, ...] = ()
