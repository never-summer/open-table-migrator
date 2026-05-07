"""Pass 2 — find SQL literals (@Query, JdbcTemplate) and jOOQ DSL chains."""
from dataclasses import dataclass
from pathlib import Path

from .project_scan import SymbolTable


@dataclass(frozen=True)
class SqlUnit:
    file: str
    line: int
    kind: str            # "jdbc_query", "jpa_query", "jooq_select", ...
    sql: str | None      # raw SQL string, None for jOOQ DSL
    jooq_columns: tuple[str, ...] = ()
    jooq_tables: tuple[str, ...] = ()


def run(project_root: Path, symbols: SymbolTable) -> list[SqlUnit]:
    return []
