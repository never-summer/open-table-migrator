"""Scan .sql / .hql files for CREATE TABLE statements and build a registry
of table names with their storage formats.

This lets us cross-reference: if code writes to ``events`` via
``saveAsTable("events")`` and the SQL file defines ``events`` as parquet,
we know that operation is a migration candidate even though the code itself
never mentions "parquet".
"""
import re
from dataclasses import dataclass, field
from pathlib import Path

from .detector import PartitionTransform

_SQL_EXTS = {".sql", ".hql", ".ddl"}

# CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [db.]table_name (...) STORED AS PARQUET|ORC
_CREATE_STORED_AS = re.compile(
    r"\bCREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:`?(\w+)`?\.)?`?(\w+)`?"   # optional db.table
    r"[^;]*?"                        # columns, etc. (lazy)
    r"\bSTORED\s+AS\s+(PARQUET|ORC)\b",
    re.IGNORECASE | re.DOTALL,
)

# CREATE TABLE [db.]table_name (...) USING parquet|orc
_CREATE_USING = re.compile(
    r"\bCREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:`?(\w+)`?\.)?`?(\w+)`?"
    r"[^;]*?"
    r"\bUSING\s+(parquet|orc)\b",
    re.IGNORECASE | re.DOTALL,
)

# CREATE TABLE ... AS SELECT ... FROM ... — CTAS with explicit format
_CTAS_STORED_AS = re.compile(
    r"\bCREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:`?(\w+)`?\.)?`?(\w+)`?"
    r"[^;]*?"
    r"\bSTORED\s+AS\s+(PARQUET|ORC)\b"
    r"[^;]*?"
    r"\bAS\s+SELECT\b",
    re.IGNORECASE | re.DOTALL,
)


_PARTITIONED_BY_RX = re.compile(
    r"\bPARTITIONED\s+BY\s*\(\s*([^)]+)\)",
    re.IGNORECASE,
)


@dataclass
class TableDef:
    """A table definition extracted from a SQL file."""
    table_name: str
    database: str | None
    format: str           # "parquet" or "orc", lowercased
    file: Path
    line: int
    snippet: str          # the matched DDL fragment
    partition_spec: tuple[PartitionTransform, ...] = field(default_factory=tuple)


def _parse_partition_clause(content: str, ddl_start: int, ddl_end: int) -> tuple[PartitionTransform, ...]:
    """Look for PARTITIONED BY (...) inside the substring [ddl_start:ddl_end+window]
    of content. Returns identity transforms for each column. Types are ignored.
    """
    window_end = min(len(content), ddl_end + 2000)
    window = content[ddl_start:window_end]
    m = _PARTITIONED_BY_RX.search(window)
    if not m:
        return ()
    cols_text = m.group(1)
    transforms: list[PartitionTransform] = []
    for part in cols_text.split(","):
        part = part.strip().strip("`").strip('"')
        if not part:
            continue
        col_name = part.split()[0].strip("`").strip('"')
        if col_name:
            transforms.append(PartitionTransform(
                kind="identity", column=col_name, n=None,
            ))
    return tuple(transforms)


def scan_sql_files(project_root: Path) -> list[TableDef]:
    """Scan all .sql/.hql/.ddl files under ``project_root`` and return
    table definitions that use parquet or ORC storage."""
    defs: list[TableDef] = []

    for sql_file in sorted(project_root.rglob("*")):
        if not sql_file.is_file():
            continue
        if sql_file.suffix.lower() not in _SQL_EXTS:
            continue
        try:
            content = sql_file.read_text(errors="replace")
        except OSError:
            continue

        for rx in (_CREATE_STORED_AS, _CREATE_USING, _CTAS_STORED_AS):
            for m in rx.finditer(content):
                db = m.group(1)
                table = m.group(2)
                fmt = m.group(3).lower()
                # Approximate line number
                line = content[:m.start()].count("\n") + 1
                snippet = m.group(0).strip()[:200]
                partition_spec = _parse_partition_clause(content, m.start(), m.end())
                defs.append(TableDef(
                    table_name=table,
                    database=db,
                    format=fmt,
                    file=sql_file,
                    line=line,
                    snippet=snippet,
                    partition_spec=partition_spec,
                ))

    # Dedup by (table_name, database) — keep the first occurrence
    seen: set[tuple[str | None, str]] = set()
    unique: list[TableDef] = []
    for d in defs:
        key = (d.database, d.table_name.lower())
        if key not in seen:
            seen.add(key)
            unique.append(d)
    return unique


@dataclass(frozen=True)
class TableReference:
    """Non-DDL reference to a table — INSERT / UPDATE / MERGE / FROM / JOIN."""
    table_name: str
    database: str | None
    role: str           # "write" or "read"
    sql_file: Path
    line: int


# Reusable name fragment: optional db prefix (with optional backticks/quotes), then table.
# Captures: group 1 = database (or None), group 2 = table name.
_NAME = r"`?\"?(?:(\w+)\.)?`?\"?(\w+)`?\"?"

_INSERT_INTO_RX = re.compile(rf"\bINSERT\s+INTO\s+(?:TABLE\s+)?{_NAME}", re.IGNORECASE)
_INSERT_OVERWRITE_RX = re.compile(rf"\bINSERT\s+OVERWRITE\s+(?:TABLE\s+)?{_NAME}", re.IGNORECASE)
_UPDATE_RX = re.compile(rf"\bUPDATE\s+{_NAME}\s+SET\b", re.IGNORECASE)
_MERGE_RX = re.compile(rf"\bMERGE\s+INTO\s+{_NAME}", re.IGNORECASE)
_FROM_RX = re.compile(rf"\bFROM\s+{_NAME}", re.IGNORECASE)
_JOIN_RX = re.compile(rf"\bJOIN\s+{_NAME}", re.IGNORECASE)
_WITH_CTE_RX = re.compile(r"(?:\bWITH|,)\s+(\w+)\s+AS\s*\(", re.IGNORECASE)


def _collect_cte_names(content: str) -> set[str]:
    return {m.group(1).lower() for m in _WITH_CTE_RX.finditer(content)}


def scan_sql_table_references(project_root: Path) -> list[TableReference]:
    """Scan .sql/.hql/.ddl for non-DDL table references.

    Returns INSERT/UPDATE/MERGE/FROM/JOIN references. CTE names (defined
    via `WITH <name> AS (...)`) are filtered out — they're aliases, not
    real tables.
    """
    refs: list[TableReference] = []
    for sql_file in sorted(project_root.rglob("*")):
        if not sql_file.is_file():
            continue
        if sql_file.suffix.lower() not in _SQL_EXTS:
            continue
        try:
            content = sql_file.read_text(errors="replace")
        except OSError:
            continue
        cte_names = _collect_cte_names(content)

        def _emit(role: str, rx) -> None:
            for m in rx.finditer(content):
                db = m.group(1)
                table = m.group(2)
                if table.lower() in cte_names:
                    continue
                line = content[:m.start()].count("\n") + 1
                refs.append(TableReference(
                    table_name=table, database=db, role=role,
                    sql_file=sql_file, line=line,
                ))

        _emit("write", _INSERT_INTO_RX)
        _emit("write", _INSERT_OVERWRITE_RX)
        _emit("write", _UPDATE_RX)
        _emit("write", _MERGE_RX)
        _emit("read",  _FROM_RX)
        _emit("read",  _JOIN_RX)
    return refs


def build_format_map(defs: list[TableDef]) -> dict[str, str]:
    """Return a ``{table_name_lower: format}`` lookup from SQL definitions.

    Both bare ``table`` and ``db.table`` keys are included so cross-reference
    works regardless of whether code uses a qualified name.
    """
    result: dict[str, str] = {}
    for d in defs:
        result[d.table_name.lower()] = d.format
        if d.database:
            result[f"{d.database.lower()}.{d.table_name.lower()}"] = d.format
    return result
