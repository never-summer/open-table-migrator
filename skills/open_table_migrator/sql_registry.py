"""Scan .sql / .hql files for CREATE TABLE statements and build a registry
of table names with their storage formats.

This lets us cross-reference: if code writes to ``events`` via
``saveAsTable("events")`` and the SQL file defines ``events`` as parquet,
we know that operation is a migration candidate even though the code itself
never mentions "parquet".
"""
import re
from dataclasses import dataclass
from pathlib import Path

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


@dataclass
class TableDef:
    """A table definition extracted from a SQL file."""
    table_name: str
    database: str | None
    format: str           # "parquet" or "orc", lowercased
    file: Path
    line: int
    snippet: str          # the matched DDL fragment


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
                defs.append(TableDef(
                    table_name=table,
                    database=db,
                    format=fmt,
                    file=sql_file,
                    line=line,
                    snippet=snippet,
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
