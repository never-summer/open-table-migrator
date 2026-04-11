import re
from dataclasses import dataclass, field
from pathlib import Path
from .detector import PatternMatch


_READ_TYPES = {
    # Python
    "pandas_read", "pandas_orc_read",
    "pyspark_read", "pyspark_orc_read", "pyspark_read_fmt",
    "pyspark_stream_read", "pyspark_stream_read_fmt",
    "pyarrow_read", "pyarrow_orc_read",
    "pyarrow_parquet_file", "pyarrow_parquet_dataset", "pyarrow_dataset_read",
    # Java
    "java_spark_read", "java_spark_orc_read", "java_spark_read_fmt",
    "java_spark_stream_read", "java_spark_stream_read_fmt",
    # Scala
    "scala_spark_read", "scala_spark_orc_read", "scala_spark_read_fmt",
    "scala_spark_stream_read", "scala_spark_stream_read_fmt",
}
_WRITE_TYPES = {
    # Python
    "pandas_write", "pandas_orc_write",
    "pyspark_write", "pyspark_orc_write", "pyspark_write_fmt",
    "pyspark_stream_write", "pyspark_stream_write_fmt",
    "pyarrow_write", "pyarrow_orc_write", "pyarrow_dataset_write",
    # Java
    "java_spark_write", "java_spark_orc_write", "java_spark_write_fmt",
    "java_spark_stream_write", "java_spark_stream_write_fmt",
    # Scala
    "scala_spark_write", "scala_spark_orc_write", "scala_spark_write_fmt",
    "scala_spark_stream_write", "scala_spark_stream_write_fmt",
    # Hive/SQL DML + API
    "hive_save_as_table", "hive_insert_overwrite", "hive_insert_into",
}
_SCHEMA_TYPES = {
    "hive_create_parquet", "hive_create_orc",
    "sql_using_parquet", "sql_using_orc",
}

# Patterns for which transformers only emit a TODO comment (not a full rewrite)
_WARN_ONLY_TYPES = {
    "pyspark_stream_read", "pyspark_stream_read_fmt",
    "pyspark_stream_write", "pyspark_stream_write_fmt",
    "java_spark_stream_read", "java_spark_stream_read_fmt",
    "java_spark_stream_write", "java_spark_stream_write_fmt",
    "scala_spark_stream_read", "scala_spark_stream_read_fmt",
    "scala_spark_stream_write", "scala_spark_stream_write_fmt",
    "pyarrow_parquet_file", "pyarrow_parquet_dataset",
    "pyarrow_dataset_read", "pyarrow_dataset_write",
}


def direction_of(pattern_type: str) -> str:
    if pattern_type in _READ_TYPES:
        return "read"
    if pattern_type in _WRITE_TYPES:
        return "write"
    if pattern_type in _SCHEMA_TYPES:
        return "schema"
    return "unknown"


def is_warn_only(pattern_type: str) -> bool:
    return pattern_type in _WARN_ONLY_TYPES


@dataclass
class Report:
    total: int
    by_file: dict[Path, list[PatternMatch]] = field(default_factory=dict)
    by_direction: dict[str, list[PatternMatch]] = field(default_factory=dict)
    by_pattern_type: dict[str, list[PatternMatch]] = field(default_factory=dict)


def build_report(matches: list[PatternMatch]) -> Report:
    by_file: dict[Path, list[PatternMatch]] = {}
    by_direction: dict[str, list[PatternMatch]] = {"read": [], "write": [], "schema": []}
    by_pattern_type: dict[str, list[PatternMatch]] = {}

    for m in matches:
        by_file.setdefault(m.file, []).append(m)
        d = direction_of(m.pattern_type)
        by_direction.setdefault(d, []).append(m)
        by_pattern_type.setdefault(m.pattern_type, []).append(m)

    return Report(
        total=len(matches),
        by_file=by_file,
        by_direction=by_direction,
        by_pattern_type=by_pattern_type,
    )


# ─── A10: secondary SQL DDL/cache references to mapped tables ────────
# Commands that refer to a table by name and change its lifecycle or cache state.
# DROP/TRUNCATE still work on Iceberg (no action needed), but CACHE/UNCACHE/REFRESH
# semantics are different and the user should be warned.
_SQL_REF_COMMANDS = (
    "DROP TABLE",
    "TRUNCATE TABLE",
    "CACHE TABLE",
    "CACHE LAZY TABLE",
    "UNCACHE TABLE",
    "REFRESH TABLE",
    "MSCK REPAIR TABLE",
    "ANALYZE TABLE",
)


@dataclass
class DdlReference:
    file: Path
    line: int
    command: str
    table_name: str
    snippet: str


def _collect_table_names(matches: list[PatternMatch]) -> set[str]:
    """Gather bare table-name identifiers from detector matches.

    We only care about *identifiers* (saveAsTable("x") → "x"), not file paths —
    `DROP TABLE s3://bucket/...` is not a thing. Filter out anything with a
    scheme or a path separator.
    """
    names: set[str] = set()
    for m in matches:
        if not m.path_arg:
            continue
        arg = m.path_arg.strip()
        if not arg:
            continue
        if "://" in arg or "/" in arg or "\\" in arg:
            continue
        # bare identifier or db.table — take the last segment too, so both match
        names.add(arg)
        if "." in arg:
            names.add(arg.rsplit(".", 1)[-1])
    return names


def find_ddl_references(
    matches: list[PatternMatch],
    project_root: Path,
) -> list[DdlReference]:
    """Scan source files for SQL DDL/cache commands referencing table names
    that appear as targets in the detector's matches.

    The detector catches `saveAsTable("UsersTbl")` but not
    `spark.sql("DROP TABLE UsersTbl")` — this helper returns the missing
    references so the CLI / agent can warn about them.
    """
    table_names = _collect_table_names(matches)
    if not table_names:
        return []

    name_alt = "|".join(sorted((re.escape(n) for n in table_names), key=len, reverse=True))
    cmd_alt = "|".join(c.replace(" ", r"\s+") for c in _SQL_REF_COMMANDS)
    pattern = re.compile(
        rf'\b({cmd_alt})\b(?:\s+IF\s+(?:NOT\s+)?EXISTS)?\s+`?({name_alt})`?',
        re.IGNORECASE,
    )

    files_with_matches = {m.file for m in matches}
    # Also scan sibling files of the same types — the DDL may live in a test or init script.
    scan_files: set[Path] = set(files_with_matches)
    for src_file in project_root.rglob("*"):
        if not src_file.is_file():
            continue
        if src_file.suffix.lower() not in {".py", ".java", ".scala"}:
            continue
        scan_files.add(src_file)

    refs: list[DdlReference] = []
    for src_file in sorted(scan_files):
        try:
            source = src_file.read_text(errors="replace")
        except OSError:
            continue
        for i, line in enumerate(source.splitlines(), start=1):
            # Only look inside string literals — crude check: the hit must be
            # surrounded somewhere on the line by a quote character.
            if '"' not in line and "'" not in line:
                continue
            for match in pattern.finditer(line):
                refs.append(DdlReference(
                    file=src_file,
                    line=i,
                    command=match.group(1).upper(),
                    table_name=match.group(2),
                    snippet=line.strip(),
                ))
    return refs


def format_report(report: Report, *, project_root: Path) -> str:
    if report.total == 0:
        return "No Parquet / Hive usage found."

    lines: list[str] = []
    lines.append(f"Found {report.total} Parquet/ORC operation(s) across {len(report.by_file)} file(s):")
    lines.append("")

    lines.append("By direction:")
    for d in ("read", "write", "schema"):
        count = len(report.by_direction.get(d, []))
        if count:
            lines.append(f"  {d:7s}: {count}")
    lines.append("")

    lines.append("By pattern type:")
    for ptype in sorted(report.by_pattern_type):
        marker = "  (warn-only)" if is_warn_only(ptype) else ""
        lines.append(f"  {ptype:28s}: {len(report.by_pattern_type[ptype])}{marker}")
    lines.append("")

    lines.append("Per-file breakdown:")
    for file in sorted(report.by_file):
        try:
            rel = file.relative_to(project_root)
        except ValueError:
            rel = file
        lines.append(f"  {rel}:")
        for m in report.by_file[file]:
            lines.append(f"    {m.pattern_type:28s} ({direction_of(m.pattern_type)}) line {m.line}:{m.line}")
            lines.append(f"      {m.original_code}")
    return "\n".join(lines)
