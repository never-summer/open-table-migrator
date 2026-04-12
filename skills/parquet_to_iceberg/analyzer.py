import re
from dataclasses import dataclass, field
from pathlib import Path
from .detector import PatternMatch


_READ_TYPES = {
    # Python — parquet/orc
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
    # Broad I/O — reads
    "pandas_csv_read", "pandas_json_read", "pandas_excel_read",
    "pyspark_csv_read", "pyspark_json_read", "pyspark_text_read",
    "pyspark_csv_read_fmt", "pyspark_json_read_fmt", "pyspark_avro_read_fmt",
    "pyspark_delta_read_fmt", "pyspark_text_read_fmt", "pyspark_jdbc_read_fmt",
    "pyspark_jdbc_read",
    "jvm_csv_read", "jvm_json_read", "jvm_text_read",
    "jvm_csv_read_fmt", "jvm_json_read_fmt", "jvm_avro_read_fmt",
    "jvm_delta_read_fmt", "jvm_text_read_fmt", "jvm_jdbc_read_fmt",
    "jvm_jdbc_read",
    "python_csv_reader",
    "spark_table_read",
}
_WRITE_TYPES = {
    # Python — parquet/orc
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
    # Broad I/O — writes
    "pandas_csv_write", "pandas_json_write", "pandas_excel_write",
    "pyspark_csv_write", "pyspark_json_write", "pyspark_text_write",
    "pyspark_csv_write_fmt", "pyspark_json_write_fmt", "pyspark_avro_write_fmt",
    "pyspark_delta_write_fmt", "pyspark_text_write_fmt", "pyspark_jdbc_write_fmt",
    "jvm_csv_write", "jvm_json_write", "jvm_text_write",
    "jvm_csv_write_fmt", "jvm_json_write_fmt", "jvm_avro_write_fmt",
    "jvm_delta_write_fmt", "jvm_text_write_fmt", "jvm_jdbc_write_fmt",
    "python_csv_writer", "java_file_writer",
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


_PARQUET_ORC_PREFIXES = (
    "pandas_read", "pandas_write", "pandas_orc",
    "pyspark_read", "pyspark_write", "pyspark_orc", "pyspark_stream",
    "pyarrow_read", "pyarrow_write", "pyarrow_orc", "pyarrow_parquet", "pyarrow_dataset",
    "java_spark_read", "java_spark_write", "java_spark_orc", "java_spark_stream",
    "scala_spark_read", "scala_spark_write", "scala_spark_orc", "scala_spark_stream",
    "hive_", "sql_using_",
)


def is_migration_candidate(pattern_type: str) -> bool:
    """True if this pattern type is a parquet/orc operation (migration target)."""
    return any(pattern_type.startswith(p) for p in _PARQUET_ORC_PREFIXES)


# ─── Deduped operation sites ─────────────────────────────────────────

@dataclass
class OperationSite:
    """A single logical read/write site — deduped across overlapping / adjacent
    detector hits (e.g. ``scala_spark_write_fmt`` + ``hive_save_as_table`` on
    the same chain).
    """
    file: Path
    start_line: int
    end_line: int
    direction: str
    subject: str | None
    path_arg: str | None
    summary: str
    pattern_types: list[str]
    matches: list[PatternMatch]


def dedup_matches(matches: list[PatternMatch]) -> list[OperationSite]:
    """Merge detector hits with overlapping or *adjacent* line ranges within
    the same file into single ``OperationSite`` objects.
    """
    from .extract import extract_subject, summarize_operation

    # Group by file, sort by start line.
    by_file: dict[Path, list[PatternMatch]] = {}
    for m in matches:
        by_file.setdefault(m.file, []).append(m)

    sites: list[OperationSite] = []
    for f, file_matches in sorted(by_file.items(), key=lambda kv: kv[0]):
        file_matches.sort(key=lambda m: (m.line, -(m.end_line or m.line)))
        groups: list[list[PatternMatch]] = []
        for m in file_matches:
            end = m.end_line or m.line
            if groups:
                prev = groups[-1]
                prev_end = max(pm.end_line or pm.line for pm in prev)
                prev_start = min(pm.line for pm in prev)
                # Merge if overlapping OR adjacent (end + 1 >= start)
                if m.line <= prev_end + 1:
                    groups[-1].append(m)
                    continue
            groups.append([m])

        for group in groups:
            # Pick the most informative match: prefer one with a path_arg,
            # then longest original_code.
            best = max(group, key=lambda m: (m.path_arg is not None, len(m.original_code)))
            subj = extract_subject(best.original_code)
            # If subject still None, try other matches in group
            if subj is None:
                for gm in group:
                    subj = extract_subject(gm.original_code)
                    if subj:
                        break
            summary = summarize_operation(best.original_code, best.pattern_type, best.path_arg, subject_override=subj)
            start = min(gm.line for gm in group)
            end = max(gm.end_line or gm.line for gm in group)
            sites.append(OperationSite(
                file=f,
                start_line=start,
                end_line=end,
                direction=direction_of(best.pattern_type),
                subject=subj,
                path_arg=best.path_arg or next((gm.path_arg for gm in group if gm.path_arg), None),
                summary=summary,
                pattern_types=sorted(set(gm.pattern_type for gm in group)),
                matches=group,
            ))
    return sites


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
