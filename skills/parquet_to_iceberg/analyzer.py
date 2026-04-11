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
