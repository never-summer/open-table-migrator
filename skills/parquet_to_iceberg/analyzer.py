from dataclasses import dataclass, field
from pathlib import Path
from .detector import PatternMatch


_READ_TYPES = {
    "pandas_read", "pyspark_read", "pyarrow_read",
    "java_spark_read", "scala_spark_read",
}
_WRITE_TYPES = {
    "pandas_write", "pyspark_write", "pyarrow_write",
    "java_spark_write", "scala_spark_write",
    "hive_save_as_table", "hive_insert_overwrite",
}
_SCHEMA_TYPES = {"hive_create_parquet"}


def direction_of(pattern_type: str) -> str:
    if pattern_type in _READ_TYPES:
        return "read"
    if pattern_type in _WRITE_TYPES:
        return "write"
    if pattern_type in _SCHEMA_TYPES:
        return "schema"
    return "unknown"


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
    lines.append(f"Found {report.total} Parquet operation(s) across {len(report.by_file)} file(s):")
    lines.append("")

    # Summary by direction
    lines.append("By direction:")
    for d in ("read", "write", "schema"):
        count = len(report.by_direction.get(d, []))
        if count:
            lines.append(f"  {d:7s}: {count}")
    lines.append("")

    # Summary by pattern type
    lines.append("By pattern type:")
    for ptype in sorted(report.by_pattern_type):
        lines.append(f"  {ptype:25s}: {len(report.by_pattern_type[ptype])}")
    lines.append("")

    # Per-file listing
    lines.append("Per-file breakdown:")
    for file in sorted(report.by_file):
        try:
            rel = file.relative_to(project_root)
        except ValueError:
            rel = file
        lines.append(f"  {rel}:")
        for m in report.by_file[file]:
            lines.append(f"    {m.pattern_type:25s} ({direction_of(m.pattern_type)}) line {m.line}:{m.line}")
            lines.append(f"      {m.original_code}")
    return "\n".join(lines)
