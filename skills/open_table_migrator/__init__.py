"""open-table-migrator: detect all data I/O and migrate to open table formats."""

from .scripts.detector import PatternMatch, detect_all_io, detect_parquet_usage
from .scripts.analyzer import build_report, format_report, direction_of, is_migration_candidate

__all__ = [
    "PatternMatch",
    "detect_all_io",
    "detect_parquet_usage",
    "build_report",
    "format_report",
    "direction_of",
    "is_migration_candidate",
]
