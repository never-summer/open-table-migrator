from dataclasses import dataclass
from pathlib import Path


@dataclass
class PatternMatch:
    file: Path
    line: int
    pattern_type: str
    original_code: str
    path_arg: str | None = None
    end_line: int | None = None  # last physical line of the logical statement
    format: str | None = None  # NEW

    @property
    def direction(self) -> str:
        # Derived from pattern_type; lazy import avoids a detector <-> analyzer cycle.
        from .analyzer import direction_of
        return direction_of(self.pattern_type)


def detect_all_io(project_root: Path) -> list[PatternMatch]:
    """Detect ALL data I/O operations — parquet, ORC, CSV, JSON, Avro,
    Delta, text, JDBC, spark.table(), etc. Used for the full inventory
    table the agent shows the user."""
    from .ts_detector import ts_detect
    return ts_detect(project_root)


def detect_parquet_usage(project_root: Path) -> list[PatternMatch]:
    """Detect parquet/ORC operations only (migration candidates)."""
    from .ts_detector import ts_detect
    from .analyzer import is_migration_candidate
    return [m for m in ts_detect(project_root) if is_migration_candidate(m.pattern_type)]
