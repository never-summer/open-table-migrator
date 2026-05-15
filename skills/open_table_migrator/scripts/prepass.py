"""Pre-pass: skip markers and Iceberg config comments.

Before the agent starts rewriting worklist entries, the CLI runs a small
pre-pass that handles two kinds of edits which have no LLM judgment in them:

1. **Skip markers** — if mapping says a path is ``skip: true``, drop a
   ``# iceberg: skipped by mapping (kept as parquet/orc)`` comment above the
   matching line and leave the code untouched.

2. **Iceberg config comment** — the first pyspark read/write op in a file gets
   a 3-line reminder about ``spark.sql.extensions`` / ``SparkSessionCatalog``
   injected above it (using the correct indent). Idempotent — re-runs skip
   files that already have the marker.

These edits are cheap, boilerplate, and would be a waste of tokens if routed
through the worklist.
"""
import difflib
import re
from dataclasses import dataclass
from pathlib import Path

from .analyzer import direction_of
from .detector import PatternMatch
from .targets import Resolver

_ICEBERG_CONF_LINES = [
    "# Iceberg: set spark.sql.extensions and catalog config in SparkSession builder",
    "# .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')",
    "# .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')",
]
_ICEBERG_CONF_MARKER_RE = re.compile(r"spark\.sql\.extensions.*IcebergSparkSessionExtensions")
_SKIP_MARKER_TEXT = "# iceberg: skipped by mapping (kept as parquet/orc)"

_PYSPARK_PATTERN_PREFIXES = ("pyspark_", "spark_")


@dataclass(frozen=True)
class PrepassPlan:
    """What prepass would do for one file."""
    file: Path
    original: str
    modified: str
    marker_count: int
    pyspark_conf_added: bool

    @property
    def diff(self) -> str:
        """Unified diff between original and modified content."""
        return "".join(difflib.unified_diff(
            self.original.splitlines(keepends=True),
            self.modified.splitlines(keepends=True),
            fromfile=str(self.file),
            tofile=str(self.file),
        ))

    @property
    def is_changed(self) -> bool:
        return self.modified != self.original


def _is_pyspark_pattern(pattern_type: str) -> bool:
    return any(pattern_type.startswith(p) for p in _PYSPARK_PATTERN_PREFIXES)


def _indent_of(line: str) -> str:
    return line[: len(line) - len(line.lstrip())]


def plan_prepass(
    matches: list[PatternMatch],
    resolver: Resolver,
) -> list[PrepassPlan]:
    """Compute what prepass would change. No filesystem writes.

    Returns only plans where the file would actually change. Order is by file path.
    """
    by_file: dict[Path, list[PatternMatch]] = {}
    for m in matches:
        by_file.setdefault(m.file, []).append(m)

    plans: list[PrepassPlan] = []
    for src_file, file_matches in sorted(by_file.items(), key=lambda kv: kv[0]):
        try:
            source = src_file.read_text(errors="replace")
        except OSError:
            continue
        plan = _plan_one_file(src_file, source, file_matches, resolver)
        if plan is not None and plan.is_changed:
            plans.append(plan)
    return plans


def _plan_one_file(
    src_file: Path,
    source: str,
    file_matches: list[PatternMatch],
    resolver: Resolver,
) -> PrepassPlan | None:
    """Compute a PrepassPlan for one file. Pure: no I/O."""
    original = source
    lines = source.splitlines(keepends=True)

    # ── Skip markers ─────────────────────────────────────────────
    skip_lines: list[int] = []
    for m in file_matches:
        direction = direction_of(m.pattern_type)
        d = resolver(m.path_arg, direction if direction in ("read", "write") else "any")
        if d.skip:
            skip_lines.append(m.line)

    unique_skip_lines = sorted(set(skip_lines), reverse=True)
    for line_no in unique_skip_lines:
        idx = line_no - 1
        if idx < 0 or idx >= len(lines):
            continue
        prev = lines[idx - 1] if idx > 0 else ""
        if _SKIP_MARKER_TEXT in prev:
            continue
        indent = _indent_of(lines[idx])
        marker = f"{indent}{_SKIP_MARKER_TEXT}\n"
        lines.insert(idx, marker)

    # ── Pyspark conf comment ─────────────────────────────────────
    pyspark_conf_added = False
    current_source = "".join(lines)
    if src_file.suffix.lower() == ".py" and not _ICEBERG_CONF_MARKER_RE.search(current_source):
        first: PatternMatch | None = None
        for m in sorted(file_matches, key=lambda m: m.line):
            if not _is_pyspark_pattern(m.pattern_type):
                continue
            direction = direction_of(m.pattern_type)
            d = resolver(m.path_arg, direction if direction in ("read", "write") else "any")
            if d.skip:
                continue
            first = m
            break

        if first is not None:
            target_idx = first.line - 1
            inserted_before = sum(
                1 for sl in unique_skip_lines if (sl - 1) <= target_idx
            )
            target_idx += inserted_before
            if 0 <= target_idx < len(lines):
                indent = _indent_of(lines[target_idx])
                block = [f"{indent}{text}\n" for text in _ICEBERG_CONF_LINES]
                for i, blk_line in enumerate(block):
                    lines.insert(target_idx + i, blk_line)
                pyspark_conf_added = True

    new_source = "".join(lines)
    marker_count = len(unique_skip_lines)
    return PrepassPlan(
        file=src_file,
        original=original,
        modified=new_source,
        marker_count=marker_count,
        pyspark_conf_added=pyspark_conf_added,
    )


def run_prepass(
    matches: list[PatternMatch],
    resolver: Resolver,
) -> dict[Path, int]:
    """Apply skip markers and pyspark conf comments in place.

    Returns ``{file: number_of_edits}`` for files actually touched.
    Backward-compatible wrapper around plan_prepass + write.
    """
    edits_per_file: dict[Path, int] = {}
    for plan in plan_prepass(matches, resolver):
        plan.file.write_text(plan.modified)
        edits_per_file[plan.file] = plan.marker_count + (1 if plan.pyspark_conf_added else 0)
    return edits_per_file
