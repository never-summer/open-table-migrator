"""Hybrid-mode deterministic pre-pass.

Before the agent starts rewriting worklist entries by hand, the skill runs a
small deterministic pass that handles two kinds of edits which have no LLM
judgment in them:

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
import re
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

_PYSPARK_PATTERN_PREFIXES = ("pyspark_", "pyspark_stream_")


def _is_pyspark_pattern(pattern_type: str) -> bool:
    return any(pattern_type.startswith(p) for p in _PYSPARK_PATTERN_PREFIXES)


def _indent_of(line: str) -> str:
    return line[: len(line) - len(line.lstrip())]


def run_prepass(
    matches: list[PatternMatch],
    resolver: Resolver,
) -> dict[Path, int]:
    """Apply skip markers and pyspark conf comments in place.

    Returns a dict of ``{file: number_of_edits}`` for reporting. Only files
    actually touched appear in the dict.
    """
    by_file: dict[Path, list[PatternMatch]] = {}
    for m in matches:
        by_file.setdefault(m.file, []).append(m)

    edits_per_file: dict[Path, int] = {}

    for src_file, file_matches in by_file.items():
        try:
            source = src_file.read_text(errors="replace")
        except OSError:
            continue

        original = source
        lines = source.splitlines(keepends=True)

        # ── 1. Skip markers ──────────────────────────────────────────
        # Walk matches whose resolver decision is skip. Insert marker above
        # the *logical* start line, once per line. Sort descending so line
        # indices stay valid as we insert.
        skip_lines: list[int] = []
        for m in file_matches:
            direction = direction_of(m.pattern_type)
            d = resolver(m.path_arg, direction if direction in ("read", "write") else "any")
            if d.skip:
                skip_lines.append(m.line)

        # Dedup by line number and skip lines that already have a marker above them
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

        # ── 2. Pyspark conf comment ──────────────────────────────────
        current_source = "".join(lines)
        if src_file.suffix.lower() == ".py" and not _ICEBERG_CONF_MARKER_RE.search(current_source):
            # First pyspark pattern whose decision is NOT skip
            first: PatternMatch | None = None
            # Match order in `file_matches` is detection order (source order)
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
                # Re-find the target line after the skip-marker insertions above may
                # have shifted indices — walk and count.
                # We only care about the line whose STRIPPED content starts the
                # original matched code. Easiest: find by matching the first
                # non-skip line whose stripped form contains something pyspark-ish,
                # but that's brittle. Better: track line offsets.
                #
                # Since skip markers were inserted for the lines listed in
                # unique_skip_lines, every inserted index ≤ target bumps the
                # target index by 1.
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

        new_source = "".join(lines)
        if new_source != original:
            src_file.write_text(new_source)
            edits_per_file[src_file] = len(unique_skip_lines) + (
                1 if src_file.suffix.lower() == ".py" and _ICEBERG_CONF_MARKER_RE.search(new_source)
                and not _ICEBERG_CONF_MARKER_RE.search(original) else 0
            )

    return edits_per_file
