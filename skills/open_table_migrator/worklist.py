"""Hybrid-mode worklist builder.

In hybrid mode the skill's job is to **find** rewrite sites, not to perform
the rewrites. For every detected operation that needs a real code change, the
CLI produces a ``WorklistEntry`` containing enough context for an agent / LLM
to do the rewrite by hand via an ``Edit`` tool:

- which file and line range (the *logical* statement, not a physical line —
  multi-line builder chains span several physical lines)
- the folded source text of the statement
- what kind of operation it is (pattern_type) and which direction
- what Iceberg target it resolved to (namespace / table), or why it didn't

Skipped entries (mapping marked them skip) are handled by the deterministic
pre-pass (skill drops an "iceberg: skipped by mapping" comment above them) and
do **not** end up in the worklist.

Warn-only entries (streaming, pyarrow dataset API, ParquetFile/ParquetDataset)
**do** go into the worklist — in hybrid mode the agent tries to rewrite them
instead of leaving a TODO comment.
"""
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from .analyzer import direction_of
from .detector import PatternMatch
from .targets import Decision, Resolver


@dataclass
class WorklistEntry:
    file: str                 # path relative to project root
    start_line: int
    end_line: int
    pattern_type: str
    direction: str            # read | write | schema | unknown
    language: str             # python | java | scala
    path_arg: str | None
    original_code: str        # folded logical statement, one string
    surrounding: str          # ±5 lines of context including the statement
    resolved_namespace: str | None
    resolved_table: str | None
    needs_manual_target: bool  # True when decision.unresolved()
    hint: str                  # short instruction for the agent

    def to_dict(self) -> dict:
        return asdict(self)


_LANG_BY_SUFFIX = {".py": "python", ".java": "java", ".scala": "scala"}


def _surrounding(source_lines: list[str], start_line: int, end_line: int, ctx: int = 5) -> str:
    """Return ``ctx`` lines before ``start_line`` and after ``end_line`` (1-indexed)."""
    lo = max(0, start_line - 1 - ctx)
    hi = min(len(source_lines), end_line + ctx)
    return "".join(source_lines[lo:hi])


def _hint_for(pattern_type: str, direction: str, decision: Decision) -> str:
    if decision.skip:
        return "skip (mapping) — should not appear in worklist"
    target = decision.migrate_to
    if target is None:
        return (
            f"unresolved target for {direction} op — ask user or edit mapping.json, "
            "then rewrite by hand"
        )
    fqn = target.fqn
    # Pattern-family hints
    if pattern_type.startswith("pandas"):
        if direction == "read":
            return f"rewrite to `{{var}} = tbl.scan().to_pandas()` using tbl bound to {fqn}"
        return f"rewrite to `tbl.overwrite({{df}})` using tbl bound to {fqn}"
    if pattern_type in ("pyarrow_read_dataset", "pyarrow_write_dataset"):
        return (
            f"pyarrow dataset / ParquetFile API — rewrite to "
            f"`catalog.load_table(('ns','table')).scan().to_arrow()` for {fqn}"
        )
    if pattern_type.startswith("pyarrow"):
        if direction == "read":
            return f"rewrite to `tbl.scan().to_arrow()` using tbl bound to {fqn}"
        return f"rewrite to `tbl.overwrite({{table}})` using tbl bound to {fqn}"
    if pattern_type.startswith("spark_stream") or pattern_type.startswith(
        "pyspark_stream"
    ) or pattern_type.startswith("java_spark_stream") or pattern_type.startswith("scala_spark_stream"):
        return (
            f"Structured Streaming sink/source — rewrite using "
            f"`.format('iceberg').option('path', '{fqn}')` or `.toTable('{fqn}')`"
        )
    if (
        pattern_type.startswith("pyspark")
        or (pattern_type.startswith("spark_") and not pattern_type.startswith("spark_stream") and pattern_type != "spark_read_table")
    ):
        if direction == "read":
            return f"rewrite to `spark.table(\"{fqn}\")`"
        return f"rewrite to `{{df}}.writeTo(\"{fqn}\").overwritePartitions()`"
    if pattern_type.startswith(("java_spark", "scala_spark")):
        if direction == "read":
            return f"rewrite to `spark.read().format(\"iceberg\").load(\"{fqn}\")`"
        return f"rewrite to `{{df}}.writeTo(\"{fqn}\").overwritePartitions()`"
    if pattern_type in ("hive_create_parquet", "hive_create_orc"):
        return "replace `STORED AS PARQUET|ORC` with `USING iceberg` in the SQL string"
    if pattern_type in ("sql_using_parquet", "sql_using_orc"):
        return "replace `USING parquet|orc` with `USING iceberg` in the SQL string"
    if pattern_type.startswith("hive_insert_"):
        return "INSERT INTO/OVERWRITE — SQL works as-is on Iceberg; verify the table is the migrated one"
    if pattern_type in ("hive_save_as_table", "hive_save_table"):
        return f"rewrite `.saveAsTable(...)` to `.writeTo(\"{fqn}\").createOrReplace()`"
    return f"rewrite this {direction} op to target {fqn}"


def build_worklist(
    matches: list[PatternMatch],
    project_root: Path,
    resolver: Resolver,
) -> list[WorklistEntry]:
    """Build the hybrid-mode worklist from detector matches.

    Excludes entries the deterministic pre-pass handles (skip-markers). Every
    remaining match — including unresolved ones and warn-only families —
    becomes an entry so the agent has a single place to read.
    """
    entries: list[WorklistEntry] = []

    # Group matches by file so we only read each file once when fetching context.
    matches_by_file: dict[Path, list[PatternMatch]] = {}
    for m in matches:
        matches_by_file.setdefault(m.file, []).append(m)

    for src_file, file_matches in sorted(matches_by_file.items(), key=lambda kv: kv[0]):
        try:
            source = src_file.read_text(errors="replace")
        except OSError:
            continue
        source_lines = source.splitlines(keepends=True)

        for m in file_matches:
            direction = direction_of(m.pattern_type)
            decision = resolver(m.path_arg, direction if direction in ("read", "write") else "any")

            if decision.skip:
                continue  # prepass handles these

            start = m.line
            end = m.end_line or m.line

            try:
                rel = src_file.relative_to(project_root)
            except ValueError:
                rel = src_file

            target = decision.migrate_to
            entries.append(WorklistEntry(
                file=str(rel),
                start_line=start,
                end_line=end,
                pattern_type=m.pattern_type,
                direction=direction,
                language=_LANG_BY_SUFFIX.get(src_file.suffix.lower(), "unknown"),
                path_arg=m.path_arg,
                original_code=m.original_code.strip(),
                surrounding=_surrounding(source_lines, start, end),
                resolved_namespace=target.namespace if target else None,
                resolved_table=target.table if target else None,
                needs_manual_target=target is None,
                hint=_hint_for(m.pattern_type, direction, decision),
            ))

    return entries


def write_worklist(entries: list[WorklistEntry], project_root: Path) -> Path:
    """Write ``iceberg-worklist.json`` at project root. Returns the path."""
    out = project_root / "iceberg-worklist.json"
    payload = {
        "version": 1,
        "count": len(entries),
        "entries": [e.to_dict() for e in entries],
    }
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    return out
