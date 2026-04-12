"""
Modes:

    --mode=hybrid         (default) regex finds, agent/LLM rewrites. The CLI
                          runs the detector + a deterministic pre-pass (skip
                          markers + pyspark conf comment) and emits
                          ``iceberg-worklist.json`` at the project root for
                          the agent to consume.

    --mode=deterministic  regex finds *and* rewrites. The CLI runs the full
                          transformer pipeline on each file in-place. Use
                          this for reproducible dry-runs or when you don't
                          want any LLM in the loop.

Single-table usage:
    python -m skills.parquet_to_iceberg.cli <project> --table <name> --namespace <ns>

Multi-table usage (mapping file):
    python -m skills.parquet_to_iceberg.cli <project> --mapping mapping.json

    mapping.json — see skills/parquet_to_iceberg/targets.py for format.
    You can still pass --table/--namespace alongside --mapping as a fallback
    for paths that don't match any glob.

    --no-deps            skip the build-file updater (pyiceberg /
                         iceberg-spark-runtime). Use when the caller wants to
                         pin a specific version by hand.
"""
import argparse
import sys
from pathlib import Path
from typing import Literal

from .analyzer import find_ddl_references
from .detector import detect_parquet_usage
from .deps import update_dependencies
from .prepass import run_prepass
from .targets import Mapping, Target, build_resolver, load_mapping
from .transformers.pandas import transform_pandas_file
from .transformers.pyspark import transform_pyspark_file
from .transformers.pyarrow import transform_pyarrow_file
from .transformers.jvm import transform_jvm_file
from .worklist import build_worklist, write_worklist

Mode = Literal["hybrid", "deterministic"]


def convert_project(
    project_root: Path,
    *,
    table_name: str | None = None,
    namespace: str | None = None,
    mapping: Mapping | None = None,
    mode: Mode = "hybrid",
    update_deps: bool = True,
) -> int:
    matches = detect_parquet_usage(project_root)
    if not matches:
        print("No Parquet/ORC usage found.")
        return 0

    if mapping is None and not (table_name and namespace):
        print("ERROR: provide --table/--namespace, or --mapping, or both.", file=sys.stderr)
        return 2

    fallback = Target(namespace=namespace, table=table_name) if (namespace and table_name) else None
    resolver = build_resolver(mapping, fallback)

    if mode == "hybrid":
        _run_hybrid(project_root, matches, resolver)
    else:
        _run_deterministic(project_root, matches, table_name, namespace, mapping)

    updated_deps = update_dependencies(project_root) if update_deps else []
    ddl_refs = find_ddl_references(matches, project_root)

    if update_deps:
        if updated_deps:
            rels = [str(p.relative_to(project_root)) for p in updated_deps]
            print(f"Updated build file(s): {', '.join(rels)}")
        else:
            print("No build files updated (none found, or all already contain Iceberg).")
    else:
        print("Deps updater skipped (--no-deps).")

    if ddl_refs:
        print(f"\nSecondary references to mapped tables ({len(ddl_refs)}) — review manually:")
        for ref in ddl_refs:
            try:
                rel = ref.file.relative_to(project_root)
            except ValueError:
                rel = ref.file
            print(f"  {rel}:{ref.line}  {ref.command} {ref.table_name}")
            print(f"    {ref.snippet}")
        print("  NOTE: DROP/TRUNCATE still work on Iceberg; CACHE/UNCACHE/REFRESH semantics differ.")

    print("\nNext steps:")
    if mode == "hybrid":
        print("  1. Open iceberg-worklist.json and rewrite each entry by hand.")
        print("  2. Re-run detector to verify zero residual matches.")
    else:
        print("  Python: pip install pyiceberg[sql-sqlite]")
        print("  JVM:    ensure iceberg-spark-runtime is on the classpath")
        print("  Create Iceberg table schema(s) and run your tests")
    return 0


def _run_hybrid(
    project_root: Path,
    matches,
    resolver,
) -> None:
    # Deterministic pre-pass — skip markers + pyspark conf comments.
    prepass_edits = run_prepass(matches, resolver)
    if prepass_edits:
        for f, count in sorted(prepass_edits.items()):
            try:
                rel = f.relative_to(project_root)
            except ValueError:
                rel = f
            print(f"  Pre-pass: {rel} ({count} marker{'s' if count != 1 else ''})")

    entries = build_worklist(matches, project_root, resolver)
    worklist_path = write_worklist(entries, project_root)
    rel_worklist = worklist_path.relative_to(project_root)

    print(f"\nHybrid mode: {len(entries)} rewrite task(s) in {rel_worklist}")
    if entries:
        unresolved = sum(1 for e in entries if e.needs_manual_target)
        if unresolved:
            print(f"  {unresolved} entry(s) need a manual target — fix mapping or rewrite by hand.")


def _run_deterministic(
    project_root: Path,
    matches,
    table_name: str | None,
    namespace: str | None,
    mapping: Mapping | None,
) -> None:
    files = sorted({m.file for m in matches})
    kw = {
        "table_name": table_name,
        "namespace": namespace,
        "mapping": mapping,
    }

    changed_files: list[Path] = []
    unchanged_files: list[Path] = []

    for src_file in files:
        before = src_file.read_text()
        after = before
        suffix = src_file.suffix.lower()

        if suffix == ".py":
            after = transform_pandas_file(after, **kw)
            after = transform_pyspark_file(after, **kw)
            after = transform_pyarrow_file(after, **kw)
        elif suffix == ".java":
            after = transform_jvm_file(after, language="java", **kw)
        elif suffix == ".scala":
            after = transform_jvm_file(after, language="scala", **kw)
        else:
            continue

        rel = src_file.relative_to(project_root)
        if after != before:
            src_file.write_text(after)
            changed_files.append(src_file)
            print(f"  Converted: {rel}")
        else:
            unchanged_files.append(src_file)
            file_hits = [m for m in matches if m.file == src_file]
            print(
                f"  WARNING: {rel} — transformer produced no changes despite "
                f"{len(file_hits)} detector hit(s). Inspect manually."
            )

    print(f"\nConverted {len(changed_files)} file(s).")
    if unchanged_files:
        print(
            f"{len(unchanged_files)} file(s) left unchanged (transformer no-op) — "
            "see warnings above."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert Parquet/ORC usage to Apache Iceberg")
    parser.add_argument("project", type=Path, help="Path to project root")
    parser.add_argument("--table", help="Iceberg table name (single-table / fallback)")
    parser.add_argument("--namespace", default="default", help="Iceberg namespace (single-table / fallback)")
    parser.add_argument("--mapping", type=Path, help="Path to JSON mapping file for multi-table routing")
    parser.add_argument(
        "--mode",
        choices=("hybrid", "deterministic"),
        default="hybrid",
        help="hybrid (default): emit worklist for agent/LLM to rewrite; "
             "deterministic: regex-only rewrites in place",
    )
    parser.add_argument(
        "--no-deps",
        action="store_true",
        help="skip automatic build-file updates (useful when pinning a specific version by hand)",
    )
    args = parser.parse_args()

    mapping = load_mapping(args.mapping) if args.mapping else None
    table = args.table
    ns = args.namespace if table else None

    sys.exit(convert_project(
        args.project,
        table_name=table,
        namespace=ns,
        mapping=mapping,
        mode=args.mode,
        update_deps=not args.no_deps,
    ))


if __name__ == "__main__":
    main()
