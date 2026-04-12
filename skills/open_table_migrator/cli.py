"""
The CLI runs the AST-based detector, a pre-pass (skip markers + pyspark conf
comment), and emits ``lakehouse-worklist.json`` at the project root for the
agent/LLM to consume.

Single-table usage:
    python -m skills.open_table_migrator.cli <project> --table <name> --namespace <ns>

Multi-table usage (mapping file):
    python -m skills.open_table_migrator.cli <project> --mapping mapping.json

    mapping.json — see skills/open_table_migrator/targets.py for format.
    You can still pass --table/--namespace alongside --mapping as a fallback
    for paths that don't match any glob.

    --no-deps            skip the build-file updater (pyiceberg /
                         iceberg-spark-runtime). Use when the caller wants to
                         pin a specific version by hand.
"""
import argparse
import sys
from pathlib import Path

from .analyzer import cross_reference_sql, dedup_matches, find_ddl_references
from .detector import detect_parquet_usage
from .sql_registry import build_format_map, scan_sql_files
from .deps import update_dependencies
from .prepass import run_prepass
from .targets import Mapping, Target, build_resolver, load_mapping
from .worklist import build_worklist, write_worklist


def convert_project(
    project_root: Path,
    *,
    table_name: str | None = None,
    namespace: str | None = None,
    mapping: Mapping | None = None,
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

    _run_hybrid(project_root, matches, resolver)

    # SQL file registry: cross-reference code ops with SQL-defined parquet/orc tables
    sql_defs = scan_sql_files(project_root)
    fmt_map = build_format_map(sql_defs)
    sites = dedup_matches(matches)
    sql_xrefs = cross_reference_sql(sites, fmt_map, sql_defs)

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

    if sql_xrefs:
        print(f"\nSQL-defined tables with {len(sql_xrefs)} code cross-reference(s):")
        for xr in sql_xrefs:
            try:
                code_rel = xr.site.file.relative_to(project_root)
            except ValueError:
                code_rel = xr.site.file
            try:
                sql_rel = xr.sql_file.relative_to(project_root)
            except ValueError:
                sql_rel = xr.sql_file
            print(f"  {code_rel}:{xr.site.start_line}  {xr.site.direction} '{xr.sql_table}'"
                  f"  — defined as {xr.sql_format} in {sql_rel}:{xr.sql_line}")
        print("  These tables are migration candidates (storage format defined in SQL, not code).")

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
    print("  1. Open lakehouse-worklist.json and rewrite each entry by hand.")
    print("  2. Re-run detector to verify zero residual matches.")
    return 0


def _run_hybrid(
    project_root: Path,
    matches,
    resolver,
) -> None:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert Parquet/ORC usage to Apache Iceberg")
    parser.add_argument("project", type=Path, help="Path to project root")
    parser.add_argument("--table", help="Iceberg table name (single-table / fallback)")
    parser.add_argument("--namespace", default="default", help="Iceberg namespace (single-table / fallback)")
    parser.add_argument("--mapping", type=Path, help="Path to JSON mapping file for multi-table routing")
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
        update_deps=not args.no_deps,
    ))


if __name__ == "__main__":
    main()
