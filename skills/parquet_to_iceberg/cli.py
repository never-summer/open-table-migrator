"""
Single-table usage:
    python -m skills.parquet_to_iceberg.cli <project> --table <name> --namespace <ns>

Multi-table usage (mapping file):
    python -m skills.parquet_to_iceberg.cli <project> --mapping mapping.json

    mapping.json — see skills/parquet_to_iceberg/targets.py for format.
    You can still pass --table/--namespace alongside --mapping as a fallback
    for paths that don't match any glob.
"""
import argparse
import sys
from pathlib import Path

from .analyzer import find_ddl_references
from .detector import detect_parquet_usage
from .deps import update_dependencies
from .targets import Mapping, load_mapping
from .transformers.pandas import transform_pandas_file
from .transformers.pyspark import transform_pyspark_file
from .transformers.pyarrow import transform_pyarrow_file
from .transformers.jvm import transform_jvm_file


def convert_project(
    project_root: Path,
    *,
    table_name: str | None = None,
    namespace: str | None = None,
    mapping: Mapping | None = None,
) -> int:
    matches = detect_parquet_usage(project_root)
    if not matches:
        print("No Parquet/ORC usage found.")
        return 0

    if mapping is None and not (table_name and namespace):
        print("ERROR: provide --table/--namespace, or --mapping, or both.", file=sys.stderr)
        return 2

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
            print(f"  WARNING: {rel} — transformer produced no changes despite {len(file_hits)} detector hit(s). Inspect manually.")

    updated_deps = update_dependencies(project_root)

    ddl_refs = find_ddl_references(matches, project_root)

    print(f"\nConverted {len(changed_files)} file(s).")
    if unchanged_files:
        print(f"{len(unchanged_files)} file(s) left unchanged (transformer no-op) — see warnings above.")
    if updated_deps:
        rels = [str(p.relative_to(project_root)) for p in updated_deps]
        print(f"Updated build file(s): {', '.join(rels)}")
    else:
        print("No build files updated (none found, or all already contain Iceberg).")

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
    print("  Python: pip install pyiceberg[sql-sqlite]")
    print("  JVM:    ensure iceberg-spark-runtime is on the classpath")
    print("  Create Iceberg table schema(s) and run your tests")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert Parquet/ORC usage to Apache Iceberg")
    parser.add_argument("project", type=Path, help="Path to project root")
    parser.add_argument("--table", help="Iceberg table name (single-table / fallback)")
    parser.add_argument("--namespace", default="default", help="Iceberg namespace (single-table / fallback)")
    parser.add_argument("--mapping", type=Path, help="Path to JSON mapping file for multi-table routing")
    args = parser.parse_args()

    mapping = load_mapping(args.mapping) if args.mapping else None
    table = args.table
    ns = args.namespace if table else None

    sys.exit(convert_project(
        args.project,
        table_name=table,
        namespace=ns,
        mapping=mapping,
    ))


if __name__ == "__main__":
    main()
