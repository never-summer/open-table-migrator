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

    converted = 0
    for src_file in files:
        source = src_file.read_text()
        suffix = src_file.suffix.lower()

        if suffix == ".py":
            source = transform_pandas_file(source, **kw)
            source = transform_pyspark_file(source, **kw)
            source = transform_pyarrow_file(source, **kw)
        elif suffix == ".java":
            source = transform_jvm_file(source, language="java", **kw)
        elif suffix == ".scala":
            source = transform_jvm_file(source, language="scala", **kw)
        else:
            continue

        src_file.write_text(source)
        print(f"  Converted: {src_file.relative_to(project_root)}")
        converted += 1

    update_dependencies(project_root)
    print(f"\nConverted {converted} file(s). Updated dependencies.")
    print("Next steps:")
    print("  Python: pip install pyiceberg[sql-sqlite]")
    print("  JVM:    ensure iceberg-spark-runtime is on the classpath (added to pom.xml/build.gradle)")
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
    # Only pass namespace/table fallback when the user actually supplied --table
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
