"""
Usage:
    python -m skills.parquet_to_iceberg.cli <project_path> --table <name> --namespace <ns>
"""
import argparse
import sys
from pathlib import Path

from .detector import detect_parquet_usage
from .deps import update_dependencies
from .transformers.pandas import transform_pandas_file
from .transformers.pyspark import transform_pyspark_file
from .transformers.pyarrow import transform_pyarrow_file
from .transformers.jvm import transform_jvm_file

_PANDAS_TYPES = {"pandas_read", "pandas_write"}
_PYSPARK_TYPES = {"pyspark_read", "pyspark_write"}
_PYARROW_TYPES = {"pyarrow_read", "pyarrow_write"}
_JVM_TYPES = {
    "java_spark_read", "java_spark_write",
    "scala_spark_read", "scala_spark_write",
    "hive_create_parquet", "hive_save_as_table", "hive_insert_overwrite",
}


def convert_project(project_root: Path, *, table_name: str, namespace: str) -> int:
    matches = detect_parquet_usage(project_root)
    if not matches:
        print("No Parquet usage found.")
        return 0

    files_by_type: dict[Path, set[str]] = {}
    for m in matches:
        files_by_type.setdefault(m.file, set()).add(m.pattern_type)

    converted = 0
    for src_file, types in files_by_type.items():
        source = src_file.read_text()
        kw = {"table_name": table_name, "namespace": namespace}
        suffix = src_file.suffix.lower()

        if suffix == ".py":
            if types & _PANDAS_TYPES:
                source = transform_pandas_file(source, **kw)
            if types & _PYSPARK_TYPES:
                source = transform_pyspark_file(source, **kw)
            if types & _PYARROW_TYPES:
                source = transform_pyarrow_file(source, **kw)
        elif suffix == ".java":
            if types & _JVM_TYPES:
                source = transform_jvm_file(source, language="java", **kw)
        elif suffix == ".scala":
            if types & _JVM_TYPES:
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
    print("  Create Iceberg table schema and run your tests")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert Parquet usage to Apache Iceberg")
    parser.add_argument("project", type=Path, help="Path to project root")
    parser.add_argument("--table", required=True, help="Iceberg table name")
    parser.add_argument("--namespace", default="default", help="Iceberg namespace")
    args = parser.parse_args()
    sys.exit(convert_project(args.project, table_name=args.table, namespace=args.namespace))


if __name__ == "__main__":
    main()
