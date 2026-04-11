import re

_ICEBERG_CONF_COMMENT = (
    "# Iceberg: set spark.sql.extensions and catalog config in SparkSession builder\n"
    "# .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\n"
    "# .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\n"
)


def transform_pyspark_file(source: str, *, table_name: str, namespace: str) -> str:
    lines = source.splitlines(keepends=True)
    out: list[str] = []
    conf_injected = False

    for line in lines:
        stripped = line.rstrip()

        # spark.read.parquet(path) → spark.table("namespace.table_name")
        if re.search(r"\.read\.parquet\s*\(", stripped):
            indent = len(line) - len(line.lstrip())
            sp = " " * indent
            if not conf_injected:
                out.append(sp + _ICEBERG_CONF_COMMENT)
                conf_injected = True
            var_match = re.match(r"(\s*\w+\s*=\s*).*\.read\.parquet\s*\(.*\)", stripped)
            var = var_match.group(1).lstrip() if var_match else ""
            out.append(f'{sp}{var}spark.table("{namespace}.{table_name}")\n')
            continue

        # .write.mode(...).parquet(path) → .writeTo("namespace.table").overwritePartitions()
        if re.search(r"\.write(?:\.\w+\([^)]*\))*\.parquet\s*\(", stripped):
            indent = len(line) - len(line.lstrip())
            sp = " " * indent
            if not conf_injected:
                out.append(sp + _ICEBERG_CONF_COMMENT)
                conf_injected = True
            obj_match = re.match(r"\s*(\w+)\.write", stripped)
            obj = obj_match.group(1) if obj_match else "df"
            out.append(f'{sp}{obj}.writeTo("{namespace}.{table_name}").overwritePartitions()\n')
            continue

        out.append(line)

    return "".join(out)
