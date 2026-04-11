import re

_ICEBERG_CONF_COMMENT = (
    "# Iceberg: set spark.sql.extensions and catalog config in SparkSession builder\n"
    "# .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\n"
    "# .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\n"
)

# Batch reads: .read.parquet(...) / .read.orc(...) / .read...format("parquet"|"orc").load(...)
_BATCH_READ_RE = re.compile(
    r"\.read"
    r"(?:"
    r"\.(?:parquet|orc)\s*\("
    r"|"
    r"(?:\.\w+\([^)]*\))*\.format\s*\(\s*['\"](?:parquet|orc)['\"]\s*\)"
    r")"
)

# Batch writes: .write...parquet(...) / .write...orc(...) / .write...format("parquet"|"orc")....
_BATCH_WRITE_RE = re.compile(
    r"\.write"
    r"(?:\.\w+\([^)]*\))*"
    r"(?:"
    r"\.(?:parquet|orc)\s*\("
    r"|"
    r"\.format\s*\(\s*['\"](?:parquet|orc)['\"]\s*\)"
    r")"
)

# Streaming — detect anything with readStream/writeStream touching parquet/orc
_STREAM_RE = re.compile(
    r"\.(?:readStream|writeStream)"
    r"(?:\.\w+\([^)]*\))*"
    r"(?:\.(?:parquet|orc)\s*\(|\.format\s*\(\s*['\"](?:parquet|orc)['\"]\s*\))"
)


def transform_pyspark_file(source: str, *, table_name: str, namespace: str) -> str:
    lines = source.splitlines(keepends=True)
    out: list[str] = []
    conf_injected = False
    fqn = f"{namespace}.{table_name}"

    for line in lines:
        stripped = line.rstrip()
        indent = len(line) - len(line.lstrip())
        sp = " " * indent

        # Streaming — warn only, don't rewrite
        if _STREAM_RE.search(stripped):
            out.append(
                f"{sp}# TODO(iceberg): Structured Streaming parquet/orc sink — "
                f"rewrite manually using .format('iceberg').option('path', '{fqn}') "
                f"or a writeStream.toTable('{fqn}') sink.\n"
            )
            out.append(line)
            continue

        # Batch read: spark.read.parquet/orc/format(...) → spark.table("ns.t")
        if _BATCH_READ_RE.search(stripped):
            if not conf_injected:
                out.append(sp + _ICEBERG_CONF_COMMENT)
                conf_injected = True
            var_match = re.match(r"(\s*\w+\s*=\s*)", stripped)
            var = var_match.group(1).lstrip() if var_match else ""
            out.append(f'{sp}{var}spark.table("{fqn}")\n')
            continue

        # Batch write: df.write...parquet/orc/format(...) → df.writeTo("ns.t").overwritePartitions()
        if _BATCH_WRITE_RE.search(stripped):
            if not conf_injected:
                out.append(sp + _ICEBERG_CONF_COMMENT)
                conf_injected = True
            obj_match = re.match(r"\s*(\w+)\.write", stripped)
            obj = obj_match.group(1) if obj_match else "df"
            out.append(f'{sp}{obj}.writeTo("{fqn}").overwritePartitions()\n')
            continue

        out.append(line)

    return "".join(out)
