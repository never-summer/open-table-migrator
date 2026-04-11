import re

from ..extract import extract_path_arg
from ..targets import Mapping, Target, build_resolver

_ICEBERG_CONF_COMMENT = (
    "# Iceberg: set spark.sql.extensions and catalog config in SparkSession builder\n"
    "# .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\n"
    "# .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\n"
)

_BATCH_READ_RE = re.compile(
    r"\.read"
    r"(?:"
    r"\.(?:parquet|orc)\s*\("
    r"|"
    r"(?:\.\w+\([^)]*\))*\.format\s*\(\s*['\"](?:parquet|orc)['\"]\s*\)"
    r")"
)
_BATCH_WRITE_RE = re.compile(
    r"\.write"
    r"(?:\.\w+\([^)]*\))*"
    r"(?:"
    r"\.(?:parquet|orc)\s*\("
    r"|"
    r"\.format\s*\(\s*['\"](?:parquet|orc)['\"]\s*\)"
    r")"
)
_STREAM_RE = re.compile(
    r"\.(?:readStream|writeStream)"
    r"(?:\.\w+\([^)]*\))*"
    r"(?:\.(?:parquet|orc)\s*\(|\.format\s*\(\s*['\"](?:parquet|orc)['\"]\s*\))"
)


def transform_pyspark_file(
    source: str,
    *,
    table_name: str | None = None,
    namespace: str | None = None,
    mapping: Mapping | None = None,
) -> str:
    fallback = Target(namespace=namespace, table=table_name) if (namespace and table_name) else None
    resolver = build_resolver(mapping, fallback)

    lines = source.splitlines(keepends=True)
    out: list[str] = []
    conf_injected = False

    for line in lines:
        stripped = line.rstrip()
        indent = len(line) - len(line.lstrip())
        sp = " " * indent

        if _STREAM_RE.search(stripped):
            target = resolver(extract_path_arg(stripped))
            fqn = target.fqn if target else "NS.TABLE"
            out.append(
                f"{sp}# TODO(iceberg): Structured Streaming parquet/orc sink — "
                f"rewrite manually using .format('iceberg').option('path', '{fqn}') "
                f"or a writeStream.toTable('{fqn}') sink.\n"
            )
            out.append(line)
            continue

        is_read = bool(_BATCH_READ_RE.search(stripped))
        is_write = bool(_BATCH_WRITE_RE.search(stripped))

        if not (is_read or is_write):
            out.append(line)
            continue

        target = resolver(extract_path_arg(stripped))
        if target is None:
            out.append(f"{sp}# TODO(iceberg): could not resolve target table; add a mapping entry.\n")
            out.append(line)
            continue

        if not conf_injected:
            out.append(sp + _ICEBERG_CONF_COMMENT)
            conf_injected = True

        if is_read:
            lhs_match = re.match(r"(\s*\w+\s*=\s*)", stripped)
            lhs = lhs_match.group(1).lstrip() if lhs_match else ""
            out.append(f'{sp}{lhs}spark.table("{target.fqn}")\n')
        else:
            obj_match = re.match(r"\s*(\w+)\.write", stripped)
            obj = obj_match.group(1) if obj_match else "df"
            out.append(f'{sp}{obj}.writeTo("{target.fqn}").overwritePartitions()\n')

    return "".join(out)
