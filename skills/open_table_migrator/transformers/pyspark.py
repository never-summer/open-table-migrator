import re

from ..extract import extract_path_arg
from ..targets import Mapping, Target, build_resolver

_ICEBERG_CONF_LINES = [
    "# Iceberg: set spark.sql.extensions and catalog config in SparkSession builder",
    "# .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')",
    "# .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')",
]
_ICEBERG_CONF_MARKER_RE = re.compile(r'spark\.sql\.extensions.*IcebergSparkSessionExtensions')

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
    conf_injected = bool(_ICEBERG_CONF_MARKER_RE.search(source))

    for line in lines:
        stripped = line.rstrip()
        indent = len(line) - len(line.lstrip())
        sp = " " * indent

        if _STREAM_RE.search(stripped):
            direction = "write" if "writeStream" in stripped else "read"
            decision = resolver(extract_path_arg(stripped), direction)
            if decision.skip:
                out.append(f"{sp}# iceberg: skipped by mapping (kept as parquet/orc)\n")
                out.append(line)
                continue
            fqn = decision.migrate_to.fqn if decision.migrate_to else "NS.TABLE"
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

        direction = "read" if is_read else "write"
        decision = resolver(extract_path_arg(stripped), direction)

        if decision.skip:
            out.append(f"{sp}# iceberg: skipped by mapping (kept as parquet/orc)\n")
            out.append(line)
            continue

        target = decision.migrate_to
        if target is None:
            out.append(f"{sp}# TODO(iceberg): could not resolve target table; add a mapping entry.\n")
            out.append(line)
            continue

        if not conf_injected:
            for comment_line in _ICEBERG_CONF_LINES:
                out.append(f"{sp}{comment_line}\n")
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
