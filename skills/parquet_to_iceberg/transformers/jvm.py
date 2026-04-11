import re
from typing import Literal


def transform_jvm_file(
    source: str,
    *,
    language: Literal["java", "scala"],
    table_name: str,
    namespace: str,
) -> str:
    fqn = f"{namespace}.{table_name}"

    # Pattern 1: Hive CREATE TABLE ... STORED AS PARQUET → USING iceberg
    source = re.sub(
        r'("\s*CREATE\s+TABLE[^"]*?)\bSTORED\s+AS\s+PARQUET\b([^"]*")',
        lambda m: m.group(1).rstrip() + " USING iceberg" + m.group(2),
        source,
        flags=re.IGNORECASE,
    )

    # Pattern 2: Java .write().[mods...].saveAsTable(...) → .writeTo("ns.x").createOrReplace()
    source = re.sub(
        r'\.write\(\)(?:\.\w+\([^)]*\))*\.saveAsTable\s*\([^)]*\)',
        f'.writeTo("{fqn}").createOrReplace()',
        source,
    )
    # Scala syntax without parens: .write
    source = re.sub(
        r'\.write(?:\.\w+\([^)]*\))*\.saveAsTable\s*\([^)]*\)',
        f'.writeTo("{fqn}").createOrReplace()',
        source,
    )

    # Pattern 3: Java spark.read().parquet(...) → spark.read().format("iceberg").load("ns.t")
    source = re.sub(
        r'\.read\(\)\.parquet\s*\([^)]*\)',
        f'.read().format("iceberg").load("{fqn}")',
        source,
    )
    # Pattern 3-Scala: spark.read.parquet(...) → spark.read.format("iceberg").load("ns.t")
    source = re.sub(
        r'\.read\.parquet\s*\([^)]*\)',
        f'.read.format("iceberg").load("{fqn}")',
        source,
    )

    # Pattern 4: .write().[mods].parquet(...) → .writeTo("ns.t").overwritePartitions()
    def _replace_write(match: re.Match) -> str:
        chain = match.group(0)
        pb_match = re.search(r'\.partitionBy\(([^)]*)\)', chain)
        replacement = f'.writeTo("{fqn}").overwritePartitions()'
        if pb_match:
            replacement += f" /* TODO: partitionBy({pb_match.group(1)}) — add to Iceberg partition spec */"
        return replacement

    source = re.sub(
        r'\.write\(\)(?:\.\w+\([^)]*\))*\.parquet\s*\([^)]*\)',
        _replace_write,
        source,
    )
    # Scala write chain
    source = re.sub(
        r'\.write(?:\.\w+\([^)]*\))*\.parquet\s*\([^)]*\)',
        _replace_write,
        source,
    )

    return source
