import re
from typing import Literal


_STREAM_RE = re.compile(
    r"\.(?:readStream|writeStream)(?:\(\))?"
    r"(?:\.\w+\([^)]*\))*"
    r"(?:\.(?:parquet|orc)\s*\(|\.format\s*\(\s*\"(?:parquet|orc)\"\s*\))"
)


def transform_jvm_file(
    source: str,
    *,
    language: Literal["java", "scala"],
    table_name: str,
    namespace: str,
) -> str:
    fqn = f"{namespace}.{table_name}"

    # ── 0. Streaming — warn only, leave original code in place ──────────
    new_lines: list[str] = []
    for line in source.splitlines(keepends=True):
        if _STREAM_RE.search(line):
            indent = len(line) - len(line.lstrip())
            sp = " " * indent
            comment = "//" if language in ("java", "scala") else "#"
            new_lines.append(
                f"{sp}{comment} TODO(iceberg): Structured Streaming parquet/orc — "
                f"rewrite using .format(\"iceberg\").option(\"path\", \"{fqn}\") "
                f"or writeStream().toTable(\"{fqn}\").\n"
            )
        new_lines.append(line)
    source = "".join(new_lines)

    # ── 1. SQL DDL: STORED AS PARQUET/ORC → USING iceberg ───────────────
    source = re.sub(
        r'("\s*CREATE\s+(?:EXTERNAL\s+)?TABLE[^"]*?)\bSTORED\s+AS\s+(?:PARQUET|ORC)\b([^"]*")',
        lambda m: m.group(1).rstrip() + " USING iceberg" + m.group(2),
        source,
        flags=re.IGNORECASE,
    )

    # ── 2. SQL DDL: USING parquet/orc → USING iceberg ───────────────────
    source = re.sub(
        r'(\bUSING\s+)(?:parquet|orc)\b',
        lambda m: m.group(1) + "iceberg",
        source,
        flags=re.IGNORECASE,
    )

    # ── 3. Java .write().[mods].saveAsTable(...) → .writeTo("ns.t").createOrReplace()
    source = re.sub(
        r'\.write\(\)(?:\.\w+\([^)]*\))*\.saveAsTable\s*\([^)]*\)',
        f'.writeTo("{fqn}").createOrReplace()',
        source,
    )
    # Scala .write.[mods].saveAsTable(...)
    source = re.sub(
        r'\.write(?:\.\w+\([^)]*\))*\.saveAsTable\s*\([^)]*\)',
        f'.writeTo("{fqn}").createOrReplace()',
        source,
    )

    # ── 4. Java/Scala reads ─────────────────────────────────────────────
    # Java: .read().parquet(...) / .read().orc(...) → format("iceberg").load("ns.t")
    source = re.sub(
        r'\.read\(\)\.(?:parquet|orc)\s*\([^)]*\)',
        f'.read().format("iceberg").load("{fqn}")',
        source,
    )
    # Java: .read().[mods].format("parquet"|"orc").load(...) → .read().format("iceberg").load("ns.t")
    source = re.sub(
        r'\.read\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)(?:\.\w+\([^)]*\))*\.load\s*\([^)]*\)',
        f'.read().format("iceberg").load("{fqn}")',
        source,
    )
    # Scala: .read.parquet(...) / .read.orc(...)
    source = re.sub(
        r'\.read\.(?:parquet|orc)\s*\([^)]*\)',
        f'.read.format("iceberg").load("{fqn}")',
        source,
    )
    # Scala: .read.[mods].format("parquet"|"orc").load(...)
    source = re.sub(
        r'\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)(?:\.\w+\([^)]*\))*\.load\s*\([^)]*\)',
        f'.read.format("iceberg").load("{fqn}")',
        source,
    )

    # ── 5. Java/Scala writes ────────────────────────────────────────────
    def _replace_write(match: re.Match) -> str:
        chain = match.group(0)
        pb_match = re.search(r'\.partitionBy\(([^)]*)\)', chain)
        replacement = f'.writeTo("{fqn}").overwritePartitions()'
        if pb_match:
            replacement += f" /* TODO: partitionBy({pb_match.group(1)}) — add to Iceberg partition spec */"
        return replacement

    # Java: .write().[mods].parquet/orc(...)
    source = re.sub(
        r'\.write\(\)(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\([^)]*\)',
        _replace_write,
        source,
    )
    # Java: .write().[mods].format("parquet"|"orc")...save(...)
    source = re.sub(
        r'\.write\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)(?:\.\w+\([^)]*\))*\.save\s*\([^)]*\)',
        _replace_write,
        source,
    )
    # Scala: .write.[mods].parquet/orc(...)
    source = re.sub(
        r'\.write(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\([^)]*\)',
        _replace_write,
        source,
    )
    # Scala: .write.[mods].format("parquet"|"orc")...save(...)
    source = re.sub(
        r'\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)(?:\.\w+\([^)]*\))*\.save\s*\([^)]*\)',
        _replace_write,
        source,
    )

    return source
