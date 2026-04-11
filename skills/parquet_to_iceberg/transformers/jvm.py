import re
from typing import Literal

from ..extract import extract_path_arg
from ..targets import Mapping, Target, build_resolver

_STREAM_RE = re.compile(
    r"\.(?:readStream|writeStream)(?:\(\))?"
    r"(?:\.\w+\([^)]*\))*"
    r"(?:\.(?:parquet|orc)\s*\(|\.format\s*\(\s*\"(?:parquet|orc)\"\s*\))"
)


def transform_jvm_file(
    source: str,
    *,
    language: Literal["java", "scala"],
    table_name: str | None = None,
    namespace: str | None = None,
    mapping: Mapping | None = None,
) -> str:
    fallback = Target(namespace=namespace, table=table_name) if (namespace and table_name) else None
    resolver = build_resolver(mapping, fallback)

    def target_for_line(line: str) -> Target | None:
        return resolver(extract_path_arg(line))

    # ── 0. Streaming — warn only, leave original code in place ──────────
    new_lines: list[str] = []
    for line in source.splitlines(keepends=True):
        if _STREAM_RE.search(line):
            indent = len(line) - len(line.lstrip())
            sp = " " * indent
            target = target_for_line(line)
            fqn = target.fqn if target else "NS.TABLE"
            new_lines.append(
                f"{sp}// TODO(iceberg): Structured Streaming parquet/orc — "
                f"rewrite using .format(\"iceberg\").option(\"path\", \"{fqn}\") "
                f"or writeStream().toTable(\"{fqn}\").\n"
            )
        new_lines.append(line)
    source = "".join(new_lines)

    # Helper: resolve target for a line of source; returns FQN or None
    def fqn_or_none(line: str) -> str | None:
        t = target_for_line(line)
        return t.fqn if t else None

    # We need per-line replacements because different lines can route to
    # different tables. Walk line by line and apply substitutions.
    output_lines: list[str] = []
    for raw_line in source.splitlines(keepends=True):
        line = raw_line
        fqn = fqn_or_none(line)

        # ── 1. SQL DDL: STORED AS PARQUET/ORC → USING iceberg (no FQN needed)
        line = re.sub(
            r'("\s*CREATE\s+(?:EXTERNAL\s+)?TABLE[^"]*?)\bSTORED\s+AS\s+(?:PARQUET|ORC)\b([^"]*")',
            lambda m: m.group(1).rstrip() + " USING iceberg" + m.group(2),
            line,
            flags=re.IGNORECASE,
        )

        # ── 2. SQL: USING parquet|orc → USING iceberg
        line = re.sub(
            r'(\bUSING\s+)(?:parquet|orc)\b',
            lambda m: m.group(1) + "iceberg",
            line,
            flags=re.IGNORECASE,
        )

        if fqn is None:
            # Could be a non-resolvable read/write OR a pure SQL line (already handled above)
            # Check if this line would have been a read/write — if so, add TODO
            needs_target = re.search(
                r'\.read\(\)\.(?:parquet|orc)\s*\('
                r'|\.read\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'
                r'|\.read\.(?:parquet|orc)\s*\('
                r'|\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'
                r'|\.write\(\)(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\('
                r'|\.write\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'
                r'|\.write(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\('
                r'|\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'
                r'|\.saveAsTable\s*\(',
                line,
            )
            if needs_target:
                indent = len(line) - len(line.lstrip())
                sp = " " * indent
                output_lines.append(f"{sp}// TODO(iceberg): could not resolve target table; add a mapping entry.\n")
            output_lines.append(line)
            continue

        # ── 3. saveAsTable → writeTo(fqn).createOrReplace()
        line = re.sub(
            r'\.write\(\)(?:\.\w+\([^)]*\))*\.saveAsTable\s*\([^)]*\)',
            f'.writeTo("{fqn}").createOrReplace()',
            line,
        )
        line = re.sub(
            r'\.write(?:\.\w+\([^)]*\))*\.saveAsTable\s*\([^)]*\)',
            f'.writeTo("{fqn}").createOrReplace()',
            line,
        )

        # ── 4. Reads ───────────────────────────────────────────────────
        line = re.sub(
            r'\.read\(\)\.(?:parquet|orc)\s*\([^)]*\)',
            f'.read().format("iceberg").load("{fqn}")',
            line,
        )
        line = re.sub(
            r'\.read\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)(?:\.\w+\([^)]*\))*\.load\s*\([^)]*\)',
            f'.read().format("iceberg").load("{fqn}")',
            line,
        )
        line = re.sub(
            r'\.read\.(?:parquet|orc)\s*\([^)]*\)',
            f'.read.format("iceberg").load("{fqn}")',
            line,
        )
        line = re.sub(
            r'\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)(?:\.\w+\([^)]*\))*\.load\s*\([^)]*\)',
            f'.read.format("iceberg").load("{fqn}")',
            line,
        )

        # ── 5. Writes ──────────────────────────────────────────────────
        def _replace_write(match: re.Match) -> str:
            chain = match.group(0)
            pb_match = re.search(r'\.partitionBy\(([^)]*)\)', chain)
            replacement = f'.writeTo("{fqn}").overwritePartitions()'
            if pb_match:
                replacement += f" /* TODO: partitionBy({pb_match.group(1)}) — add to Iceberg partition spec */"
            return replacement

        line = re.sub(
            r'\.write\(\)(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\([^)]*\)',
            _replace_write,
            line,
        )
        line = re.sub(
            r'\.write\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)(?:\.\w+\([^)]*\))*\.save\s*\([^)]*\)',
            _replace_write,
            line,
        )
        line = re.sub(
            r'\.write(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\([^)]*\)',
            _replace_write,
            line,
        )
        line = re.sub(
            r'\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)(?:\.\w+\([^)]*\))*\.save\s*\([^)]*\)',
            _replace_write,
            line,
        )

        output_lines.append(line)

    return "".join(output_lines)
