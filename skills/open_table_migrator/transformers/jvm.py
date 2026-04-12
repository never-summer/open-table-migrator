import re
from typing import Literal

from ..extract import extract_path_arg
from ..folding import LogicalLine, fold_chains
from ..targets import Decision, Mapping, Target, build_resolver

_STREAM_RE = re.compile(
    r"\.(?:readStream|writeStream)(?:\(\))?"
    r"(?:\.\w+\([^)]*\))*"
    r"(?:\.(?:parquet|orc)\s*\(|\.format\s*\(\s*\"(?:parquet|orc)\"\s*\))"
)

_READ_OP_RE = re.compile(
    r'\.read\(\)\.(?:parquet|orc)\s*\('
    r'|\.read\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'
    r'|\.read\.(?:parquet|orc)\s*\('
    r'|\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'
)
_WRITE_OP_RE = re.compile(
    r'\.write\(\)(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\('
    r'|\.write\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'
    r'|\.write(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\('
    r'|\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'
    r'|\.write\(\)(?:\.\w+\([^)]*\))*\.saveAsTable\s*\('
    r'|\.write(?:\.\w+\([^)]*\))*\.saveAsTable\s*\('
)
# A naked .write.format("parquet"|"orc") fragment with no terminator on the
# logical line (chain got split awkwardly and didn't fold). We emit a TODO
# for these so silent failure becomes visible.
_WRITE_FORMAT_NAKED_RE = re.compile(
    r'\.write(?:\(\))?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'
)


def _emit_todo(indent: str, comment: str, block: LogicalLine) -> list[str]:
    out = [f"{indent}// TODO(iceberg): {comment}\n"]
    out.extend(block.physical_lines)
    return out


def _emit_skip(indent: str, block: LogicalLine) -> list[str]:
    out = [f"{indent}// iceberg: skipped by mapping (kept as parquet/orc)\n"]
    out.extend(block.physical_lines)
    return out


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

    def decide(text: str, direction: str) -> Decision:
        return resolver(extract_path_arg(text), direction)

    logicals = fold_chains(source)
    out: list[str] = []

    for block in logicals:
        text = block.folded_text
        indent = block.indent

        # ── 0. Streaming — warn only, leave originals ───────────────────
        if _STREAM_RE.search(text):
            direction = "write" if "writeStream" in text else "read"
            decision = decide(text, direction)
            if decision.skip:
                out.extend(_emit_skip(indent, block))
                continue
            fqn = decision.migrate_to.fqn if decision.migrate_to else "NS.TABLE"
            out.append(
                f"{indent}// TODO(iceberg): Structured Streaming parquet/orc — "
                f"rewrite using .format(\"iceberg\").option(\"path\", \"{fqn}\") "
                f"or writeStream().toTable(\"{fqn}\").\n"
            )
            out.extend(block.physical_lines)
            continue

        # ── 1. SQL DDL rewrites on folded text. If the text only contains
        #      SQL (no read/write op), we still need to emit it — but since
        #      we prefer to preserve formatting, apply SQL rewrites on each
        #      physical line separately and only collapse when a real
        #      read/write op triggers a rewrite.
        is_read_op = bool(_READ_OP_RE.search(text))
        is_write_op = bool(_WRITE_OP_RE.search(text))

        if not (is_read_op or is_write_op):
            # Still apply SQL DDL rewrites line by line (preserves formatting)
            rewritten_lines: list[str] = []
            for raw in block.physical_lines:
                line = raw
                line = re.sub(
                    r'("\s*CREATE\s+(?:EXTERNAL\s+)?TABLE[^"]*?)\bSTORED\s+AS\s+(?:PARQUET|ORC)\b([^"]*")',
                    lambda m: m.group(1).rstrip() + " USING iceberg" + m.group(2),
                    line,
                    flags=re.IGNORECASE,
                )
                line = re.sub(
                    r'(\bUSING\s+)(?:parquet|orc)\b',
                    lambda m: m.group(1) + "iceberg",
                    line,
                    flags=re.IGNORECASE,
                )
                rewritten_lines.append(line)
            # Detect orphan .write.format("parquet") with no terminator in this block
            if _WRITE_FORMAT_NAKED_RE.search(text):
                out.append(
                    f"{indent}// TODO(iceberg): write chain references parquet/orc but "
                    f"no terminator (.save / .saveAsTable / .parquet) found in the same "
                    f"statement — rewrite manually.\n"
                )
            out.extend(rewritten_lines)
            continue

        # Now we know this block contains a read or write op.
        direction = "read" if is_read_op else "write"
        decision = decide(text, direction)

        if decision.skip:
            out.extend(_emit_skip(indent, block))
            continue

        target = decision.migrate_to
        if target is None:
            out.extend(_emit_todo(indent, "could not resolve target table; add a mapping entry.", block))
            continue

        fqn = target.fqn
        rewritten = _rewrite_text(text, fqn)

        if rewritten == text:
            # Regex matched but substitution was a no-op — shouldn't happen,
            # but be honest about it.
            out.extend(_emit_todo(indent, "matched a read/write op but rewrite produced no change.", block))
            continue

        out.append(indent + rewritten.lstrip() + "\n")

    return "".join(out)


def _chain_annotations(chain: str) -> str:
    extras = ""
    pb = re.search(r'\.partitionBy\(([^)]*)\)', chain)
    bb = re.search(r'\.bucketBy\(([^)]*)\)', chain)
    sb = re.search(r'\.sortBy\(([^)]*)\)', chain)
    if pb:
        extras += f" /* TODO: partitionBy({pb.group(1)}) — add to Iceberg partition spec */"
    if bb:
        extras += f" /* TODO: bucketBy({bb.group(1)}) — Iceberg uses hidden partitioning, not bucketing */"
    if sb:
        extras += f" /* TODO: sortBy({sb.group(1)}) — add to Iceberg sort order */"
    return extras


def _rewrite_text(text: str, fqn: str) -> str:
    line = text

    # saveAsTable → writeTo(fqn).createOrReplace() (preserve bucketBy/partitionBy TODOs)
    def _replace_save_as_table(match: re.Match) -> str:
        return f'.writeTo("{fqn}").createOrReplace()' + _chain_annotations(match.group(0))

    line = re.sub(
        r'\.write\(\)(?:\.\w+\([^)]*\))*\.saveAsTable\s*\([^)]*\)',
        _replace_save_as_table,
        line,
    )
    line = re.sub(
        r'\.write(?:\.\w+\([^)]*\))*\.saveAsTable\s*\([^)]*\)',
        _replace_save_as_table,
        line,
    )

    # Reads
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

    # Writes
    def _replace_write(match: re.Match) -> str:
        return f'.writeTo("{fqn}").overwritePartitions()' + _chain_annotations(match.group(0))

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

    return line
