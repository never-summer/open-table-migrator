import re

from ..extract import extract_path_arg
from ..targets import Decision, Mapping, Target, build_resolver

_PYICEBERG_IMPORT = "from pyiceberg.catalog import load_catalog"
_PYICEBERG_IMPORT_RE = re.compile(r'from\s+pyiceberg\.catalog\s+import\s+load_catalog')
_READ_RE = re.compile(r"pd\.read_(?:parquet|orc)\s*\(")
_WRITE_RE = re.compile(r"\.to_(?:parquet|orc)\s*\(")


def _sanitize(fqn: str) -> str:
    return re.sub(r"\W+", "_", fqn)


def _var_for(target: Target, single: bool) -> str:
    return "tbl" if single else f"tbl_{_sanitize(target.fqn)}"


def transform_pandas_file(
    source: str,
    *,
    table_name: str | None = None,
    namespace: str | None = None,
    mapping: Mapping | None = None,
) -> str:
    fallback = Target(namespace=namespace, table=table_name) if (namespace and table_name) else None
    resolver = build_resolver(mapping, fallback)

    already_has_import = bool(_PYICEBERG_IMPORT_RE.search(source))
    lines = source.splitlines(keepends=True)

    # ── Pre-scan: decide per-line target and gather unique targets ──────
    line_decisions: list[Decision | None] = []
    unique_fqns: dict[str, Target] = {}
    for line in lines:
        s = line.rstrip()
        direction = "read" if _READ_RE.search(s) else ("write" if _WRITE_RE.search(s) else None)
        if direction is None:
            line_decisions.append(None)
            continue
        d = resolver(extract_path_arg(s), direction)
        line_decisions.append(d)
        if d.migrate_to is not None:
            unique_fqns.setdefault(d.migrate_to.fqn, d.migrate_to)

    single = len(unique_fqns) <= 1

    # ── Locate end of import block for header injection ─────────────────
    import_end = -1
    for i, line in enumerate(lines):
        if re.match(r"^(?:import |from )\S", line):
            import_end = i

    out: list[str] = []
    header_injected = False

    def emit_header(indent: str = "") -> list[str]:
        if not unique_fqns:
            return []
        block = [f'{indent}catalog = load_catalog("default", **{{"type": "sql", "uri": "sqlite:///iceberg.db"}})\n']
        for fqn, target in unique_fqns.items():
            var = _var_for(target, single)
            block.append(f'{indent}{var} = catalog.load_table(("{target.namespace}", "{target.table}"))\n')
        return block

    for i, line in enumerate(lines):
        s = line.rstrip()

        if i == import_end:
            out.append(line)
            if not header_injected and unique_fqns and not already_has_import:
                out.append(_PYICEBERG_IMPORT + "\n")
                out.extend(emit_header())
                header_injected = True
            elif already_has_import:
                header_injected = True  # assume previous run already set up catalog + tbl vars
            continue

        decision = line_decisions[i]
        if decision is None:
            out.append(line)
            continue

        indent = len(line) - len(line.lstrip())
        sp = " " * indent

        if decision.skip:
            out.append(f"{sp}# iceberg: skipped by mapping (kept as parquet/orc)\n")
            out.append(line)
            continue

        if decision.migrate_to is None:
            out.append(f"{sp}# TODO(iceberg): could not resolve target table for this call; add a mapping entry.\n")
            out.append(line)
            continue

        target = decision.migrate_to
        is_read = bool(_READ_RE.search(s))

        if not header_injected:
            out.extend(emit_header(sp))
            header_injected = True

        var = _var_for(target, single)

        if is_read:
            assign_match = re.match(r"(\s*\w+\s*=\s*)pd\.read_(?:parquet|orc)\s*\(.*\)", s)
            lhs = assign_match.group(1).lstrip() if assign_match else ""
            out.append(f"{sp}{lhs}{var}.scan().to_pandas()\n")
        else:
            obj_match = re.match(r"\s*(\w+)\.to_(?:parquet|orc)\s*\(.*\)", s)
            obj = obj_match.group(1) if obj_match else "df"
            out.append(f"{sp}{var}.overwrite({obj})\n")

    return "".join(out)
