import re

from ..extract import extract_path_arg
from ..targets import Mapping, Target, build_resolver

_PYICEBERG_IMPORT = "from pyiceberg.catalog import load_catalog"
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

    lines = source.splitlines(keepends=True)

    # ── Pre-scan: decide per-line target and gather unique targets ──────
    line_targets: list[Target | None] = []
    unresolved_lines: set[int] = set()
    unique_fqns: dict[str, Target] = {}
    for idx, line in enumerate(lines):
        s = line.rstrip()
        if _READ_RE.search(s) or _WRITE_RE.search(s):
            t = resolver(extract_path_arg(s))
            line_targets.append(t)
            if t is None:
                unresolved_lines.add(idx)
            else:
                unique_fqns.setdefault(t.fqn, t)
        else:
            line_targets.append(None)

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
            if not header_injected:
                out.append(_PYICEBERG_IMPORT + "\n")
                out.extend(emit_header())
                header_injected = True
            continue

        # Replace a read/write line if it matched pre-scan
        target = line_targets[i]
        is_read = bool(_READ_RE.search(s))
        is_write = bool(_WRITE_RE.search(s))

        if not (is_read or is_write):
            out.append(line)
            continue

        indent = len(line) - len(line.lstrip())
        sp = " " * indent

        if target is None:
            out.append(f"{sp}# TODO(iceberg): could not resolve target table for this call; add a mapping entry.\n")
            out.append(line)
            continue

        # If header wasn't injected yet (no imports), inject it now at this indent
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
