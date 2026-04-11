import re

from ..extract import extract_path_arg
from ..targets import Mapping, Target, build_resolver

_PYICEBERG_IMPORT = "from pyiceberg.catalog import load_catalog"

_READ_RE = re.compile(r"(?:pq|orc|po)\.read_table\s*\(")
_WRITE_RE = re.compile(r"(?:pq|orc|po)\.write_table\s*\(")
_DATASET_RE = re.compile(
    r"pq\.ParquetFile\s*\("
    r"|pq\.ParquetDataset\s*\("
    r"|(?:pa|pyarrow)\.dataset\.dataset\s*\("
    r"|(?:pa|pyarrow)\.dataset\.write_dataset\s*\("
)


def _sanitize(fqn: str) -> str:
    return re.sub(r"\W+", "_", fqn)


def _var_for(target: Target, single: bool) -> str:
    return "tbl" if single else f"tbl_{_sanitize(target.fqn)}"


def transform_pyarrow_file(
    source: str,
    *,
    table_name: str | None = None,
    namespace: str | None = None,
    mapping: Mapping | None = None,
) -> str:
    fallback = Target(namespace=namespace, table=table_name) if (namespace and table_name) else None
    resolver = build_resolver(mapping, fallback)

    lines = source.splitlines(keepends=True)

    # Pre-scan unique targets
    unique_fqns: dict[str, Target] = {}
    line_targets: list[Target | None] = []
    for line in lines:
        s = line.rstrip()
        if _READ_RE.search(s) or _WRITE_RE.search(s):
            t = resolver(extract_path_arg(s))
            line_targets.append(t)
            if t:
                unique_fqns.setdefault(t.fqn, t)
        else:
            line_targets.append(None)

    single = len(unique_fqns) <= 1

    last_import_idx = -1
    for i, line in enumerate(lines):
        if re.match(r"^(?:import |from )\S", line):
            last_import_idx = i

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
        indent = len(line) - len(line.lstrip())
        sp = " " * indent

        if i == last_import_idx:
            out.append(line)
            if not header_injected:
                out.append(_PYICEBERG_IMPORT + "\n")
                out.extend(emit_header())
                header_injected = True
            continue

        # Dataset / ParquetFile / ParquetDataset — warn only, keep original
        if _DATASET_RE.search(s):
            target = resolver(extract_path_arg(s))
            fqn = target.fqn if target else "NS.TABLE"
            out.append(
                f"{sp}# TODO(iceberg): pyarrow dataset/ParquetFile API — "
                f"rewrite to catalog.load_table(({target.namespace!r}, {target.table!r})).scan().to_arrow() "
                f"(target: {fqn}).\n" if target else
                f"{sp}# TODO(iceberg): pyarrow dataset/ParquetFile API — "
                f"rewrite to catalog.load_table((ns, table)).scan().to_arrow() or tbl.overwrite(table); no mapping match.\n"
            )
            out.append(line)
            continue

        target = line_targets[i]
        is_read = bool(_READ_RE.search(s))
        is_write = bool(_WRITE_RE.search(s))

        if not (is_read or is_write):
            out.append(line)
            continue

        if target is None:
            out.append(f"{sp}# TODO(iceberg): could not resolve target table; add a mapping entry.\n")
            out.append(line)
            continue

        if not header_injected:
            out.extend(emit_header(sp))
            header_injected = True

        var = _var_for(target, single)

        if is_read:
            lhs_match = re.match(r"(\s*\w+\s*=\s*)(?:pq|orc|po)\.read_table\s*\(.*\)", s)
            lhs = lhs_match.group(1).lstrip() if lhs_match else ""
            out.append(f"{sp}{lhs}{var}.scan().to_arrow()\n")
        else:
            obj_match = re.search(r"(?:pq|orc|po)\.write_table\s*\(\s*(\w+)\s*,", s)
            obj = obj_match.group(1) if obj_match else "table"
            out.append(f"{sp}{var}.overwrite({obj})\n")

    return "".join(out)
