import re

from ..extract import extract_path_arg
from ..targets import Decision, Mapping, Target, build_resolver

_PYICEBERG_IMPORT = "from pyiceberg.catalog import load_catalog"
_PYICEBERG_IMPORT_RE = re.compile(r'from\s+pyiceberg\.catalog\s+import\s+load_catalog')

_READ_RE = re.compile(r"(?:pq|orc|po)\.read_table\s*\(")
_WRITE_RE = re.compile(r"(?:pq|orc|po)\.write_table\s*\(")
_DATASET_RE = re.compile(
    r"pq\.ParquetFile\s*\("
    r"|pq\.ParquetDataset\s*\("
    r"|(?:pa|pyarrow)\.dataset\.dataset\s*\("
    r"|(?:pa|pyarrow)\.dataset\.write_dataset\s*\("
)
_DATASET_WRITE_RE = re.compile(r"(?:pa|pyarrow)\.dataset\.write_dataset\s*\(")


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

    already_has_import = bool(_PYICEBERG_IMPORT_RE.search(source))
    lines = source.splitlines(keepends=True)

    # Pre-scan unique targets (only for real read/write; dataset API is warn-only)
    unique_fqns: dict[str, Target] = {}
    line_decisions: list[Decision | None] = []
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
            if not header_injected and unique_fqns and not already_has_import:
                out.append(_PYICEBERG_IMPORT + "\n")
                out.extend(emit_header())
                header_injected = True
            elif already_has_import:
                header_injected = True  # assume prior run / other transformer set up catalog + tbl vars
            continue

        # Dataset / ParquetFile / ParquetDataset — warn only, keep original
        if _DATASET_RE.search(s):
            direction = "write" if _DATASET_WRITE_RE.search(s) else "read"
            decision = resolver(extract_path_arg(s), direction)
            if decision.skip:
                out.append(f"{sp}# iceberg: skipped by mapping (kept as parquet/orc)\n")
                out.append(line)
                continue
            target = decision.migrate_to
            if target is not None:
                out.append(
                    f"{sp}# TODO(iceberg): pyarrow dataset/ParquetFile API — "
                    f"rewrite to catalog.load_table(({target.namespace!r}, {target.table!r})).scan().to_arrow() "
                    f"(target: {target.fqn}).\n"
                )
            else:
                out.append(
                    f"{sp}# TODO(iceberg): pyarrow dataset/ParquetFile API — "
                    f"rewrite to catalog.load_table((ns, table)).scan().to_arrow() or tbl.overwrite(table); no mapping match.\n"
                )
            out.append(line)
            continue

        decision = line_decisions[i]
        if decision is None:
            out.append(line)
            continue

        if decision.skip:
            out.append(f"{sp}# iceberg: skipped by mapping (kept as parquet/orc)\n")
            out.append(line)
            continue

        target = decision.migrate_to
        if target is None:
            out.append(f"{sp}# TODO(iceberg): could not resolve target table; add a mapping entry.\n")
            out.append(line)
            continue

        if not header_injected:
            out.extend(emit_header(sp))
            header_injected = True

        var = _var_for(target, single)
        is_read = bool(_READ_RE.search(s))

        if is_read:
            lhs_match = re.match(r"(\s*\w+\s*=\s*)(?:pq|orc|po)\.read_table\s*\(.*\)", s)
            lhs = lhs_match.group(1).lstrip() if lhs_match else ""
            out.append(f"{sp}{lhs}{var}.scan().to_arrow()\n")
        else:
            obj_match = re.search(r"(?:pq|orc|po)\.write_table\s*\(\s*(\w+)\s*,", s)
            obj = obj_match.group(1) if obj_match else "table"
            out.append(f"{sp}{var}.overwrite({obj})\n")

    return "".join(out)
