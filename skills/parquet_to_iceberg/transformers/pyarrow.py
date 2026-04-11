import re

_PYICEBERG_IMPORT = "from pyiceberg.catalog import load_catalog"
_CATALOG_TPL = (
    'catalog = load_catalog("default", **{{"type": "sql", "uri": "sqlite:///iceberg.db"}})\n'
    'tbl = catalog.load_table(("{namespace}", "{table_name}"))\n'
)


def transform_pyarrow_file(source: str, *, table_name: str, namespace: str) -> str:
    lines = source.splitlines(keepends=True)
    out: list[str] = []
    catalog_injected = False
    import_injected = False
    last_import_idx = -1

    for i, line in enumerate(lines):
        if re.match(r"^(?:import |from )\S", line):
            last_import_idx = i

    for i, line in enumerate(lines):
        stripped = line.rstrip()
        indent = len(line) - len(line.lstrip())
        sp = " " * indent

        if i == last_import_idx and not import_injected:
            out.append(line)
            out.append(_PYICEBERG_IMPORT + "\n")
            import_injected = True
            continue

        # pq.read_table(path) → tbl.scan().to_arrow()
        if re.search(r"pq\.read_table\s*\(", stripped):
            if not catalog_injected:
                for cl in _CATALOG_TPL.format(namespace=namespace, table_name=table_name).splitlines(keepends=True):
                    out.append(sp + cl)
                catalog_injected = True
            var_match = re.match(r"(\s*\w+\s*=\s*)pq\.read_table\s*\(.*\)", stripped)
            var = var_match.group(1).lstrip() if var_match else ""
            out.append(f"{sp}{var}tbl.scan().to_arrow()\n")
            continue

        # pq.write_table(table, path) → tbl.overwrite(table)
        if re.search(r"pq\.write_table\s*\(", stripped):
            if not catalog_injected:
                for cl in _CATALOG_TPL.format(namespace=namespace, table_name=table_name).splitlines(keepends=True):
                    out.append(sp + cl)
                catalog_injected = True
            obj_match = re.search(r"pq\.write_table\s*\(\s*(\w+)\s*,", stripped)
            obj = obj_match.group(1) if obj_match else "table"
            out.append(f"{sp}tbl.overwrite({obj})\n")
            continue

        out.append(line)

    return "".join(out)
