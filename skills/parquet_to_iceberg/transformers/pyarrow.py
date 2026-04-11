import re

_PYICEBERG_IMPORT = "from pyiceberg.catalog import load_catalog"
_CATALOG_TPL = (
    'catalog = load_catalog("default", **{{"type": "sql", "uri": "sqlite:///iceberg.db"}})\n'
    'tbl = catalog.load_table(("{namespace}", "{table_name}"))\n'
)

# Classic pq API — covers parquet and orc (orc module aliased as `orc` or `po`)
_READ_RE = re.compile(r"(?:pq|orc|po)\.read_table\s*\(")
_WRITE_RE = re.compile(r"(?:pq|orc|po)\.write_table\s*\(")

# Dataset API and ParquetFile/ParquetDataset — warn only
_DATASET_RE = re.compile(
    r"pq\.ParquetFile\s*\("
    r"|pq\.ParquetDataset\s*\("
    r"|(?:pa|pyarrow)\.dataset\.dataset\s*\("
    r"|(?:pa|pyarrow)\.dataset\.write_dataset\s*\("
)


def transform_pyarrow_file(source: str, *, table_name: str, namespace: str) -> str:
    lines = source.splitlines(keepends=True)
    out: list[str] = []
    catalog_injected = False
    import_injected = False
    last_import_idx = -1
    fqn = f"{namespace}.{table_name}"

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

        # Dataset / ParquetFile / ParquetDataset — warn only
        if _DATASET_RE.search(stripped):
            out.append(
                f"{sp}# TODO(iceberg): pyarrow dataset/ParquetFile API — "
                f"rewrite to `catalog.load_table(({namespace!r}, {table_name!r})).scan().to_arrow()` "
                f"or `tbl.overwrite(table)` depending on direction.\n"
            )
            out.append(line)
            continue

        if _READ_RE.search(stripped):
            if not catalog_injected:
                for cl in _CATALOG_TPL.format(namespace=namespace, table_name=table_name).splitlines(keepends=True):
                    out.append(sp + cl)
                catalog_injected = True
            var_match = re.match(r"(\s*\w+\s*=\s*)(?:pq|orc|po)\.read_table\s*\(.*\)", stripped)
            var = var_match.group(1).lstrip() if var_match else ""
            out.append(f"{sp}{var}tbl.scan().to_arrow()\n")
            continue

        if _WRITE_RE.search(stripped):
            if not catalog_injected:
                for cl in _CATALOG_TPL.format(namespace=namespace, table_name=table_name).splitlines(keepends=True):
                    out.append(sp + cl)
                catalog_injected = True
            obj_match = re.search(r"(?:pq|orc|po)\.write_table\s*\(\s*(\w+)\s*,", stripped)
            obj = obj_match.group(1) if obj_match else "table"
            out.append(f"{sp}tbl.overwrite({obj})\n")
            continue

        out.append(line)

    return "".join(out)
