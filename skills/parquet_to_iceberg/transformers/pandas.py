import re
import textwrap

_PYICEBERG_IMPORT = "from pyiceberg.catalog import load_catalog"


def _catalog_block(namespace: str, table_name: str) -> str:
    return textwrap.dedent(f"""\
        catalog = load_catalog("default", **{{"type": "sql", "uri": "sqlite:///iceberg.db"}})
        tbl = catalog.load_table(("{namespace}", "{table_name}"))
    """)


def transform_pandas_file(source: str, *, table_name: str, namespace: str) -> str:
    lines = source.splitlines(keepends=True)
    out: list[str] = []
    catalog_injected = False
    import_injected = False

    # Inject pyiceberg import after last existing import block
    import_end = -1
    for i, line in enumerate(lines):
        if re.match(r"^(?:import |from )\S", line):
            import_end = i

    for i, line in enumerate(lines):
        stripped = line.rstrip()

        # After imports, inject pyiceberg import
        if i == import_end and not import_injected:
            out.append(line)
            out.append(_PYICEBERG_IMPORT + "\n")
            import_injected = True
            continue

        # Replace pd.read_parquet(...)
        if re.search(r"pd\.read_parquet\s*\(", stripped):
            indent = len(line) - len(line.lstrip())
            sp = " " * indent
            var_match = re.match(r"(\s*\w+\s*=\s*)pd\.read_parquet\s*\(.*\)", stripped)
            var = var_match.group(1).lstrip() if var_match else ""
            if not catalog_injected:
                for catalog_line in _catalog_block(namespace, table_name).splitlines(keepends=True):
                    out.append(sp + catalog_line)
                catalog_injected = True
            out.append(f"{sp}{var}tbl.scan().to_pandas()\n")
            continue

        # Replace .to_parquet(...)
        if re.search(r"\.to_parquet\s*\(", stripped):
            indent = len(line) - len(line.lstrip())
            sp = " " * indent
            obj_match = re.match(r"\s*(\w+)\.to_parquet\s*\(.*\)", stripped)
            obj = obj_match.group(1) if obj_match else "df"
            if not catalog_injected:
                for catalog_line in _catalog_block(namespace, table_name).splitlines(keepends=True):
                    out.append(sp + catalog_line)
                catalog_injected = True
            out.append(f"{sp}tbl.overwrite({obj})\n")
            continue

        out.append(line)

    return "".join(out)
