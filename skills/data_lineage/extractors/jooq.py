"""jOOQ DSL extractor.

Recognizes jOOQ's DSLContext fluent chains:
- dsl.select(TABLE.COL, ...).from(TABLE)
- dsl.selectFrom(TABLE)
- dsl.insertInto(TABLE, TABLE.COL...).values(...)
- dsl.update(TABLE).set(TABLE.COL, ...)
- dsl.deleteFrom(TABLE)

Identifiers are detected by convention: SCREAMING_SNAKE field accesses
mapped to lowercase. Generated jOOQ classes follow this convention.
"""
from skills.data_lineage.passes.sql_extract import SqlUnit
from skills.data_lineage.ts_parser import parse_java


def extract(source: bytes, file: str) -> list[SqlUnit]:
    tree = parse_java(source)
    out: list[SqlUnit] = []
    _walk(tree.root_node, source, file, out)
    return out


def _walk(node, source: bytes, file: str, out: list[SqlUnit]) -> None:
    if node.type == "method_invocation":
        unit = _try_jooq_chain(node, source, file)
        if unit is not None:
            out.append(unit)
            return  # don't recurse into the chain we just consumed
    for child in node.children:
        _walk(child, source, file, out)


_ENTRY_METHODS = {
    "select": "jooq_select",
    "selectFrom": "jooq_select",
    "insertInto": "jooq_insert",
    "update": "jooq_update",
    "deleteFrom": "jooq_delete",
}


def _try_jooq_chain(node, source: bytes, file: str) -> SqlUnit | None:
    head_name, head_call, head_args = _walk_to_chain_head(node, source)
    if head_name is None or head_name not in _ENTRY_METHODS:
        return None
    kind = _ENTRY_METHODS[head_name]

    tables: list[str] = []
    columns: list[str] = []

    if head_name in ("selectFrom", "deleteFrom", "update"):
        if head_args:
            tables.extend(_collect_table_names(head_args, source))
    elif head_name == "select":
        if head_args:
            columns.extend(_collect_column_names(head_args, source))
        # `from(TABLE)` somewhere in the outer chain
        tables.extend(_chain_from_clause_tables(node, source))
    elif head_name == "insertInto":
        if head_args:
            args_named = head_args
            if args_named:
                tables.extend(_collect_table_names(args_named[0:1], source))
                columns.extend(_collect_column_names(args_named[1:], source))
    if head_name == "update":
        columns.extend(_chain_set_columns(node, source))

    if not tables and not columns:
        return None

    return SqlUnit(
        file=file,
        line=node.start_point[0] + 1,
        kind=kind,
        sql=None,
        jooq_tables=tuple(dict.fromkeys(tables)),
        jooq_columns=tuple(dict.fromkeys(columns)),
    )


def _walk_to_chain_head(node, source: bytes):
    """Walk down `.method(...)` chain to the leftmost method invocation."""
    cur = node
    while True:
        obj = cur.child_by_field_name("object")
        if obj is not None and obj.type == "method_invocation":
            cur = obj
            continue
        name_n = cur.child_by_field_name("name")
        args_n = cur.child_by_field_name("arguments")
        if name_n is None:
            return None, None, None
        head_name = source[name_n.start_byte:name_n.end_byte].decode()
        args_children = args_n.named_children if args_n is not None else []
        return head_name, cur, args_children


def _collect_table_names(arg_nodes, source: bytes) -> list[str]:
    names: list[str] = []
    for n in arg_nodes:
        if n.type == "identifier":
            text = source[n.start_byte:n.end_byte].decode()
            if text.isupper() or "_" in text:
                names.append(text.lower())
    return names


def _collect_column_names(arg_nodes, source: bytes) -> list[str]:
    """Args are typically `TABLE.COLUMN` field accesses."""
    cols: list[str] = []
    for n in arg_nodes:
        if n.type == "field_access":
            field_n = n.child_by_field_name("field")
            if field_n is not None:
                cols.append(source[field_n.start_byte:field_n.end_byte].decode().lower())
    return cols


def _chain_from_clause_tables(node, source: bytes) -> list[str]:
    tables: list[str] = []
    cur = node
    while cur is not None and cur.type == "method_invocation":
        name_n = cur.child_by_field_name("name")
        if name_n is not None:
            name = source[name_n.start_byte:name_n.end_byte].decode()
            if name == "from":
                args_n = cur.child_by_field_name("arguments")
                if args_n is not None:
                    tables.extend(_collect_table_names(args_n.named_children, source))
        cur = cur.child_by_field_name("object")
    return tables


def _chain_set_columns(node, source: bytes) -> list[str]:
    cols: list[str] = []
    cur = node
    while cur is not None and cur.type == "method_invocation":
        name_n = cur.child_by_field_name("name")
        if name_n is not None and source[name_n.start_byte:name_n.end_byte].decode() == "set":
            args_n = cur.child_by_field_name("arguments")
            if args_n is not None and args_n.named_child_count >= 1:
                cols.extend(_collect_column_names(args_n.named_children[:1], source))
        cur = cur.child_by_field_name("object")
    return cols
