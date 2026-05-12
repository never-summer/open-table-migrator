"""Spring Data sources of SQL/JPQL.

Two paths:
1. @Query("...") annotations — both JPQL and nativeQuery=true.
2. Derived query methods (findBy*, readBy*, queryBy*, getBy*) — name-parsed
   into column references. Heuristic; column names are camelCase → snake_case.
"""
import re

from skills.data_lineage.passes.sql_unit import SqlUnit
from skills.data_lineage.ts_parser import parse_java


_DERIVED_PREFIX = re.compile(r"^(findBy|readBy|queryBy|getBy|countBy|existsBy)(.+)$")
_AND_OR = re.compile(r"And|Or")


def _camel_to_snake(s: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


def extract(source: bytes, file: str) -> list[SqlUnit]:
    tree = parse_java(source)
    out: list[SqlUnit] = []
    _walk(tree.root_node, source, file, out)
    return out


def _walk(node, source: bytes, file: str, out: list[SqlUnit]) -> None:
    if node.type == "annotation" or node.type == "marker_annotation":
        _handle_annotation(node, source, file, out)
    if node.type == "method_declaration":
        _handle_derived(node, source, file, out)
    for child in node.children:
        _walk(child, source, file, out)


def _handle_annotation(node, source: bytes, file: str, out: list[SqlUnit]) -> None:
    name_node = node.child_by_field_name("name")
    if name_node is None:
        return
    name = source[name_node.start_byte:name_node.end_byte].decode()
    if name != "Query":
        return
    args = node.child_by_field_name("arguments")
    if args is None:
        return
    sql = None
    is_native = False
    for child in args.named_children:
        if child.type == "string_literal":
            sql = source[child.start_byte:child.end_byte].decode().strip('"')
        elif child.type == "element_value_pair":
            key_n = child.child_by_field_name("key")
            val_n = child.child_by_field_name("value")
            if key_n is None or val_n is None:
                continue
            key = source[key_n.start_byte:key_n.end_byte].decode()
            if key == "value" and val_n.type == "string_literal":
                sql = source[val_n.start_byte:val_n.end_byte].decode().strip('"')
            elif key == "nativeQuery":
                is_native = source[val_n.start_byte:val_n.end_byte].decode() == "true"
    if sql is None:
        return
    out.append(SqlUnit(
        file=file,
        line=node.start_point[0] + 1,
        kind="jpa_query_native" if is_native else "jpa_query",
        sql=sql,
    ))


def _handle_derived(node, source: bytes, file: str, out: list[SqlUnit]) -> None:
    name_n = node.child_by_field_name("name")
    if name_n is None:
        return
    method_name = source[name_n.start_byte:name_n.end_byte].decode()
    m = _DERIVED_PREFIX.match(method_name)
    if not m:
        return
    suffix = m.group(2)
    parts = _AND_OR.split(suffix)
    columns = tuple(_camel_to_snake(p) for p in parts if p)
    out.append(SqlUnit(
        file=file,
        line=node.start_point[0] + 1,
        kind="spring_data_derived",
        sql=None,
        jooq_columns=columns,
    ))
