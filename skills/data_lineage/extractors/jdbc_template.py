"""Find JdbcTemplate / NamedParameterJdbcTemplate calls with literal SQL.

Recognized methods: query, queryForObject, queryForList, queryForMap,
queryForRowSet, update, batchUpdate, execute. The first argument must be
a single string literal; concatenations and variables are skipped (they
become Unresolved entries elsewhere).
"""
from skills.data_lineage.passes.sql_unit import SqlUnit
from skills.data_lineage.ts_parser import parse_java


_METHODS = {
    "query", "queryForObject", "queryForList", "queryForMap",
    "queryForRowSet", "update", "batchUpdate", "execute",
}


def extract(source: bytes, file: str) -> list[SqlUnit]:
    tree = parse_java(source)
    out: list[SqlUnit] = []
    _walk(tree.root_node, source, file, out)
    return out


def _walk(node, source: bytes, file: str, out: list[SqlUnit]) -> None:
    if node.type == "method_invocation":
        name_node = node.child_by_field_name("name")
        if name_node is not None and source[name_node.start_byte:name_node.end_byte].decode() in _METHODS:
            args = node.child_by_field_name("arguments")
            if args is not None and args.named_child_count >= 1:
                first = args.named_children[0]
                if first.type == "string_literal":
                    sql = source[first.start_byte:first.end_byte].decode().strip('"')
                    out.append(SqlUnit(
                        file=file,
                        line=node.start_point[0] + 1,
                        kind="jdbc_query",
                        sql=sql,
                    ))
    for child in node.children:
        _walk(child, source, file, out)
