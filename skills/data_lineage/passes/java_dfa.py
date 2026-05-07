"""Pass 4 — heuristic Java data-flow tracking.

Scope: per method body, track local variable assignments and DTO field
writes. Emits 'transform' edges between code.var.* nodes and code.var.<obj>.<field>
nodes. Recognizes getter method calls (e.g. record.getEmail()) when the
target class is in the SymbolTable and resolves them to the corresponding
DTO field.

Confidence: every edge from this pass is at most 'medium'. graph_build
upgrades to 'high' when sql_parse already produced a high-confidence
read/write into the same code.var.
"""
from pathlib import Path

from skills.data_lineage.extractors import kafka as kafka_extract, rest as rest_extract
from skills.data_lineage.extractors.kafka import KafkaSite
from skills.data_lineage.extractors.rest import RestSite
from skills.data_lineage.model import Edge, Evidence
from skills.data_lineage.ts_parser import parse_java

from .project_scan import SymbolTable
from .sql_extract import SqlUnit


_IGNORED_DIRS = {"generated", "build", "target", ".gradle", "node_modules", ".git"}


def run(
    project_root: Path, symbols: SymbolTable, units: list[SqlUnit]
) -> tuple[list[Edge], list[KafkaSite], list[RestSite], dict[str, str]]:
    edges: list[Edge] = []
    kafka_sites: list[KafkaSite] = []
    rest_sites: list[RestSite] = []
    var_types: dict[str, str] = {}
    for path in project_root.rglob("*.java"):
        if any(part in _IGNORED_DIRS for part in path.parts):
            continue
        rel = str(path.relative_to(project_root))
        source = path.read_bytes()
        tree = parse_java(source)
        for method in _iter_methods(tree.root_node):
            method_edges, method_var_types = _method_edges(method, source, rel, symbols)
            edges.extend(method_edges)
            var_types.update(method_var_types)
        kafka_sites.extend(kafka_extract.extract(source, file=rel))
        rest_sites.extend(rest_extract.extract(source, file=rel))
    return edges, kafka_sites, rest_sites, var_types


def _iter_methods(node):
    if node.type in ("method_declaration", "constructor_declaration"):
        yield node
    for child in node.children:
        yield from _iter_methods(child)


def _method_edges(
    method, source: bytes, file: str, symbols: SymbolTable
) -> tuple[list[Edge], dict[str, str]]:
    body = method.child_by_field_name("body")
    if body is None:
        return [], {}
    edges: list[Edge] = []
    var_types: dict[str, str] = {}
    getter_index = _build_getter_index(symbols)

    for stmt in _iter_statements(body):
        if stmt.type == "local_variable_declaration":
            # Collect the declared type name for var-type tracking
            type_n = stmt.child_by_field_name("type")
            declared_type: str | None = None
            if type_n is not None:
                declared_type = source[type_n.start_byte:type_n.end_byte].decode()
            for decl in stmt.named_children:
                if decl.type != "variable_declarator":
                    continue
                target_n = decl.child_by_field_name("name")
                value_n = decl.child_by_field_name("value")
                if target_n is None:
                    continue
                tgt = source[target_n.start_byte:target_n.end_byte].decode()
                if declared_type is not None:
                    var_types[tgt] = declared_type
                if value_n is None:
                    continue
                src = _expr_source(value_n, source, getter_index)
                if src is None:
                    continue
                edges.append(_edge(file, stmt, src, f"code.var.{tgt}"))

        elif stmt.type == "expression_statement":
            inner = stmt.named_children[0] if stmt.named_children else None
            if inner is not None and inner.type == "assignment_expression":
                left_n = inner.child_by_field_name("left")
                right_n = inner.child_by_field_name("right")
                if left_n is None or right_n is None:
                    continue
                tgt = _lhs_id(left_n, source)
                src = _expr_source(right_n, source, getter_index)
                if tgt is None or src is None:
                    continue
                edges.append(_edge(file, stmt, src, tgt))

    return edges, var_types


def _build_getter_index(symbols: SymbolTable) -> dict[str, str]:
    """Return getter-name → field-name across all known DTOs (last writer wins)."""
    idx: dict[str, str] = {}
    for dto in symbols.classes.values():
        for getter, field in dto.getter_to_field.items():
            idx[getter] = field
    return idx


def _iter_statements(node):
    yield node
    for child in node.children:
        yield from _iter_statements(child)


def _lhs_id(node, source: bytes) -> str | None:
    if node.type == "identifier":
        return f"code.var.{source[node.start_byte:node.end_byte].decode()}"
    if node.type == "field_access":
        obj_n = node.child_by_field_name("object")
        field_n = node.child_by_field_name("field")
        if obj_n is None or field_n is None:
            return None
        obj = source[obj_n.start_byte:obj_n.end_byte].decode()
        field = source[field_n.start_byte:field_n.end_byte].decode()
        return f"code.var.{obj}.{field}"
    return None


def _expr_source(node, source: bytes, getter_index: dict[str, str]) -> str | None:
    if node.type == "identifier":
        return f"code.var.{source[node.start_byte:node.end_byte].decode()}"
    if node.type == "field_access":
        obj_n = node.child_by_field_name("object")
        field_n = node.child_by_field_name("field")
        if obj_n is not None and field_n is not None:
            obj = source[obj_n.start_byte:obj_n.end_byte].decode()
            field = source[field_n.start_byte:field_n.end_byte].decode()
            return f"code.var.{obj}.{field}"
    if node.type == "method_invocation":
        obj_n = node.child_by_field_name("object")
        name_n = node.child_by_field_name("name")
        if obj_n is None or name_n is None:
            return None
        method = source[name_n.start_byte:name_n.end_byte].decode()
        if method in getter_index:
            obj = source[obj_n.start_byte:obj_n.end_byte].decode()
            return f"code.var.{obj}.{getter_index[method]}"
    return None


def _edge(file: str, stmt, src: str, dst: str) -> Edge:
    return Edge(
        src_id=src,
        dst_id=dst,
        kind="transform",
        evidence=(Evidence(file=file, line=stmt.start_point[0] + 1,
                           pattern="dfa_assign", snippet=""),),
        confidence="medium",
    )
