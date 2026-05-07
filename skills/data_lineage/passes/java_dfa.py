"""Pass 4 — heuristic Java data-flow tracking.

Scope: per method body, track local variable assignments and DTO field
writes. Emits 'transform' edges between code.var.* nodes and code.var.<obj>.<field>
nodes. No type resolution — assumes the AST shape is local enough for
useful signal.

Confidence: every edge from this pass is at most 'medium'. graph_build
upgrades to 'high' when sql_parse already produced a high-confidence
read/write into the same code.var.
"""
from pathlib import Path

from skills.data_lineage.model import Edge, Evidence
from skills.data_lineage.ts_parser import parse_java

from .project_scan import SymbolTable
from .sql_extract import SqlUnit


_IGNORED_DIRS = {"generated", "build", "target", ".gradle", "node_modules", ".git"}


def run(project_root: Path, symbols: SymbolTable, units: list[SqlUnit]) -> list[Edge]:
    edges: list[Edge] = []
    for path in project_root.rglob("*.java"):
        if any(part in _IGNORED_DIRS for part in path.parts):
            continue
        rel = str(path.relative_to(project_root))
        source = path.read_bytes()
        tree = parse_java(source)
        for method in _iter_methods(tree.root_node):
            edges.extend(_method_edges(method, source, rel))
    return edges


def _iter_methods(node):
    if node.type in ("method_declaration", "constructor_declaration"):
        yield node
    for child in node.children:
        yield from _iter_methods(child)


def _method_edges(method, source: bytes, file: str) -> list[Edge]:
    body = method.child_by_field_name("body")
    if body is None:
        return []
    edges: list[Edge] = []

    for stmt in _iter_statements(body):
        if stmt.type == "local_variable_declaration":
            for decl in stmt.named_children:
                if decl.type != "variable_declarator":
                    continue
                target_n = decl.child_by_field_name("name")
                value_n = decl.child_by_field_name("value")
                if target_n is None or value_n is None:
                    continue
                tgt = source[target_n.start_byte:target_n.end_byte].decode()
                src = _expr_source(value_n, source)
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
                src = _expr_source(right_n, source)
                if tgt is None or src is None:
                    continue
                edges.append(_edge(file, stmt, src, tgt))

    return edges


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


def _expr_source(node, source: bytes) -> str | None:
    if node.type == "identifier":
        return f"code.var.{source[node.start_byte:node.end_byte].decode()}"
    if node.type == "field_access":
        obj_n = node.child_by_field_name("object")
        field_n = node.child_by_field_name("field")
        if obj_n is not None and field_n is not None:
            obj = source[obj_n.start_byte:obj_n.end_byte].decode()
            field = source[field_n.start_byte:field_n.end_byte].decode()
            return f"code.var.{obj}.{field}"
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
