"""Pass 5 — merge all edges into a unified Graph."""
from skills.data_lineage.model import Edge, Graph, Unresolved

from .project_scan import SymbolTable


def run(
    symbols: SymbolTable,
    sql_edges: list[Edge],
    java_edges: list[Edge],
    unresolved: list[Unresolved] | None = None,
) -> Graph:
    g = Graph()
    for e in sql_edges + java_edges:
        g.edges.append(e)
    g.unresolved.extend(unresolved or [])
    return g
