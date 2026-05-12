"""Mermaid `flowchart LR` renderer.

Mermaid IDs disallow dots/dashes; we replace with underscores. The original
ID is kept as the node label so the diagram remains readable.
"""
import re
from io import StringIO

from skills.data_lineage.model import Graph

_ID_SAFE = re.compile(r"[^A-Za-z0-9]")


def _mid(node_id: str) -> str:
    return _ID_SAFE.sub("_", node_id)


def render(graph: Graph) -> str:
    out = StringIO()
    out.write("flowchart LR\n")
    out.write(f"  %% {len(graph.nodes)} nodes, {len(graph.edges)} edges\n")

    for node in graph.nodes.values():
        out.write(f'  {_mid(node.id)}["{node.id}"]\n')

    for e in graph.edges:
        arrow = {"read": "==>", "write": "-->", "transform": "-.->"}[e.kind]
        out.write(f"  {_mid(e.src_id)} {arrow} {_mid(e.dst_id)}\n")

    return out.getvalue()
