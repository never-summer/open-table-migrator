"""JSON serialization for the lineage Graph.

Edges are emitted as a list (Graph.edges list is ordered). Evidence is
unrolled into nested objects, not flattened.
"""
import json
from dataclasses import asdict

from skills.data_lineage.model import Graph


def render(graph: Graph) -> str:
    payload = {
        "nodes": [asdict(n) for n in graph.nodes.values()],
        "edges": [
            {
                "src_id": e.src_id,
                "dst_id": e.dst_id,
                "kind": e.kind,
                "confidence": e.confidence,
                "evidence": [asdict(ev) for ev in e.evidence],
            }
            for e in graph.edges
        ],
        "unresolved": [asdict(u) for u in graph.unresolved],
    }
    return json.dumps(payload, indent=2, sort_keys=False)
