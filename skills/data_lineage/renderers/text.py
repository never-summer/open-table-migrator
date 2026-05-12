"""Plain-text lineage report. Three sections: summary, edges by confidence, unresolved."""
from io import StringIO

from skills.data_lineage.model import Graph


def render(graph: Graph) -> str:
    out = StringIO()
    out.write(f"Lineage report — {len(graph.nodes)} nodes, {len(graph.edges)} edges, "
              f"{len(graph.unresolved)} unresolved\n\n")

    by_conf: dict[str, list] = {"high": [], "medium": [], "low": []}
    for e in graph.edges:
        by_conf[e.confidence].append(e)

    for conf in ("high", "medium", "low"):
        edges = by_conf[conf]
        if not edges:
            continue
        out.write(f"=== {conf.upper()} confidence ({len(edges)}) ===\n")
        for e in edges:
            ev = e.evidence[0] if e.evidence else None
            site = f"{ev.file}:{ev.line}" if ev else "—"
            out.write(f"  {e.src_id}  --{e.kind}-->  {e.dst_id}    [{site}]\n")
        out.write("\n")

    if graph.unresolved:
        out.write(f"=== Unresolved ({len(graph.unresolved)}) ===\n")
        for u in graph.unresolved:
            out.write(f"  {u.file}:{u.line}  {u.reason}  — {u.context}\n")

    return out.getvalue()
