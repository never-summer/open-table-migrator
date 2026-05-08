"""Subgraph filters for the lineage Graph."""
from collections import deque
from dataclasses import dataclass

from skills.data_lineage.model import Graph


@dataclass(frozen=True)
class FilterSpec:
    only_table: str | None = None
    only_topic: str | None = None
    max_depth: int | None = None


def apply(graph: Graph, spec: FilterSpec) -> Graph:
    if spec.only_table is None and spec.only_topic is None and spec.max_depth is None:
        return graph

    seeds = _seeds(graph, spec)
    if not seeds:
        return Graph()

    keep_node_ids = _bfs(graph, seeds, max_depth=spec.max_depth)
    new = Graph()
    for nid in keep_node_ids:
        if nid in graph.nodes:
            new.nodes[nid] = graph.nodes[nid]
    for e in graph.edges:
        if e.src_id in keep_node_ids and e.dst_id in keep_node_ids:
            new.edges.append(e)
    new.unresolved.extend(graph.unresolved)
    return new


def _seeds(graph: Graph, spec: FilterSpec) -> set[str]:
    seeds: set[str] = set()
    if spec.only_table:
        prefix = f"db.{spec.only_table}"
        seeds |= {nid for nid in graph.nodes if nid == prefix or nid.startswith(prefix + ".")}
    if spec.only_topic:
        prefix = f"kafka.{spec.only_topic}"
        seeds |= {nid for nid in graph.nodes if nid == prefix or nid.startswith(prefix + ".")}
    if not spec.only_table and not spec.only_topic:
        seeds = set(graph.nodes.keys())
    return seeds


def _bfs(graph: Graph, seeds: set[str], *, max_depth: int | None) -> set[str]:
    adj_out: dict[str, list[str]] = {}
    adj_in: dict[str, list[str]] = {}
    for e in graph.edges:
        adj_out.setdefault(e.src_id, []).append(e.dst_id)
        adj_in.setdefault(e.dst_id, []).append(e.src_id)

    visited = set(seeds)
    queue = deque((s, 0) for s in seeds)
    while queue:
        node, depth = queue.popleft()
        if max_depth is not None and depth >= max_depth:
            continue
        for nbr in adj_out.get(node, []) + adj_in.get(node, []):
            if nbr in visited:
                continue
            visited.add(nbr)
            queue.append((nbr, depth + 1))
    return visited
