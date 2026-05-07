"""Subgraph filters. For Phase 0 these are no-ops; populated in Phase 4."""
from dataclasses import dataclass

from skills.data_lineage.model import Graph


@dataclass(frozen=True)
class FilterSpec:
    only_table: str | None = None
    only_topic: str | None = None
    max_depth: int | None = None


def apply(graph: Graph, spec: FilterSpec) -> Graph:
    return graph  # filled in during Phase 4
