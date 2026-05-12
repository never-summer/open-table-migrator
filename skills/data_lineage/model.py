"""Read-only data classes for the lineage graph.

No logic lives here — only shapes. Every pass returns frozen instances
of these types so each stage is independently testable and serializable.
"""
from dataclasses import dataclass, field
from typing import Literal


NodeKind = Literal[
    "db.table",
    "db.column",
    "kafka.topic",
    "kafka.field",
    "http.endpoint",
    "http.field",
    "code.method",
]

EdgeKind = Literal["read", "write", "transform"]
Confidence = Literal["high", "medium", "low"]


@dataclass(frozen=True)
class Evidence:
    file: str
    line: int
    pattern: str
    snippet: str


@dataclass(frozen=True)
class Node:
    id: str
    kind: NodeKind
    name: str
    parent_id: str | None
    attrs: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Edge:
    src_id: str
    dst_id: str
    kind: EdgeKind
    evidence: tuple[Evidence, ...]
    confidence: Confidence


@dataclass(frozen=True)
class Unresolved:
    file: str
    line: int
    reason: str
    context: str


@dataclass
class Graph:
    nodes: dict[str, Node] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)
    unresolved: list[Unresolved] = field(default_factory=list)

    def has_edge(self, src_id: str, dst_id: str) -> bool:
        return any(e.src_id == src_id and e.dst_id == dst_id for e in self.edges)
