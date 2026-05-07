"""Pass 5 — assemble the unified Graph.

Inputs:
- SymbolTable (DTOs, getters)
- SQL edges from sql_parse (always 'high' confidence)
- Java DFA edges (always 'medium' / 'low')
- Kafka sites and REST sites (turned into nodes + write/read edges to DTO fields)

Output: Graph with all referenced nodes registered. Confidence semantics:
- high  — sourced from sqlglot/jOOQ-typed DSL
- medium — DTO-resolved Java edge OR Kafka/REST site connected to a known DTO
- low   — Java edge whose target is unresolved
"""
from skills.data_lineage.extractors.dto import Dto
from skills.data_lineage.extractors.kafka import KafkaSite
from skills.data_lineage.extractors.rest import RestSite
from skills.data_lineage.model import Edge, Evidence, Graph, Node, Unresolved

from .project_scan import SymbolTable


def run(
    symbols: SymbolTable,
    sql_edges: list[Edge],
    java_edges: list[Edge],
    *,
    kafka_sites: list[KafkaSite] | None = None,
    rest_sites: list[RestSite] | None = None,
    unresolved: list[Unresolved] | None = None,
) -> Graph:
    g = Graph()
    g.unresolved.extend(unresolved or [])

    for e in sql_edges:
        _register_for_edge(g, e)
        g.edges.append(e)

    for e in java_edges:
        _register_for_edge(g, e)
        g.edges.append(e)

    for site in kafka_sites or []:
        _register_kafka(g, site, symbols)

    for site in rest_sites or []:
        _register_rest(g, site, symbols)

    return g


def _register_for_edge(g: Graph, e: Edge) -> None:
    for nid in (e.src_id, e.dst_id):
        if nid in g.nodes:
            continue
        g.nodes[nid] = _node_for_id(nid)
    parent = g.nodes[e.src_id].parent_id
    if parent and parent not in g.nodes:
        g.nodes[parent] = _node_for_id(parent)
    parent = g.nodes[e.dst_id].parent_id
    if parent and parent not in g.nodes:
        g.nodes[parent] = _node_for_id(parent)


def _node_for_id(node_id: str) -> Node:
    parts = node_id.split(".")
    if node_id.startswith("db."):
        if len(parts) == 3:
            return Node(id=node_id, kind="db.column", name=f"{parts[1]}.{parts[2]}",
                        parent_id=f"db.{parts[1]}", attrs={})
        return Node(id=node_id, kind="db.table", name=parts[1], parent_id=None, attrs={})
    if node_id.startswith("kafka."):
        if len(parts) == 3:
            return Node(id=node_id, kind="kafka.field",
                        name=f"{parts[1]}.{parts[2]}",
                        parent_id=f"kafka.{parts[1]}", attrs={})
        return Node(id=node_id, kind="kafka.topic", name=parts[1],
                    parent_id=None, attrs={})
    if node_id.startswith("http."):
        prefix, _, suffix = node_id.partition(".")
        if ".response." in node_id or ".request." in node_id:
            base, sep, field = node_id.rpartition(".")
            return Node(id=node_id, kind="http.field", name=field,
                        parent_id=base.rsplit(".", 1)[0], attrs={})
        return Node(id=node_id, kind="http.endpoint", name=suffix, parent_id=None,
                    attrs={})
    if node_id.startswith("code."):
        return Node(id=node_id, kind="code.method",
                    name=node_id, parent_id=None, attrs={})
    return Node(id=node_id, kind="code.method", name=node_id, parent_id=None, attrs={})


def _register_kafka(g: Graph, site: KafkaSite, symbols: SymbolTable) -> None:
    topic_id = f"kafka.{site.topic}"
    if topic_id not in g.nodes:
        g.nodes[topic_id] = Node(id=topic_id, kind="kafka.topic",
                                 name=site.topic or "", parent_id=None, attrs={})

    dto = _dto_for_kafka(site, symbols)
    ev = Evidence(file=site.file, line=site.line,
                  pattern=site.kind, snippet=f"kafka {site.kind} {site.topic}")
    if dto is None:
        if site.kind == "kafka_send" and site.payload_var:
            src = f"code.var.{site.payload_var}"
            if src not in g.nodes:
                g.nodes[src] = _node_for_id(src)
            g.edges.append(Edge(src_id=src, dst_id=topic_id,
                                kind="write", evidence=(ev,), confidence="low"))
        return

    direction = "write" if site.kind == "kafka_send" else "read"
    for f in dto.fields.values():
        if f.ignored:
            continue
        field_id = f"{topic_id}.{f.serialized_as}"
        if field_id not in g.nodes:
            g.nodes[field_id] = Node(id=field_id, kind="kafka.field",
                                     name=f"{site.topic}.{f.serialized_as}",
                                     parent_id=topic_id, attrs={})
        if direction == "write":
            src = f"code.var.{site.payload_var}.{f.name}" if site.payload_var else f"code.dto.{dto.fqn}.{f.name}"
            if src not in g.nodes:
                g.nodes[src] = _node_for_id(src)
            g.edges.append(Edge(src_id=src, dst_id=field_id,
                                kind="write", evidence=(ev,), confidence="medium"))
        else:
            dst = f"code.dto.{dto.fqn}.{f.name}"
            if dst not in g.nodes:
                g.nodes[dst] = _node_for_id(dst)
            g.edges.append(Edge(src_id=field_id, dst_id=dst,
                                kind="read", evidence=(ev,), confidence="medium"))


def _register_rest(g: Graph, site: RestSite, symbols: SymbolTable) -> None:
    ev = Evidence(file=site.file, line=site.line,
                  pattern=site.kind, snippet=f"{site.http_method} {site.path}")

    if site.kind == "rest_endpoint":
        endpoint_id = f"http.{site.http_method}:{site.path}"
        if endpoint_id not in g.nodes:
            g.nodes[endpoint_id] = Node(id=endpoint_id, kind="http.endpoint",
                                        name=f"{site.http_method} {site.path}",
                                        parent_id=None,
                                        attrs={"method": site.http_method,
                                               "path": site.path or ""})
        if site.response_type:
            _add_dto_fields(g, endpoint_id, site.response_type, "response", symbols, ev,
                            direction="read")
        if site.request_body_type:
            _add_dto_fields(g, endpoint_id, site.request_body_type, "request", symbols, ev,
                            direction="write")
        return

    # rest_client_call
    endpoint_id = f"http.{site.http_method}:{site.path or ''}"
    if endpoint_id not in g.nodes:
        g.nodes[endpoint_id] = Node(id=endpoint_id, kind="http.endpoint",
                                    name=f"{site.http_method} {site.path or ''}",
                                    parent_id=None,
                                    attrs={"method": site.http_method,
                                           "path": site.path or "",
                                           "outbound": "true"})
    if site.request_body_var:
        src = f"code.var.{site.request_body_var}"
        if src not in g.nodes:
            g.nodes[src] = _node_for_id(src)
        g.edges.append(Edge(src_id=src, dst_id=endpoint_id,
                            kind="write", evidence=(ev,), confidence="low"))


def _dto_for_kafka(site: KafkaSite, symbols: SymbolTable) -> Dto | None:
    if site.payload_type:
        for fqn, dto in symbols.classes.items():
            if fqn.endswith("." + site.payload_type) or fqn == site.payload_type:
                return dto
    # Fallback: if exactly one DTO is known, assume it is the payload shape.
    if len(symbols.classes) == 1:
        return next(iter(symbols.classes.values()))
    return None


def _add_dto_fields(g: Graph, endpoint_id: str, type_name: str, side: str,
                    symbols: SymbolTable, ev: Evidence, *, direction: str) -> None:
    dto = None
    for fqn, candidate in symbols.classes.items():
        if fqn.endswith("." + type_name) or fqn == type_name:
            dto = candidate
            break
    if dto is None:
        return
    for f in dto.fields.values():
        if f.ignored:
            continue
        field_id = f"{endpoint_id}.{side}.{f.serialized_as}"
        g.nodes[field_id] = Node(id=field_id, kind="http.field",
                                 name=f.serialized_as, parent_id=endpoint_id, attrs={})
        if direction == "read":
            dst = f"code.dto.{dto.fqn}.{f.name}"
            if dst not in g.nodes:
                g.nodes[dst] = _node_for_id(dst)
            g.edges.append(Edge(src_id=field_id, dst_id=dst,
                                kind="read", evidence=(ev,), confidence="medium"))
        else:
            src = f"code.dto.{dto.fqn}.{f.name}"
            if src not in g.nodes:
                g.nodes[src] = _node_for_id(src)
            g.edges.append(Edge(src_id=src, dst_id=field_id,
                                kind="write", evidence=(ev,), confidence="medium"))
