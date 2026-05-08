from skills.data_lineage.filters import FilterSpec, apply
from skills.data_lineage.model import Edge, Evidence, Graph, Node


def _ev(): return (Evidence("X", 1, "p", "s"),)


def _build():
    g = Graph()
    for nid, kind in [
        ("db.clients", "db.table"),
        ("db.clients.email", "db.column"),
        ("code.var.email", "code.method"),
        ("kafka.client-updates", "kafka.topic"),
        ("kafka.client-updates.email", "kafka.field"),
        ("db.devices", "db.table"),
        ("db.devices.id", "db.column"),
    ]:
        g.nodes[nid] = Node(id=nid, kind=kind, name=nid, parent_id=None, attrs={})
    g.edges = [
        Edge("db.clients.email", "code.var.email", "read", _ev(), "high"),
        Edge("code.var.email", "kafka.client-updates.email", "write", _ev(), "medium"),
        Edge("db.devices.id", "code.var.dev_id", "read", _ev(), "high"),
    ]
    return g


def test_only_table_keeps_edges_touching_table():
    g = _build()
    out = apply(g, FilterSpec(only_table="clients"))
    src_ids = {e.src_id for e in out.edges}
    assert "db.clients.email" in src_ids
    assert "db.devices.id" not in src_ids


def test_only_topic_keeps_edges_touching_topic():
    g = _build()
    out = apply(g, FilterSpec(only_topic="client-updates"))
    assert any(e.dst_id == "kafka.client-updates.email" for e in out.edges)
    assert all("devices" not in e.src_id for e in out.edges)


def test_max_depth_limits_chain_from_seeds():
    g = _build()
    out = apply(g, FilterSpec(only_table="clients", max_depth=1))
    src_ids = {e.src_id for e in out.edges}
    dst_ids = {e.dst_id for e in out.edges}
    assert "db.clients.email" in src_ids
    assert "kafka.client-updates.email" not in dst_ids
