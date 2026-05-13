from skills.data_lineage.model import Graph, Node, Edge, Evidence
from skills.data_lineage.renderers.text import render


def test_render_empty_graph():
    out = render(Graph())
    assert "0 nodes" in out
    assert "0 edges" in out


def test_render_single_edge_high_confidence():
    n1 = Node(id="db.users.email", kind="db.column", name="users.email",
              parent_id="db.users", attrs={})
    n2 = Node(id="kafka.client-updates.email", kind="kafka.field",
              name="client-updates.email",
              parent_id="kafka.client-updates", attrs={})
    ev = Evidence(file="X.java", line=12, pattern="jooq_select", snippet="dsl.select(USERS.EMAIL)")
    e = Edge(src_id=n1.id, dst_id=n2.id, kind="write",
             evidence=(ev,), confidence="high")
    g = Graph(nodes={n1.id: n1, n2.id: n2}, edges=[e])
    out = render(g)
    assert "db.users.email" in out
    assert "kafka.client-updates.email" in out
    assert "X.java:12" in out
