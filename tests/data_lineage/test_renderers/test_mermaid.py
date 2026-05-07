from skills.data_lineage.model import Graph, Node, Edge, Evidence
from skills.data_lineage.renderers.mermaid import render


def test_render_empty():
    out = render(Graph())
    assert out.startswith("flowchart LR")
    assert "%% 0 nodes, 0 edges" in out


def test_render_one_edge():
    n1 = Node(id="db.users.email", kind="db.column", name="users.email",
              parent_id="db.users", attrs={})
    n2 = Node(id="kafka.client-updates.email", kind="kafka.field",
              name="client-updates.email",
              parent_id="kafka.client-updates", attrs={})
    ev = Evidence(file="X.java", line=1, pattern="p", snippet="s")
    e = Edge(src_id=n1.id, dst_id=n2.id, kind="write",
             evidence=(ev,), confidence="high")
    g = Graph(nodes={n1.id: n1, n2.id: n2}, edges=[e])
    out = render(g)
    assert "db_users_email" in out
    assert "kafka_client_updates_email" in out
    assert "-->" in out
