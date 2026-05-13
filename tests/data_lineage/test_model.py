import json
from dataclasses import asdict
from skills.data_lineage.model import Node, Edge, Evidence, Graph, Unresolved


def test_node_construct_minimal():
    n = Node(id="db.users.email", kind="db.column", name="users.email",
             parent_id="db.users", attrs={})
    assert n.id == "db.users.email"
    assert n.parent_id == "db.users"


def test_edge_with_evidence():
    ev = Evidence(file="Foo.java", line=10, pattern="jooq_select",
                  snippet="dsl.select(USERS.EMAIL)")
    e = Edge(src_id="db.users.email", dst_id="code.var.email",
             kind="read", evidence=(ev,), confidence="high")
    assert e.evidence[0].pattern == "jooq_select"


def test_graph_serializes_to_json_via_asdict():
    n = Node(id="db.users", kind="db.table", name="users", parent_id=None, attrs={})
    g = Graph(nodes={"db.users": n}, edges=[], unresolved=[])
    blob = json.dumps(asdict(g))
    assert "db.users" in blob


def test_unresolved_record():
    u = Unresolved(file="Foo.java", line=42,
                   reason="string concat sql", context="sb.append(...)")
    assert u.reason == "string concat sql"
