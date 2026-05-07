import json

from skills.data_lineage.model import Graph, Node, Edge, Evidence
from skills.data_lineage.renderers.json import render


def test_render_empty():
    out = render(Graph())
    blob = json.loads(out)
    assert blob == {"nodes": [], "edges": [], "unresolved": []}


def test_render_node_and_edge_round_trip():
    n = Node(id="db.users", kind="db.table", name="users", parent_id=None, attrs={})
    ev = Evidence(file="X.java", line=1, pattern="p", snippet="s")
    e = Edge(src_id="a", dst_id="b", kind="read",
             evidence=(ev,), confidence="medium")
    g = Graph(nodes={n.id: n}, edges=[e])
    out = render(g)
    blob = json.loads(out)
    assert blob["nodes"] == [
        {"id": "db.users", "kind": "db.table", "name": "users",
         "parent_id": None, "attrs": {}},
    ]
    assert blob["edges"][0]["evidence"][0]["file"] == "X.java"
    assert blob["edges"][0]["confidence"] == "medium"
