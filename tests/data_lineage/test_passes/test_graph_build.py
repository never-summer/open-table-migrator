from skills.data_lineage.extractors.kafka import KafkaSite
from skills.data_lineage.extractors.rest import RestSite
from skills.data_lineage.extractors.dto import Dto, DtoField
from skills.data_lineage.model import Edge, Evidence
from skills.data_lineage.passes import graph_build
from skills.data_lineage.passes.project_scan import SymbolTable


def _ev(): return (Evidence("X.java", 1, "p", "s"),)


def test_graph_registers_db_nodes_for_sql_edges():
    sql_edges = [Edge("db.users.email", "code.var.email", "read", _ev(), "high")]
    g = graph_build.run(SymbolTable(), sql_edges, [], kafka_sites=[], rest_sites=[])
    assert "db.users" in g.nodes
    assert "db.users.email" in g.nodes
    assert g.nodes["db.users.email"].parent_id == "db.users"


def test_graph_kafka_send_produces_topic_and_field_nodes():
    dto = Dto(fqn="x.Event", file="X.java",
              fields={"email": DtoField("email", "String", "user_email", False)},
              getter_to_field={"getEmail": "email"}, annotations=())
    syms = SymbolTable(classes={"x.Event": dto})
    sites = [KafkaSite(file="P.java", line=2, kind="kafka_send",
                       topic="client-updates", payload_var="ev", payload_type=None)]
    g = graph_build.run(syms, [], [], kafka_sites=sites, rest_sites=[],
                        var_types={"ev": "Event"})
    assert "kafka.client-updates" in g.nodes
    assert "kafka.client-updates.user_email" in g.nodes


def test_rest_endpoint_creates_endpoint_and_response_field_nodes():
    dto = Dto(fqn="x.ClientDto", file="X.java",
              fields={"id": DtoField("id", "Long", "id", False)},
              getter_to_field={"getId": "id"}, annotations=())
    syms = SymbolTable(classes={"x.ClientDto": dto})
    sites = [RestSite(file="A.java", line=3, kind="rest_endpoint",
                      http_method="GET", path="/clients/{id}",
                      request_body_type=None, response_type="ClientDto",
                      request_body_var=None)]
    g = graph_build.run(syms, [], [], kafka_sites=[], rest_sites=sites)
    assert "http.GET:/clients/{id}" in g.nodes
    assert "http.GET:/clients/{id}.response.id" in g.nodes
