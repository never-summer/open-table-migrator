from pathlib import Path

from skills.data_lineage.passes import project_scan, sql_extract, sql_parse, java_dfa, graph_build


def test_empty_pipeline_returns_empty_graph(tmp_path: Path):
    symbols = project_scan.run(tmp_path)
    units = sql_extract.run(tmp_path, symbols)
    sql_edges = sql_parse.run(units)
    java_edges, _, _ = java_dfa.run(tmp_path, symbols, units)
    graph = graph_build.run(symbols, sql_edges, java_edges)
    assert graph.nodes == {}
    assert graph.edges == []
    assert graph.unresolved == []
