from pathlib import Path

from skills.data_lineage.cli import run_pipeline


def test_empty_project_produces_empty_artifacts(tmp_path: Path):
    out = tmp_path / "out"
    out.mkdir()
    code = run_pipeline(tmp_path, output_dir=out, formats=("text", "json", "mermaid"))
    assert code == 0
    assert (out / "lineage-report.txt").exists()
    assert (out / "lineage-graph.json").exists()
    assert (out / "lineage.mmd").exists()
    assert "0 nodes" in (out / "lineage-report.txt").read_text()


def test_phase1_jdbc_sql_emits_high_confidence_edges(tmp_path: Path):
    (tmp_path / "Repo.java").write_text(
        'public class Repo {\n'
        '  public Foo a() { return jdbc.queryForObject(\n'
        '    "SELECT id, email FROM users WHERE active = true", null); }\n'
        '}\n'
    )
    out = tmp_path / "out"
    code = run_pipeline(tmp_path, output_dir=out, formats=("json",), quiet=True)
    assert code == 0
    import json
    blob = json.loads((out / "lineage-graph.json").read_text())
    edges = blob["edges"]
    pairs = {(e["src_id"], e["dst_id"]) for e in edges}
    assert ("db.users.id", "code.var.id") in pairs
    assert ("db.users.email", "code.var.email") in pairs
