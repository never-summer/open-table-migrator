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
