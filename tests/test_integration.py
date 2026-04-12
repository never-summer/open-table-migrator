import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"
PANDAS_FIXTURE = FIXTURES / "python-pandas"
JAVA_FIXTURE = FIXTURES / "java-spark"
HIVE_FIXTURE = FIXTURES / "java-hive"
REPO_ROOT = Path(__file__).parent.parent


def _run_cli(
    project: Path,
    table: str = "events",
    namespace: str = "default",
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    return subprocess.run(
        [sys.executable, "-m", "skills.open_table_migrator.cli",
         str(project), "--table", table, "--namespace", namespace],
        capture_output=True, text=True, env=env,
    )


def test_converts_pandas_fixture(tmp_path):
    project = tmp_path / "project"
    shutil.copytree(PANDAS_FIXTURE, project)

    result = _run_cli(project)
    assert result.returncode == 0, result.stderr

    worklist = json.loads((project / "lakehouse-worklist.json").read_text())
    assert worklist["count"] >= 1
    assert any("pandas" in e["pattern_type"] for e in worklist["entries"])

    req = (project / "requirements.txt").read_text()
    assert "pyiceberg" in req


def test_converts_java_spark_fixture(tmp_path):
    project = tmp_path / "project"
    shutil.copytree(JAVA_FIXTURE, project)

    result = _run_cli(project)
    assert result.returncode == 0, result.stderr

    worklist = json.loads((project / "lakehouse-worklist.json").read_text())
    assert worklist["count"] >= 1
    assert any("spark" in e["pattern_type"] for e in worklist["entries"])

    pom = (project / "pom.xml").read_text()
    assert "iceberg-spark-runtime" in pom


def test_converts_java_hive_fixture(tmp_path):
    project = tmp_path / "project"
    shutil.copytree(HIVE_FIXTURE, project)

    result = _run_cli(project)
    assert result.returncode == 0, result.stderr

    worklist = json.loads((project / "lakehouse-worklist.json").read_text())
    assert worklist["count"] >= 1
    assert any("hive" in e["pattern_type"] for e in worklist["entries"])

    pom = (project / "pom.xml").read_text()
    assert "iceberg-spark-runtime" in pom


def test_cli_prints_summary(tmp_path):
    project = tmp_path / "project"
    shutil.copytree(PANDAS_FIXTURE, project)

    result = _run_cli(project)
    assert "rewrite task" in result.stdout
