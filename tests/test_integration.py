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
    mode: str = "deterministic",
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    return subprocess.run(
        [sys.executable, "-m", "skills.parquet_to_iceberg.cli",
         str(project), "--table", table, "--namespace", namespace, f"--mode={mode}"],
        capture_output=True, text=True, env=env,
    )


def test_converts_pandas_fixture(tmp_path):
    project = tmp_path / "project"
    shutil.copytree(PANDAS_FIXTURE, project)

    result = _run_cli(project)
    assert result.returncode == 0, result.stderr

    etl = (project / "src" / "etl.py").read_text()
    assert "pd.read_parquet" not in etl
    assert "to_parquet" not in etl
    assert "pyiceberg" in etl or "load_catalog" in etl

    req = (project / "requirements.txt").read_text()
    assert "pyiceberg" in req


def test_converts_java_spark_fixture(tmp_path):
    project = tmp_path / "project"
    shutil.copytree(JAVA_FIXTURE, project)

    result = _run_cli(project)
    assert result.returncode == 0, result.stderr

    java_src = (project / "src/main/java/com/example/EventsJob.java").read_text()
    assert ".read().parquet(" not in java_src
    assert ".write().mode(\"overwrite\").parquet(" not in java_src
    assert 'format("iceberg")' in java_src
    assert 'writeTo("default.events")' in java_src

    pom = (project / "pom.xml").read_text()
    assert "iceberg-spark-runtime" in pom


def test_converts_java_hive_fixture(tmp_path):
    project = tmp_path / "project"
    shutil.copytree(HIVE_FIXTURE, project)

    result = _run_cli(project)
    assert result.returncode == 0, result.stderr

    hive_src = (project / "src/main/java/com/example/HiveEtl.java").read_text()
    assert "STORED AS PARQUET" not in hive_src
    assert "USING iceberg" in hive_src
    assert "saveAsTable" not in hive_src
    assert 'writeTo("default.events")' in hive_src

    pom = (project / "pom.xml").read_text()
    assert "iceberg-spark-runtime" in pom


def test_cli_prints_summary(tmp_path):
    project = tmp_path / "project"
    shutil.copytree(PANDAS_FIXTURE, project)

    result = _run_cli(project)
    assert "Converted" in result.stdout or "converted" in result.stdout
