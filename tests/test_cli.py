"""Tests for CLI."""
import hashlib
from pathlib import Path
from textwrap import dedent

from skills.open_table_migrator.cli import convert_project


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else "MISSING"


def test_dry_run_returns_zero_and_prints_header(tmp_path, capsys):
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x.parquet")\n'
    )
    rc = convert_project(
        tmp_path,
        table_name="x", namespace="analytics",
        mapping=None,
        update_deps=True,
        dry_run=True,
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out


def test_dry_run_does_not_create_worklist(tmp_path, capsys):
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x.parquet")\n'
    )
    convert_project(
        tmp_path,
        table_name="x", namespace="analytics",
        mapping=None,
        update_deps=True,
        dry_run=True,
    )
    assert not (tmp_path / "lakehouse-worklist.json").exists()


def test_dry_run_does_not_modify_source_files(tmp_path, capsys):
    src = tmp_path / "job.py"
    src.write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x.parquet")\n'
    )
    before = _sha(src)
    convert_project(
        tmp_path,
        table_name="x", namespace="analytics",
        mapping=None,
        update_deps=True,
        dry_run=True,
    )
    after = _sha(src)
    assert before == after


def test_dry_run_does_not_modify_pyproject_toml(tmp_path, capsys):
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x.parquet")\n'
    )
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(dedent('''
        [project]
        name = "x"
        dependencies = ["pandas>=2.0.0"]
    '''))
    before = _sha(pyproject)
    convert_project(
        tmp_path,
        table_name="x", namespace="analytics",
        mapping=None,
        update_deps=True,
        dry_run=True,
    )
    after = _sha(pyproject)
    assert before == after


def test_dry_run_outputs_four_sections(tmp_path, capsys):
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x.parquet")\n'
    )
    (tmp_path / "pyproject.toml").write_text(dedent('''
        [project]
        name = "x"
        dependencies = ["pandas>=2.0.0"]
    '''))
    convert_project(
        tmp_path,
        table_name="x", namespace="analytics",
        mapping=None,
        update_deps=True,
        dry_run=True,
    )
    out = capsys.readouterr().out
    assert "--- Summary ---" in out
    assert "--- Worklist preview" in out
    # Build-file section appears because pyproject.toml will get pyiceberg added
    assert "--- Build-file updates" in out
