"""Tests for CLI."""
import hashlib
from pathlib import Path
from textwrap import dedent

from skills.open_table_migrator.scripts.cli import convert_project


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


def test_convert_project_normal_run_writes_runbook(tmp_path):
    """convert_project (no dry-run) writes iceberg-runbook/ directory."""
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/events.parquet")\n'
    )
    rc = convert_project(
        tmp_path,
        table_name="events", namespace="analytics",
        mapping=None,
        update_deps=False,
        dry_run=False,
    )
    assert rc == 0
    runbook_dir = tmp_path / "iceberg-runbook"
    assert runbook_dir.is_dir()
    assert (runbook_dir / "README.md").exists()
    assert (runbook_dir / "analytics.events").is_dir()
    assert (runbook_dir / "analytics.events" / "phase1_add_files.sql").exists()


def test_convert_project_dry_run_does_not_write_runbook(tmp_path, capsys):
    """convert_project(dry_run=True) does NOT create iceberg-runbook/."""
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/events.parquet")\n'
    )
    convert_project(
        tmp_path,
        table_name="events", namespace="analytics",
        mapping=None,
        update_deps=False,
        dry_run=True,
    )
    assert not (tmp_path / "iceberg-runbook").exists()
    out = capsys.readouterr().out
    assert "--- Runbook preview" in out


def test_main_accepts_dry_run_flag(tmp_path, monkeypatch):
    """The --dry-run flag is accepted by argparse and threads through main()."""
    import sys
    from skills.open_table_migrator.scripts import cli

    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x")\n'
    )

    argv = [
        "prog", str(tmp_path),
        "--table", "x", "--namespace", "ns",
        "--no-deps",
        "--dry-run",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    try:
        cli.main()
    except SystemExit as e:
        assert e.code == 0
    else:
        raise AssertionError("main() should call sys.exit")
