from pathlib import Path
from textwrap import dedent

from skills.open_table_migrator.dynamic_sql import (
    DynamicSqlLoader,
    detect_dynamic_sql_loaders,
)


def test_python_open_literal(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        sql = open("queries/events.sql").read()
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    loader = loaders[0]
    assert loader.pattern == "py_open"
    assert loader.sql_filename == "queries/events.sql"
    assert loader.confidence == "high"
    assert loader.line == 2


def test_python_open_non_sql_extension_skipped(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        data = open("data.csv").read()
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert loaders == []


def test_python_open_identifier_arg_skipped_without_const_table(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        SQL_FILE = "queries/x.sql"
        sql = open(SQL_FILE).read()
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    # Without const_table_for_file callback, identifier arguments are skipped.
    assert all(l.pattern != "py_open" or "queries/x.sql" not in l.sql_filename for l in loaders)


def test_python_path_read_text(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        sql = Path("queries/x.sql").read_text()
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    assert loaders[0].pattern == "py_path_read_text"
    assert loaders[0].sql_filename == "queries/x.sql"


def test_python_path_read_bytes(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        raw = Path("queries/x.sql").read_bytes()
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    assert loaders[0].pattern == "py_path_read_text"


def test_python_pkgutil_get_data(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        sql = pkgutil.get_data(__name__, "queries/events.sql")
    '''))
    loaders = detect_dynamic_sql_loaders(tmp_path)
    assert len(loaders) == 1
    assert loaders[0].pattern == "py_pkgutil_get_data"
    assert loaders[0].sql_filename == "queries/events.sql"


def test_python_open_identifier_with_const_table(tmp_path):
    (tmp_path / "job.py").write_text(dedent('''
        SQL_FILE = "queries/x.sql"
        sql = open(SQL_FILE).read()
    '''))
    from skills.open_table_migrator import scope
    loaders = detect_dynamic_sql_loaders(
        tmp_path,
        const_table_for_file=lambda p: scope.build_const_table(p.read_bytes(), "python", str(p)),
    )
    open_loaders = [l for l in loaders if l.pattern == "py_open"]
    assert len(open_loaders) == 1
    assert open_loaders[0].sql_filename == "queries/x.sql"
    assert open_loaders[0].confidence == "medium"
