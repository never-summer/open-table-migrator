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
