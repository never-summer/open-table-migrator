# tests/test_ts_parser.py
from pathlib import Path
from skills.open_table_migrator.ts_parser import parse, language_for_file


def test_language_for_py():
    assert language_for_file(Path("etl.py")) == "python"

def test_language_for_java():
    assert language_for_file(Path("src/Job.java")) == "java"

def test_language_for_scala():
    assert language_for_file(Path("src/Job.scala")) == "scala"

def test_language_for_unknown():
    assert language_for_file(Path("data.csv")) is None

def test_parse_python():
    tree = parse(b'df = pd.read_parquet("data.parquet")\n', "python")
    assert tree.root_node.type == "module"
    assert tree.root_node.child_count > 0

def test_parse_java():
    tree = parse(b'class Job { void run() {} }\n', "java")
    assert tree.root_node.type == "program"

def test_parse_scala():
    tree = parse(b'object Job { val x = 1 }\n', "scala")
    assert tree.root_node.child_count > 0
