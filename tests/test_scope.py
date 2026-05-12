from skills.open_table_migrator.scope import (
    ConstBinding, ConstTable, build_const_table,
)


def test_empty_python_returns_empty_table():
    table = build_const_table(b"", "python", "x.py")
    assert table.bindings == {}


def test_empty_java_returns_empty_table():
    table = build_const_table(b"", "java", "X.java")
    assert table.bindings == {}


def test_empty_scala_returns_empty_table():
    table = build_const_table(b"", "scala", "X.scala")
    assert table.bindings == {}


def test_resolve_missing_returns_none():
    table = ConstTable()
    assert table.resolve("FOO") is None


def test_resolve_module_scope_only():
    binding = ConstBinding(
        name="X", value="literal",
        file="x.py", line=1, scope="module", reason=None,
    )
    table = ConstTable(bindings={("module", "X"): binding})
    assert table.resolve("X") == binding
    assert table.resolve("X", scope_hint="some_func") == binding


def test_resolve_local_shadows_module():
    module = ConstBinding(
        name="X", value="module-val",
        file="x.py", line=1, scope="module", reason=None,
    )
    local = ConstBinding(
        name="X", value="local-val",
        file="x.py", line=5, scope="f", reason=None,
    )
    table = ConstTable(bindings={
        ("module", "X"): module,
        ("f", "X"): local,
    })
    assert table.resolve("X", scope_hint="f") == local
    assert table.resolve("X") == module
