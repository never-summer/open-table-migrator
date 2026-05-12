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


from textwrap import dedent


def test_python_module_literal():
    src = dedent('''
        PATH = "s3://bucket/x"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("PATH")
    assert binding is not None
    assert binding.value == "s3://bucket/x"
    assert binding.scope == "module"


def test_python_module_literal_with_annotation():
    src = dedent('''
        PATH: str = "s3://bucket/x"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    assert table.resolve("PATH").value == "s3://bucket/x"


def test_python_module_concat_two_literals():
    src = dedent('''
        PATH = "s3://" + "bucket/x"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    assert table.resolve("PATH").value == "s3://bucket/x"


def test_python_module_concat_literal_plus_known_const():
    src = dedent('''
        BASE = "s3://bucket"
        PATH = BASE + "/events"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    assert table.resolve("PATH").value == "s3://bucket/events"


def test_python_module_concat_dependency_unresolved():
    src = dedent('''
        PATH = UNKNOWN + "/events"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("PATH")
    assert binding.value is None
    assert binding.reason == "dependency_unresolved"


def test_python_function_local_literal():
    src = dedent('''
        def job():
            p = "s3://bucket/x"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("p", scope_hint="job")
    assert binding is not None
    assert binding.value == "s3://bucket/x"
    assert binding.scope == "job"


def test_python_function_shadows_module():
    src = dedent('''
        PATH = "s3://module"
        def job():
            PATH = "s3://local"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    assert table.resolve("PATH").value == "s3://module"
    assert table.resolve("PATH", scope_hint="job").value == "s3://local"


def test_python_reassignment_marks_unresolvable():
    src = dedent('''
        PATH = "s3://a"
        PATH = "s3://b"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("PATH")
    assert binding.value is None
    assert binding.reason == "reassigned"


def test_python_fstring_skipped():
    src = dedent('''
        bucket = "x"
        PATH = f"s3://{bucket}/events"
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("PATH")
    assert binding is None or binding.value is None


def test_python_non_literal_rhs_skipped():
    src = dedent('''
        import os
        PATH = os.getenv("X")
    ''').encode()
    table = build_const_table(src, "python", "x.py")
    binding = table.resolve("PATH")
    assert binding is None or binding.value is None


def test_java_class_static_final():
    src = dedent('''
        public class Job {
            private static final String PATH = "s3://bucket/x";
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    binding = table.resolve("PATH")
    assert binding is not None
    assert binding.value == "s3://bucket/x"


def test_java_class_final_inline_no_static():
    src = dedent('''
        public class Job {
            private final String PATH = "s3://bucket/x";
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    assert table.resolve("PATH").value == "s3://bucket/x"


def test_java_non_final_skipped():
    src = dedent('''
        public class Job {
            private String PATH = "s3://bucket/x";
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    binding = table.resolve("PATH")
    assert binding is None or binding.value is None


def test_java_class_concat():
    src = dedent('''
        public class Job {
            private static final String BASE = "s3://bucket";
            private static final String PATH = BASE + "/events";
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    assert table.resolve("PATH").value == "s3://bucket/events"


def test_java_method_local_final():
    src = dedent('''
        public class Job {
            void run() {
                final String localPath = "s3://other";
            }
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    binding = table.resolve("localPath", scope_hint="run")
    assert binding is not None
    assert binding.value == "s3://other"


def test_java_method_local_non_final_skipped():
    src = dedent('''
        public class Job {
            void run() {
                String localPath = "s3://other";
            }
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    assert table.resolve("localPath", scope_hint="run") is None


def test_java_reassignment_in_method():
    src = dedent('''
        public class Job {
            void run() {
                final String p = "s3://a";
                final String p = "s3://b";
            }
        }
    ''').encode()
    table = build_const_table(src, "java", "Job.java")
    binding = table.resolve("p", scope_hint="run")
    assert binding is not None
    assert binding.value is None
    assert binding.reason == "reassigned"
