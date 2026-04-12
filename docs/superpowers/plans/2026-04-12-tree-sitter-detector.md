# Tree-sitter Detector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the regex-based detector with a tree-sitter AST-based detector that dynamically discovers all I/O formats.

**Architecture:** Three layers — `ts_parser.py` (parse files → AST), `queries/*.scm` (S-expression patterns per runtime), `extract.py` (AST node navigation → PatternMatch). Public API unchanged: `detect_parquet_usage()`, `detect_all_io()` return `list[PatternMatch]`.

**Tech Stack:** `tree-sitter>=0.23`, `tree-sitter-python>=0.23`, `tree-sitter-java>=0.23`, `tree-sitter-scala>=0.23`, Python 3.10+

**Spec:** `docs/superpowers/specs/2026-04-12-tree-sitter-detector-design.md`

**Test runner:** `PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/ --ignore=tests/fixtures --tb=short`

---

### Task 1: Install dependencies and create ts_parser.py

**Files:**
- Create: `skills/open_table_migrator/ts_parser.py`
- Test: `tests/test_ts_parser.py`

- [ ] **Step 1: Install tree-sitter packages**

```bash
/Users/maksim/miniforge3/bin/pip install "tree-sitter>=0.23" "tree-sitter-python>=0.23" "tree-sitter-java>=0.23" "tree-sitter-scala>=0.23"
```

- [ ] **Step 2: Write the failing test**

```python
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
```

- [ ] **Step 3: Run test to verify it fails**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_ts_parser.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'skills.open_table_migrator.ts_parser'`

- [ ] **Step 4: Implement ts_parser.py**

```python
# skills/open_table_migrator/ts_parser.py
"""Thin wrapper around tree-sitter: parse source code into AST trees.

Caches Language and Parser objects per language (created once per process).
"""
import tree_sitter_java as tsjava
import tree_sitter_python as tspython
import tree_sitter_scala as tsscala
from pathlib import Path
from tree_sitter import Language, Parser, Tree

_LANGUAGES: dict[str, Language] = {}
_PARSERS: dict[str, Parser] = {}

_LANG_MODULES = {
    "python": tspython,
    "java": tsjava,
    "scala": tsscala,
}

_EXT_TO_LANG = {
    ".py": "python",
    ".java": "java",
    ".scala": "scala",
}


def _get_parser(lang: str) -> Parser:
    if lang not in _PARSERS:
        mod = _LANG_MODULES[lang]
        language = Language(mod.language())
        _LANGUAGES[lang] = language
        _PARSERS[lang] = Parser(language)
    return _PARSERS[lang]


def get_language(lang: str) -> Language:
    """Return the tree-sitter Language object, initializing if needed."""
    if lang not in _LANGUAGES:
        _get_parser(lang)
    return _LANGUAGES[lang]


def parse(source: bytes, language: str) -> Tree:
    """Parse source bytes into a tree-sitter AST."""
    return _get_parser(language).parse(source)


def language_for_file(path: Path) -> str | None:
    """Return language name by file extension, or None if unsupported."""
    return _EXT_TO_LANG.get(path.suffix.lower())
```

- [ ] **Step 5: Run test to verify it passes**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_ts_parser.py -v
```

Expected: 7 PASSED

- [ ] **Step 6: Commit**

```bash
git add skills/open_table_migrator/ts_parser.py tests/test_ts_parser.py
git commit -m "feat: add ts_parser.py — tree-sitter wrapper with Language/Parser caching"
```

---

### Task 2: PatternMatch — add `format` field, refactor analyzer.py

**Files:**
- Modify: `skills/open_table_migrator/detector.py` (PatternMatch dataclass only)
- Modify: `skills/open_table_migrator/analyzer.py` (replace sets with parsing)
- Test: `tests/test_analyzer.py`

- [ ] **Step 1: Add `format` field to PatternMatch**

In `skills/open_table_migrator/detector.py`, add `format` field to the dataclass:

```python
@dataclass
class PatternMatch:
    file: Path
    line: int
    pattern_type: str
    original_code: str
    path_arg: str | None = None
    end_line: int | None = None
    format: str | None = None  # "parquet", "csv", "protobuf", etc.

    @property
    def direction(self) -> str:
        from .analyzer import direction_of
        return direction_of(self.pattern_type)
```

- [ ] **Step 2: Write failing tests for new analyzer functions**

Add to `tests/test_analyzer.py`:

```python
from skills.open_table_migrator.analyzer import direction_of, is_migration_candidate


def test_direction_of_new_taxonomy():
    assert direction_of("spark_read_parquet") == "read"
    assert direction_of("spark_write_csv") == "write"
    assert direction_of("spark_stream_read_orc") == "read"
    assert direction_of("spark_stream_write_parquet") == "write"
    assert direction_of("pandas_read_json") == "read"
    assert direction_of("pandas_write_excel") == "write"
    assert direction_of("pyarrow_read_parquet") == "read"
    assert direction_of("pyarrow_write_orc") == "write"
    assert direction_of("hive_create_parquet") == "schema"
    assert direction_of("hive_insert_orc") == "write"
    assert direction_of("hive_save_parquet") == "write"
    assert direction_of("stdlib_read_csv") == "read"
    assert direction_of("stdlib_write_file") == "write"


def test_direction_of_old_taxonomy_compat():
    """Old pattern_types still resolve correctly during transition."""
    assert direction_of("pandas_read") == "read"
    assert direction_of("pyspark_write") == "write"
    assert direction_of("hive_create_parquet") == "schema"
    assert direction_of("hive_save_as_table") == "write"


def test_is_migration_candidate_new():
    assert is_migration_candidate("spark_read_parquet") is True
    assert is_migration_candidate("spark_write_orc") is True
    assert is_migration_candidate("spark_read_csv") is False
    assert is_migration_candidate("pandas_write_json") is False
    assert is_migration_candidate("hive_create_parquet") is True


def test_is_migration_candidate_format_field():
    """When format is available directly, use it."""
    from skills.open_table_migrator.detector import PatternMatch
    from pathlib import Path
    m = PatternMatch(Path("x.py"), 1, "spark_read_parquet", "...", format="parquet")
    assert m.format == "parquet"
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_analyzer.py -v
```

Expected: new tests FAIL (direction_of doesn't handle new taxonomy yet)

- [ ] **Step 4: Rewrite direction_of and is_migration_candidate in analyzer.py**

Replace the `_READ_TYPES`, `_WRITE_TYPES`, `_SCHEMA_TYPES` sets and the `direction_of` / `is_migration_candidate` functions with:

```python
# ─── Old taxonomy sets (kept for backwards compat during transition) ───
_OLD_READ_TYPES = {
    "pandas_read", "pandas_orc_read",
    "pyspark_read", "pyspark_orc_read", "pyspark_read_fmt",
    "pyspark_stream_read", "pyspark_stream_read_fmt",
    "pyarrow_read", "pyarrow_orc_read",
    "pyarrow_parquet_file", "pyarrow_parquet_dataset", "pyarrow_dataset_read",
    "java_spark_read", "java_spark_orc_read", "java_spark_read_fmt",
    "java_spark_stream_read", "java_spark_stream_read_fmt",
    "scala_spark_read", "scala_spark_orc_read", "scala_spark_read_fmt",
    "scala_spark_stream_read", "scala_spark_stream_read_fmt",
    "pandas_csv_read", "pandas_json_read", "pandas_excel_read",
    "pyspark_csv_read", "pyspark_json_read", "pyspark_text_read",
    "pyspark_csv_read_fmt", "pyspark_json_read_fmt", "pyspark_avro_read_fmt",
    "pyspark_delta_read_fmt", "pyspark_text_read_fmt", "pyspark_jdbc_read_fmt",
    "pyspark_jdbc_read",
    "jvm_csv_read", "jvm_json_read", "jvm_text_read",
    "jvm_csv_read_fmt", "jvm_json_read_fmt", "jvm_avro_read_fmt",
    "jvm_delta_read_fmt", "jvm_text_read_fmt", "jvm_jdbc_read_fmt",
    "jvm_jdbc_read",
    "python_csv_reader",
    "spark_table_read",
}
_OLD_WRITE_TYPES = {
    "pandas_write", "pandas_orc_write",
    "pyspark_write", "pyspark_orc_write", "pyspark_write_fmt",
    "pyspark_stream_write", "pyspark_stream_write_fmt",
    "pyarrow_write", "pyarrow_orc_write", "pyarrow_dataset_write",
    "java_spark_write", "java_spark_orc_write", "java_spark_write_fmt",
    "java_spark_stream_write", "java_spark_stream_write_fmt",
    "scala_spark_write", "scala_spark_orc_write", "scala_spark_write_fmt",
    "scala_spark_stream_write", "scala_spark_stream_write_fmt",
    "hive_save_as_table", "hive_insert_overwrite", "hive_insert_into",
    "pandas_csv_write", "pandas_json_write", "pandas_excel_write",
    "pyspark_csv_write", "pyspark_json_write", "pyspark_text_write",
    "pyspark_csv_write_fmt", "pyspark_json_write_fmt", "pyspark_avro_write_fmt",
    "pyspark_delta_write_fmt", "pyspark_text_write_fmt", "pyspark_jdbc_write_fmt",
    "jvm_csv_write", "jvm_json_write", "jvm_text_write",
    "jvm_csv_write_fmt", "jvm_json_write_fmt", "jvm_avro_write_fmt",
    "jvm_delta_write_fmt", "jvm_text_write_fmt", "jvm_jdbc_write_fmt",
    "python_csv_writer", "java_file_writer",
}
_OLD_SCHEMA_TYPES = {
    "hive_create_parquet", "hive_create_orc",
    "sql_using_parquet", "sql_using_orc",
}

_READ_DIRECTIONS = {"read"}
_WRITE_DIRECTIONS = {"write", "insert", "save"}
_SCHEMA_DIRECTIONS = {"create"}


def direction_of(pattern_type: str) -> str:
    # Old taxonomy — exact match
    if pattern_type in _OLD_READ_TYPES:
        return "read"
    if pattern_type in _OLD_WRITE_TYPES:
        return "write"
    if pattern_type in _OLD_SCHEMA_TYPES:
        return "schema"

    # New taxonomy — parse from {runtime}_{direction}_{format}
    parts = pattern_type.split("_")
    for part in parts:
        if part in _READ_DIRECTIONS:
            return "read"
        if part in _WRITE_DIRECTIONS:
            return "write"
        if part in _SCHEMA_DIRECTIONS:
            return "schema"
    # Handle compound: spark_stream_read_parquet
    if "read" in pattern_type:
        return "read"
    if "write" in pattern_type:
        return "write"
    return "unknown"


def is_migration_candidate(pattern_type: str) -> bool:
    """True if this pattern type targets parquet or orc storage."""
    fmt = pattern_type.rsplit("_", 1)[-1]
    return fmt in {"parquet", "orc"}
```

Also update `_WARN_ONLY_TYPES` to handle both old and new:

```python
_WARN_ONLY_TYPES = {
    # Old taxonomy
    "pyspark_stream_read", "pyspark_stream_read_fmt",
    "pyspark_stream_write", "pyspark_stream_write_fmt",
    "java_spark_stream_read", "java_spark_stream_read_fmt",
    "java_spark_stream_write", "java_spark_stream_write_fmt",
    "scala_spark_stream_read", "scala_spark_stream_read_fmt",
    "scala_spark_stream_write", "scala_spark_stream_write_fmt",
    "pyarrow_parquet_file", "pyarrow_parquet_dataset",
    "pyarrow_dataset_read", "pyarrow_dataset_write",
}


def is_warn_only(pattern_type: str) -> bool:
    if pattern_type in _WARN_ONLY_TYPES:
        return True
    # New taxonomy: spark_stream_* and pyarrow_*_dataset
    if pattern_type.startswith("spark_stream_"):
        return True
    if pattern_type.startswith("pyarrow_") and "dataset" in pattern_type:
        return True
    return False
```

- [ ] **Step 5: Run all tests**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/ --ignore=tests/fixtures --tb=short -q
```

Expected: all 190+ tests PASS (old taxonomy still works, new taxonomy tests pass too)

- [ ] **Step 6: Commit**

```bash
git add skills/open_table_migrator/detector.py skills/open_table_migrator/analyzer.py tests/test_analyzer.py
git commit -m "feat: add format field to PatternMatch, refactor analyzer for dual taxonomy"
```

---

### Task 3: Write tree-sitter queries for pandas

**Files:**
- Create: `skills/open_table_migrator/queries/pandas.scm`
- Create: `skills/open_table_migrator/queries/__init__.py` (empty)
- Test: `tests/test_ts_queries.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_ts_queries.py
"""Tests for tree-sitter query-based detection."""
import textwrap
from pathlib import Path
from skills.open_table_migrator.ts_detector import ts_detect


def write(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(content))
    return p


# ─── pandas ──────────────────────────────────────────────────────────

def test_pandas_read_parquet(tmp_path):
    write(tmp_path, "etl.py", 'import pandas as pd\ndf = pd.read_parquet("data.parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_read_parquet" and m.format == "parquet" for m in matches)


def test_pandas_write_parquet(tmp_path):
    write(tmp_path, "etl.py", 'df.to_parquet("out.parquet", index=False)\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_write_parquet" and m.format == "parquet" for m in matches)


def test_pandas_read_csv(tmp_path):
    write(tmp_path, "etl.py", 'import pandas as pd\ndf = pd.read_csv("data.csv")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_read_csv" and m.format == "csv" for m in matches)


def test_pandas_write_json(tmp_path):
    write(tmp_path, "etl.py", 'df.to_json("out.json")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_write_json" and m.format == "json" for m in matches)


def test_pandas_read_orc(tmp_path):
    write(tmp_path, "etl.py", 'import pandas as pd\ndf = pd.read_orc("data.orc")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_read_orc" and m.format == "orc" for m in matches)


def test_pandas_write_excel(tmp_path):
    write(tmp_path, "etl.py", 'df.to_excel("out.xlsx")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_write_excel" and m.format == "excel" for m in matches)


def test_pandas_read_unknown_format(tmp_path):
    write(tmp_path, "etl.py", 'import pandas as pd\ndf = pd.read_feather("data.feather")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pandas_read_feather" and m.format == "feather" for m in matches)


def test_pandas_no_false_positive_in_comment(tmp_path):
    write(tmp_path, "etl.py", '# df = pd.read_parquet("data.parquet")\nx = 1\n')
    matches = ts_detect(tmp_path)
    assert not any("pandas" in m.pattern_type for m in matches)


def test_pandas_no_false_positive_in_string(tmp_path):
    write(tmp_path, "etl.py", 'msg = "use pd.read_parquet to load data"\n')
    matches = ts_detect(tmp_path)
    assert not any("pandas" in m.pattern_type for m in matches)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_ts_queries.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'skills.open_table_migrator.ts_detector'`

- [ ] **Step 3: Create queries directory and pandas.scm**

```bash
touch skills/open_table_migrator/queries/__init__.py
```

Write `skills/open_table_migrator/queries/pandas.scm`:

```scheme
;; pd.read_FORMAT("path") — captures FORMAT dynamically
(call
  function: (attribute
    object: (identifier) @_obj
    attribute: (identifier) @_method)
  arguments: (argument_list
    (string) @path_arg)
  (#match? @_obj "^pd$")
  (#match? @_method "^read_")) @match

;; df.to_FORMAT("path") — captures FORMAT dynamically
(call
  function: (attribute
    attribute: (identifier) @_method)
  arguments: (argument_list
    (string) @path_arg)
  (#match? @_method "^to_")) @match
```

- [ ] **Step 4: Implement ts_detector.py — initial scaffold with pandas support**

```python
# skills/open_table_migrator/ts_detector.py
"""Tree-sitter based detector for data I/O operations.

Replaces the regex-based detector.py. Public API:
    ts_detect(project_root) -> list[PatternMatch]
"""
import re
from pathlib import Path

from .detector import PatternMatch
from .ts_parser import get_language, language_for_file, parse

_PY_EXTS = {".py"}
_JVM_EXTS = {".java", ".scala"}
_ALL_EXTS = _PY_EXTS | _JVM_EXTS

_QUERY_CACHE: dict[tuple[str, str], "tree_sitter.Query"] = {}


def _load_query(lang: str, query_name: str) -> "tree_sitter.Query":
    from tree_sitter import Query
    key = (lang, query_name)
    if key not in _QUERY_CACHE:
        query_dir = Path(__file__).parent / "queries"
        scm_path = query_dir / f"{query_name}.scm"
        scm_text = scm_path.read_text()
        _QUERY_CACHE[key] = Query(get_language(lang), scm_text)
    return _QUERY_CACHE[key]


def _extract_pandas_matches(
    src_file: Path, source: bytes, tree, lang: str
) -> list[PatternMatch]:
    from tree_sitter import QueryCursor
    matches: list[PatternMatch] = []
    query = _load_query(lang, "pandas")
    cursor = QueryCursor(query)
    caps = cursor.matches(tree.root_node)

    for _pattern_idx, capture_dict in caps:
        match_node = capture_dict.get("match")
        method_node = capture_dict.get("_method")
        path_node = capture_dict.get("path_arg")

        if not match_node or not method_node:
            continue
        # Unwrap list if needed
        if isinstance(match_node, list):
            match_node = match_node[0]
        if isinstance(method_node, list):
            method_node = method_node[0]

        method_name = method_node.text.decode()

        # pd.read_FORMAT → ("read", FORMAT)
        if method_name.startswith("read_"):
            fmt = method_name[5:]  # "read_parquet" → "parquet"
            direction = "read"
        elif method_name.startswith("to_"):
            fmt = method_name[3:]  # "to_parquet" → "parquet"
            direction = "write"
        else:
            continue

        path_arg = None
        if path_node:
            if isinstance(path_node, list):
                path_node = path_node[0]
            path_arg = path_node.text.decode().strip("\"'")

        start_line = match_node.start_point[0] + 1
        end_line = match_node.end_point[0] + 1
        code = source[match_node.start_byte:match_node.end_byte].decode(errors="replace")

        matches.append(PatternMatch(
            file=src_file,
            line=start_line,
            pattern_type=f"pandas_{direction}_{fmt}",
            original_code=code.strip(),
            path_arg=path_arg,
            end_line=end_line,
            format=fmt,
        ))
    return matches


def ts_detect(project_root: Path) -> list[PatternMatch]:
    """Detect all data I/O operations using tree-sitter AST parsing."""
    all_matches: list[PatternMatch] = []
    for src_file in sorted(project_root.rglob("*")):
        if not src_file.is_file():
            continue
        lang = language_for_file(src_file)
        if not lang:
            continue
        try:
            source = src_file.read_bytes()
        except OSError:
            continue
        tree = parse(source, lang)

        if lang == "python":
            all_matches.extend(_extract_pandas_matches(src_file, source, tree, lang))

    return all_matches
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_ts_queries.py -v -k pandas
```

Expected: 9 pandas tests PASS

Note: the query may need adjustment based on actual tree-sitter Python grammar node types. If `call` should be `call_expression` or `argument_list` should be `arguments`, fix accordingly. Use `tree.root_node.sexp()` to inspect the actual AST structure.

- [ ] **Step 6: Commit**

```bash
git add skills/open_table_migrator/queries/ skills/open_table_migrator/ts_detector.py tests/test_ts_queries.py
git commit -m "feat: tree-sitter pandas detection with dynamic format extraction"
```

---

### Task 4: Write tree-sitter queries for Spark (batch + streaming)

**Files:**
- Create: `skills/open_table_migrator/queries/spark.scm`
- Modify: `skills/open_table_migrator/ts_detector.py`
- Modify: `tests/test_ts_queries.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_ts_queries.py`:

```python
# ─── Spark batch (Python) ────────────────────────────────────────────

def test_spark_read_parquet_py(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.read.parquet("s3://bucket/events/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_parquet" and m.format == "parquet" for m in matches)


def test_spark_write_parquet_py(tmp_path):
    write(tmp_path, "jobs.py", 'df.write.mode("overwrite").parquet("output/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_write_parquet" and m.format == "parquet" for m in matches)


def test_spark_read_format_orc_py(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.read.format("orc").load("data/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_orc" and m.format == "orc" for m in matches)


def test_spark_write_format_csv_py(tmp_path):
    write(tmp_path, "jobs.py", 'df.write.format("csv").save("out/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_write_csv" and m.format == "csv" for m in matches)


def test_spark_read_csv_py(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.read.csv("data.csv")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_csv" for m in matches)


# ─── Spark batch (Java) ─────────────────────────────────────────────

def test_spark_read_parquet_java(tmp_path):
    write(tmp_path, "src/Job.java", 'Dataset<Row> df = spark.read().parquet("data/events/");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_parquet" for m in matches)


def test_spark_write_parquet_java(tmp_path):
    write(tmp_path, "src/Job.java", 'df.write().mode("overwrite").parquet("output/");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_write_parquet" for m in matches)


def test_spark_read_format_parquet_java(tmp_path):
    write(tmp_path, "src/Job.java", 'Dataset<Row> df = spark.read().format("parquet").load("data/");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_parquet" for m in matches)


# ─── Spark batch (Scala) ────────────────────────────────────────────

def test_spark_read_parquet_scala(tmp_path):
    write(tmp_path, "src/Job.scala", 'val df = spark.read.parquet("data/events/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_parquet" for m in matches)


def test_spark_write_parquet_scala(tmp_path):
    write(tmp_path, "src/Job.scala", 'df.write.mode("overwrite").parquet("output/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_write_parquet" for m in matches)


def test_spark_multiline_chain_scala(tmp_path):
    write(tmp_path, "src/Job.scala", textwrap.dedent("""\
        usersDF.write
          .format("parquet")
          .bucketBy(8, "uid")
          .mode(SaveMode.OverWrite)
          .saveAsTable("UsersTbl")
    """))
    matches = ts_detect(tmp_path)
    assert any(m.format == "parquet" for m in matches)
    assert any(m.path_arg == "UsersTbl" for m in matches)


# ─── Spark streaming ────────────────────────────────────────────────

def test_spark_stream_read_parquet_py(tmp_path):
    write(tmp_path, "stream.py", 'df = spark.readStream.parquet("s3://stream/")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_stream_read_parquet" for m in matches)


def test_spark_stream_write_format_py(tmp_path):
    write(tmp_path, "stream.py", 'df.writeStream.format("parquet").option("path", "out/").start()\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_stream_write_parquet" for m in matches)


# ─── spark.table() ──────────────────────────────────────────────────

def test_spark_table_read(tmp_path):
    write(tmp_path, "jobs.py", 'df = spark.table("events")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "spark_read_table" for m in matches)
```

- [ ] **Step 2: Run to verify failures**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_ts_queries.py -v -k spark
```

Expected: all new spark tests FAIL

- [ ] **Step 3: Write spark.scm query file**

The Spark query needs to match method chains. The exact node types depend on the grammar — use `tree.root_node.sexp()` to inspect. The query should capture:

1. `.read.FORMAT(path)` / `.read().FORMAT(path)` — direct format call
2. `.read.format("X").load(path)` / `.read().format("X").load(path)` — generic format
3. `.write.FORMAT(path)` / `.write().FORMAT(path)` — direct write
4. `.write.format("X").save(path)` — generic format write
5. `.readStream.FORMAT(path)` / `.readStream.format("X").load(path)` — streaming read
6. `.writeStream.format("X")...start()` — streaming write
7. `.saveAsTable("name")` — Hive save
8. `spark.table("name")` — table read

Write `skills/open_table_migrator/queries/spark.scm` with these patterns. The S-expression syntax will vary per language grammar — Python, Java, and Scala have different AST structures. Write one `.scm` per language if needed, or handle the differences in the Python extraction code.

- [ ] **Step 4: Implement `_extract_spark_matches` in ts_detector.py**

Add a function that:
1. Loads the spark query for the appropriate language
2. Runs it against the tree
3. For direct calls (`.parquet()`, `.orc()`, `.csv()` etc.): format = method name
4. For `.format("X").load/save()`: format = string argument to `.format()`
5. For `.readStream` / `.writeStream`: prefix with `spark_stream_`
6. For `.saveAsTable()`: pattern_type = `hive_save_{format}` (format from SQL cross-ref or "unknown")
7. For `spark.table()`: pattern_type = `spark_read_table`
8. Sets direction from read/write context
9. Extracts path_arg from the terminal call's string argument

Register the extractor in `ts_detect()` for all three languages.

- [ ] **Step 5: Run tests, iterate until green**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_ts_queries.py -v -k spark
```

Expected: all spark tests PASS

- [ ] **Step 6: Commit**

```bash
git add skills/open_table_migrator/queries/spark.scm skills/open_table_migrator/ts_detector.py tests/test_ts_queries.py
git commit -m "feat: tree-sitter Spark detection (batch, streaming, all formats, 3 languages)"
```

---

### Task 5: Write tree-sitter queries for pyarrow + Hive SQL + stdlib

**Files:**
- Create: `skills/open_table_migrator/queries/pyarrow.scm`
- Create: `skills/open_table_migrator/queries/hive_sql.scm`
- Create: `skills/open_table_migrator/queries/misc_io.scm`
- Modify: `skills/open_table_migrator/ts_detector.py`
- Modify: `tests/test_ts_queries.py`

- [ ] **Step 1: Write failing tests for pyarrow, hive SQL, stdlib**

Append to `tests/test_ts_queries.py`:

```python
# ─── pyarrow ─────────────────────────────────────────────────────────

def test_pyarrow_read_parquet(tmp_path):
    write(tmp_path, "store.py", 'import pyarrow.parquet as pq\nt = pq.read_table("data.parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pyarrow_read_parquet" for m in matches)


def test_pyarrow_write_parquet(tmp_path):
    write(tmp_path, "store.py", 'import pyarrow.parquet as pq\npq.write_table(table, "data.parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pyarrow_write_parquet" for m in matches)


def test_pyarrow_read_orc(tmp_path):
    write(tmp_path, "store.py", 'import pyarrow.orc as orc\nt = orc.read_table("data.orc")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pyarrow_read_orc" for m in matches)


def test_pyarrow_parquet_file(tmp_path):
    write(tmp_path, "store.py", 'import pyarrow.parquet as pq\nf = pq.ParquetFile("data.parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pyarrow_read_dataset" for m in matches)


def test_pyarrow_dataset_write(tmp_path):
    write(tmp_path, "store.py", 'import pyarrow as pa\npa.dataset.write_dataset(tbl, "out/", format="parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "pyarrow_write_dataset" for m in matches)


# ─── Hive SQL in string literals ─────────────────────────────────────

def test_hive_stored_as_parquet_java(tmp_path):
    write(tmp_path, "src/Hive.java", 'spark.sql("CREATE TABLE events (id BIGINT) STORED AS PARQUET");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "hive_create_parquet" for m in matches)


def test_hive_stored_as_orc_scala(tmp_path):
    write(tmp_path, "src/Hive.scala", 'spark.sql("CREATE TABLE events (id BIGINT) STORED AS ORC")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "hive_create_orc" for m in matches)


def test_hive_using_parquet_py(tmp_path):
    write(tmp_path, "jobs.py", 'spark.sql("CREATE TABLE events (id BIGINT) USING parquet")\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "hive_create_parquet" for m in matches)


def test_hive_insert_overwrite_java(tmp_path):
    write(tmp_path, "src/Hive.java", 'spark.sql("INSERT OVERWRITE TABLE events SELECT * FROM staging");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "hive_insert_events" or "hive_insert" in m.pattern_type for m in matches)


def test_hive_save_as_table_java(tmp_path):
    write(tmp_path, "src/Hive.java", 'df.write().mode("overwrite").saveAsTable("events");\n')
    matches = ts_detect(tmp_path)
    assert any(m.path_arg == "events" for m in matches)


# ─── stdlib ──────────────────────────────────────────────────────────

def test_stdlib_csv_writer_py(tmp_path):
    write(tmp_path, "export.py", 'import csv\nw = csv.writer(open("out.csv", "w"))\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "stdlib_write_csv" for m in matches)


def test_stdlib_csv_reader_py(tmp_path):
    write(tmp_path, "load.py", 'import csv\nr = csv.reader(open("data.csv"))\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "stdlib_read_csv" for m in matches)


def test_java_file_writer(tmp_path):
    write(tmp_path, "src/Export.java", 'FileWriter fw = new FileWriter("out.txt");\n')
    matches = ts_detect(tmp_path)
    assert any(m.pattern_type == "stdlib_write_file" for m in matches)
```

- [ ] **Step 2: Run to verify failures**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_ts_queries.py -v -k "pyarrow or hive or stdlib"
```

- [ ] **Step 3: Write the .scm query files**

Write `pyarrow.scm`, `hive_sql.scm`, `misc_io.scm` with appropriate patterns.

For `hive_sql.scm` — tree-sitter gives us string literal nodes. Match those nodes and apply regex inside the string content to find `STORED AS FORMAT`, `USING format`, `INSERT INTO TABLE`, etc. This is a hybrid approach: tree-sitter finds the string nodes, regex parses the SQL content.

- [ ] **Step 4: Implement extractors in ts_detector.py**

Add `_extract_pyarrow_matches`, `_extract_hive_sql_matches`, `_extract_misc_io_matches`. For Hive SQL, the extractor:
1. Gets string literal nodes from the tree
2. Applies regex (from existing `sql_registry.py` patterns) to the string content
3. Produces PatternMatch with appropriate pattern_type

Register all extractors in `ts_detect()`.

- [ ] **Step 5: Run tests, iterate until green**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_ts_queries.py -v
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add skills/open_table_migrator/queries/ skills/open_table_migrator/ts_detector.py tests/test_ts_queries.py
git commit -m "feat: tree-sitter pyarrow, Hive SQL, stdlib detection"
```

---

### Task 6: Implement extract.py — AST-based path_arg and subject extraction

**Files:**
- Rewrite: `skills/open_table_migrator/extract.py`
- Test: `tests/test_ts_queries.py` (add extraction tests)

- [ ] **Step 1: Write failing tests for extraction**

Append to `tests/test_ts_queries.py`:

```python
# ─── path_arg and subject extraction ─────────────────────────────────

def test_extracts_path_arg_from_pandas_read(tmp_path):
    write(tmp_path, "etl.py", 'import pandas as pd\ndf = pd.read_parquet("data/events.parquet")\n')
    matches = ts_detect(tmp_path)
    m = next(m for m in matches if "pandas" in m.pattern_type)
    assert m.path_arg == "data/events.parquet"


def test_extracts_path_arg_from_write_table(tmp_path):
    write(tmp_path, "store.py", 'import pyarrow.parquet as pq\npq.write_table(table, "data.parquet")\n')
    matches = ts_detect(tmp_path)
    m = next(m for m in matches if "pyarrow" in m.pattern_type)
    assert m.path_arg == "data.parquet"


def test_extracts_path_arg_from_save_as_table_scala(tmp_path):
    write(tmp_path, "src/Job.scala", textwrap.dedent("""\
        usersDF.write
          .format("parquet")
          .saveAsTable("UsersTbl")
    """))
    matches = ts_detect(tmp_path)
    assert any(m.path_arg == "UsersTbl" for m in matches)


def test_end_line_covers_multiline_chain(tmp_path):
    write(tmp_path, "src/Job.scala", textwrap.dedent("""\
        usersDF.write
          .format("parquet")
          .bucketBy(8, "uid")
          .saveAsTable("UsersTbl")
    """))
    matches = ts_detect(tmp_path)
    m = next(m for m in matches if m.path_arg == "UsersTbl")
    assert m.end_line >= m.line + 2  # at least 3 lines


def test_original_code_contains_full_chain(tmp_path):
    write(tmp_path, "src/Job.scala", textwrap.dedent("""\
        usersDF.write
          .format("parquet")
          .saveAsTable("UsersTbl")
    """))
    matches = ts_detect(tmp_path)
    m = next(m for m in matches if m.path_arg == "UsersTbl")
    assert "format" in m.original_code
    assert "saveAsTable" in m.original_code
```

- [ ] **Step 2: Run to verify failures, then fix**

The extraction logic is built into each `_extract_*_matches` function in ts_detector.py. Ensure path_arg, end_line, and original_code are set correctly from AST nodes.

- [ ] **Step 3: Update extract.py**

Keep `summarize_operation()` and `_format_of()` — they work on PatternMatch data, not on regex. Remove `_EXTRACTORS` list and `extract_path_arg()` (now done in ts_detector.py). Rewrite `extract_subject()` to accept an AST node optionally:

```python
def extract_subject_from_code(code: str) -> str | None:
    """Fallback subject extraction from code text (for summarize_operation)."""
    # Keep existing _SUBJECT_PATTERNS regex as fallback
    ...
```

- [ ] **Step 4: Run tests**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_ts_queries.py -v
```

- [ ] **Step 5: Commit**

```bash
git add skills/open_table_migrator/extract.py skills/open_table_migrator/ts_detector.py tests/test_ts_queries.py
git commit -m "feat: AST-based path_arg/subject extraction, multiline chain support"
```

---

### Task 7: Wire ts_detector into public API, delete folding.py

**Files:**
- Modify: `skills/open_table_migrator/detector.py` — delegate to ts_detector
- Delete: `skills/open_table_migrator/folding.py`
- Modify: `skills/open_table_migrator/ts_detector.py` — add `detect_parquet_usage` / `detect_all_io` wrappers

- [ ] **Step 1: Write the bridge in detector.py**

Replace the body of `detector.py` but keep the public API:

```python
# skills/open_table_migrator/detector.py
"""Public API for I/O detection.

This module delegates to ts_detector.py (tree-sitter AST-based).
The regex-based detector is preserved in the `regex-detector` git branch.
"""
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PatternMatch:
    file: Path
    line: int
    pattern_type: str
    original_code: str
    path_arg: str | None = None
    end_line: int | None = None
    format: str | None = None

    @property
    def direction(self) -> str:
        from .analyzer import direction_of
        return direction_of(self.pattern_type)


def detect_parquet_usage(project_root: Path) -> list[PatternMatch]:
    """Detect parquet/ORC operations only (migration candidates)."""
    from .ts_detector import ts_detect
    from .analyzer import is_migration_candidate
    return [m for m in ts_detect(project_root) if is_migration_candidate(m.pattern_type)]


def detect_all_io(project_root: Path) -> list[PatternMatch]:
    """Detect ALL data I/O operations."""
    from .ts_detector import ts_detect
    return ts_detect(project_root)
```

- [ ] **Step 2: Delete folding.py**

```bash
git rm skills/open_table_migrator/folding.py
```

Remove any imports of `fold_chains` from other files (grep for `from .folding` and `folding`).

- [ ] **Step 3: Run the full existing test suite**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/ --ignore=tests/fixtures --tb=short -q
```

This is the critical step — all 190 existing tests must pass with the new detector. Tests assert specific `pattern_type` values (old taxonomy), so we need the detector to still produce compatible types OR we update the tests.

**Strategy:** If existing tests expect old `pattern_type` like `pandas_read`, we have two options:
1. Add a compatibility mapping in ts_detector that emits old-style types
2. Update tests to expect new types

Choose option 2 — update tests. The old taxonomy was an implementation detail. Go through each failing test and update the asserted `pattern_type` to the new taxonomy:
- `pandas_read` → `pandas_read_parquet`
- `pyspark_write` → `spark_write_parquet`
- `java_spark_read` → `spark_read_parquet`
- `scala_spark_write_fmt` → `spark_write_parquet`
- `hive_create_parquet` → `hive_create_parquet` (unchanged)
- `hive_save_as_table` → `hive_save_unknown` or `hive_save_table`
- etc.

- [ ] **Step 4: Fix each failing test, one file at a time**

Update pattern_type assertions in:
- `tests/test_detector.py`
- `tests/test_detector_extended.py`
- `tests/test_filters.py`
- `tests/test_bug_regressions.py`
- `tests/test_hybrid_mode.py`
- `tests/test_analyzer.py`
- `tests/test_integration.py`
- `tests/test_multi_table.py`

- [ ] **Step 5: Run full suite until green**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/ --ignore=tests/fixtures --tb=short -q
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: wire tree-sitter detector as primary, remove folding.py, update all tests"
```

---

### Task 8: Update downstream modules (worklist, prepass, filters)

**Files:**
- Modify: `skills/open_table_migrator/worklist.py`
- Modify: `skills/open_table_migrator/prepass.py`
- Modify: `skills/open_table_migrator/filters.py`

- [ ] **Step 1: Update prepass.py**

Replace `_PYSPARK_PATTERN_PREFIXES` with new taxonomy check:

```python
def _is_pyspark_pattern(pattern_type: str) -> bool:
    return pattern_type.startswith("spark_") and "stream" not in pattern_type
```

- [ ] **Step 2: Update worklist.py `_hint_for()`**

Update the pattern_type checks to use new taxonomy:

```python
def _hint_for(pattern_type: str, direction: str, decision: Decision) -> str:
    if decision.skip:
        return "skip (mapping) — should not appear in worklist"
    target = decision.migrate_to
    if target is None:
        return (
            f"unresolved target for {direction} op — ask user or edit mapping.json, "
            "then rewrite by hand"
        )
    fqn = target.fqn
    if pattern_type.startswith("pandas_"):
        if direction == "read":
            return f"rewrite to `{{var}} = tbl.scan().to_pandas()` using tbl bound to {fqn}"
        return f"rewrite to `tbl.overwrite({{df}})` using tbl bound to {fqn}"
    if "dataset" in pattern_type or "parquet_file" in pattern_type:
        return (
            f"pyarrow dataset / ParquetFile API — rewrite to "
            f"`catalog.load_table(('ns','table')).scan().to_arrow()` for {fqn}"
        )
    if pattern_type.startswith("pyarrow_"):
        if direction == "read":
            return f"rewrite to `tbl.scan().to_arrow()` using tbl bound to {fqn}"
        return f"rewrite to `tbl.overwrite({{table}})` using tbl bound to {fqn}"
    if "stream" in pattern_type:
        return (
            f"Structured Streaming sink/source — rewrite using "
            f"`.format('iceberg').option('path', '{fqn}')` or `.toTable('{fqn}')`"
        )
    if pattern_type.startswith("spark_"):
        if direction == "read":
            return f"rewrite to `spark.table(\"{fqn}\")`"
        return f"rewrite to `{{df}}.writeTo(\"{fqn}\").overwritePartitions()`"
    if pattern_type.startswith("hive_create_"):
        return "replace `STORED AS PARQUET|ORC` / `USING parquet|orc` with `USING iceberg`"
    if pattern_type.startswith("hive_insert_"):
        return "INSERT INTO/OVERWRITE — SQL works as-is on Iceberg; verify the table is the migrated one"
    if pattern_type.startswith("hive_save_"):
        return f"rewrite `.saveAsTable(...)` to `.writeTo(\"{fqn}\").createOrReplace()`"
    return f"rewrite this {direction} op to target {fqn}"
```

- [ ] **Step 3: Run full suite**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/ --ignore=tests/fixtures --tb=short -q
```

- [ ] **Step 4: Commit**

```bash
git add skills/open_table_migrator/worklist.py skills/open_table_migrator/prepass.py skills/open_table_migrator/filters.py
git commit -m "refactor: update worklist, prepass, filters for new pattern_type taxonomy"
```

---

### Task 9: Integration tests + full regression pass

**Files:**
- Modify: `tests/test_integration.py`
- Run: full suite

- [ ] **Step 1: Run integration tests**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_integration.py -v
```

If any fail due to pattern_type changes in CLI output, update the test assertions.

- [ ] **Step 2: Run the full 190+ test suite**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/ --ignore=tests/fixtures --tb=short -v
```

Every test must PASS. If any test fails:
1. Check if it's a pattern_type mismatch → update the assertion
2. Check if it's a missing detection → add the query pattern to the appropriate `.scm` file
3. Check if it's a false positive → tighten the query or add a filter

- [ ] **Step 3: Verify no regressions with LearningSparkV2-style fixtures**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/test_bug_regressions.py -v
```

- [ ] **Step 4: Commit final state**

```bash
git add -A
git commit -m "test: all 190+ tests green with tree-sitter detector"
```

---

### Task 10: Cleanup and documentation

**Files:**
- Modify: `skills/open_table_migrator/SKILL.md`
- Modify: `README.md`
- Remove: stale references to folding.py, regex patterns

- [ ] **Step 1: Update SKILL.md**

Update the "What This Skill Does" section to mention tree-sitter. Remove references to regex pattern counts. Update the pattern type table to show new taxonomy.

- [ ] **Step 2: Update README.md**

Move Roadmap section content into the main description (tree-sitter is no longer planned, it's done). Update structure section (remove folding.py, add ts_parser.py and queries/). Update pattern count.

- [ ] **Step 3: Verify no stale imports**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -c "from skills.open_table_migrator.detector import detect_parquet_usage, detect_all_io; print('OK')"
```

- [ ] **Step 4: Final full suite run**

```bash
PYTHONPATH=. /Users/maksim/miniforge3/bin/python -m pytest tests/ --ignore=tests/fixtures --tb=short -q
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs: update SKILL.md and README for tree-sitter detector"
```
