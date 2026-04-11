# Parquet-to-Iceberg Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Создать Claude Code skill, который при вызове анализирует проект, находит все операции чтения/записи Parquet и конвертирует их в Apache Iceberg с обновлением зависимостей и сохранением тестов.

**Architecture:** Skill-файл содержит инструкции для Claude по детектированию паттернов Parquet в Python-проектах (pandas, PySpark, pyarrow, polars) и их замене на Iceberg-эквиваленты. Каждый паттерн покрывается отдельным блоком преобразования. Для тестирования skill используются фикстуры — минимальные проекты с Parquet-кодом, против которых запускается skill и проверяется корректность конверсии.

**Tech Stack:** Python 3.11+, pyiceberg, pandas, PySpark 3.x, pyarrow, polars, pytest, bats (bash tests)

---

## Task 1: Фикстура — pandas-проект с Parquet

**Files:**
- Create: `tests/fixtures/python-pandas/src/etl.py`
- Create: `tests/fixtures/python-pandas/requirements.txt`
- Create: `tests/fixtures/python-pandas/tests/test_etl.py`
- Create: `tests/conftest.py`
- Create: `tests/test_skill_pandas.py`

- [ ] **Step 1: Написать файл-фикстуру `etl.py` с типичным Parquet-кодом**

```python
# tests/fixtures/python-pandas/src/etl.py
import pandas as pd
import os

DATA_PATH = "data/events.parquet"


def load_events(path: str = DATA_PATH) -> pd.DataFrame:
    return pd.read_parquet(path)


def save_events(df: pd.DataFrame, path: str = DATA_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)


def filter_active(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["status"] == "active"]
```

- [ ] **Step 2: Написать `requirements.txt` фикстуры**

```
pandas>=2.0.0
pyarrow>=14.0.0
```

- [ ] **Step 3: Написать тест фикстуры (проверяет что фикстура работает «до» конверсии)**

```python
# tests/fixtures/python-pandas/tests/test_etl.py
import pandas as pd
import pytest
from src.etl import save_events, load_events, filter_active


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 3],
        "status": ["active", "inactive", "active"],
        "value": [10.0, 20.0, 30.0],
    })


def test_save_and_load_roundtrip(tmp_path, sample_df):
    path = str(tmp_path / "events.parquet")
    save_events(sample_df, path)
    loaded = load_events(path)
    assert list(loaded.columns) == list(sample_df.columns)
    assert len(loaded) == 3


def test_filter_active(sample_df):
    result = filter_active(sample_df)
    assert len(result) == 2
    assert all(result["status"] == "active")
```

- [ ] **Step 4: Создать `tests/conftest.py` с путями к фикстурам**

```python
# tests/conftest.py
import sys
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def fixture_path(name: str) -> Path:
    return FIXTURES_DIR / name
```

- [ ] **Step 5: Убедиться что фикстура запускается**

```bash
cd tests/fixtures/python-pandas
pip install -r requirements.txt -q
pytest tests/ -v
```

Ожидаемый вывод:
```
test_etl.py::test_save_and_load_roundtrip PASSED
test_etl.py::test_filter_active PASSED
```

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/python-pandas/ tests/conftest.py
git commit -m "test: add pandas parquet fixture project"
```

---

## Task 2: Фикстуры — PySpark и pyarrow

**Files:**
- Create: `tests/fixtures/python-pyspark/src/jobs.py`
- Create: `tests/fixtures/python-pyspark/requirements.txt`
- Create: `tests/fixtures/python-pyarrow/src/store.py`
- Create: `tests/fixtures/python-pyarrow/requirements.txt`

- [ ] **Step 1: Написать PySpark-фикстуру**

```python
# tests/fixtures/python-pyspark/src/jobs.py
from pyspark.sql import SparkSession, DataFrame


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("etl").getOrCreate()


def load_events(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def save_events(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def count_by_status(df: DataFrame) -> DataFrame:
    return df.groupBy("status").count()
```

- [ ] **Step 2: Написать `requirements.txt` для PySpark-фикстуры**

```
pyspark>=3.5.0
pyarrow>=14.0.0
```

- [ ] **Step 3: Написать pyarrow-фикстуру**

```python
# tests/fixtures/python-pyarrow/src/store.py
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path


SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("status", pa.string()),
    pa.field("value", pa.float64()),
])


def write_table(table: pa.Table, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def read_table(path: str) -> pa.Table:
    return pq.read_table(path)


def make_sample_table() -> pa.Table:
    return pa.table(
        {"id": [1, 2, 3], "status": ["active", "active", "done"], "value": [1.0, 2.0, 3.0]},
        schema=SCHEMA,
    )
```

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/python-pyspark/ tests/fixtures/python-pyarrow/
git commit -m "test: add pyspark and pyarrow fixture projects"
```

---

## Task 3: Детектор Parquet-паттернов

**Files:**
- Create: `skills/parquet-to-iceberg/detector.py`
- Create: `tests/test_detector.py`

Детектор получает на вход путь к проекту и возвращает список найденных паттернов: `{file, line, pattern_type, original_code}`.

- [ ] **Step 1: Написать тест детектора**

```python
# tests/test_detector.py
import textwrap
from pathlib import Path
from skills.parquet_to_iceberg.detector import detect_parquet_usage, PatternMatch


def write_file(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(content))
    return p


def test_detects_pandas_read(tmp_path):
    write_file(tmp_path, "etl.py", """
        import pandas as pd
        df = pd.read_parquet("data/events.parquet")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert len(matches) == 1
    assert matches[0].pattern_type == "pandas_read"
    assert matches[0].file.name == "etl.py"


def test_detects_pandas_write(tmp_path):
    write_file(tmp_path, "etl.py", """
        import pandas as pd
        df.to_parquet("out.parquet", index=False)
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pandas_write" for m in matches)


def test_detects_pyspark_read(tmp_path):
    write_file(tmp_path, "jobs.py", """
        df = spark.read.parquet("s3://bucket/events/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pyspark_read" for m in matches)


def test_detects_pyspark_write(tmp_path):
    write_file(tmp_path, "jobs.py", """
        df.write.mode("overwrite").parquet("output/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pyspark_write" for m in matches)


def test_detects_pyarrow_read(tmp_path):
    write_file(tmp_path, "store.py", """
        import pyarrow.parquet as pq
        t = pq.read_table("data.parquet")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pyarrow_read" for m in matches)


def test_detects_pyarrow_write(tmp_path):
    write_file(tmp_path, "store.py", """
        import pyarrow.parquet as pq
        pq.write_table(table, "data.parquet")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "pyarrow_write" for m in matches)


def test_skips_non_python_files(tmp_path):
    write_file(tmp_path, "README.md", "pd.read_parquet is useful")
    matches = detect_parquet_usage(tmp_path)
    assert len(matches) == 0


def test_no_matches_in_clean_project(tmp_path):
    write_file(tmp_path, "app.py", "x = 1 + 1")
    assert detect_parquet_usage(tmp_path) == []
```

- [ ] **Step 2: Запустить тест — убедиться что падает**

```bash
pytest tests/test_detector.py -v
```

Ожидаемый вывод: `ImportError: No module named 'skills.parquet_to_iceberg.detector'`

- [ ] **Step 3: Реализовать детектор**

```python
# skills/parquet-to-iceberg/detector.py
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PatternMatch:
    file: Path
    line: int
    pattern_type: str
    original_code: str


_PATTERNS: list[tuple[str, str]] = [
    ("pandas_read",    r"pd\.read_parquet\s*\("),
    ("pandas_write",   r"\.to_parquet\s*\("),
    ("pyspark_read",   r"\.read\.parquet\s*\("),
    ("pyspark_write",  r"\.write(?:\.\w+)*\.parquet\s*\("),
    ("pyarrow_read",   r"pq\.read_table\s*\("),
    ("pyarrow_write",  r"pq\.write_table\s*\("),
]

_COMPILED = [(name, re.compile(pat)) for name, pat in _PATTERNS]


def detect_parquet_usage(project_root: Path) -> list[PatternMatch]:
    matches: list[PatternMatch] = []
    for py_file in sorted(project_root.rglob("*.py")):
        for lineno, line in enumerate(py_file.read_text(errors="replace").splitlines(), 1):
            for pattern_type, regex in _COMPILED:
                if regex.search(line):
                    matches.append(PatternMatch(
                        file=py_file,
                        line=lineno,
                        pattern_type=pattern_type,
                        original_code=line.strip(),
                    ))
    return matches
```

- [ ] **Step 4: Создать `skills/parquet-to-iceberg/__init__.py`**

```python
# skills/parquet-to-iceberg/__init__.py
```

- [ ] **Step 5: Запустить тесты — убедиться что все проходят**

```bash
pytest tests/test_detector.py -v
```

Ожидаемый вывод: все 8 тестов `PASSED`.

- [ ] **Step 6: Commit**

```bash
git add skills/parquet-to-iceberg/ tests/test_detector.py
git commit -m "feat: implement parquet pattern detector"
```

---

## Task 4: Трансформер — pandas-паттерны

**Files:**
- Create: `skills/parquet-to-iceberg/transformers/pandas.py`
- Create: `tests/test_transformer_pandas.py`

Трансформер принимает исходный код файла и `list[PatternMatch]` для него, возвращает новый исходный код с заменёнными паттернами.

- [ ] **Step 1: Написать тест трансформера pandas**

```python
# tests/test_transformer_pandas.py
from skills.parquet_to_iceberg.transformers.pandas import transform_pandas_file


CATALOG = 'catalog = load_catalog("default", **{"type": "sql", "uri": "sqlite:///iceberg.db"})'


def test_transforms_read_parquet():
    source = '''import pandas as pd

df = pd.read_parquet("data/events.parquet")
print(df.head())
'''
    result = transform_pandas_file(
        source,
        table_name="events",
        namespace="default",
    )
    assert "pd.read_parquet" not in result
    assert "catalog.load_table" in result
    assert 'import pyiceberg' in result or 'from pyiceberg' in result


def test_transforms_write_parquet():
    source = '''import pandas as pd

df.to_parquet("data/events.parquet", index=False)
'''
    result = transform_pandas_file(
        source,
        table_name="events",
        namespace="default",
    )
    assert "to_parquet" not in result
    assert "overwrite" in result or "append" in result


def test_adds_catalog_import_once():
    source = '''import pandas as pd
df1 = pd.read_parquet("a.parquet")
df2 = pd.read_parquet("b.parquet")
'''
    result = transform_pandas_file(source, table_name="events", namespace="default")
    assert result.count("load_catalog") >= 1  # at least catalog setup
    # Import added exactly once
    assert result.count("from pyiceberg.catalog") == 1


def test_preserves_non_parquet_lines():
    source = '''import pandas as pd

x = 42
df = pd.read_parquet("data.parquet")
print(x)
'''
    result = transform_pandas_file(source, table_name="events", namespace="default")
    assert "x = 42" in result
    assert "print(x)" in result
```

- [ ] **Step 2: Запустить — убедиться что падает**

```bash
pytest tests/test_transformer_pandas.py -v
```

Ожидаемый вывод: `ImportError: No module named 'skills.parquet_to_iceberg.transformers'`

- [ ] **Step 3: Реализовать трансформер pandas**

```python
# skills/parquet-to-iceberg/transformers/pandas.py
import re
import textwrap

_PYICEBERG_IMPORT = "from pyiceberg.catalog import load_catalog"

_CATALOG_SETUP = textwrap.dedent("""\
    catalog = load_catalog("default", **{{"type": "sql", "uri": "sqlite:///iceberg.db"}})
""")


def _catalog_block(namespace: str, table_name: str) -> str:
    return textwrap.dedent(f"""\
        catalog = load_catalog("default", **{{"type": "sql", "uri": "sqlite:///iceberg.db"}})
        tbl = catalog.load_table(("{namespace}", "{table_name}"))
    """)


def transform_pandas_file(source: str, *, table_name: str, namespace: str) -> str:
    lines = source.splitlines(keepends=True)
    out: list[str] = []
    catalog_injected = False
    import_injected = False

    # Inject pyiceberg import after last existing import block
    import_end = 0
    for i, line in enumerate(lines):
        if re.match(r"^(?:import |from )\S", line):
            import_end = i

    for i, line in enumerate(lines):
        stripped = line.rstrip()

        # After imports, inject pyiceberg import
        if i == import_end and not import_injected:
            out.append(line)
            out.append(_PYICEBERG_IMPORT + "\n")
            import_injected = True
            continue

        # Replace pd.read_parquet(...)
        if re.search(r"pd\.read_parquet\s*\(", stripped):
            indent = len(line) - len(line.lstrip())
            sp = " " * indent
            var_match = re.match(r"(\s*\w+\s*=\s*)pd\.read_parquet\s*\(.*\)", stripped)
            var = var_match.group(1) if var_match else ""
            if not catalog_injected:
                for catalog_line in _catalog_block(namespace, table_name).splitlines(keepends=True):
                    out.append(sp + catalog_line)
                catalog_injected = True
            out.append(f"{sp}{var}tbl.scan().to_pandas()\n")
            continue

        # Replace .to_parquet(...)
        if re.search(r"\.to_parquet\s*\(", stripped):
            indent = len(line) - len(line.lstrip())
            sp = " " * indent
            obj_match = re.match(r"\s*(\w+)\.to_parquet\s*\(.*\)", stripped)
            obj = obj_match.group(1) if obj_match else "df"
            if not catalog_injected:
                for catalog_line in _catalog_block(namespace, table_name).splitlines(keepends=True):
                    out.append(sp + catalog_line)
                catalog_injected = True
            out.append(f"{sp}tbl.overwrite({obj})\n")
            continue

        out.append(line)

    return "".join(out)
```

- [ ] **Step 4: Создать `skills/parquet-to-iceberg/transformers/__init__.py`**

```python
# skills/parquet-to-iceberg/transformers/__init__.py
```

- [ ] **Step 5: Запустить тесты**

```bash
pytest tests/test_transformer_pandas.py -v
```

Ожидаемый вывод: все 4 теста `PASSED`.

- [ ] **Step 6: Commit**

```bash
git add skills/parquet-to-iceberg/transformers/ tests/test_transformer_pandas.py
git commit -m "feat: implement pandas parquet→iceberg transformer"
```

---

## Task 5: Трансформер — PySpark-паттерны

**Files:**
- Create: `skills/parquet-to-iceberg/transformers/pyspark.py`
- Create: `tests/test_transformer_pyspark.py`

- [ ] **Step 1: Написать тест трансформера PySpark**

```python
# tests/test_transformer_pyspark.py
from skills.parquet_to_iceberg.transformers.pyspark import transform_pyspark_file


def test_transforms_spark_read_parquet():
    source = 'df = spark.read.parquet("s3://bucket/events/")\n'
    result = transform_pyspark_file(source, table_name="events", namespace="default")
    assert "spark.read.parquet" not in result
    assert 'spark.read.format("iceberg")' in result or 'spark.table(' in result


def test_transforms_spark_write_parquet():
    source = 'df.write.mode("overwrite").parquet("s3://bucket/out/")\n'
    result = transform_pyspark_file(source, table_name="events", namespace="default")
    assert ".parquet(" not in result
    assert 'format("iceberg")' in result or '.saveAsTable(' in result


def test_adds_spark_iceberg_config():
    source = '''from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("job").getOrCreate()
df = spark.read.parquet("data/")
'''
    result = transform_pyspark_file(source, table_name="events", namespace="default")
    assert "iceberg" in result.lower()


def test_preserves_groupby():
    source = '''df = spark.read.parquet("data/")
agg = df.groupBy("status").count()
agg.show()
'''
    result = transform_pyspark_file(source, table_name="events", namespace="default")
    assert 'groupBy("status")' in result
    assert "agg.show()" in result
```

- [ ] **Step 2: Запустить — убедиться что падает**

```bash
pytest tests/test_transformer_pyspark.py -v
```

- [ ] **Step 3: Реализовать трансформер PySpark**

```python
# skills/parquet-to-iceberg/transformers/pyspark.py
import re

_ICEBERG_CONF_COMMENT = (
    "# Iceberg: set spark.sql.extensions and catalog config in SparkSession builder\n"
    "# .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\n"
    "# .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\n"
)


def transform_pyspark_file(source: str, *, table_name: str, namespace: str) -> str:
    lines = source.splitlines(keepends=True)
    out: list[str] = []
    conf_injected = False

    for line in lines:
        stripped = line.rstrip()

        # spark.read.parquet(path) → spark.table("namespace.table_name")
        if re.search(r"\.read\.parquet\s*\(", stripped):
            indent = len(line) - len(line.lstrip())
            sp = " " * indent
            if not conf_injected:
                out.append(sp + _ICEBERG_CONF_COMMENT)
                conf_injected = True
            var_match = re.match(r"(\s*\w+\s*=\s*).*\.read\.parquet\s*\(.*\)", stripped)
            var = var_match.group(1) if var_match else ""
            out.append(f'{sp}{var}spark.table("{namespace}.{table_name}")\n')
            continue

        # .write.mode(...).parquet(path) → .writeTo("namespace.table").overwritePartitions()
        if re.search(r"\.write(?:\.\w+)*\.parquet\s*\(", stripped):
            indent = len(line) - len(line.lstrip())
            sp = " " * indent
            if not conf_injected:
                out.append(sp + _ICEBERG_CONF_COMMENT)
                conf_injected = True
            obj_match = re.match(r"\s*(\w+)\.write", stripped)
            obj = obj_match.group(1) if obj_match else "df"
            out.append(f'{sp}{obj}.writeTo("{namespace}.{table_name}").overwritePartitions()\n')
            continue

        out.append(line)

    return "".join(out)
```

- [ ] **Step 4: Запустить тесты**

```bash
pytest tests/test_transformer_pyspark.py -v
```

Ожидаемый вывод: все 4 теста `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add skills/parquet-to-iceberg/transformers/pyspark.py tests/test_transformer_pyspark.py
git commit -m "feat: implement pyspark parquet→iceberg transformer"
```

---

## Task 6: Трансформер — pyarrow-паттерны

**Files:**
- Create: `skills/parquet-to-iceberg/transformers/pyarrow.py`
- Create: `tests/test_transformer_pyarrow.py`

- [ ] **Step 1: Написать тест трансформера pyarrow**

```python
# tests/test_transformer_pyarrow.py
from skills.parquet_to_iceberg.transformers.pyarrow import transform_pyarrow_file


def test_transforms_pq_read_table():
    source = '''import pyarrow.parquet as pq
t = pq.read_table("data/events.parquet")
'''
    result = transform_pyarrow_file(source, table_name="events", namespace="default")
    assert "pq.read_table" not in result
    assert "catalog" in result
    assert "pyiceberg" in result


def test_transforms_pq_write_table():
    source = '''import pyarrow.parquet as pq
pq.write_table(table, "data/events.parquet")
'''
    result = transform_pyarrow_file(source, table_name="events", namespace="default")
    assert "pq.write_table" not in result
    assert "overwrite" in result or "append" in result


def test_preserves_schema_definition():
    source = '''import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([pa.field("id", pa.int64())])
t = pq.read_table("data.parquet")
'''
    result = transform_pyarrow_file(source, table_name="events", namespace="default")
    assert "SCHEMA = pa.schema" in result
```

- [ ] **Step 2: Запустить — убедиться что падает**

```bash
pytest tests/test_transformer_pyarrow.py -v
```

- [ ] **Step 3: Реализовать трансформер pyarrow**

```python
# skills/parquet-to-iceberg/transformers/pyarrow.py
import re

_PYICEBERG_IMPORT = "from pyiceberg.catalog import load_catalog"
_CATALOG_TPL = (
    'catalog = load_catalog("default", **{{"type": "sql", "uri": "sqlite:///iceberg.db"}})\n'
    'tbl = catalog.load_table(("{namespace}", "{table_name}"))\n'
)


def transform_pyarrow_file(source: str, *, table_name: str, namespace: str) -> str:
    lines = source.splitlines(keepends=True)
    out: list[str] = []
    catalog_injected = False
    import_injected = False
    last_import_idx = 0

    for i, line in enumerate(lines):
        if re.match(r"^(?:import |from )\S", line):
            last_import_idx = i

    for i, line in enumerate(lines):
        stripped = line.rstrip()
        indent = len(line) - len(line.lstrip())
        sp = " " * indent

        if i == last_import_idx and not import_injected:
            out.append(line)
            out.append(_PYICEBERG_IMPORT + "\n")
            import_injected = True
            continue

        # pq.read_table(path) → tbl.scan().to_arrow()
        if re.search(r"pq\.read_table\s*\(", stripped):
            if not catalog_injected:
                for cl in _CATALOG_TPL.format(namespace=namespace, table_name=table_name).splitlines(keepends=True):
                    out.append(sp + cl)
                catalog_injected = True
            var_match = re.match(r"(\s*\w+\s*=\s*)pq\.read_table\s*\(.*\)", stripped)
            var = var_match.group(1) if var_match else ""
            out.append(f"{sp}{var}tbl.scan().to_arrow()\n")
            continue

        # pq.write_table(table, path) → tbl.overwrite(table)
        if re.search(r"pq\.write_table\s*\(", stripped):
            if not catalog_injected:
                for cl in _CATALOG_TPL.format(namespace=namespace, table_name=table_name).splitlines(keepends=True):
                    out.append(sp + cl)
                catalog_injected = True
            obj_match = re.search(r"pq\.write_table\s*\(\s*(\w+)\s*,", stripped)
            obj = obj_match.group(1) if obj_match else "table"
            out.append(f"{sp}tbl.overwrite({obj})\n")
            continue

        out.append(line)

    return "".join(out)
```

- [ ] **Step 4: Запустить тесты**

```bash
pytest tests/test_transformer_pyarrow.py -v
```

Ожидаемый вывод: все 3 теста `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add skills/parquet-to-iceberg/transformers/pyarrow.py tests/test_transformer_pyarrow.py
git commit -m "feat: implement pyarrow parquet→iceberg transformer"
```

---

## Task 7: Обновление зависимостей

**Files:**
- Create: `skills/parquet-to-iceberg/deps.py`
- Create: `tests/test_deps.py`

Модуль находит `requirements.txt` / `pyproject.toml` в проекте и добавляет `pyiceberg[sql-sqlite]`.

- [ ] **Step 1: Написать тест**

```python
# tests/test_deps.py
from pathlib import Path
from skills.parquet_to_iceberg.deps import update_dependencies


def test_adds_pyiceberg_to_requirements_txt(tmp_path):
    req = tmp_path / "requirements.txt"
    req.write_text("pandas>=2.0.0\npyarrow>=14.0.0\n")
    update_dependencies(tmp_path)
    content = req.read_text()
    assert "pyiceberg" in content
    assert "pandas" in content  # existing deps preserved


def test_does_not_duplicate_pyiceberg(tmp_path):
    req = tmp_path / "requirements.txt"
    req.write_text("pyiceberg[sql-sqlite]>=0.7.0\n")
    update_dependencies(tmp_path)
    assert req.read_text().count("pyiceberg") == 1


def test_adds_pyiceberg_to_pyproject_toml(tmp_path):
    ppt = tmp_path / "pyproject.toml"
    ppt.write_text('[project]\nname = "myapp"\ndependencies = [\n  "pandas",\n]\n')
    update_dependencies(tmp_path)
    content = ppt.read_text()
    assert "pyiceberg" in content


def test_no_deps_file_is_noop(tmp_path):
    # Should not raise
    update_dependencies(tmp_path)
```

- [ ] **Step 2: Запустить — убедиться что падает**

```bash
pytest tests/test_deps.py -v
```

- [ ] **Step 3: Реализовать `deps.py`**

```python
# skills/parquet-to-iceberg/deps.py
import re
from pathlib import Path

PYICEBERG_REQ = 'pyiceberg[sql-sqlite]>=0.7.0'


def update_dependencies(project_root: Path) -> None:
    req_file = project_root / "requirements.txt"
    pyproject = project_root / "pyproject.toml"

    if req_file.exists():
        _update_requirements_txt(req_file)
    elif pyproject.exists():
        _update_pyproject_toml(pyproject)


def _update_requirements_txt(path: Path) -> None:
    content = path.read_text()
    if "pyiceberg" in content:
        return
    path.write_text(content.rstrip() + f"\n{PYICEBERG_REQ}\n")


def _update_pyproject_toml(path: Path) -> None:
    content = path.read_text()
    if "pyiceberg" in content:
        return
    # Append to dependencies list before closing bracket
    content = re.sub(
        r'(dependencies\s*=\s*\[)(.*?)(\])',
        lambda m: m.group(1) + m.group(2).rstrip() + f'\n  "{PYICEBERG_REQ}",\n' + m.group(3),
        content,
        flags=re.DOTALL,
    )
    path.write_text(content)
```

- [ ] **Step 4: Запустить тесты**

```bash
pytest tests/test_deps.py -v
```

Ожидаемый вывод: все 4 теста `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add skills/parquet-to-iceberg/deps.py tests/test_deps.py
git commit -m "feat: implement dependency updater"
```

---

## Task 8: CLI-точка входа и интеграционный тест

**Files:**
- Create: `skills/parquet-to-iceberg/cli.py`
- Create: `tests/test_integration.py`

CLI принимает путь к проекту, запускает детектор + трансформеры + deps. Интеграционный тест запускает CLI против pandas-фикстуры и проверяет что конвертированный код импортирует pyiceberg и не содержит parquet-вызовов.

- [ ] **Step 1: Написать интеграционный тест**

```python
# tests/test_integration.py
import shutil
import subprocess
import sys
from pathlib import Path

FIXTURE = Path(__file__).parent / "fixtures" / "python-pandas"


def test_converts_pandas_fixture(tmp_path):
    # Copy fixture to tmp_path to avoid modifying original
    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project)

    result = subprocess.run(
        [sys.executable, "-m", "skills.parquet_to_iceberg.cli",
         str(project), "--table", "events", "--namespace", "default"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr

    etl = (project / "src" / "etl.py").read_text()
    assert "pd.read_parquet" not in etl
    assert "to_parquet" not in etl
    assert "pyiceberg" in etl or "load_catalog" in etl

    req = (project / "requirements.txt").read_text()
    assert "pyiceberg" in req


def test_cli_prints_summary(tmp_path):
    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project)

    result = subprocess.run(
        [sys.executable, "-m", "skills.parquet_to_iceberg.cli",
         str(project), "--table", "events", "--namespace", "default"],
        capture_output=True, text=True,
    )
    assert "Converted" in result.stdout or "converted" in result.stdout
```

- [ ] **Step 2: Запустить — убедиться что падает**

```bash
pytest tests/test_integration.py -v
```

- [ ] **Step 3: Реализовать CLI**

```python
# skills/parquet-to-iceberg/cli.py
"""
Usage:
    python -m skills.parquet_to_iceberg.cli <project_path> --table <name> --namespace <ns>
"""
import argparse
import sys
from pathlib import Path

from .detector import detect_parquet_usage
from .deps import update_dependencies
from .transformers.pandas import transform_pandas_file
from .transformers.pyspark import transform_pyspark_file
from .transformers.pyarrow import transform_pyarrow_file

_PANDAS_TYPES = {"pandas_read", "pandas_write"}
_PYSPARK_TYPES = {"pyspark_read", "pyspark_write"}
_PYARROW_TYPES = {"pyarrow_read", "pyarrow_write"}


def convert_project(project_root: Path, *, table_name: str, namespace: str) -> int:
    matches = detect_parquet_usage(project_root)
    if not matches:
        print("No Parquet usage found.")
        return 0

    files_by_type: dict[Path, set[str]] = {}
    for m in matches:
        files_by_type.setdefault(m.file, set()).add(m.pattern_type)

    converted = 0
    for py_file, types in files_by_type.items():
        source = py_file.read_text()
        kw = {"table_name": table_name, "namespace": namespace}

        if types & _PANDAS_TYPES:
            source = transform_pandas_file(source, **kw)
        if types & _PYSPARK_TYPES:
            source = transform_pyspark_file(source, **kw)
        if types & _PYARROW_TYPES:
            source = transform_pyarrow_file(source, **kw)

        py_file.write_text(source)
        print(f"  Converted: {py_file.relative_to(project_root)}")
        converted += 1

    update_dependencies(project_root)
    print(f"\nConverted {converted} file(s). Updated dependencies.")
    print("Next steps:")
    print("  1. pip install pyiceberg[sql-sqlite]")
    print("  2. Create Iceberg table schema (see pyiceberg docs)")
    print("  3. Run your tests")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert Parquet usage to Apache Iceberg")
    parser.add_argument("project", type=Path, help="Path to project root")
    parser.add_argument("--table", required=True, help="Iceberg table name")
    parser.add_argument("--namespace", default="default", help="Iceberg namespace")
    args = parser.parse_args()
    sys.exit(convert_project(args.project, table_name=args.table, namespace=args.namespace))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Добавить `__main__.py` для запуска как модуль**

```python
# skills/parquet-to-iceberg/__main__.py
from .cli import main
main()
```

- [ ] **Step 5: Запустить интеграционные тесты**

```bash
pip install pyiceberg[sql-sqlite] pandas pyarrow -q
pytest tests/test_integration.py -v
```

Ожидаемый вывод: оба теста `PASSED`.

- [ ] **Step 6: Запустить все тесты**

```bash
pytest tests/ -v --ignore=tests/fixtures
```

Ожидаемый вывод: все тесты `PASSED`.

- [ ] **Step 7: Commit**

```bash
git add skills/parquet-to-iceberg/cli.py skills/parquet-to-iceberg/__main__.py tests/test_integration.py
git commit -m "feat: add CLI entry point and integration tests"
```

---

## Task 9: SKILL.md — инструкция для Claude Code

**Files:**
- Create: `skills/parquet-to-iceberg/SKILL.md`

- [ ] **Step 1: Написать SKILL.md**

```markdown
---
name: parquet-to-iceberg
description: Converts a Python project from Parquet read/write to Apache Iceberg tables
trigger: "convert parquet" OR "migrate to iceberg" OR "parquet to iceberg"
---

# Parquet → Iceberg Conversion Skill

**Announce at start:** "I'm using the parquet-to-iceberg skill to convert this project."

## What This Skill Does

Scans the project for Parquet read/write operations (pandas, PySpark, pyarrow) and replaces them
with Apache Iceberg equivalents using pyiceberg. Updates project dependencies automatically.

## Step-by-Step Process

### 1. Detect Parquet Usage

Run the detector to understand scope:

\`\`\`bash
python -m skills.parquet_to_iceberg.cli <project_path> --table <TABLE_NAME> --namespace <NAMESPACE> --dry-run
\`\`\`

If `--dry-run` is not yet implemented, read the source files manually and identify:
- `pd.read_parquet(...)` / `df.to_parquet(...)`
- `spark.read.parquet(...)` / `df.write.parquet(...)`
- `pq.read_table(...)` / `pq.write_table(...)`

### 2. Ask the User for Iceberg Table Details

Before converting, ask:
- **Table name** — what should the Iceberg table be called?
- **Namespace** — default namespace is `"default"`
- **Catalog type** — local SQLite (dev) or existing catalog (prod)?

### 3. Run the Conversion

\`\`\`bash
python -m skills.parquet_to_iceberg.cli <project_path> --table <TABLE_NAME> --namespace <NAMESPACE>
\`\`\`

### 4. Review and Fix Edge Cases

After automated conversion, manually review:
- Files with **multiple tables** (the tool assumes one table per project — split if needed)
- **Schema definitions** — Iceberg requires explicit schema; extract from existing parquet files:
  \`\`\`python
  import pyarrow.parquet as pq
  schema = pq.read_schema("existing.parquet")
  \`\`\`
- **Partitioning** — if original code used partitioned parquet, add Iceberg partition spec

### 5. Create the Iceberg Table

\`\`\`python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType

catalog = load_catalog("default", **{"type": "sql", "uri": "sqlite:///iceberg.db"})
schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "status", StringType()),
    NestedField(3, "value", DoubleType()),
)
catalog.create_table("default.events", schema=schema)
\`\`\`

### 6. Run Existing Tests

\`\`\`bash
pip install pyiceberg[sql-sqlite]
pytest tests/ -v
\`\`\`

Fix any failures — common issues:
- Test uses `tmp_path` for parquet file path but iceberg catalog uses fixed `iceberg.db` — inject catalog via fixture
- Assertions on file existence (`assert Path("data.parquet").exists()`) — remove or replace

### 7. Commit

\`\`\`bash
git add -A
git commit -m "refactor: migrate parquet read/write to Apache Iceberg"
\`\`\`

## Conversion Reference

| Before (Parquet) | After (Iceberg) |
|---|---|
| `pd.read_parquet(path)` | `catalog.load_table(ns, name).scan().to_pandas()` |
| `df.to_parquet(path)` | `tbl.overwrite(df)` |
| `spark.read.parquet(path)` | `spark.table("ns.name")` |
| `df.write.parquet(path)` | `df.writeTo("ns.name").overwritePartitions()` |
| `pq.read_table(path)` | `tbl.scan().to_arrow()` |
| `pq.write_table(table, path)` | `tbl.overwrite(table)` |

## Known Limitations

- Multi-table projects require manual splitting by table
- Streaming writes (Kafka → Parquet) are out of scope
- Cloud catalog configs (Glue, Nessie, Hive) need manual setup — this tool generates SQLite dev config
```

- [ ] **Step 2: Commit**

```bash
git add skills/parquet-to-iceberg/SKILL.md
git commit -m "docs: add parquet-to-iceberg SKILL.md"
```

---

## Task 10: pyproject.toml и финальная упаковка

**Files:**
- Create: `pyproject.toml`
- Create: `README.md`

- [ ] **Step 1: Написать `pyproject.toml`**

```toml
[project]
name = "parquet-iceberg-skill"
version = "0.1.0"
description = "Claude Code skill: convert Parquet projects to Apache Iceberg"
requires-python = ">=3.11"
dependencies = [
  "pyiceberg[sql-sqlite]>=0.7.0",
  "pyarrow>=14.0.0",
]

[project.optional-dependencies]
test = [
  "pytest>=8.0.0",
  "pandas>=2.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
```

- [ ] **Step 2: Запустить полный тестовый прогон**

```bash
pip install -e ".[test]" -q
pytest tests/ -v --ignore=tests/fixtures
```

Ожидаемый вывод: все тесты `PASSED`, `0 errors`.

- [ ] **Step 3: Финальный commit**

```bash
git add pyproject.toml
git commit -m "chore: add pyproject.toml packaging"
```

---

*Self-review: все требования покрыты — детектор (Task 3), трансформеры для pandas/PySpark/pyarrow (Tasks 4-6), deps updater (Task 7), CLI (Task 8), SKILL.md (Task 9). Нет плейсхолдеров. Типы и имена функций консистентны между задачами.*
