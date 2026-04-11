# Parquet-to-Iceberg Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Создать Claude Code skill, который при вызове анализирует проект (Python или Java/Scala), находит все операции чтения/записи Parquet и Hive-таблиц с parquet-хранением, и конвертирует их в Apache Iceberg с обновлением зависимостей и сохранением тестов.

**Architecture:** Skill-файл содержит инструкции для Claude по детектированию паттернов Parquet в Python-проектах (pandas, PySpark, pyarrow) и Java/Scala-проектах (Spark Dataset API, Hive через HiveContext/SparkSQL), и их замене на Iceberg-эквиваленты. Каждый паттерн покрывается отдельным блоком преобразования. Для тестирования skill используются фикстуры — минимальные проекты с Parquet-кодом, против которых запускается skill и проверяется корректность конверсии.

**Tech Stack:** Python 3.11+, pyiceberg, pandas, PySpark 3.x, pyarrow, Java 11+, Scala 2.12, Apache Spark 3.5, Apache Iceberg Spark Runtime 1.5.x, Maven/Gradle, pytest

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

## Task 2: Фикстуры — PySpark, pyarrow, Java/Spark, Hive/SparkSQL

**Files:**
- Create: `tests/fixtures/python-pyspark/src/jobs.py`
- Create: `tests/fixtures/python-pyspark/requirements.txt`
- Create: `tests/fixtures/python-pyarrow/src/store.py`
- Create: `tests/fixtures/python-pyarrow/requirements.txt`
- Create: `tests/fixtures/java-spark/src/main/java/com/example/EventsJob.java`
- Create: `tests/fixtures/java-spark/pom.xml`
- Create: `tests/fixtures/scala-spark/src/main/scala/com/example/EventsJob.scala`
- Create: `tests/fixtures/scala-spark/build.gradle`
- Create: `tests/fixtures/java-hive/src/main/java/com/example/HiveEtl.java`
- Create: `tests/fixtures/java-hive/pom.xml`

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

- [ ] **Step 4: Написать Java/Spark фикстуру**

```java
// tests/fixtures/java-spark/src/main/java/com/example/EventsJob.java
package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EventsJob {
    public static Dataset<Row> loadEvents(SparkSession spark, String path) {
        return spark.read().parquet(path);
    }

    public static void saveEvents(Dataset<Row> df, String path) {
        df.write().mode("overwrite").parquet(path);
    }

    public static Dataset<Row> countByStatus(Dataset<Row> df) {
        return df.groupBy("status").count();
    }
}
```

- [ ] **Step 5: Написать `pom.xml` для Java/Spark фикстуры**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>events-job</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.5.0</version>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 6: Написать Scala/Spark фикстуру**

```scala
// tests/fixtures/scala-spark/src/main/scala/com/example/EventsJob.scala
package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object EventsJob {
  def loadEvents(spark: SparkSession, path: String): DataFrame =
    spark.read.parquet(path)

  def saveEvents(df: DataFrame, path: String): Unit =
    df.write.mode("overwrite").parquet(path)

  def countByStatus(df: DataFrame): DataFrame =
    df.groupBy("status").count()
}
```

- [ ] **Step 7: Написать `build.gradle` для Scala-фикстуры**

```groovy
// tests/fixtures/scala-spark/build.gradle
plugins {
    id 'scala'
}

repositories {
    mavenCentral()
}

dependencies {
    implementation 'org.scala-lang:scala-library:2.12.18'
    implementation 'org.apache.spark:spark-sql_2.12:3.5.0'
}
```

- [ ] **Step 8: Написать Java/Hive фикстуру (Hive через SparkSQL)**

```java
// tests/fixtures/java-hive/src/main/java/com/example/HiveEtl.java
package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveEtl {
    public static void createHiveTable(SparkSession spark) {
        spark.sql("CREATE TABLE IF NOT EXISTS events (id BIGINT, status STRING, value DOUBLE) STORED AS PARQUET");
    }

    public static Dataset<Row> readHive(SparkSession spark) {
        return spark.sql("SELECT * FROM events");
    }

    public static void writeHive(Dataset<Row> df) {
        df.write().mode("overwrite").saveAsTable("events");
    }
}
```

- [ ] **Step 9: Написать `pom.xml` для Hive-фикстуры**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>hive-etl</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.5.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_2.12</artifactId>
            <version>3.5.0</version>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 10: Commit**

```bash
git add tests/fixtures/python-pyspark/ tests/fixtures/python-pyarrow/ \
        tests/fixtures/java-spark/ tests/fixtures/scala-spark/ tests/fixtures/java-hive/
git commit -m "test: add pyspark, pyarrow, java-spark, scala-spark, java-hive fixtures"
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


def test_skips_non_source_files(tmp_path):
    write_file(tmp_path, "README.md", "pd.read_parquet is useful")
    matches = detect_parquet_usage(tmp_path)
    assert len(matches) == 0


def test_no_matches_in_clean_project(tmp_path):
    write_file(tmp_path, "app.py", "x = 1 + 1")
    assert detect_parquet_usage(tmp_path) == []


# ─── Java/Scala Spark patterns ─────────────────────────────────────────

def test_detects_java_spark_read(tmp_path):
    write_file(tmp_path, "src/Job.java", """
        import org.apache.spark.sql.Dataset;
        public class Job {
            Dataset<Row> df = spark.read().parquet("data/events/");
        }
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "java_spark_read" for m in matches)


def test_detects_java_spark_write(tmp_path):
    write_file(tmp_path, "src/Job.java", """
        df.write().mode("overwrite").parquet("output/");
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "java_spark_write" for m in matches)


def test_detects_scala_spark_read(tmp_path):
    write_file(tmp_path, "src/Job.scala", """
        val df = spark.read.parquet("data/events/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "scala_spark_read" for m in matches)


def test_detects_scala_spark_write(tmp_path):
    write_file(tmp_path, "src/Job.scala", """
        df.write.mode("overwrite").parquet("output/")
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "scala_spark_write" for m in matches)


# ─── Hive / SparkSQL patterns ──────────────────────────────────────────

def test_detects_hive_stored_as_parquet(tmp_path):
    write_file(tmp_path, "src/Hive.java", '''
        spark.sql("CREATE TABLE events (id BIGINT) STORED AS PARQUET");
    ''')
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "hive_create_parquet" for m in matches)


def test_detects_hive_save_as_table(tmp_path):
    write_file(tmp_path, "src/Hive.java", """
        df.write().mode("overwrite").saveAsTable("events");
    """)
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "hive_save_as_table" for m in matches)


def test_detects_hive_insert_into(tmp_path):
    write_file(tmp_path, "src/Hive.java", '''
        spark.sql("INSERT OVERWRITE TABLE events SELECT * FROM staging");
    ''')
    matches = detect_parquet_usage(tmp_path)
    assert any(m.pattern_type == "hive_insert_overwrite" for m in matches)
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


# File extensions to scan, grouped by language family
_PY_EXTS = {".py"}
_JVM_EXTS = {".java", ".scala"}

# Python patterns (pandas / pyspark / pyarrow)
_PY_PATTERNS: list[tuple[str, str]] = [
    ("pandas_read",    r"pd\.read_parquet\s*\("),
    ("pandas_write",   r"\.to_parquet\s*\("),
    ("pyspark_read",   r"\.read\.parquet\s*\("),
    ("pyspark_write",  r"\.write(?:\.\w+\([^)]*\))*\.parquet\s*\("),
    ("pyarrow_read",   r"pq\.read_table\s*\("),
    ("pyarrow_write",  r"pq\.write_table\s*\("),
]

# Java Spark patterns — Java uses .read().parquet() with parens
_JAVA_SPARK_PATTERNS: list[tuple[str, str]] = [
    ("java_spark_read",   r"\.read\(\)\.parquet\s*\("),
    ("java_spark_write",  r"\.write\(\)(?:\.\w+\([^)]*\))*\.parquet\s*\("),
]

# Scala Spark patterns — Scala omits parens: .read.parquet
_SCALA_SPARK_PATTERNS: list[tuple[str, str]] = [
    ("scala_spark_read",   r"\.read\.parquet\s*\("),
    ("scala_spark_write",  r"\.write(?:\.\w+\([^)]*\))*\.parquet\s*\("),
]

# Hive/SparkSQL patterns — look inside SQL string literals and API calls
_HIVE_PATTERNS: list[tuple[str, str]] = [
    ("hive_create_parquet",    r'"[^"]*\bSTORED\s+AS\s+PARQUET\b[^"]*"'),
    ("hive_save_as_table",     r"\.saveAsTable\s*\("),
    ("hive_insert_overwrite",  r'"[^"]*\bINSERT\s+OVERWRITE\s+TABLE\b[^"]*"'),
]

_COMPILED_PY = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _PY_PATTERNS]
_COMPILED_JAVA = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _JAVA_SPARK_PATTERNS + _HIVE_PATTERNS]
_COMPILED_SCALA = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _SCALA_SPARK_PATTERNS + _HIVE_PATTERNS]


def _patterns_for_file(path: Path) -> list[tuple[str, re.Pattern]]:
    suffix = path.suffix.lower()
    if suffix == ".py":
        return _COMPILED_PY
    if suffix == ".java":
        return _COMPILED_JAVA
    if suffix == ".scala":
        return _COMPILED_SCALA
    return []


def detect_parquet_usage(project_root: Path) -> list[PatternMatch]:
    matches: list[PatternMatch] = []
    for src_file in sorted(project_root.rglob("*")):
        if not src_file.is_file():
            continue
        if src_file.suffix.lower() not in (_PY_EXTS | _JVM_EXTS):
            continue
        compiled = _patterns_for_file(src_file)
        for lineno, line in enumerate(src_file.read_text(errors="replace").splitlines(), 1):
            for pattern_type, regex in compiled:
                if regex.search(line):
                    matches.append(PatternMatch(
                        file=src_file,
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

Ожидаемый вывод: все 15 тестов `PASSED` (pandas ×2, pyspark ×2, pyarrow ×2, java_spark ×2, scala_spark ×2, hive ×3, skip/empty ×2).

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

## Task 6.5: Трансформер — Java/Scala Spark + Hive

**Files:**
- Create: `skills/parquet-to-iceberg/transformers/jvm.py`
- Create: `tests/test_transformer_jvm.py`

Трансформер обрабатывает `.java` и `.scala` файлы. Общая логика: заменить вызовы Parquet API на Iceberg-эквиваленты через `writeTo(...)` / `spark.table(...)`, а Hive SQL `STORED AS PARQUET` / `saveAsTable` — на `USING iceberg` / `writeTo(...).createOrReplace()`.

### Таблица соответствий (JVM)

| Было (Java) | Стало |
|---|---|
| `spark.read().parquet(path)` | `spark.read().format("iceberg").load("ns.table")` |
| `df.write().mode("overwrite").parquet(path)` | `df.writeTo("ns.table").overwritePartitions()` |
| `df.write().saveAsTable("t")` | `df.writeTo("ns.t").createOrReplace()` |
| `CREATE TABLE ... STORED AS PARQUET` | `CREATE TABLE ... USING iceberg` |
| `INSERT OVERWRITE TABLE t SELECT ...` | `INSERT OVERWRITE TABLE ns.t SELECT ...` *(без изменений, работает с Iceberg через Spark)* |

| Было (Scala) | Стало |
|---|---|
| `spark.read.parquet(path)` | `spark.read.format("iceberg").load("ns.table")` |
| `df.write.mode("overwrite").parquet(path)` | `df.writeTo("ns.table").overwritePartitions()` |

- [ ] **Step 1: Написать тест JVM-трансформера**

```python
# tests/test_transformer_jvm.py
from skills.parquet_to_iceberg.transformers.jvm import transform_jvm_file


def test_transforms_java_spark_read():
    source = '''package com.example;
import org.apache.spark.sql.Dataset;

public class Job {
    Dataset<Row> df = spark.read().parquet("data/events/");
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert 'spark.read().parquet' not in result
    assert 'format("iceberg")' in result
    assert '"default.events"' in result


def test_transforms_java_spark_write():
    source = '''package com.example;
public class Job {
    public void run(Dataset<Row> df) {
        df.write().mode("overwrite").parquet("output/");
    }
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert '.parquet(' not in result
    assert 'writeTo("default.events")' in result
    assert 'overwritePartitions()' in result


def test_transforms_scala_spark_read():
    source = '''package com.example
import org.apache.spark.sql.{DataFrame, SparkSession}

object Job {
  def load(spark: SparkSession): DataFrame = spark.read.parquet("data/")
}
'''
    result = transform_jvm_file(source, language="scala", table_name="events", namespace="default")
    assert 'spark.read.parquet' not in result
    assert 'format("iceberg")' in result


def test_transforms_scala_spark_write():
    source = '''object Job {
  def save(df: DataFrame): Unit = df.write.mode("overwrite").parquet("out/")
}
'''
    result = transform_jvm_file(source, language="scala", table_name="events", namespace="default")
    assert '.parquet(' not in result
    assert 'writeTo("default.events")' in result


def test_transforms_hive_stored_as_parquet():
    source = '''public class Hive {
    public void run() {
        spark.sql("CREATE TABLE events (id BIGINT, status STRING) STORED AS PARQUET");
    }
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert 'STORED AS PARQUET' not in result
    assert 'USING iceberg' in result


def test_transforms_save_as_table():
    source = '''public class Hive {
    public void run(Dataset<Row> df) {
        df.write().mode("overwrite").saveAsTable("events");
    }
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert 'saveAsTable' not in result
    assert 'writeTo("default.events")' in result
    assert 'createOrReplace()' in result


def test_preserves_unrelated_lines():
    source = '''public class Job {
    private static final String APP = "etl";
    public Dataset<Row> load() {
        Dataset<Row> df = spark.read().parquet("in/");
        return df.filter("status = 'active'");
    }
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert 'APP = "etl"' in result
    assert 'filter("status = \\'active\\'")' in result or "filter(\"status = 'active'\")" in result


def test_java_spark_write_preserves_partition_by():
    # Chained .partitionBy(...) should become part of writeTo flow; we accept it as a comment hint
    source = '''df.write().partitionBy("day").mode("overwrite").parquet("out/");
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert '.parquet(' not in result
    assert 'writeTo("default.events")' in result
    # partitionBy info preserved as TODO comment for manual adjustment
    assert 'partitionBy' in result or 'TODO' in result
```

- [ ] **Step 2: Запустить — убедиться что падает**

```bash
pytest tests/test_transformer_jvm.py -v
```

Ожидаемый вывод: `ImportError: No module named 'skills.parquet_to_iceberg.transformers.jvm'`

- [ ] **Step 3: Реализовать `jvm.py`**

```python
# skills/parquet-to-iceberg/transformers/jvm.py
import re
from typing import Literal


_JAVA_LINE_TERM = ";"
_SCALA_LINE_TERM = ""


def transform_jvm_file(
    source: str,
    *,
    language: Literal["java", "scala"],
    table_name: str,
    namespace: str,
) -> str:
    fqn = f"{namespace}.{table_name}"
    term = _JAVA_LINE_TERM if language == "java" else _SCALA_LINE_TERM

    # Pattern 1: Hive CREATE TABLE ... STORED AS PARQUET → USING iceberg
    source = re.sub(
        r'("\s*CREATE\s+TABLE[^"]*?)\bSTORED\s+AS\s+PARQUET\b([^"]*")',
        lambda m: m.group(1).rstrip() + " USING iceberg" + m.group(2),
        source,
        flags=re.IGNORECASE,
    )

    # Pattern 2: Java .write().[mods...].saveAsTable("x") → .writeTo("ns.x").createOrReplace()
    # Java syntax with parens: .write()
    source = re.sub(
        r'\.write\(\)(?:\.\w+\([^)]*\))*\.saveAsTable\s*\(\s*"[^"]*"\s*\)',
        f'.writeTo("{fqn}").createOrReplace()',
        source,
    )
    # Scala syntax without parens: .write
    source = re.sub(
        r'\.write(?:\.\w+\([^)]*\))*\.saveAsTable\s*\(\s*"[^"]*"\s*\)',
        f'.writeTo("{fqn}").createOrReplace()',
        source,
    )

    # Pattern 3: Java spark.read().parquet(path) → spark.read().format("iceberg").load("ns.t")
    source = re.sub(
        r'\.read\(\)\.parquet\s*\(\s*"[^"]*"\s*\)',
        f'.read().format("iceberg").load("{fqn}")',
        source,
    )
    # Pattern 3-Scala: spark.read.parquet("...") → spark.read.format("iceberg").load("ns.t")
    source = re.sub(
        r'\.read\.parquet\s*\(\s*"[^"]*"\s*\)',
        f'.read.format("iceberg").load("{fqn}")',
        source,
    )

    # Pattern 4: Java .write().[mods].parquet("path") → .writeTo("ns.t").overwritePartitions()
    # Preserve partitionBy info as comment if present
    def _replace_java_write(match: re.Match) -> str:
        chain = match.group(0)
        pb_match = re.search(r'\.partitionBy\(([^)]*)\)', chain)
        replacement = f'.writeTo("{fqn}").overwritePartitions()'
        if pb_match:
            replacement += f" /* TODO: partitionBy({pb_match.group(1)}) — add to Iceberg partition spec */"
        return replacement

    source = re.sub(
        r'\.write\(\)(?:\.\w+\([^)]*\))*\.parquet\s*\(\s*"[^"]*"\s*\)',
        _replace_java_write,
        source,
    )
    # Scala write chain
    source = re.sub(
        r'\.write(?:\.\w+\([^)]*\))*\.parquet\s*\(\s*"[^"]*"\s*\)',
        _replace_java_write,
        source,
    )

    return source
```

- [ ] **Step 4: Запустить тесты JVM-трансформера**

```bash
pytest tests/test_transformer_jvm.py -v
```

Ожидаемый вывод: все 8 тестов `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add skills/parquet-to-iceberg/transformers/jvm.py tests/test_transformer_jvm.py
git commit -m "feat: implement java/scala spark + hive parquet→iceberg transformer"
```

---

## Task 7: Обновление зависимостей

**Files:**
- Create: `skills/parquet-to-iceberg/deps.py`
- Create: `tests/test_deps.py`

Модуль находит `requirements.txt` / `pyproject.toml` / `pom.xml` / `build.gradle` в проекте и добавляет нужные Iceberg-зависимости:
- **Python:** `pyiceberg[sql-sqlite]>=0.7.0`
- **Maven:** `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0`
- **Gradle:** то же самое, через `implementation` или `compile`

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


# ─── Maven (pom.xml) ───────────────────────────────────────────────────

def test_adds_iceberg_to_pom_xml(tmp_path):
    pom = tmp_path / "pom.xml"
    pom.write_text('''<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>job</artifactId>
    <version>1.0.0</version>
    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.5.0</version>
        </dependency>
    </dependencies>
</project>
''')
    update_dependencies(tmp_path)
    content = pom.read_text()
    assert "iceberg-spark-runtime" in content
    assert "org.apache.iceberg" in content
    # Existing spark-sql dep preserved
    assert "spark-sql_2.12" in content


def test_does_not_duplicate_iceberg_in_pom(tmp_path):
    pom = tmp_path / "pom.xml"
    pom.write_text('''<project>
    <dependencies>
        <dependency>
            <groupId>org.apache.iceberg</groupId>
            <artifactId>iceberg-spark-runtime-3.5_2.12</artifactId>
            <version>1.5.0</version>
        </dependency>
    </dependencies>
</project>
''')
    update_dependencies(tmp_path)
    assert pom.read_text().count("iceberg-spark-runtime") == 1


# ─── Gradle (build.gradle) ─────────────────────────────────────────────

def test_adds_iceberg_to_build_gradle(tmp_path):
    gradle = tmp_path / "build.gradle"
    gradle.write_text('''plugins {
    id 'java'
}
repositories { mavenCentral() }
dependencies {
    implementation 'org.apache.spark:spark-sql_2.12:3.5.0'
}
''')
    update_dependencies(tmp_path)
    content = gradle.read_text()
    assert "iceberg-spark-runtime" in content
    assert "implementation" in content


def test_does_not_duplicate_iceberg_in_gradle(tmp_path):
    gradle = tmp_path / "build.gradle"
    gradle.write_text('''dependencies {
    implementation 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0'
}
''')
    update_dependencies(tmp_path)
    assert gradle.read_text().count("iceberg-spark-runtime") == 1
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

# Iceberg Spark runtime coordinates (matches Spark 3.5 + Scala 2.12)
ICEBERG_GROUP = 'org.apache.iceberg'
ICEBERG_ARTIFACT = 'iceberg-spark-runtime-3.5_2.12'
ICEBERG_VERSION = '1.5.0'


def update_dependencies(project_root: Path) -> None:
    req_file = project_root / "requirements.txt"
    pyproject = project_root / "pyproject.toml"
    pom = project_root / "pom.xml"
    gradle = project_root / "build.gradle"
    gradle_kts = project_root / "build.gradle.kts"

    if req_file.exists():
        _update_requirements_txt(req_file)
    if pyproject.exists():
        _update_pyproject_toml(pyproject)
    if pom.exists():
        _update_pom_xml(pom)
    if gradle.exists():
        _update_build_gradle(gradle)
    if gradle_kts.exists():
        _update_build_gradle(gradle_kts)


def _update_requirements_txt(path: Path) -> None:
    content = path.read_text()
    if "pyiceberg" in content:
        return
    path.write_text(content.rstrip() + f"\n{PYICEBERG_REQ}\n")


def _update_pyproject_toml(path: Path) -> None:
    content = path.read_text()
    if "pyiceberg" in content:
        return
    content = re.sub(
        r'(dependencies\s*=\s*\[)(.*?)(\])',
        lambda m: m.group(1) + m.group(2).rstrip() + f'\n  "{PYICEBERG_REQ}",\n' + m.group(3),
        content,
        flags=re.DOTALL,
    )
    path.write_text(content)


def _update_pom_xml(path: Path) -> None:
    content = path.read_text()
    if ICEBERG_ARTIFACT in content:
        return
    iceberg_dep = (
        f'        <dependency>\n'
        f'            <groupId>{ICEBERG_GROUP}</groupId>\n'
        f'            <artifactId>{ICEBERG_ARTIFACT}</artifactId>\n'
        f'            <version>{ICEBERG_VERSION}</version>\n'
        f'        </dependency>\n'
    )
    # Insert before closing </dependencies>
    if '</dependencies>' in content:
        content = content.replace('</dependencies>', iceberg_dep + '    </dependencies>', 1)
    else:
        # No <dependencies> block — add one before </project>
        block = f'    <dependencies>\n{iceberg_dep}    </dependencies>\n'
        content = content.replace('</project>', block + '</project>', 1)
    path.write_text(content)


def _update_build_gradle(path: Path) -> None:
    content = path.read_text()
    if ICEBERG_ARTIFACT in content:
        return
    coordinate = f"'{ICEBERG_GROUP}:{ICEBERG_ARTIFACT}:{ICEBERG_VERSION}'"
    line = f"    implementation {coordinate}\n"
    # Insert inside dependencies { ... } block
    if re.search(r'dependencies\s*\{', content):
        content = re.sub(
            r'(dependencies\s*\{)',
            lambda m: m.group(1) + "\n" + line,
            content,
            count=1,
        )
    else:
        content = content.rstrip() + f"\n\ndependencies {{\n{line}}}\n"
    path.write_text(content)
```

- [ ] **Step 4: Запустить тесты**

```bash
pytest tests/test_deps.py -v
```

Ожидаемый вывод: все 8 тестов `PASSED` (Python ×4, Maven ×2, Gradle ×2).

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

FIXTURES = Path(__file__).parent / "fixtures"
PANDAS_FIXTURE = FIXTURES / "python-pandas"
JAVA_FIXTURE = FIXTURES / "java-spark"
HIVE_FIXTURE = FIXTURES / "java-hive"


def _run_cli(project: Path, table: str = "events", namespace: str = "default") -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "skills.parquet_to_iceberg.cli",
         str(project), "--table", table, "--namespace", namespace],
        capture_output=True, text=True,
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
from .transformers.jvm import transform_jvm_file

_PANDAS_TYPES = {"pandas_read", "pandas_write"}
_PYSPARK_TYPES = {"pyspark_read", "pyspark_write"}
_PYARROW_TYPES = {"pyarrow_read", "pyarrow_write"}
_JVM_TYPES = {
    "java_spark_read", "java_spark_write",
    "scala_spark_read", "scala_spark_write",
    "hive_create_parquet", "hive_save_as_table", "hive_insert_overwrite",
}


def convert_project(project_root: Path, *, table_name: str, namespace: str) -> int:
    matches = detect_parquet_usage(project_root)
    if not matches:
        print("No Parquet usage found.")
        return 0

    files_by_type: dict[Path, set[str]] = {}
    for m in matches:
        files_by_type.setdefault(m.file, set()).add(m.pattern_type)

    converted = 0
    for src_file, types in files_by_type.items():
        source = src_file.read_text()
        kw = {"table_name": table_name, "namespace": namespace}
        suffix = src_file.suffix.lower()

        if suffix == ".py":
            if types & _PANDAS_TYPES:
                source = transform_pandas_file(source, **kw)
            if types & _PYSPARK_TYPES:
                source = transform_pyspark_file(source, **kw)
            if types & _PYARROW_TYPES:
                source = transform_pyarrow_file(source, **kw)
        elif suffix == ".java":
            if types & _JVM_TYPES:
                source = transform_jvm_file(source, language="java", **kw)
        elif suffix == ".scala":
            if types & _JVM_TYPES:
                source = transform_jvm_file(source, language="scala", **kw)
        else:
            continue

        src_file.write_text(source)
        print(f"  Converted: {src_file.relative_to(project_root)}")
        converted += 1

    update_dependencies(project_root)
    print(f"\nConverted {converted} file(s). Updated dependencies.")
    print("Next steps:")
    print("  Python: pip install pyiceberg[sql-sqlite]")
    print("  JVM:    ensure iceberg-spark-runtime is on the classpath (added to pom.xml/build.gradle)")
    print("  Create Iceberg table schema and run your tests")
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
description: Converts a Python or Java/Scala project from Parquet (and Hive-parquet) read/write to Apache Iceberg tables
trigger: "convert parquet" OR "migrate to iceberg" OR "parquet to iceberg" OR "migrate hive to iceberg"
---

# Parquet → Iceberg Conversion Skill

**Announce at start:** "I'm using the parquet-to-iceberg skill to convert this project."

## What This Skill Does

Scans the project for Parquet / Hive-parquet operations and replaces them with Apache Iceberg equivalents:

- **Python:** pandas, PySpark, pyarrow → pyiceberg
- **Java / Scala:** Spark Dataset API (`spark.read().parquet()`) → Iceberg Spark runtime (`spark.read().format("iceberg")`)
- **Hive via SparkSQL:** `STORED AS PARQUET` / `saveAsTable` / `INSERT OVERWRITE TABLE` → Iceberg-backed tables (`USING iceberg`, `writeTo(...)`)

Also updates project dependencies (`requirements.txt`, `pyproject.toml`, `pom.xml`, `build.gradle`).

## Step-by-Step Process

### 1. Identify Project Type

Look for build files to determine stack:
- `requirements.txt` / `pyproject.toml` → **Python**
- `pom.xml` → **Java + Maven**
- `build.gradle` / `build.gradle.kts` → **Java/Scala + Gradle**
- `*.java` / `*.scala` files → JVM project

The skill handles all of these automatically — the detector scans `.py`, `.java`, `.scala` files.

### 2. Detect Parquet & Hive Usage

Read source files and identify patterns. The skill looks for:

**Python:**
- `pd.read_parquet(...)` / `df.to_parquet(...)`
- `spark.read.parquet(...)` / `df.write.parquet(...)`
- `pq.read_table(...)` / `pq.write_table(...)`

**Java:**
- `spark.read().parquet(...)` / `df.write().parquet(...)`
- `df.write().saveAsTable("...")`
- `spark.sql("CREATE TABLE ... STORED AS PARQUET")`
- `spark.sql("INSERT OVERWRITE TABLE ...")`

**Scala:**
- `spark.read.parquet(...)` / `df.write.parquet(...)` (без скобок после `read`/`write`)

### 3. Ask the User for Iceberg Table Details

Before converting, ask:
- **Table name** — what should the Iceberg table be called?
- **Namespace** — default namespace is `"default"`
- **Catalog type** — local SQLite (dev), Hive Metastore, AWS Glue, REST?
- **For JVM projects:** does the target Spark cluster already have `iceberg-spark-runtime` on the classpath, or should we add it?

### 4. Run the Conversion

\`\`\`bash
python -m skills.parquet_to_iceberg.cli <project_path> --table <TABLE_NAME> --namespace <NAMESPACE>
\`\`\`

### 5. Review and Fix Edge Cases

After automated conversion, manually review:

- **Multiple tables** — the tool assumes one table per project; split and re-run per table if needed
- **Schema definitions** — Iceberg requires explicit schema. Extract from existing parquet:
  \`\`\`python
  import pyarrow.parquet as pq
  schema = pq.read_schema("existing.parquet")
  \`\`\`
- **Partitioning** — `partitionBy("day")` is preserved as a `TODO` comment in the JVM transformer output. Add to the Iceberg partition spec manually:
  \`\`\`sql
  ALTER TABLE default.events ADD PARTITION FIELD day
  \`\`\`
- **Hive metastore catalog** — if the original project used Hive MetaStore, configure Iceberg's HiveCatalog:
  \`\`\`
  spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.hive_prod.type = hive
  spark.sql.catalog.hive_prod.uri = thrift://metastore:9083
  \`\`\`
- **Existing Hive tables with data** — use Iceberg's `system.migrate` procedure to convert in place:
  \`\`\`sql
  CALL hive_prod.system.migrate('db.events')
  \`\`\`

### 6. Create the Iceberg Table

**Python (pyiceberg):**
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

**Java/Scala (Spark SQL):**
\`\`\`sql
CREATE TABLE default.events (
  id BIGINT NOT NULL,
  status STRING,
  value DOUBLE
) USING iceberg
\`\`\`

### 7. Run Existing Tests

**Python:**
\`\`\`bash
pip install pyiceberg[sql-sqlite]
pytest tests/ -v
\`\`\`

**Java/Maven:**
\`\`\`bash
mvn test
\`\`\`

**Scala/Gradle:**
\`\`\`bash
./gradlew test
\`\`\`

Common test failures:
- Tests use `tmp_path` for parquet file path but Iceberg catalog uses a fixed URI — inject catalog via fixture
- Assertions on file existence (`Path("data.parquet").exists()`) — replace with table existence checks

### 8. Commit

\`\`\`bash
git add -A
git commit -m "refactor: migrate parquet read/write to Apache Iceberg"
\`\`\`

## Conversion Reference — Python

| Before (Parquet) | After (Iceberg) |
|---|---|
| `pd.read_parquet(path)` | `catalog.load_table((ns, name)).scan().to_pandas()` |
| `df.to_parquet(path)` | `tbl.overwrite(df)` |
| `spark.read.parquet(path)` | `spark.table("ns.name")` |
| `df.write.parquet(path)` | `df.writeTo("ns.name").overwritePartitions()` |
| `pq.read_table(path)` | `tbl.scan().to_arrow()` |
| `pq.write_table(table, path)` | `tbl.overwrite(table)` |

## Conversion Reference — Java/Scala Spark

| Before (Java) | After (Iceberg) |
|---|---|
| `spark.read().parquet("path")` | `spark.read().format("iceberg").load("ns.table")` |
| `df.write().mode("overwrite").parquet("path")` | `df.writeTo("ns.table").overwritePartitions()` |
| `df.write().saveAsTable("t")` | `df.writeTo("ns.t").createOrReplace()` |
| `df.write().partitionBy("day").parquet(...)` | `df.writeTo("ns.t").overwritePartitions()` *(+ TODO comment for partition spec)* |

| Before (Scala) | After (Iceberg) |
|---|---|
| `spark.read.parquet("path")` | `spark.read.format("iceberg").load("ns.table")` |
| `df.write.mode("overwrite").parquet("path")` | `df.writeTo("ns.table").overwritePartitions()` |

## Conversion Reference — Hive / SparkSQL

| Before | After |
|---|---|
| `"CREATE TABLE t (...) STORED AS PARQUET"` | `"CREATE TABLE t (...) USING iceberg"` |
| `df.write().saveAsTable("t")` | `df.writeTo("ns.t").createOrReplace()` |
| `"INSERT OVERWRITE TABLE t SELECT ..."` | *(no change — Spark handles Iceberg tables via the same SQL, assuming catalog is configured)* |
| Existing Hive table with data | `CALL catalog.system.migrate('db.t')` — manual step |

## Dependencies Added

| Ecosystem | File | Dependency |
|---|---|---|
| Python | `requirements.txt` / `pyproject.toml` | `pyiceberg[sql-sqlite]>=0.7.0` |
| Maven | `pom.xml` | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0` |
| Gradle | `build.gradle` | `implementation 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0'` |

## Known Limitations

- **Multi-table projects** require manual splitting by table (run CLI per table)
- **Streaming writes** (Kafka → Parquet, Structured Streaming to parquet sinks) are out of scope
- **Cloud catalog configs** (Glue, Nessie, REST) need manual setup — this tool generates SQLite dev config for Python, and leaves JVM catalog config untouched
- **Hive table data migration** — the tool rewrites *code* but does not migrate existing parquet data; use `CALL system.migrate(...)` for in-place migration or `CTAS` for copy
- **Schema inference** — partition specs are not automatically derived; `partitionBy(...)` becomes a `TODO` comment for the JVM transformer
- **Scala Scala 2.13 / Spark 3.4** — the generated Maven/Gradle coordinates target Spark 3.5 + Scala 2.12; adjust manually for other versions
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

*Self-review: все требования покрыты — фикстуры Python/Java/Scala/Hive (Tasks 1-2), детектор для `.py` + `.java` + `.scala` с Hive-паттернами (Task 3), Python-трансформеры pandas/PySpark/pyarrow (Tasks 4-6), JVM-трансформер Java/Scala Spark + Hive (Task 6.5), deps updater для requirements.txt / pyproject.toml / pom.xml / build.gradle (Task 7), CLI с диспатчем по расширению файла (Task 8), интеграционные тесты против pandas/java-spark/java-hive фикстур, SKILL.md с таблицами соответствий для Python/JVM/Hive (Task 9). Нет плейсхолдеров. Имена функций консистентны: `transform_jvm_file(source, language=..., table_name=..., namespace=...)` используется и в тестах, и в CLI.*
