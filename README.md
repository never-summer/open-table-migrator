# open-table-migrator

Skill + субагент для Claude Code.

Анализирует data-проекты на **Python, Java и Scala** (расширяемо): находит все точки чтения/записи данных, строит карту I/O-операций и мигрирует на open table format.

| | |
|---|---|
| **Детектор** | Парсит код в AST (Abstract Syntax Tree) через tree-sitter; алгоритмический обход дерева находит все I/O |
| **Миграция** | Parquet / ORC → Iceberg (протестировано), архитектура — any → any |
| **Целевые форматы** | Iceberg сейчас, Paimon / Delta / Hudi / итд планируются |

![I/O detection and migration](docs/assets/open_table_migrator_io_v3.svg)

---

## Быстрый старт

### Вариант 1: Субагент в Claude Code

Просто скажите агенту:

> *возьми скилл из https://github.com/never-summer/open-table-migrator и давай проверим на проекте LearningSparkV2 — проведем анализ I/O*

Агент подхватит скилл, запустит детектор и выдаст отчёт. Пример на [LearningSparkV2](https://github.com/databricks/LearningSparkV2):

> **Обнаружено 11 I/O-операций в 7 файлах.**
>
> По направлению: **reads = 7**, **writes = 4**
>
> | Тип | Кол-во | Направление |
> |---|---|---|
> | `spark_read_csv` | 3 | read |
> | `spark_read_table` | 2 | read |
> | `hive_save_table` | 2 | write |
> | `spark_read_parquet` | 1 | read |
> | `spark_read_json` | 1 | read |
> | `pandas_write_csv` | 1 | write |
> | `stdlib_write_csv` | 1 | write |
>
> **Кандидаты на миграцию в Iceberg (формат-зависимые):**
>
> - `chapter7/scala/.../SortMergeJoinBucketed_7_6.scala:54` — `write.format("parquet")...saveAsTable("UsersTbl")` + `.bucketBy(8,"uid")`
> - `chapter7/scala/.../SortMergeJoinBucketed_7_6.scala:62` — `write.format("parquet")...saveAsTable("OrdersTbl")` + `.bucketBy(8,"users_id")`
> - `mlflow-project-example/train.py:24` — `spark.read.parquet(file_path)` (airbnb dataset)

Дальше агент спрашивает по каждой таблице — мигрировать или оставить, в какой namespace/table — и выдаёт `lakehouse-worklist.json` для LLM, который переписывает код. После — повторный прогон детектора: ноль остаточных паттернов.

[Субагент](.claude/agents/open-table-migrator.md) делает всё это автоматически по одной фразе: *"мигрируй на iceberg"* / *"migrate this project to iceberg"*.

### Вариант 2: CLI (без LLM)

Анализ проекта:

```bash
PYTHONPATH=. python -c "
from pathlib import Path
from skills.open_table_migrator.detector import detect_all_io
from skills.open_table_migrator.analyzer import build_report, format_report

matches = detect_all_io(Path('путь/к/проекту'))
print(format_report(build_report(matches), project_root=Path('путь/к/проекту')))
"
```

Миграция одной таблицы — выдаёт `lakehouse-worklist.json`:

```bash
PYTHONPATH=. python -m skills.open_table_migrator.cli путь/к/проекту \
    --table events --namespace analytics
```

Миграция нескольких таблиц:

```bash
PYTHONPATH=. python -m skills.open_table_migrator.cli путь/к/проекту \
    --mapping ./iceberg-mapping.json
```

Формат маппинга — в [SKILL.md](skills/open_table_migrator/SKILL.md#multi-table-projects).

---

## Возможности

### Инвентаризация I/O

Сканирует `.py`, `.java`, `.scala` файлы через tree-sitter AST и находит **все** операции чтения и записи. Для каждой определяется:

- **Направление** — read / write / schema
- **Объект** (subject) — имя DataFrame или переменной
- **Цель** (path_arg) — путь или имя таблицы
- **Краткое описание** — например: `usersDF — writes Parquet to s3://bucket/users [partitionBy("region")]`

Таксономия pattern_type: `{runtime}_{direction}_{format}` (напр. `spark_read_parquet`, `pandas_write_csv`).

### Миграция → Lakehouse

AST-детектор находит операции, CLI выдает `lakehouse-worklist.json`, агент/LLM переписывает код.

Конвертирует:

- pandas → pyiceberg (`catalog.load_table(...).scan().to_pandas()`)
- PySpark → `spark.table()` / `df.writeTo().overwritePartitions()`
- Java/Scala Spark → `format("iceberg")` / `writeTo()`
- Hive DDL → `USING iceberg`
- Зависимости: `requirements.txt`, `pyproject.toml`, `pom.xml`, `build.gradle[.kts]`, `build.sbt`

### SQL-реестр

Сканирует `.sql`/`.hql`/`.ddl` файлы, находит `CREATE TABLE ... STORED AS FORMAT` и строит кросс-ссылки с кодом — когда код пишет в таблицу через `saveAsTable("events")`, а формат определен в отдельном SQL-файле.

---

## Тесты

```bash
PYTHONPATH=. pytest tests/ --ignore=tests/fixtures -v
```

236 тестов. Фикстуры в `tests/fixtures/` — входные данные, не тестовые модули.

## Структура

```
skills/open_table_migrator/
├── SKILL.md              # Справочная документация
├── detector.py           # Публичный API (detect_parquet_usage / detect_all_io)
├── ts_detector.py        # Tree-sitter AST-детектор (Python/Java/Scala)
├── ts_parser.py          # Обёртка tree-sitter: парсинг, кеш Language/Parser
├── analyzer.py           # Отчеты, дедупликация, SQL кросс-ссылки
├── sql_registry.py       # Реестр таблиц из .sql/.hql/.ddl
├── extract.py            # Извлечение path_arg, subject, описания
├── folding.py            # Склейка многострочных цепочек (только для JVM-трансформера)
├── filters.py            # Фильтрация по направлению/паттерну/glob
├── targets.py            # Мульти-таблица: маппинг, резолвер
├── deps.py               # Обновление зависимостей (5 форматов)
├── prepass.py            # Skip-маркеры + pyspark conf
├── worklist.py           # lakehouse-worklist.json (hybrid)
├── cli.py                # CLI entry point
└── transformers/
    ├── pandas.py
    ├── pyspark.py
    ├── pyarrow.py
    └── jvm.py            # Java + Scala

.claude/agents/
└── open-table-migrator.md  # Субагент
```

## Детектор: tree-sitter AST

Детектор использует [tree-sitter](https://tree-sitter.github.io/) для парсинга Python, Java и Scala. Вместо regex — обход AST-дерева:

- **Нет ложных срабатываний** — AST отличает код от строк и комментариев
- **Нет ручного folding** — дерево знает границы выражений
- **Динамические форматы** — любой `.read.FORMAT()` попадает автоматически
- **Единая таксономия** — `{runtime}_{direction}_{format}` (напр. `spark_read_parquet`, `pandas_write_csv`)

Regex-детектор сохранён в ветке `regex-detector`.

### Поддерживаемые паттерны

| Формат | Примеры паттернов |
|---|---|
| Parquet | `pd.read_parquet`, `spark.read.parquet`, `pq.write_table`, `.format("parquet")` |
| ORC | `pd.read_orc`, `orc.read_table`, `.format("orc")` |
| CSV | `pd.read_csv`, `spark.read.csv`, `.format("csv")`, `csv.reader` |
| JSON | `pd.read_json`, `.format("json")` |
| Avro | `.format("avro")` |
| Delta | `.format("delta")` |
| JDBC | `spark.read.jdbc`, `.format("jdbc")` |
| Text | `spark.read.text`, `.format("text")` |
| Hive DDL | `CREATE TABLE ... STORED AS FORMAT`, `USING format` |
| Hive DML | `INSERT INTO TABLE`, `INSERT OVERWRITE TABLE`, `saveAsTable` |
| SQL-файлы | `.sql`, `.hql`, `.ddl` — реестр таблиц + кросс-ссылки с кодом |
| *Любой* | Динамическое извлечение — `.read.protobuf()`, `.format("tfrecord")`, и т.д. |

## Ограничения

- Path-аргументы должны быть строковыми литералами (переменные → `TODO(iceberg)`)
- Streaming — только warn-only (TODO-комментарий)
- Данные не мигрируются — только код; для Hive используйте `CALL catalog.system.migrate(...)`
- JVM-координаты: Spark 3.5 + Scala 2.12
- `partitionBy(...)` в JVM → TODO для ручного добавления в Iceberg partition spec

Полный список — в [SKILL.md § Known Limitations](skills/open_table_migrator/SKILL.md#known-limitations).
