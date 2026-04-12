# Tree-sitter Detector — Design Spec

**Дата:** 2026-04-12
**Scope:** Замена regex-детектора на tree-sitter AST-based детектор в `skills/open_table_migrator/`
**Ветка с текущим regex-кодом:** `regex-detector` (коммит `68444e6`)

## Контекст и мотивация

Текущий детектор (`detector.py`) использует ~80 скомпилированных regex-паттернов + `folding.py` (ручная склейка многострочных fluent chains) + `extract.py` (regex-извлечение path_arg и subject). Проблемы:

1. **Хрупкость на многострочных цепочках** — `folding.py` склеивает строки начинающиеся с `.`, но ломается на нестандартных отступах, комментариях между строками цепочки, и конструкциях вида `val x = \n  spark.read...`
2. **Ложные срабатывания** — regex не отличает код от строковых литералов и комментариев. `logger.info("writing parquet to ...")` матчится как parquet-запись.
3. **Дублирование паттернов** — `.read.parquet()` (Scala) vs `.read().parquet()` (Java) — два отдельных regex, хотя семантически одно и то же. Аналогично `.format("parquet")` vs `.parquet()`.
4. **Фиксированный список форматов** — чтобы добавить новый формат (protobuf, feather, tfrecord), нужно дописать regex-паттерн вручную.

## Решения

| Вопрос | Решение |
|---|---|
| Зависимости | `tree-sitter` + `tree-sitter-python` + `tree-sitter-java` + `tree-sitter-scala` (отдельные пакеты) |
| Scope | Только детектор. Трансформеры остаются regex-based |
| Стратегия замены | Чистая замена (не параллельная работа двух детекторов) |
| Поиск паттернов | Tree-sitter S-expression queries (декларативно) |
| Извлечение данных | Программная навигация по AST-нодам |
| Таксономия pattern_type | Упрощённая: `{runtime}_{direction}_{format}` с динамическим format |

## Архитектура

Три слоя:

```
┌─────────────────────────────────────────┐
│  1. Parser layer  (ts_parser.py)        │
│     tree-sitter парсит файл → AST Tree  │
├─────────────────────────────────────────┤
│  2. Query layer   (queries/*.scm)       │
│     S-expression queries матчат паттерны│
├─────────────────────────────────────────┤
│  3. Extract layer (extract.py)          │
│     matched-ноды → PatternMatch         │
│     (path_arg, subject, format, line)   │
└─────────────────────────────────────────┘
```

### 1. Parser layer — `ts_parser.py`

Новый модуль. Обёртка над tree-sitter:

```python
from tree_sitter import Language, Parser

def parse(source: bytes, language: str) -> Tree:
    """Parse source code into a tree-sitter AST.
    language: 'python' | 'java' | 'scala'
    """

def language_for_file(path: Path) -> str | None:
    """Return language name by file extension, or None if unsupported."""
```

Кеширует `Language` и `Parser` объекты (создаются один раз за процесс). Выбор грамматики по расширению: `.py` → python, `.java` → java, `.scala` → scala.

### 2. Query layer — `queries/*.scm`

Директория с S-expression query-файлами, по одному на runtime:

| Файл | Что матчит |
|---|---|
| `spark.scm` | `.read.FORMAT()`, `.write.FORMAT()`, `.read.format("X").load()`, `.write.format("X").save()`, streaming variants |
| `pandas.scm` | `pd.read_FORMAT()`, `.to_FORMAT()` |
| `pyarrow.scm` | `pq.read_table()`, `pq.write_table()`, `orc.read_table()`, `pa.dataset.*` |
| `hive_sql.scm` | `STORED AS FORMAT`, `USING format`, `INSERT INTO/OVERWRITE`, `saveAsTable` — внутри строковых литералов |
| `misc_io.scm` | `csv.reader/writer`, `FileWriter`, `spark.table()` |

Каждый query использует named captures:
- `@match` — вся нода операции (для line range и original_code)
- `@format` — нода с именем формата (для динамического извлечения)
- `@path_arg` — нода с аргументом-путём (строковый литерал)
- `@subject` — нода с переменной-субъектом (если доступна в query)

Пример query для spark:

```scheme
;; spark.read.FORMAT(path) — прямой вызов
(call_expression
  function: (field_expression
    value: (field_expression
      field: (identifier) @_read)
    field: (identifier) @format)
  arguments: (arguments
    (string) @path_arg)
  (#match? @_read "^read(Stream)?$")) @match

;; .write.format("X").save(path)
(call_expression
  function: (field_expression
    field: (identifier) @_save)
  arguments: (arguments
    (string) @path_arg)
  (#match? @_save "^(save|load)$")) @match
```

**Формат извлекается из кода, а не из предопределённого списка.** Если кто-то напишет `.read.protobuf("data/")` — мы его поймаем.

### 3. Extract layer — обновлённый `extract.py`

Получает matched-ноды от query layer. Навигация по AST:

- **path_arg**: спускается в `arguments` children, ищет первый/второй `string` node в зависимости от API (write_table — второй arg, read_parquet — первый).
- **subject**: поднимается по `node.parent` до `assignment` / `variable_declarator`, берёт left-hand side. Для method chains — находит корень цепочки.
- **format**: берётся из `@format` capture, или из имени метода (`read_parquet` → `parquet`), или из аргумента `.format("X")`.
- **direction**: определяется из контекста — `read`/`readStream` vs `write`/`writeStream` vs `create`/`insert`.

`summarize_operation()` остаётся, но получает данные из AST вместо regex.

## Новая таксономия pattern_type

Формат: `{runtime}_{direction}_{format}`

**runtime** извлекается из структуры вызова в AST:
- `pandas` — `pd.read_*`, `.to_*`
- `spark` — `.read.*`, `.write.*` (batch)
- `spark_stream` — `.readStream.*`, `.writeStream.*`
- `pyarrow` — `pq.*`, `orc.*`, `pa.dataset.*`
- `hive` — SQL DDL/DML внутри строковых литералов, `.saveAsTable()`
- `stdlib` — `csv.*`, `FileWriter`, `BufferedWriter`

**direction** извлекается из AST:
- `read` / `write` — чтение / запись данных
- `create` / `insert` / `save` — DDL / DML

**format** извлекается динамически из AST-ноды:
- Из имени метода: `.parquet()` → `parquet`, `read_csv()` → `csv`
- Из аргумента: `.format("avro")` → `avro`
- Из SQL: `STORED AS ORC` → `orc`, `USING delta` → `delta`
- Любой формат попадает — список не ограничен

Примеры:
- `spark_read_parquet` (бывшие `pyspark_read`, `java_spark_read`, `scala_spark_read`, `pyspark_read_fmt`, ...)
- `pandas_write_csv` (бывший `pandas_csv_write`)
- `spark_read_protobuf` (новый — regex не ловил)
- `hive_create_orc` (бывший `hive_create_orc`)

### PatternMatch — изменения

```python
@dataclass
class PatternMatch:
    file: Path
    line: int
    pattern_type: str          # "{runtime}_{direction}_{format}"
    original_code: str
    path_arg: str | None = None
    end_line: int | None = None
    format: str | None = None  # НОВОЕ: "parquet", "csv", "protobuf", ...
```

### Обратная совместимость в analyzer.py

Текущие `_READ_TYPES`, `_WRITE_TYPES`, `_SCHEMA_TYPES` sets (80+ строк) заменяются на парсинг из pattern_type:

```python
def direction_of(pattern_type: str) -> str:
    parts = pattern_type.split("_")
    # second part is direction: read, write, create, insert, save
    if len(parts) >= 2:
        d = parts[1]
        if d in ("read", "stream") and "read" in pattern_type:
            return "read"
        if d in ("write", "stream") and "write" in pattern_type:
            return "write"
        if d in ("create", "insert", "save"):
            return "schema" if d == "create" else "write"
    return "unknown"

def is_migration_candidate(pattern_type: str) -> bool:
    fmt = pattern_type.rsplit("_", 1)[-1]
    return fmt in {"parquet", "orc"}
```

## Удаляемые / изменяемые файлы

| Файл | Действие |
|---|---|
| `folding.py` | Удаляется — tree-sitter знает границы выражений |
| `detector.py` | Переписывается с нуля — regex → tree-sitter queries |
| `extract.py` | Переписывается — regex → навигация по AST |
| `analyzer.py` | Рефакторинг — sets → парсинг direction/format из pattern_type |
| `sql_registry.py` | Без изменений — regex для .sql файлов остаётся |
| `ts_parser.py` | Новый — обёртка tree-sitter |
| `queries/*.scm` | Новые — S-expression query файлы |
| `worklist.py` | Минимальные правки — новые pattern_type |
| `prepass.py` | Минимальные правки — новые pattern_type |
| `cli.py` | Без изменений (вызывает detect_parquet_usage / detect_all_io) |
| `targets.py`, `filters.py` | Минимальные правки |
| Трансформеры | Без изменений |
| Тесты | Обновление pattern_type в assertions |

## Зависимости

```
tree-sitter>=0.23
tree-sitter-python>=0.23
tree-sitter-java>=0.23
tree-sitter-scala>=0.23
```

Все пакеты имеют pre-built wheels. Не требуют C-компилятора при установке.

## Что НЕ входит в scope

- Замена трансформеров (остаются regex-based)
- Замена sql_registry.py (regex для .sql файлов достаточен)
- Семантический анализ (резолв типов, imports) — это LSP-территория
- Поддержка новых языков (R, Go, Rust) — можно добавить позже
