# open-table-migrator

*[Русская версия](README.md)*

Skill + subagent for Claude Code.

Analyzes data projects in **Python, Java, and Scala** (extensible): finds all data read/write operations, builds an I/O map, and migrates to open table formats.

| | |
|---|---|
| **Detector** | Parses code into an AST (Abstract Syntax Tree) via tree-sitter; algorithmic tree walking finds all I/O |
| **Migration** | Parquet / ORC → Iceberg (tested), architecture — any → any |
| **Target formats** | Iceberg now; Paimon / Delta / Hudi planned |

![I/O detection and migration](docs/assets/open_table_migrator_io_v3.svg)

---

## Features

### I/O Inventory

Scans `.py`, `.java`, `.scala` files via tree-sitter AST and discovers **all** read/write operations. For each operation it extracts:

- **Direction** — read / write / schema
- **Subject** — DataFrame or variable name
- **Target** (path_arg) — file path or table name
- **Summary** — e.g. `usersDF — writes Parquet to s3://bucket/users [partitionBy("region")]`

Pattern type taxonomy: `{runtime}_{direction}_{format}` (e.g. `spark_read_parquet`, `pandas_write_csv`).

### Migration → Lakehouse

The AST detector finds operations, the CLI produces `lakehouse-worklist.json`, and an agent/LLM rewrites the code.

Converts:

- pandas → pyiceberg (`catalog.load_table(...).scan().to_pandas()`)
- PySpark → `spark.table()` / `df.writeTo().overwritePartitions()`
- Java/Scala Spark → `format("iceberg")` / `writeTo()`
- Hive DDL → `USING iceberg`
- Dependencies: `requirements.txt`, `pyproject.toml`, `pom.xml`, `build.gradle[.kts]`, `build.sbt`

### SQL Registry

Scans `.sql`/`.hql`/`.ddl` files, finds `CREATE TABLE ... STORED AS FORMAT`, and cross-references with code — when code writes to a table via `saveAsTable("events")` but the format is defined in a separate SQL file.

---

## Quick Start

### Option 1: Subagent in Claude Code

Say:

> *"analyze all reads and writes in the project"*
> *"migrate to iceberg"*

The [subagent](.claude/agents/open-table-migrator.md) will automatically:
1. Run the detector and show a table of all I/O operations
2. Scan SQL files and show cross-references
3. Ask which tables to migrate and which to skip
4. Execute the migration via worklist
5. Verify the result — detector should return zero residual patterns

### Option 2: CLI

Analysis (no LLM):

```bash
PYTHONPATH=. python -c "
from pathlib import Path
from skills.open_table_migrator.detector import detect_all_io
from skills.open_table_migrator.analyzer import build_report, format_report

matches = detect_all_io(Path('path/to/project'))
print(format_report(build_report(matches), project_root=Path('path/to/project')))
"
```

Migration (single table — produces `lakehouse-worklist.json`):

```bash
PYTHONPATH=. python -m skills.open_table_migrator.cli path/to/project \
    --table events --namespace analytics
```

Migration (multiple tables):

```bash
PYTHONPATH=. python -m skills.open_table_migrator.cli path/to/project \
    --mapping ./lakehouse-mapping.json
```

Mapping format — see [SKILL.md](skills/open_table_migrator/SKILL.md#multi-table-projects).

---

## Example: LearningSparkV2

[LearningSparkV2](https://github.com/databricks/LearningSparkV2) — examples from the *Learning Spark* book, ~30 Scala/Java/Python files with diverse Spark I/O.

### Step 1: Analysis

```
> analyze all reads and writes in LearningSparkV2
```

The detector finds ~25 operations across 12 files:

```
Found 25 data I/O operation(s) across 12 file(s):

By direction:
  read   : 11
  write  : 12
  schema :  2

Per-file breakdown:
  chapter04/scala/src/main/scala/SparkJob.scala:
    spark_write_parquet  (write)  line 54:59
      usersDF — writes Parquet to UsersTbl [bucketBy(8, "uid"), partitionBy("region")]
    spark_read_parquet   (read)   line 62:62
      logsDF — reads Parquet from s3://bucket/logs
  ...
```

Plus cross-references with SQL:

```
SQL-defined tables with 2 code cross-reference(s):
  SparkJob.scala:57  write 'UsersTbl'  — defined as parquet in schema.sql:3
  SparkJob.scala:72  write 'EventsTbl' — defined as parquet in schema.sql:8
```

### Step 2: Per-table decisions

The agent asks for each sink/source:

> *Operation `write` on `UsersTbl` (2 call sites) — migrate to Iceberg? Namespace/table?*

User responds:
- `UsersTbl` → `analytics.users`
- `EventsTbl` → `analytics.events`
- `s3://bucket/logs` → keep as-is (`skip: true`)

### Step 3: Migration

```bash
PYTHONPATH=. python -m skills.open_table_migrator.cli ./LearningSparkV2 \
    --mapping lakehouse-mapping.json
```

The CLI produces `lakehouse-worklist.json` with tasks for the agent. The agent rewrites each operation via `Edit`, then reruns the detector — zero residual patterns.

---

## Tests

```bash
PYTHONPATH=. pytest tests/ --ignore=tests/fixtures -v
```

236 tests. Fixtures in `tests/fixtures/` are input data, not test modules.

## Structure

```
skills/open_table_migrator/
├── SKILL.md              # Reference documentation
├── detector.py           # Public API (detect_parquet_usage / detect_all_io)
├── ts_detector.py        # Tree-sitter AST detector (Python/Java/Scala)
├── ts_parser.py          # Tree-sitter wrapper: parsing, Language/Parser cache
├── analyzer.py           # Reports, deduplication, SQL cross-references
├── sql_registry.py       # Table registry from .sql/.hql/.ddl
├── extract.py            # path_arg, subject, summary extraction
├── folding.py            # Multi-line chain folding (JVM transformer only)
├── filters.py            # Filter by direction/pattern/glob
├── targets.py            # Multi-table: mapping, resolver
├── deps.py               # Dependency updater (5 formats)
├── prepass.py            # Skip markers + pyspark conf
├── worklist.py           # lakehouse-worklist.json builder
├── cli.py                # CLI entry point
└── transformers/
    ├── pandas.py
    ├── pyspark.py
    ├── pyarrow.py
    └── jvm.py            # Java + Scala

.claude/agents/
└── open-table-migrator.md  # Subagent
```

## Detector: tree-sitter AST

The detector uses [tree-sitter](https://tree-sitter.github.io/) to parse Python, Java, and Scala. Instead of regex — AST tree walking:

- **No false positives** — AST distinguishes code from strings and comments
- **No manual folding** — the tree knows expression boundaries
- **Dynamic formats** — any `.read.FORMAT()` is captured automatically
- **Unified taxonomy** — `{runtime}_{direction}_{format}` (e.g. `spark_read_parquet`, `pandas_write_csv`)

### Supported Patterns

| Format | Example patterns |
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
| SQL files | `.sql`, `.hql`, `.ddl` — table registry + code cross-references |
| *Any* | Dynamic extraction — `.read.protobuf()`, `.format("tfrecord")`, etc. |

The regex detector is preserved in the `regex-detector` branch.

## Limitations

- Path arguments must be string literals (variables → `TODO(iceberg)`)
- Streaming — warn-only (TODO comment)
- Data is not migrated — code only; for Hive use `CALL catalog.system.migrate(...)`
- JVM coordinates: Spark 3.5 + Scala 2.12
- `partitionBy(...)` in JVM → TODO for manual Iceberg partition spec

Full list — in [SKILL.md § Known Limitations](skills/open_table_migrator/SKILL.md#known-limitations).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
