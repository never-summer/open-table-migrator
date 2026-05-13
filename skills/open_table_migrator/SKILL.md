---
name: open-table-migrator
description: Converts a Python or Java/Scala project from Parquet/ORC (including Hive and generic format() idioms) read/write to Apache Iceberg tables
trigger: "convert parquet" OR "migrate to iceberg" OR "parquet to iceberg" OR "migrate hive to iceberg" OR "convert orc" OR "migrate orc to iceberg"
---

# Parquet/ORC → Iceberg Conversion Skill

**Announce at start:** "I'm using the open-table-migrator skill to convert this project."

## What This Skill Does

Scans the project for Parquet and ORC operations (Hive DDL, Spark Dataset API, generic `format(...)` calls) and replaces them with Apache Iceberg equivalents:

- **Python:** pandas, PySpark (batch + generic format + streaming warn), pyarrow (classic + dataset warn) → pyiceberg
- **Java / Scala:** Spark Dataset API `spark.read().parquet|orc()` and `spark.read().format("parquet"|"orc").load()` → Iceberg Spark runtime (`format("iceberg")`)
- **Hive via SparkSQL:** `STORED AS PARQUET|ORC`, `USING parquet|orc`, `saveAsTable`, `INSERT INTO|OVERWRITE TABLE` → Iceberg-backed tables (`USING iceberg`, `writeTo(...)`)
- **Structured Streaming** and **pyarrow dataset/ParquetFile** are *detected* and left with `TODO(iceberg)` comments for manual rewrite.

Also updates project dependencies (`requirements.txt`, `pyproject.toml`, `pom.xml`, `build.gradle`).

## Step-by-Step Process

### 1. Identify Project Type

Look for build files to determine stack:
- `requirements.txt` / `pyproject.toml` → **Python**
- `pom.xml` → **Java + Maven**
- `build.gradle` / `build.gradle.kts` → **Java/Scala + Gradle**
- `*.java` / `*.scala` files → JVM project

The skill handles all of these automatically — the detector scans `.py`, `.java`, `.scala` files.

### 2. Detect Parquet / ORC / Hive Usage

Read source files and identify patterns. The skill scans for **both Parquet and ORC** and covers the Spark/pandas/pyarrow idioms below.

**Python:**
- pandas: `pd.read_parquet(...)` / `pd.read_orc(...)` / `.to_parquet(...)` / `.to_orc(...)`
- PySpark batch: `spark.read.parquet|orc(...)` / `df.write.parquet|orc(...)`
- PySpark generic: `spark.read.format("parquet"|"orc").load(...)` / `df.write.format(...).save(...)`
- PySpark streaming *(warn-only)*: `readStream.parquet|orc|format(...)`, `writeStream...`
- pyarrow classic: `pq.read_table(...)` / `pq.write_table(...)`
- pyarrow ORC: `orc.read_table(...)` / `orc.write_table(...)`
- pyarrow dataset *(warn-only)*: `pq.ParquetFile`, `pq.ParquetDataset`, `pa.dataset.dataset`, `pa.dataset.write_dataset`
- SparkSQL via `spark.sql(...)`: `STORED AS PARQUET|ORC`, `USING parquet|orc`, `INSERT INTO|OVERWRITE TABLE`

**Java:**
- Batch: `spark.read().parquet|orc(...)` / `df.write()...parquet|orc(...)`
- Generic: `spark.read().format("parquet"|"orc").load(...)` / `df.write()...format(...).save(...)`
- Streaming *(warn-only)*: `readStream()....`, `writeStream()....`
- `df.write().saveAsTable("...")`
- `spark.sql("CREATE [EXTERNAL] TABLE ... STORED AS PARQUET|ORC")`
- `spark.sql("CREATE TABLE ... USING parquet|orc")`
- `spark.sql("INSERT INTO|OVERWRITE TABLE ...")`

**Scala:** same as Java but with the parens-less `.read.parquet` / `.write.parquet` idiom.

### 3. Ask the User for Iceberg Table Details

Before converting, ask:
- **Table name** — what should the Iceberg table be called?
- **Namespace** — default namespace is `"default"`
- **Catalog type** — local SQLite (dev), Hive Metastore, AWS Glue, REST?
- **For JVM projects:** does the target Spark cluster already have `iceberg-spark-runtime` on the classpath, or should we add it?

### 4. Generate the Worklist, Then Rewrite

The CLI does **not** rewrite code on its own. It runs the detector, resolves each match against the user's table mapping, updates dependency manifests, and emits `lakehouse-worklist.json` — a per-call-site task list for the agent/LLM to execute via `Edit`.

```bash
python -m skills.open_table_migrator.cli <project_path> --table <TABLE_NAME> --namespace <NAMESPACE>
# or, for multi-table projects:
python -m skills.open_table_migrator.cli <project_path> --mapping ./lakehouse-mapping.json
```

After the worklist is written, the agent walks each task and applies the rewrite using the Conversion Reference tables below. When all tasks are done, rerun the detector — the migrated patterns should be gone, and only `skip: true` entries or `TODO(iceberg)` markers remain.

### 5. Review and Fix Edge Cases

After automated conversion, manually review:

- **Multiple tables** — the tool assumes one table per project; split and re-run per table if needed
- **Schema definitions** — Iceberg requires explicit schema. Extract from existing parquet:
  ```python
  import pyarrow.parquet as pq
  schema = pq.read_schema("existing.parquet")
  ```
- **Partitioning** — partition specifications are extracted structurally from `partitionBy(...)` / `bucketBy(...)` calls and propagated into the worklist. The migration agent uses them to generate the correct Iceberg `PartitionSpec`. See the "Partition spec extraction" section for details.
- **Hive metastore catalog** — if the original project used Hive MetaStore, configure Iceberg's HiveCatalog:
  ```
  spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.hive_prod.type = hive
  spark.sql.catalog.hive_prod.uri = thrift://metastore:9083
  ```
- **Existing Hive tables with data** — use Iceberg's `system.migrate` procedure to convert in place:
  ```sql
  CALL hive_prod.system.migrate('db.events')
  ```

### 6. Create the Iceberg Table

**Python (pyiceberg):**
```python
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
```

**Java/Scala (Spark SQL):**
```sql
CREATE TABLE default.events (
  id BIGINT NOT NULL,
  status STRING,
  value DOUBLE
) USING iceberg
```

### 7. Run Existing Tests

**Python:**
```bash
pip install pyiceberg[sql-sqlite]
pytest tests/ -v
```

**Java/Maven:**
```bash
mvn test
```

**Scala/Gradle:**
```bash
./gradlew test
```

Common test failures:
- Tests use `tmp_path` for parquet file path but Iceberg catalog uses a fixed URI — inject catalog via fixture
- Assertions on file existence (`Path("data.parquet").exists()`) — replace with table existence checks

### 8. Commit

```bash
git add -A
git commit -m "refactor: migrate parquet read/write to Apache Iceberg"
```

## Conversion Reference — Python

| Before (Parquet) | After (Iceberg) |
|---|---|
| `pd.read_parquet(path)` / `pd.read_orc(path)` | `catalog.load_table((ns, name)).scan().to_pandas()` |
| `df.to_parquet(path)` / `df.to_orc(path)` | `tbl.overwrite(df)` |
| `spark.read.parquet(path)` / `.orc(path)` | `spark.table("ns.name")` |
| `spark.read.format("parquet"\|"orc").load(path)` | `spark.table("ns.name")` |
| `df.write.parquet(path)` / `.orc(path)` | `df.writeTo("ns.name").overwritePartitions()` |
| `df.write.format("parquet"\|"orc").save(path)` | `df.writeTo("ns.name").overwritePartitions()` |
| `pq.read_table(path)` / `orc.read_table(path)` | `tbl.scan().to_arrow()` |
| `pq.write_table(table, path)` / `orc.write_table(...)` | `tbl.overwrite(table)` |
| `pq.ParquetFile` / `pq.ParquetDataset` / `pa.dataset.*` | *(TODO comment — rewrite manually)* |
| `spark.readStream.parquet/orc/format(...)` | *(TODO comment — manual migration)* |

## Conversion Reference — Java/Scala Spark

| Before (Java) | After (Iceberg) |
|---|---|
| `spark.read().parquet("p")` / `.orc("p")` | `spark.read().format("iceberg").load("ns.table")` |
| `spark.read().format("parquet"\|"orc").load("p")` | `spark.read().format("iceberg").load("ns.table")` |
| `df.write().mode("overwrite").parquet("p")` / `.orc("p")` | `df.writeTo("ns.table").overwritePartitions()` |
| `df.write()...format("parquet"\|"orc").save("p")` | `df.writeTo("ns.table").overwritePartitions()` |
| `df.write().saveAsTable("t")` *(no partitionBy / bucketBy)* | `df.writeTo("ns.t").createOrReplace()` |
| `df.write().partitionBy(...).saveAsTable("t")` / `.bucketBy(...).saveAsTable("t")` | **Two steps:** (1) pre-create `CREATE TABLE ns.t (...) USING iceberg PARTITIONED BY (...)` with the desired Iceberg partition spec; (2) rewrite call site as `df.writeTo("ns.t").overwritePartitions()`. `createOrReplace()` would silently drop the spec. |
| `df.write().partitionBy("day").parquet(...)` | `df.writeTo("ns.t").overwritePartitions()` *(+ TODO comment — user must pre-create the table with the right partition spec)* |
| `spark.readStream()....parquet\|orc\|format(...)` | *(TODO comment — manual migration)* |

| Before (Scala) | After (Iceberg) |
|---|---|
| `spark.read.parquet("p")` / `.orc("p")` | `spark.read.format("iceberg").load("ns.table")` |
| `spark.read.format("parquet"\|"orc").load("p")` | `spark.read.format("iceberg").load("ns.table")` |
| `df.write.mode("overwrite").parquet("p")` / `.orc("p")` | `df.writeTo("ns.table").overwritePartitions()` |
| `df.write.format("parquet"\|"orc").save("p")` | `df.writeTo("ns.table").overwritePartitions()` |

## Conversion Reference — Hive / SparkSQL

| Before | After |
|---|---|
| `"CREATE TABLE t (...) STORED AS PARQUET\|ORC"` | `"CREATE TABLE t (...) USING iceberg"` |
| `"CREATE [EXTERNAL] TABLE t (...) STORED AS PARQUET LOCATION '...'"` | `"CREATE TABLE t (...) USING iceberg LOCATION '...'"` *(review LOCATION semantics manually)* |
| `"CREATE TABLE t (...) USING parquet\|orc"` | `"CREATE TABLE t (...) USING iceberg"` |
| `df.write().saveAsTable("t")` *(no partitionBy / bucketBy)* | `df.writeTo("ns.t").createOrReplace()` |
| `df.write().bucketBy(...).saveAsTable("t")` / `.partitionBy(...).saveAsTable("t")` | Pre-create `CREATE TABLE ns.t (...) USING iceberg PARTITIONED BY (bucket(N, col))` then `df.writeTo("ns.t").overwritePartitions()`. **Do not** use `createOrReplace()` here — it resets the partition spec. |
| `"INSERT INTO TABLE t ..."` / `"INSERT OVERWRITE TABLE t ..."` | *(no change — Spark handles Iceberg tables via the same SQL, assuming catalog is configured)* |
| Existing Hive table with data | `CALL catalog.system.migrate('db.t')` — manual step |

## Dependencies Added

| Ecosystem | File | Dependency |
|---|---|---|
| Python | `requirements.txt` / `pyproject.toml` | `pyiceberg[sql-sqlite]>=0.7.0` |
| Maven | `pom.xml` | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0` |
| Gradle | `build.gradle` | `implementation 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0'` |

## Multi-table projects

Projects that touch several logical tables use a **mapping file** to route each path to its Iceberg target. JSON format:

```json
{
  "default": {"namespace": "default", "table": "unmapped"},
  "tables": [
    {"path_glob": "s3://bucket/events/*", "namespace": "analytics", "table": "events"},
    {"path_glob": "*/users/*",            "namespace": "analytics", "table": "users"},
    {"path_glob": "s3://legacy/*",        "skip": true},
    {"path_glob": "s3://bucket/logs/*",   "direction": "write", "namespace": "analytics", "table": "logs"},
    {"path_glob": "s3://bucket/logs/*",   "direction": "read",  "skip": true}
  ]
}
```

- `tables` — ordered list; first matching `path_glob` (fnmatch style) wins.
- `default` — optional fallback target when no glob matches.
- `skip: true` — matching operations are **left as parquet/ORC**. The transformer drops an `iceberg: skipped by mapping` marker above the line so it's obvious on review.
- `direction: "read" | "write" | "any"` (default `"any"`) — restrict an entry to one side only. Paired entries let you migrate writes but keep reads (or vice versa).
- You can also pass `--table`/`--namespace` **alongside** `--mapping` as a CLI-level fallback.

Run it:
```bash
python -m skills.open_table_migrator.cli <project> --mapping mapping.json
```

In a single source file with multiple targets, the worklist emits one entry per call site with the resolved `(namespace, table)`. Calls whose path is a variable or doesn't match any glob (and has no fallback) are flagged as unresolved so the LLM rewriter surfaces them to the user.

## Post-Migration Operational Concerns

The skill rewrites **code**, not the operational model of the tables. After the migration lands, surface these three questions to the user — they change how the tables behave in production.

### 1. Format version & write mode (Copy-on-Write vs Merge-on-Read)

MoR/CoW is a **table property**, not a code choice. `INSERT` / `append` / `overwritePartitions()` are unaffected — the mode only changes how `UPDATE` / `DELETE` / `MERGE INTO` materialize changes.

```sql
ALTER TABLE ns.events SET TBLPROPERTIES (
  'format-version'       = '2',               -- required for MoR
  'write.update.mode'    = 'merge-on-read',   -- or 'copy-on-write'
  'write.delete.mode'    = 'merge-on-read',
  'write.merge.mode'     = 'merge-on-read'
);
```

| | Copy-on-Write (default pre-v2) | Merge-on-Read (v2) |
|---|---|---|
| What is written on UPDATE/DELETE | Fully rewritten data files | Original files + position/equality delete files |
| Write latency | High (rewrite partition) | Low |
| Read latency | Low | Higher — reader applies deletes on the fly |
| Use when | Rare updates, read-heavy analytics | Frequent upserts, CDC, streaming |

Ask the user whether the table has row-level mutations before defaulting; if it does, MoR + v2 is usually the right pick.

### 2. Compaction

Iceberg compaction is a **procedure call**, not a background process. Nothing auto-compacts unless the user runs one of:

```sql
-- Bin-pack or sort small files, apply pending deletes (for MoR)
CALL catalog.system.rewrite_data_files(
  table   => 'ns.events',
  strategy => 'binpack',
  options => map('min-input-files','5','target-file-size-bytes','536870912')
);

-- Compact the manifest list (speeds up scan planning)
CALL catalog.system.rewrite_manifests('ns.events');
```

**Where it runs:** any Spark session with `iceberg-spark-runtime` and catalog access. Typical deployment patterns:

- **Airflow / Dagster DAG** — scheduled `rewrite_data_files` every N hours
- **Streaming job post-hook** — compact after each micro-batch commit window
- **Managed catalog** — Tabular, Snowflake Polaris, AWS Glue and Dremio can run compaction for you; no user job needed
- **Ad-hoc cron** — plain `spark-submit` on a schedule

Compaction does not block writers — it creates a new snapshot and the old files remain until expiration.

### 3. Snapshot expiration & orphan cleanup

Iceberg keeps old snapshots forever unless told otherwise. Two calls every user should schedule (usually together with `rewrite_data_files`):

```sql
CALL catalog.system.expire_snapshots(
  table => 'ns.events',
  older_than => TIMESTAMP '2026-04-01 00:00:00'
);

CALL catalog.system.remove_orphan_files(table => 'ns.events');
```

Without this the catalog grows unboundedly and old data files never get GC'd.

**Note:** none of these procedures are written by the skill. They are a deployment concern — mention them after the rewrite is green, and let the user wire them into whatever scheduler they already use.

## Path schemes

The mapping resolver is URI-aware. Sub-scheme variants of the same storage are treated as equivalent:

| Canonical scheme | Aliases | Example |
|---|---|---|
| `s3` | `s3a`, `s3n` | `s3://bucket/key` |
| `hdfs` | `webhdfs` | `hdfs://nameservice/path` |
| `abfs` | `abfss` | `abfs://container@account.dfs.core.windows.net/path` |
| `gs` | — | `gs://bucket/key` |
| `viewfs` | — (kept distinct from `hdfs`) | `viewfs://nameservice/path` |
| `file` | bare paths (`/tmp/...`, `./data/...`) | `file:///tmp/x` |

A mapping entry `s3://bucket/users/*` matches paths in the code regardless of whether the code writes `s3://`, `s3a://`, or `s3n://`. The same is true for `hdfs`/`webhdfs` and `abfs`/`abfss`.

**`path_arg` in the worklist is preserved verbatim** — the equivalence is applied only when comparing against mapping globs.

### Glob syntax

Mapping patterns support shell-style globs in the authority and path components:

- `*` matches any character sequence (including `/`) — same as `s3://bucket/users/*` matching `s3://bucket/users/2024/01/data.parquet`
- `**` is provided for clarity and behaves the same as `*` in single-pattern matches
- `?` matches one character
- Brackets like `[abc]` are not supported

This matches the convention used by `aws s3` and similar cloud-storage tools.

### Bare local paths

Bare paths (`./data/x`, `/tmp/fixtures/x`) are treated as `file://` for matching. Relative paths are resolved against the project root passed via the CLI.

### viewfs limitation

`viewfs://` is **not** auto-resolved against an underlying `hdfs://` cluster. Mount-point resolution depends on cluster config we do not read. If your project uses both `viewfs://` and `hdfs://` (e.g., logical mount in jobs, physical path in DDL), list both schemes explicitly in the mapping:

```json
{
  "tables": [
    { "path_glob": "viewfs://nameservice/data/users/*", "namespace": "analytics", "table": "users" },
    { "path_glob": "hdfs://realCluster/data/users/*",   "namespace": "analytics", "table": "users" }
  ]
}
```

### Scheme-less globs (backward compat)

Glob patterns without a scheme and not starting with `/` (e.g., `*users*`, `data/*`) fall back to `fnmatch` against the **full raw `path_arg` string** — including any scheme prefix. So `*users*` matches `s3://bucket/users/x` because the full raw string contains the literal substring `users`. This preserves legacy mapping files that pre-date URI awareness. New patterns should prefer explicit schemes for clarity.

### Unknown schemes

Unknown schemes (e.g., `ftp://`, custom enterprise schemes) emit a one-time stderr warning per scheme. They participate in matching only via exact `raw_scheme` equality — no aliasing applied.

## Constant folding

The detector resolves name-to-literal bindings at the file level so that I/O calls using a named constant become as informative as those using a literal directly.

```python
EVENTS_PATH = "s3://bucket/events"
df = pd.read_parquet(EVENTS_PATH)   # path_arg = "s3://bucket/events"
```

### What is resolved

- **Python:** module-level `X = "..."`, function-local `X = "..."`, one level of `+` concat (`X = BASE + "/events"` when `BASE` is already a known literal).
- **Java:** class `static final` and inline-initialised `final` fields, method-local `final` variables, one level of `+` concat across fields.
- **Scala:** object/class-level `val`, def-local `val`, one level of `+` concat.

### What is skipped

- **f-strings** (`f"..."`, `s"..."` Scala) — interpolation depends on runtime values.
- **`.format()`, `%`-format, multiplication** — only `+` concat is recognised.
- **Reassignment** — any reassigned name is marked unresolvable. `attrs.skipped_reason = "reassigned"` records the reason.
- **Constructor-only Java fields** — `private final String x;` initialised inside a constructor is not parsed.
- **Cross-file references** — `from config import X` is not followed; constants in other files are out of scope.
- **3+ operand concat** — `A + B + C` is not resolved; only single-`+` expressions.
- **Non-literal RHS** — `os.getenv(...)`, function calls, etc.

### Audit trail

When a match resolves a name, `match.attrs["resolved_from"] = "NAME@file:line"` is set so the worklist preserves the original source location. The detector emits the same `path_arg` value regardless of whether it came from a literal at the call site or a resolved binding.

## Partition spec extraction

When a Spark write site uses `.partitionBy(...)` or `.bucketBy(N, ...)`, the detector captures the partition specification structurally and propagates it into the worklist so the migration agent can generate the correct Iceberg `PartitionSpec` per target.

### Supported transforms (MVP)

| Spark API | Iceberg transform |
|---|---|
| `.partitionBy("col")` | `identity(col)` |
| `.partitionBy("col1", "col2", ...)` | one `identity(col)` per arg |
| `.bucketBy(N, "col1", "col2", ...)` | one `bucket(N, col)` per col arg |

Time-based transforms (`year`/`month`/`day`/`hour`) and `truncate(N, col)` are out of MVP scope — Spark uses these through `withColumn(...)` pre-pass + identity partitioning, which would require intra-method data flow.

### Detection scope

- **Python PySpark** — `df.write.partitionBy/.bucketBy`
- **JVM Spark** — Java/Scala DataFrame API
- **Hive DDL in `.sql` files** — `PARTITIONED BY (col1, col2)` (identity transforms only; Hive `PARTITIONED BY` has no `bucket(N)` form)

Constants used as args resolve via the const-folding module (1.1): `REGION = "region"; df.write.partitionBy(REGION)` resolves to `identity(region)`.

### Edge cases

- `partitionBy()` (no args) → no transforms
- `partitionBy(*cols)` (splat) → skipped silently
- `partitionBy(year("ts"))` (function call) → skipped silently (out of scope)
- `bucketBy(N_var, "col")` where N is an identifier → skipped (MVP requires int literal for N)
- Multiple `partitionBy` calls in one chain → merged in chain order

### Worklist output

Write-direction entries gain an optional `partition_spec` array:

```json
{
  "site": "src/jobs/users.py:54",
  "kind": "write",
  "target": "analytics.users",
  "partition_spec": [
    {"kind": "identity", "column": "region"},
    {"kind": "bucket", "column": "uid", "n": 8}
  ]
}
```

When no partitions are detected, the field is **omitted** from the JSON (not `[]`).

### Code↔DDL mismatch detection

If both `df.write.partitionBy(...).saveAsTable(t)` (code) and `CREATE TABLE t ... PARTITIONED BY (...)` (DDL in `.sql` file) exist for the same table, the analyzer compares both `partition_spec` values. If they diverge, the match's `attrs["partition_mismatch"]` is populated:

```
code: identity(region); ddl: identity(date_col)
```

The agent sees this warning and can flag it before applying the rewrite.

## Dynamic SQL loading

The detector finds call-sites that load `.sql` files at runtime and cross-references them with parquet/orc tables defined in those (or related) files. This catches the common pattern where SQL is stored separately from code:

```python
sql = open("queries/events_update.sql").read()
spark.sql(sql)
```

### Detected patterns

| Language | Pattern | Example |
|---|---|---|
| Python | `py_open` | `open("x.sql")` |
| Python | `py_path_read_text` | `Path("x.sql").read_text()` |
| Python | `py_pkgutil_get_data` | `pkgutil.get_data(__name__, "x.sql")` |
| Java | `java_files_read` | `Files.readAllBytes(Path.of("x.sql"))` |
| Java/Scala | `java_resource_stream` | `getClass().getResourceAsStream("/sql/x.sql")` |

### What's parsed in the loaded SQL

Beyond `CREATE TABLE ... STORED AS PARQUET` (already handled), the SQL registry now also extracts non-DDL references:

- `INSERT INTO <table>` and `INSERT OVERWRITE TABLE <table>` → write reference
- `UPDATE <table> SET ...` → write reference
- `MERGE INTO <table>` → write reference
- `FROM <table>` / `JOIN <table>` → read reference

CTE names introduced by `WITH <name> AS (...)` are skipped from `FROM`/`JOIN` references.

### Cross-reference behavior

For each loader, the loader's `sql_filename` is resolved against the project tree in three steps:

1. Path relative to the loader's containing file's directory.
2. Path relative to project root.
3. Basename match across all registered `.sql` files (with `match_kind="basename_unique"` or `"basename_ambiguous"`).

Tables mentioned in the resolved SQL file (via CREATE / INSERT / FROM / JOIN / etc.) are joined against the registry of parquet/orc `CREATE TABLE` definitions across all `.sql` files. This handles the common pattern of `schema.sql` (with CREATE) and `queries/*.sql` (with INSERT only).

### Worklist output

Each cross-reference appears in `lakehouse-worklist.json` under `dynamic_sql_loaders`:

```json
{
  "file": "src/jobs/events.py",
  "line": 42,
  "pattern": "py_open",
  "sql_filename": "queries/events_update.sql",
  "confidence": "high",
  "resolved_to": "queries/events_update.sql",
  "match_kind": "exact_path",
  "tables": [
    {"name": "events", "format": "parquet", "ddl_file": "schema.sql", "ddl_line": 8}
  ]
}
```

### Limitations

- SQL templating (Jinja, `.format`, `${...}`) is not parsed.
- In-SQL `\i` / `SOURCE` / `!include` directives are not followed.
- ORM-generated SQL (SQLAlchemy `select(...)`, jOOQ DSL) is not detected.
- Dynamic table names within SQL (`INSERT INTO {schema}.events`) are not resolved.

## Dry run

The `--dry-run` flag runs the full migration pipeline (detection, cross-reference, worklist building, prepass planning, dependency-update planning) but writes **nothing** to disk. Output is printed to stdout in four sections, suitable for change-review documentation.

### Usage

```bash
PYTHONPATH=. python3 -m skills.open_table_migrator.cli <project_path> \
    --table users --namespace analytics --dry-run
```

### What is suppressed

- `lakehouse-worklist.json` is not written.
- Source files (`.py`/`.java`/`.scala`) are not modified (skip-markers, pyspark conf comments).
- Build files (`pyproject.toml`, `pom.xml`, `build.gradle[.kts]`, `build.sbt`, `requirements.txt`) are not modified.

### Output sections

1. **Summary** — counts: entries that would go in the worklist, files that would be prepass'd, build files that would be updated.
2. **Worklist preview** — the full `lakehouse-worklist.json` content that would be written, printed to stdout as JSON.
3. **Prepass diff preview** — unified diff of skip-markers and pyspark conf comments that would be added to source files, per file.
4. **Build-file updates** — unified diff of the pyiceberg / iceberg-spark-runtime dependency additions to each build file.

Sections without content are omitted (e.g., no prepass section if nothing would be touched).

### Composition with other flags

- `--dry-run --no-deps` — valid; `--no-deps` is redundant.
- `--dry-run --mapping foo.json` — valid; mapping is read normally.
- `--dry-run --table X --namespace Y` — valid.

Validation rules (e.g., requiring `--table` with `--namespace`) still apply in dry-run mode.

### Exit code

Always 0 on successful dry-run. Pre-existing argument-validation errors still return exit code 2.

## Phased migration runbook

For each target Iceberg table found in the worklist, the migrator emits a per-table directory under `iceberg-runbook/` containing:

- `migration-plan.md` — phase descriptions, pre-flight checklist, code-sites table, warnings
- `phase1_add_files.sql` — Spark SQL for `system.add_files` (in-place metadata creation)
- `phase2_rewrite.sql` — Spark SQL for `system.rewrite_data_files` (compaction)
- `phase3_switchover.sql` — three OPTION blocks: Spark VIEW, HMS direct rename, Application-level rename per worklist

Plus `iceberg-runbook/README.md` as a top-level index with a summary table of all migrations.

### Phases

| Phase | Estimated runtime | Risk |
|---|---|---|
| 1: add_files | minutes | Low — reversible by dropping target table |
| 2: rewrite_data_files | hours | Medium — resource-heavy, can be deferred |
| 3: switchover | minutes | Coordinated cutover — requires consumer alignment |

### Phase 3 options

Phase 3 has three switchover patterns. The user picks ONE per stack and comments out the others:

- **OPTION A (Spark VIEW)** — replace old Hive table with a view pointing at Iceberg. Works for SELECT-only consumers; breaks `INSERT INTO` clients.
- **OPTION B (HMS direct rename)** — atomic at metastore level. Requires admin access.
- **OPTION C (Application-level rename)** — update each call site in the code per `lakehouse-worklist.json`. Listed in the SQL file as comments.

### Automation

Runbook generation runs alongside worklist generation on every `convert_project`. The `--dry-run` flag suppresses the directory write and prints the runbook contents as a 5th preview section.

### Limitations

- Spark SQL syntax only (no Trino, ClickHouse, Snowflake).
- Schema in `phase1_add_files.sql` is a placeholder — the operator must run `spark.read.parquet(...).printSchema()` and paste the result.
- No data-size estimation or runtime prediction.
- No DAG between tables (each migration is independent).
- `partition_mismatch` warning is included in the runbook, but the operator must decide which side (code or DDL) is correct.

## Known Limitations

- **FQN propagation on read sites** — after rewriting `saveAsTable("Foo")` → `writeTo("ns.Foo")`, every downstream reference to the bare name (`spark.table("Foo")`, `CACHE TABLE Foo`, `SELECT ... FROM Foo`, `DROP TABLE Foo`) must be updated to the fully-qualified `ns.Foo`. Otherwise those calls resolve through the default session catalog and miss the Iceberg table entirely. The detector reports these reads, but the worklist does not automatically bind them to the write-site rewrite — walk each `spark_read_table` / SparkSQL match in the same file and rename by hand.
- **Partitioning and bucketing** — `partitionBy(...)` / `bucketBy(...)` on a write is preserved as a `TODO(iceberg)` comment, never auto-rewritten. If the original table was partitioned or bucketed, **ask the user** in Step 3 whether the layout matters, then pre-create the Iceberg table with the matching partition spec (`PARTITIONED BY (bucket(N, col))` / `PARTITIONED BY (days(col))`) before letting the rewrite run. Using `createOrReplace()` on a saveAsTable with bucketing silently drops the spec.
- **Structured Streaming** (readStream/writeStream with parquet/orc sinks) is **detected but not rewritten** — the transformer inserts a `TODO(iceberg)` comment. Migrate manually using `.format("iceberg")` + `writeStream.toTable("ns.t")` / `.option("path", ...)`.
- **pyarrow dataset API** (`ParquetFile`, `ParquetDataset`, `pa.dataset.*`) is also warn-only — rewrite to `catalog.load_table(...).scan().to_arrow()` by hand.
- **Cloud catalog configs** (Glue, Nessie, REST) need manual setup — this tool generates SQLite dev config for Python, and leaves JVM catalog config untouched
- **Hive table data migration** — the tool rewrites *code* but does not migrate existing parquet/ORC data; use `CALL system.migrate(...)` for in-place migration or `CTAS` for copy
- **Scala 2.13 / Spark 3.4** — the generated Maven/Gradle coordinates target Spark 3.5 + Scala 2.12; adjust manually for other versions
- **`LOCATION` clause** on `CREATE EXTERNAL TABLE` — rewritten to `USING iceberg LOCATION ...` but Iceberg's LOCATION semantics differ from Hive's; review manually.
- **INSERT INTO / OVERWRITE** — detected but intentionally not rewritten (same SQL works on Iceberg tables). Reported only so you know the file is touching table data.
- **False positives** — `INSERT INTO`, `saveAsTable`, and `USING parquet` regexes match *any* table by that pattern, not just the table you're migrating. Use `filter_matches` to scope if a project touches multiple tables.
