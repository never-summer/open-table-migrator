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

### 4. Run the Conversion

```bash
python -m skills.open_table_migrator.cli <project_path> --table <TABLE_NAME> --namespace <NAMESPACE>
```

### 5. Review and Fix Edge Cases

After automated conversion, manually review:

- **Multiple tables** — the tool assumes one table per project; split and re-run per table if needed
- **Schema definitions** — Iceberg requires explicit schema. Extract from existing parquet:
  ```python
  import pyarrow.parquet as pq
  schema = pq.read_schema("existing.parquet")
  ```
- **Partitioning** — `partitionBy("day")` is preserved as a `TODO` comment in the JVM transformer output. Add to the Iceberg partition spec manually:
  ```sql
  ALTER TABLE default.events ADD PARTITION FIELD day
  ```
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
| `df.write().saveAsTable("t")` | `df.writeTo("ns.t").createOrReplace()` |
| `df.write().partitionBy("day").parquet(...)` | `df.writeTo("ns.t").overwritePartitions()` *(+ TODO comment for partition spec)* |
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
| `df.write().saveAsTable("t")` | `df.writeTo("ns.t").createOrReplace()` |
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

In a single source file with multiple targets, the Python transformers emit one `catalog = ...` header plus one `tbl_<ns>_<name>` per target, and rewrite each call site to use the right variable. Calls whose path is a variable or doesn't match any glob (and has no fallback) become `# TODO(iceberg): could not resolve target ...` comments.

## Known Limitations

- **Structured Streaming** (readStream/writeStream with parquet/orc sinks) is **detected but not rewritten** — the transformer inserts a `TODO(iceberg)` comment. Migrate manually using `.format("iceberg")` + `writeStream.toTable("ns.t")` / `.option("path", ...)`.
- **pyarrow dataset API** (`ParquetFile`, `ParquetDataset`, `pa.dataset.*`) is also warn-only — rewrite to `catalog.load_table(...).scan().to_arrow()` by hand.
- **Cloud catalog configs** (Glue, Nessie, REST) need manual setup — this tool generates SQLite dev config for Python, and leaves JVM catalog config untouched
- **Hive table data migration** — the tool rewrites *code* but does not migrate existing parquet/ORC data; use `CALL system.migrate(...)` for in-place migration or `CTAS` for copy
- **Schema inference** — partition specs are not automatically derived; `partitionBy(...)` becomes a `TODO` comment for the JVM transformer
- **Scala 2.13 / Spark 3.4** — the generated Maven/Gradle coordinates target Spark 3.5 + Scala 2.12; adjust manually for other versions
- **`LOCATION` clause** on `CREATE EXTERNAL TABLE` — rewritten to `USING iceberg LOCATION ...` but Iceberg's LOCATION semantics differ from Hive's; review manually.
- **INSERT INTO / OVERWRITE** — detected but intentionally not rewritten (same SQL works on Iceberg tables). Reported only so you know the file is touching table data.
- **False positives** — `INSERT INTO`, `saveAsTable`, and `USING parquet` regexes match *any* table by that pattern, not just the table you're migrating. Use `filter_matches` to scope if a project touches multiple tables.
