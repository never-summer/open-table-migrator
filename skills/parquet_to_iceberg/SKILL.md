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

```bash
python -m skills.parquet_to_iceberg.cli <project_path> --table <TABLE_NAME> --namespace <NAMESPACE>
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
- **Scala 2.13 / Spark 3.4** — the generated Maven/Gradle coordinates target Spark 3.5 + Scala 2.12; adjust manually for other versions
