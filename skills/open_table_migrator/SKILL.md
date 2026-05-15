---
name: open-table-migrator
description: Convert Parquet/ORC read/write to Apache Iceberg in Python, Java, or Scala projects. Use when the user says "convert parquet", "migrate to iceberg", "parquet to iceberg", "migrate hive to iceberg", "convert orc", "migrate orc to iceberg", or asks to move Hive-parquet tables to Iceberg.
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

For the full before/after rewrite matrix per language and the multi-table mapping format, see [examples.md](./examples.md).
For subsystem deep-dives (path schemes, constant folding, partition spec extraction, dynamic SQL loading, dry-run, phased runbook, operational concerns, known limitations) see [reference.md](./reference.md).

## Project-specific guides

**Before Step 3 (table details) and before Step 6 (table creation), look for these files at the project root and read whichever exist:**

- **`S2T_GUIDE.md`** — source-to-target mapping. Authoritative for table specs, namespace, column metadata (names, types, nullability, descriptions), partition keys, and row-level rules. **If this file exists, prefer it over asking the user for table details in Step 3.** Use it to construct the Iceberg schema in Step 6 instead of inferring from existing Parquet.

- **`ICEBERG_WF_GUIDE.md`** — workflow architecture (Oozie or successor), MoR vs CoW choice, runtime parameters, existing pipeline launches and how the migrated tables plug into them. **Read this before Step 6** to set the right `TBLPROPERTIES` (`write.delete.mode`, `write.update.mode`, `format-version`) and **before the runbook is consumed** to align Phase 2 compaction schedule with existing batch windows.

- **Any other `*_GUIDE.md` at project root** — read it; project owners use this naming convention for migration-relevant context. If a guide contradicts SKILL.md defaults, **the guide wins** (it reflects local constraints).

If none of these files exist, fall back to the interactive Step 3 questions and defaults below.

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

For each detected pattern, refer to [examples.md](./examples.md) for the Iceberg equivalent.

### 3. Ask the User for Iceberg Table Details

**First check whether `S2T_GUIDE.md` exists at the project root — if so, use it as the source of truth and skip these questions.**

Otherwise, ask:
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

For the mapping file format see the "Multi-table projects" section in [examples.md](./examples.md).
For the `--dry-run` flag (preview the worklist + diffs without writing) see the "Dry run" section in [reference.md](./reference.md).

After the worklist is written, walk each task and apply the rewrite using the Conversion Reference tables in [examples.md](./examples.md). When all tasks are done, rerun the detector — the migrated patterns should be gone, and only `skip: true` entries or `TODO(iceberg)` markers remain.

### 5. Review and Fix Edge Cases

After automated conversion, manually review:

- **Multiple tables** — the tool assumes one table per project; split and re-run per table if needed
- **Schema definitions** — Iceberg requires explicit schema. Extract from existing parquet:
  ```python
  import pyarrow.parquet as pq
  schema = pq.read_schema("existing.parquet")
  ```
- **Partitioning** — partition specifications are extracted structurally from `partitionBy(...)` / `bucketBy(...)` calls and propagated into the worklist. See "Partition spec extraction" in [reference.md](./reference.md) for what's supported and the code↔DDL mismatch detection.
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

For other known caveats (FQN propagation, streaming, pyarrow dataset, viewfs, etc.) see "Known Limitations" in [reference.md](./reference.md).

### 6. Create the Iceberg Table

**First check `S2T_GUIDE.md` for column metadata and `ICEBERG_WF_GUIDE.md` for `TBLPROPERTIES` (MoR vs CoW, format-version).** If they exist, use them. Otherwise:

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

For the operational dimension of Step 6 (Copy-on-Write vs Merge-on-Read, compaction, snapshot expiration), see "Post-Migration Operational Concerns" in [reference.md](./reference.md).

For the phased operational rollout (Phase 1 `add_files`, Phase 2 `rewrite_data_files`, Phase 3 switchover) emitted as `iceberg-runbook/`, see "Phased migration runbook" in [reference.md](./reference.md).

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

## Where to look next

- **[examples.md](./examples.md)** — conversion reference tables (Python, Java/Scala, Hive/SparkSQL), dependencies added per ecosystem, multi-table mapping file format.
- **[reference.md](./reference.md)** — operational concerns (MoR/CoW, compaction, snapshot expiration), path schemes (s3/hdfs/abfs/gs/viewfs/file), constant folding rules, partition spec extraction, dynamic SQL loading, dry-run mode, phased migration runbook, known limitations.
