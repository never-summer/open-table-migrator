---
name: open-table-migrator
description: Convert Parquet/ORC read/write to Apache Iceberg in Python, Java, or Scala projects. Use when the user says "convert parquet", "migrate to iceberg", "parquet to iceberg", "migrate hive to iceberg", "convert orc", "migrate orc to iceberg", or asks to move Hive-parquet tables to Iceberg.
---

# Parquet/ORC → Iceberg Conversion Skill

**Announce at start:** "I'm using the open-table-migrator skill to convert this project."

## ⚠ MANDATORY — read these two guides before doing anything

**Before Step 1, you MUST read both of these files. They ship with this skill — same directory as this `SKILL.md`. They override every default below when they conflict, and ignoring them will produce a broken migration.**

- 📖 **[S2T_GUIDE.md](./S2T_GUIDE.md)** — Spec-to-Test spec system used in OpenFlow / Sberbank `custom_blago_dzo_*` projects. Authoritative for:
  - Table specs (sheets `Tables`, `Columns`, `Partitions`, `Indexes`, `Constraints` in `s2t.xlsx` / `hadoop_S2T_*.xlsx`)
  - Column metadata (names, types, nullability, descriptions)
  - DDL is **generated** from S2T, not from existing parquet. Pull schema from `src/main/resources/s2t/s2t.xlsx` (or `hadoop_S2T_<PROJECT>_v<n>.xlsx`), not from `pq.read_schema`.
  - Gherkin feature files (`ift.feature`, `st_skl.feature`) under `src/main/resources/s2t/qaapi/` must be updated with the new table scenarios. ТУЗ (`u_<id>`), очередь yarn, `datamart_name`, `entity_id` come from S2T + `devops.json` + `1_ctl_entities.yml`.

- 📖 **[ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md)** — Oozie-based `wf/ctl/*.yml` workflow system. Authoritative for:
  - Required Spark conf for Iceberg (`spark.sql.extensions=...IcebergSparkSessionExtensions`, `spark_catalog.type=hive`, `rewrite.partial-progress.enabled=true`, etc.) — see "Spark-конфигурация для Iceberg" section.
  - Compaction / expire_snapshots / remove_orphan_files run via `wf_schema_hdfs_care` workflow, NOT via standalone Spark jobs. Every new Iceberg table needs `exp_iceberg_<table>.sql` and `upd_iceberg_<table>.sql` scripts under `src/main/resources/sql/dml/`.
  - Lock operations through CTL (`init_locks: checks/sets`, `*ctlCheckLockWrite`, `*ctlSetLockRead`).
  - Data-flow layers: `Sources → AUX (Parquet) → HIST (Iceberg) → AL (Iceberg)`. Read this section before deciding what to migrate vs. leave as parquet.

**If a recommendation in this `SKILL.md`, `examples.md`, `reference.md`, or generated `iceberg-runbook/` contradicts these guides, the guides win.** Specifically:
- Phase 2 in the phased runbook prescribes `CALL system.rewrite_data_files(...)` as a standalone call — in OpenFlow projects, this MUST be wired through `wf_schema_hdfs_care` per `ICEBERG_WF_GUIDE.md` "Сценарий 1/2".
- Step 3 "Ask the User for Iceberg Table Details" is **skipped** — the answer comes from `S2T_GUIDE.md` + the project's S2T Excel file.
- Step 6 "Create the Iceberg Table" — schema comes from S2T Excel, `TBLPROPERTIES` come from `ICEBERG_WF_GUIDE.md`, NOT from inference or interactive prompt.

### ⚠ MANDATORY — Iceberg Spark conf on every wf that touches a migrated table

For every workflow (`wf/ctl/*.yml`) that reads, writes, or runs any procedure (compaction, expire_snapshots, remove_orphan_files, MERGE, UPDATE, DELETE, ad-hoc spark-submit) against a migrated Iceberg table, the `spark_submit_cmd` (or the corresponding `--conf` block) **MUST include all three** of these conf flags. Adding two of three is a broken migration — the third one silently makes the catalog Hive-typed and queries fall back to the wrong code path.

```
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
--conf spark.sql.catalog.spark_catalog.type=hive
```

Where to add:
- New `wf_<table>_service` (per-table compaction) → in `spark_submit_cmd` param. Reuse `{{mart.ssc_schema_hdfs_care}}` if it already wires these three flags; otherwise add them inline.
- Existing wf that previously read/wrote the parquet table → patch its `spark_submit_cmd` to add these flags. **Do not leave the old wf untouched** — same wf, new table format means new conf.
- Ad-hoc `spark-submit` in CI/CD or local runs → same three flags.
- `iceberg-runbook/<ns>.<table>/phase1_add_files.sql` and `phase2_rewrite.sql` execution wrappers in OpenFlow projects must also carry these flags.

The full Spark conf block from `ICEBERG_WF_GUIDE.md` "Spark-конфигурация для Iceberg" includes additional retry/partial-progress flags — use the full block for compaction wf, and at minimum the three flags above for any other wf that touches the table.

Confirm in your announce that both guides were read AND that you will propagate the three Iceberg conf flags into every affected wf.

### ⚠ MANDATORY — analyze the pipeline, propose Iceberg-native alternatives (do NOT translate 1:1)

The skill rewrites individual call sites. But Iceberg unlocks pipeline-level simplifications that Parquet does not. **Before producing the worklist, scan the project for the patterns below and surface a concrete proposal to the user** rather than mechanically translating existing parquet-era SQL.

| Anti-pattern in the Parquet pipeline | Iceberg-native replacement | Why |
|---|---|---|
| Custom "increment" SQL (`*_inc.sql`, `*_diff*.sql`, `*_changes*.sql`, `*_delta*.sql`) joining today's snapshot with yesterday's to detect inserts / updates / deletes | `MERGE INTO target USING source ON ...`<br>`WHEN NOT MATCHED THEN INSERT *`<br>`WHEN MATCHED AND (src.attr <> tgt.attr OR ...) THEN UPDATE SET *`<br>`WHEN NOT MATCHED BY SOURCE THEN DELETE` | One statement does insert/update/delete atomically; no manual full-outer-join, no snapshot CTEs. |
| Reading "current" + "previous" snapshot and diffing them to emit change events downstream | `SELECT _change_type, * FROM target.changes` (Iceberg changelog scan) | `_change_type` is `INSERT`/`DELETE`/`UPDATE_BEFORE`/`UPDATE_AFTER` — Iceberg tracks this at the metadata layer, no diff query needed. |
| Manual tombstone columns (`is_deleted`, `deleted_at`) used because parquet has no row-level delete | MoR + `format-version=2` + `write.delete.mode=merge-on-read` + ordinary `DELETE FROM` | Iceberg deletes rows natively (position/equality deletes); the tombstone column becomes redundant. |
| Full-partition rewrite for late-arriving data | `MERGE INTO ... ON tgt.part_dt BETWEEN ... AND ...` (partition-aware MERGE) | Iceberg writes only affected files; old data files remain. |

**Detection signals:** SQL files named `*_inc.sql` / `*_diff*` / `*_changes*` / `*_delta*`, `FULL OUTER JOIN` / `LEFT JOIN ... WHERE x.id IS NULL` between two snapshots of the same logical table, `EXCEPT` / `MINUS` between filtered slices of the same source, or `WITH today AS (...), yesterday AS (...)` CTE pattern.

**Workflow:**
1. List each detected anti-pattern with file:line + the canonical replacement.
2. Ask the user to confirm before rewriting — they may have business reasons to keep the explicit increment logic (audit trail, downstream contract).
3. If the user accepts the MERGE-based rewrite, also propose retiring the now-redundant `*_inc.sql` and its `wf_*_inc` workflow entry — they would otherwise keep computing diffs against a table whose history is already in `.changes`.

Full pattern catalogue with before/after SQL examples: [reference.md § Iceberg-native pipeline optimizations](./reference.md#iceberg-native-pipeline-optimizations).

Confirm in your announce that you will run this pipeline analysis pass and surface findings to the user.

## What This Skill Does

Scans the project for Parquet and ORC operations (Hive DDL, Spark Dataset API, generic `format(...)` calls) and replaces them with Apache Iceberg equivalents:

- **Python:** pandas, PySpark (batch + generic format + streaming warn), pyarrow (classic + dataset warn) → pyiceberg
- **Java / Scala:** Spark Dataset API `spark.read().parquet|orc()` and `spark.read().format("parquet"|"orc").load()` → Iceberg Spark runtime (`format("iceberg")`)
- **Hive via SparkSQL:** `STORED AS PARQUET|ORC`, `USING parquet|orc`, `saveAsTable`, `INSERT INTO|OVERWRITE TABLE` → Iceberg-backed tables (`USING iceberg`, `writeTo(...)`)
- **Structured Streaming** and **pyarrow dataset/ParquetFile** are *detected* and left with `TODO(iceberg)` comments for manual rewrite.

Also updates project dependencies (`requirements.txt`, `pyproject.toml`, `pom.xml`, `build.gradle`).

For the full before/after rewrite matrix per language and the multi-table mapping format, see [examples.md](./examples.md).
For subsystem deep-dives (path schemes, constant folding, partition spec extraction, dynamic SQL loading, dry-run, phased runbook, operational concerns, known limitations) see [reference.md](./reference.md).

## Additional project-specific guides (optional)

`S2T_GUIDE.md` and `ICEBERG_WF_GUIDE.md` listed above are **mandatory** — they ship with this skill and always apply.

Beyond them, **also look for any `*_GUIDE.md` files at the project root of the project being migrated** — project owners use this naming convention for migration-relevant context. If such a guide contradicts SKILL.md defaults (and does not contradict the two mandatory skill guides), the project guide wins.

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

### 3. Get table details from S2T (skip the interactive prompt)

**Per the mandatory [S2T_GUIDE.md](./S2T_GUIDE.md), table details come from S2T, NOT from asking the user.** Concrete steps:

1. Locate `s2t.xlsx` (or `hadoop_S2T_<PROJECT>_v<n>.xlsx`) under `<project>/src/main/resources/s2t/`.
2. Read sheets `Tables` (name, description, storage, location), `Columns` (name, type, nullability, description), `Partitions` (partition column + type).
3. The target Iceberg `(namespace, table)` = `(datamart_name from devops.json, table name from S2T)`. Storage column in S2T flips from `HIVE`/parquet to Iceberg.
4. Capture `entity_id` per table from `src/main/resources/wf/ctl/1_ctl_entities.yml`. You will need it in Step 6 and in the wf yaml.
5. Capture `ТУЗ` (yarn user, `u_<id>`) and `очередь ярн` (`root.g_<...>`) from `mart.yml` / `devops.json`. These go into the Gherkin scenario in `qaapi/*.feature`.

Catalog config comes from [ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md) "Spark-конфигурация для Iceberg" — use `spark_catalog` with `type=hive`. Do NOT propose SQLite or REST — those are out of OpenFlow scope.

Only ask the user when S2T is missing, ambiguous, or when a table is not yet declared in S2T (in which case add it to S2T first per S2T_GUIDE "Как добавить новую таблицу в S2T" before continuing).

### 4. Generate the Worklist, Then Rewrite

The CLI does **not** rewrite code on its own. It runs the detector, resolves each match against the user's table mapping, updates dependency manifests, and emits `lakehouse-worklist.json` — a per-call-site task list for the agent/LLM to execute via `Edit`.

```bash
python -m skills.open_table_migrator <project_path> --table <TABLE_NAME> --namespace <NAMESPACE>
# or, for multi-table projects:
python -m skills.open_table_migrator <project_path> --mapping ./lakehouse-mapping.json
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

**Schema is generated from S2T Excel (per S2T_GUIDE.md), TBLPROPERTIES come from ICEBERG_WF_GUIDE.md, catalog is `spark_catalog` with `type=hive`. Do not invent schemas or properties.**

Concrete:

1. Generate / regenerate DDL from `s2t.xlsx` per S2T_GUIDE "Контрольный список перед запуском" (the DDL generator step that turns S2T into `src/main/resources/sql/ddl/<layer>/<table>.sql`). Then change `STORED AS PARQUET` → `USING iceberg`.

```sql
CREATE TABLE {{datamart_name}}.<table> (
  -- columns FROM S2T sheet `Columns` — types/nullability AS-IS from S2T
)
USING iceberg
PARTITIONED BY (
  -- FROM S2T sheet `Partitions` — typically (ctl_loading INT) or part_report_dt
)
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
  -- MoR vs CoW: ICEBERG_WF_GUIDE does not prescribe a default per table.
  -- Add 'write.update.mode' / 'write.delete.mode' / 'write.merge.mode' = 'merge-on-read'
  -- only when the table has row-level UPDATE/DELETE/MERGE — confirm from DML scripts under
  -- src/main/resources/sql/dml/ before setting these.
);
```

2. Wire the table into compaction per [ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md) — this is **not optional**:
   - Create `src/main/resources/sql/dml/exp_iceberg_<table>.sql` (expire_snapshots) — template in ICEBERG_WF_GUIDE "Expire snapshots only".
   - Create `src/main/resources/sql/dml/upd_iceberg_<table>.sql` (full cycle) — template in ICEBERG_WF_GUIDE "Полный цикл обслуживания".
   - Add the two `spark_driver_extraJavaOptions__hdfs_care_*` params for the table to existing `wf_schema_hdfs_care` in `src/main/resources/wf/ctl/ctl.yml`. If `wf_schema_hdfs_care` does not exist for the project, follow "Сценарий 2: Создай отдельный wf для таблицы".
   - Use `entity_id` captured in Step 3.

3. Update the Gherkin scenario (`ift.feature` / `st_skl.feature`) per S2T_GUIDE "Шаг 4: Добавь сценарий в Gherkin-файл" — add the new table to the DDL check sub-scenarios.

The `iceberg-runbook/<ns>.<table>/phase2_rewrite.sql` emitted by the migrator is a **template** — in OpenFlow projects, replace it with the `wf_schema_hdfs_care` wiring above. Phase 1 (`add_files`) and Phase 3 (switchover) still apply.

For the rest of the phased rollout (Phase 1 `add_files`, Phase 3 switchover options) and operational background (MoR/CoW, snapshot expiration), see [reference.md](./reference.md) sections "Phased migration runbook" and "Post-Migration Operational Concerns" — but read them through the lens of ICEBERG_WF_GUIDE.md (wf/ctl wiring, not standalone Spark jobs).

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
