# open-table-migrator (скилл)

Агентский скилл для миграции data-проектов на **Python, Java и Scala** с Parquet / ORC / Hive-parquet на Apache Iceberg. Запускается ИИ-агентом (Claude Code, Qwen Code) — агент читает `SKILL.md`, выполняет шаги, при необходимости вызывает CLI из `scripts/`.

Скилл **не переписывает код своими руками**. Он:
1. Парсит `.py` / `.java` / `.scala` / `.sql` в AST через tree-sitter, находит все Parquet/ORC операции.
2. Резолвит каждое место под конкретную целевую таблицу `(namespace, table)`.
3. Генерирует `lakehouse-worklist.json` — список задач для агента (что и где переписать).
4. Генерирует `iceberg-runbook/<ns>.<table>/` — фазовый план для DBA / релиз-инженера (`phase1_add_files.sql`, `phase2_rewrite.sql`, `phase3_switchover.sql`).
5. Обновляет зависимости проекта (`requirements.txt`, `pyproject.toml`, `pom.xml`, `build.gradle`).

Дальше агент по worklist'у применяет правки через `Edit` — то есть код переписывает агент, не скрипт.

---

## Что внутри директории

```
open_table_migrator/
├── SKILL.md              ← главный entry-point для агента (что делать, шаги 1–8)
├── README.md             ← вы здесь
├── examples.md           ← before/after таблицы по языкам, multi-table mapping
├── reference.md          ← глубокие разделы (path schemes, partition spec,
│                            dynamic SQL, dry-run, runbook, ops-concerns)
├── S2T_GUIDE.md          ← ⚠ ОБЯЗАТЕЛЬНО к прочтению агентом перед миграцией
├── ICEBERG_WF_GUIDE.md   ← ⚠ ОБЯЗАТЕЛЬНО к прочтению агентом перед миграцией
├── __init__.py
├── __main__.py           ← python -m skills.open_table_migrator
└── scripts/              ← Python-реализация (Qwen canonical layout)
    ├── cli.py
    ├── detector.py       ← поиск I/O через tree-sitter
    ├── analyzer.py       ← сопоставление code ↔ DDL, partition mismatch
    ├── sql_registry.py   ← парсинг CREATE TABLE из .sql
    ├── dynamic_sql.py    ← поиск runtime-загрузки .sql файлов
    ├── scope.py          ← constant folding на уровне файла
    ├── targets.py        ← резолвер path/table → (ns, table) по mapping
    ├── prepass.py        ← skip-маркеры + pyspark conf
    ├── worklist.py       ← lakehouse-worklist.json builder
    ├── runbook.py        ← iceberg-runbook/ builder
    ├── deps.py           ← обновление build-файлов
    ├── filters.py, extract.py, uri.py
    ├── ts_detector.py    ← tree-sitter queries
    └── ts_parser.py
```

---

## ⚠ Обязательные гайды

Перед запуском миграции агент **обязан** прочитать два файла, поставляемых вместе со скиллом:

- **[S2T_GUIDE.md](./S2T_GUIDE.md)** — система Spec-to-Test, используемая в проектах OpenFlow (`custom_blago_dzo_*` и др.). Описывает откуда брать схему таблиц (`s2t.xlsx`, листы `Tables`/`Columns`/`Partitions`), как заполнить Gherkin-сценарии, где взять `entity_id` / ТУЗ / yarn queue / `datamart_name`. **DDL генерируется из S2T, не из существующего parquet.**

- **[ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md)** — система workflow `wf/ctl/*.yml` над Oozie. Описывает:
  - обязательную Spark-конфигурацию для Iceberg (три флага `--conf spark.sql.extensions=...`, `spark_catalog=...SparkSessionCatalog`, `spark_catalog.type=hive`) — определяется **одной разделяемой переменной** в `mart.yml` (сначала grep'ом ищется существующая: `spark_submit_cmd_iceberg_service` / `spark_iceberg` / `iceberg_conf` / project-specific; если нет — создаётся новая и подключается через `{{mart.<name>}}` во все затронутые wf, без инлайна);
  - подключение compaction через **maintenance wf проекта** (сначала найди `grep -rn 'rewrite_data_files\|expire_snapshots\|hdfs_care' src/main/resources/wf/ctl/`; типовые имена — `wf_schema_hdfs_care`, `wf_<table>_service`, project-specific), а не через standalone `CALL system.rewrite_data_files`;
  - шаблоны `exp_iceberg_<table>.sql` / `upd_iceberg_<table>.sql`;
  - locks через CTL (`init_locks: checks/sets`).

Если рекомендация из `SKILL.md` / `examples.md` / `reference.md` или сгенерированного `iceberg-runbook/` противоречит этим гайдам — **гайды побеждают**. Подробнее — в самом начале [SKILL.md](./SKILL.md).

---

## Как пользоваться

### Способ 1. Через ИИ-агента (рекомендуется)

В Claude Code или Qwen Code просто скажите агенту:

> *мигрируй этот проект с parquet на iceberg*
> *переведи hive-таблицы на iceberg*
> *convert parquet to iceberg*

Агент сам подберёт скилл (по описанию из frontmatter `SKILL.md`), прочитает гайды, прогонит детектор, спросит подтверждения по найденным анти-паттернам пайплайна (см. секцию "analyze the pipeline" в SKILL.md) и применит правки.

### Способ 2. Напрямую через CLI

CLI запускает только аналитическую часть — парсинг, резолвинг таблиц, генерацию `lakehouse-worklist.json` и `iceberg-runbook/`. Правки в код CLI сам не вносит (это работа агента).

**Single-table проект:**
```bash
PYTHONPATH=. python -m skills.open_table_migrator <project_path> \
    --table users --namespace analytics
```

**Multi-table проект (несколько целевых таблиц через mapping-файл):**
```bash
PYTHONPATH=. python -m skills.open_table_migrator <project_path> \
    --mapping ./lakehouse-mapping.json
```

Формат `lakehouse-mapping.json` — в [examples.md § Multi-table projects](./examples.md#multi-table-projects).

**Превью без записи на диск (для change-review):**
```bash
PYTHONPATH=. python -m skills.open_table_migrator <project_path> \
    --table users --namespace analytics --dry-run
```

`--dry-run` прогоняет всё, но ничего не пишет — печатает 5 секций в stdout (Summary, Worklist preview, Prepass diff, Build-file diff, Runbook preview). Подходит для прикладывания к change-ticket в банковских процессах. Подробнее — [reference.md § Dry run](./reference.md#dry-run).

**Без обновления зависимостей** (если версии пинятся вручную):
```bash
PYTHONPATH=. python -m skills.open_table_migrator <project_path> \
    --table users --namespace analytics --no-deps
```

---

## Что детектируется

**Python:**
- pandas: `pd.read_parquet` / `pd.read_orc` / `.to_parquet` / `.to_orc`
- PySpark batch: `spark.read.parquet|orc(...)` / `df.write.parquet|orc(...)`
- PySpark generic: `spark.read.format("parquet"|"orc").load(...)` / `df.write.format(...).save(...)`
- PySpark streaming *(warn-only)*: `readStream.*` / `writeStream.*`
- pyarrow: `pq.read_table` / `pq.write_table` / `orc.read_table` / `orc.write_table`
- pyarrow dataset *(warn-only)*: `ParquetFile`, `ParquetDataset`, `pa.dataset.*`
- SparkSQL через `spark.sql(...)`: `STORED AS PARQUET|ORC`, `USING parquet|orc`, `INSERT INTO|OVERWRITE TABLE`

**Java / Scala:** те же паттерны на Spark Dataset API + `saveAsTable` + Hive DDL.

**SQL (`.sql` файлы):**
- `CREATE TABLE ... STORED AS PARQUET|ORC` / `USING parquet|orc`
- `INSERT INTO|OVERWRITE TABLE` / `UPDATE` / `MERGE INTO` / `FROM` / `JOIN`
- Поля партиционирования из `PARTITIONED BY (...)`

**Кросс-референсы:**
- Динамическая загрузка SQL (`open("queries/x.sql")`, `Path.read_text()`, `getClass().getResourceAsStream`, `pkgutil.get_data`) — резолвится через project tree.
- Code↔DDL partition mismatch — если `df.write.partitionBy(...).saveAsTable(t)` в коде расходится с `CREATE TABLE t ... PARTITIONED BY (...)` в DDL.

Полный список и edge cases — [reference.md](./reference.md).

---

## Возможности

| Что | Где описано |
|---|---|
| Детекция I/O через tree-sitter AST | [SKILL.md § Step 2](./SKILL.md) |
| Constant folding для путей (`PATH = "s3://..."`) | [reference.md § Constant folding](./reference.md#constant-folding) |
| Извлечение partition spec из `partitionBy` / `bucketBy` / DDL | [reference.md § Partition spec extraction](./reference.md#partition-spec-extraction) |
| Multi-table mapping (`--mapping foo.json`) | [examples.md § Multi-table projects](./examples.md#multi-table-projects) |
| URI-aware path matching (s3/s3a, hdfs/webhdfs, abfs/abfss, viewfs, gs, file) | [reference.md § Path schemes](./reference.md#path-schemes) |
| Dynamic SQL loading (`spark.sql(open("queries/*.sql").read())`) | [reference.md § Dynamic SQL loading](./reference.md#dynamic-sql-loading) |
| Dry-run preview без записи на диск | [reference.md § Dry run](./reference.md#dry-run) |
| Фазовый runbook (phase1 add_files / phase2 rewrite / phase3 switchover) | [reference.md § Phased migration runbook](./reference.md#phased-migration-runbook) |
| Анализ pipeline-уровня → MERGE / `.changes` / MoR вместо ручного diff | [SKILL.md § MANDATORY analyze the pipeline](./SKILL.md), [reference.md § Iceberg-native pipeline optimizations](./reference.md#iceberg-native-pipeline-optimizations) |
| Обновление build-файлов (pyiceberg / iceberg-spark-runtime) | [SKILL.md § Dependencies Added](./SKILL.md) → [examples.md](./examples.md) |

---

## Что генерируется в проекте после запуска

| Файл / директория | Что внутри |
|---|---|
| `lakehouse-worklist.json` | Список задач для агента: для каждого call-сайта `file:line` + `pattern_type` + `direction` + резолвленный `(namespace, table)` + `hint` для замены |
| `iceberg-runbook/README.md` | Top-level индекс миграций (по одной строке на таблицу) |
| `iceberg-runbook/<ns>.<table>/migration-plan.md` | Markdown-план миграции таблицы: pre-flight, фазы, code sites, warnings |
| `iceberg-runbook/<ns>.<table>/phase1_add_files.sql` | Spark SQL для `CALL system.add_files` (in-place, без переписывания данных) |
| `iceberg-runbook/<ns>.<table>/phase2_rewrite.sql` | Spark SQL для `CALL system.rewrite_data_files` (компакция) — в OpenFlow проектах **подключается через maintenance wf** (найди существующий grep'ом или предложи имя: `wf_schema_hdfs_care` / `wf_<table>_service` / `wf_iceberg_maintenance`), не запускается standalone |
| `iceberg-runbook/<ns>.<table>/phase3_switchover.sql` | Три OPTION-блока для cutover (Spark VIEW / HMS rename / per-call-site update) |
| Обновления `pyproject.toml` / `pom.xml` / `build.gradle` / etc. | Добавление `pyiceberg[sql-sqlite]` или `iceberg-spark-runtime` |

---

## Известные ограничения

- Spark SQL диалект только (Trino / ClickHouse / Snowflake — не покрываются).
- Структурированный стриминг (`readStream` / `writeStream`) детектируется, но **не переписывается** — выставляется `TODO(iceberg)`.
- pyarrow dataset API (`ParquetFile`, `ParquetDataset`, `pa.dataset.*`) — warn-only.
- Cloud catalog (Glue, Nessie, REST) — конфигурируется вручную.
- Schema в `phase1_add_files.sql` — плейсхолдер; оператор подставляет результат `spark.read.parquet(...).printSchema()` или (в OpenFlow) DDL из S2T.
- FQN propagation — после `saveAsTable("Foo")` → `writeTo("ns.Foo")` все downstream обращения по короткому имени (`spark.table("Foo")`, `CACHE TABLE Foo`, `FROM Foo`) приходится править вручную.

Полный список — [reference.md § Known Limitations](./reference.md#known-limitations).

---

## Куда смотреть дальше

| Зачем | Куда |
|---|---|
| Понять алгоритм работы пошагово | [SKILL.md](./SKILL.md) |
| Найти Iceberg-эквивалент конкретного Parquet-вызова | [examples.md](./examples.md) |
| Узнать как работает конкретная подсистема (mapping, dry-run, runbook, etc.) | [reference.md](./reference.md) |
| Понять как стыковаться с S2T (OpenFlow проекты) | [S2T_GUIDE.md](./S2T_GUIDE.md) |
| Понять как стыковаться с workflow Oozie (OpenFlow проекты) | [ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md) |
| Общая информация о проекте, лицензия, репо-уровневый README | [/README.md](../../README.md) |
