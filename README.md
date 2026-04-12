# parquet-iceberg

A Claude Code **skill** and **subagent** that migrate Python, Java, and Scala projects from Apache Parquet (and Hive-parquet) to Apache Iceberg tables.

## What's in this repo

| Path | Purpose |
|---|---|
| [skills/open_table_migrator/](skills/open_table_migrator/) | The skill: detector, analyzer, filters, transformers, dependency updater, CLI |
| [skills/open_table_migrator/SKILL.md](skills/open_table_migrator/SKILL.md) | Full skill reference — patterns, conversion tables, limitations |
| [.claude/agents/open-table-migrator-migrator.md](.claude/agents/open-table-migrator-migrator.md) | The subagent that drives the migration end-to-end |
| [tests/](tests/) | 60 unit + integration tests covering detector, transformers, deps, CLI |
| [tests/fixtures/](tests/fixtures/) | Minimal sample projects in each supported stack |
| [docs/superpowers/plans/](docs/superpowers/plans/) | Implementation plan (12 tasks) |

## What it converts

- **Python** — pandas, PySpark, pyarrow (incl. ORC variants) → pyiceberg
- **JVM** — Java/Scala Spark Dataset API (`.parquet`/`.orc`/`.format("parquet"|"orc")`) → Iceberg Spark runtime
- **Hive SparkSQL** — `STORED AS PARQUET|ORC`, `USING parquet|orc`, `saveAsTable`, `INSERT INTO|OVERWRITE TABLE` → Iceberg-backed tables
- **Warn-only detection** — Structured Streaming sinks and pyarrow `dataset`/`ParquetFile`/`ParquetDataset` are flagged with `TODO(iceberg)` comments for manual rewrite.

See the [full conversion reference](skills/open_table_migrator/SKILL.md#conversion-reference--python) in SKILL.md.

## Usage

### As a CLI

Single-table:

```bash
PYTHONPATH=. python -m skills.open_table_migrator.cli <project_path> \
    --table <table_name> --namespace <namespace>
```

Multi-table (one Iceberg table per path glob):

```bash
PYTHONPATH=. python -m skills.open_table_migrator.cli <project_path> \
    --mapping ./iceberg-mapping.json
```

See [SKILL.md § Multi-table projects](skills/open_table_migrator/SKILL.md#multi-table-projects) for the mapping file format.

### Via Claude Code

Say something like *"migrate this project to iceberg"* or *"convert parquet to iceberg"* (Russian works too: *"мигрируй на iceberg"*). The [open-table-migrator-migrator](.claude/agents/open-table-migrator-migrator.md) agent takes over and:

1. Detects parquet usage across `.py`/`.java`/`.scala`
2. Shows a report broken down by file, direction, and pattern type
3. Asks for target table name, namespace, and catalog type
4. Runs the transformers and updates build files
5. Surfaces manual TODOs (partition specs, catalog config, data migration)
6. Re-runs the detector to verify zero residual patterns

## Running the tests

```bash
PYTHONPATH=. pytest tests/ --ignore=tests/fixtures -v
```

All 60 tests should pass. Fixtures under `tests/fixtures/` are deliberately excluded — they're sample inputs, not test modules.

## Project layout inside the skill

```
skills/open_table_migrator/
├── SKILL.md              # Skill frontmatter + reference doc
├── detector.py           # Regex scanner: 13 pattern types
├── analyzer.py           # build_report / format_report
├── filters.py            # filter_matches by direction/pattern/glob
├── deps.py               # update_dependencies for 4 build file formats
├── cli.py                # python -m skills.open_table_migrator.cli
└── transformers/
    ├── pandas.py
    ├── pyspark.py
    ├── pyarrow.py
    └── jvm.py            # java + scala (language= param)
```

## Known limitations

- Path arguments must be **string literals** for multi-table routing (variables and f-strings are marked `TODO(iceberg)` for manual mapping)
- Streaming writes (Structured Streaming parquet sinks) are out of scope
- Existing parquet *data* is not migrated — the skill rewrites code only; use `CALL catalog.system.migrate(...)` for in-place Hive migration
- JVM coordinates target Spark 3.5 + Scala 2.12; adjust manually for other versions
- `partitionBy(...)` in JVM code is preserved as a `TODO` comment for the user to add to the Iceberg partition spec

Full list in [SKILL.md § Known Limitations](skills/open_table_migrator/SKILL.md#known-limitations).
