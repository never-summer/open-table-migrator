# parquet-to-iceberg skill — bugs found during LearningSparkV2 test run

**Date:** 2026-04-11
**Tested on:** [databricks/LearningSparkV2](https://github.com/databricks/LearningSparkV2) cloned into `/Users/maksim/git/test-parquet-iceberg/LearningSparkV2`
**Agent:** [parquet-to-iceberg-migrator](../.claude/agents/parquet-to-iceberg-migrator.md)
**Command:** `PYTHONPATH=/Users/maksim/git/parquet-iceberg python -m skills.parquet_to_iceberg.cli <project> --mapping iceberg-mapping.json`

## Result summary

- Detector found 5 hits in 2 files (1 Python, 1 Scala).
- Only **1 of 5** call sites actually rewritten ([`train.py`](../../test-parquet-iceberg/LearningSparkV2/mlflow-project-example/train.py)).
- 4 Scala hits in [`SortMergeJoinBucketed_7_6.scala`](../../test-parquet-iceberg/LearningSparkV2/chapter7/scala/src/main/scala/chapter7/SortMergeJoinBucketed_7_6.scala) left untouched — CLI still reported "Converted".
- Detector re-run after migration: 5 → 4 (guardrail §6 requires 0). **Migration failed verification.**

## Bugs

### A1. CLI reports "Converted" even when transformer is a no-op
- **Location:** [`skills/parquet_to_iceberg/cli.py:63-66`](../skills/parquet_to_iceberg/cli.py)
- **Symptom:** CLI prints `Converted: <file>` and increments counter even though transformer returned source byte-for-byte unchanged.
- **Expected:** Diff source before/after; only report files that actually changed. Warn when transformer left file unchanged despite N detector hits.
- **Repro:** Run CLI on any file whose detector matches are all multi-line Scala/Java write chains.

### A2. JVM transformer silently fails on multi-line builder chains  *(highest priority)*
- **Location:** [`skills/parquet_to_iceberg/transformers/jvm.py:52-156`](../skills/parquet_to_iceberg/transformers/jvm.py) — `for raw_line in source.splitlines(keepends=True):`
- **Symptom:** Scala/Java chain `.write.format("parquet").mode(...).bucketBy(...).saveAsTable(...)` spanning 5+ lines is never matched — every regex requires `.write ... .saveAsTable` on one line. Even the `needs_target` fallback requires the same single-line form, so no TODO comment is emitted. Violates guardrail "Never silently skip files".
- **Expected:** Fold continuation lines into a logical statement before applying regexes, OR emit `// TODO(iceberg): multi-line write chain — rewrite manually` on any dangling `.write.format("parquet"|"orc")` / `.write.parquet(` line.
- **Repro:** Run on [`SortMergeJoinBucketed_7_6.scala:53-69`](../../test-parquet-iceberg/LearningSparkV2/chapter7/scala/src/main/scala/chapter7/SortMergeJoinBucketed_7_6.scala) — file left verbatim, detector still reports 4 matches.

### A3. `extract_path_arg` cannot capture targets across lines
- **Location:** [`skills/parquet_to_iceberg/extract.py`](../skills/parquet_to_iceberg/extract.py) (called with a single line)
- **Symptom:** `.write.format("parquet")` on its own line yields `path_arg=None`; the real target (`saveAsTable("UsersTbl")`) is 4 lines below. Router cannot resolve.
- **Expected:** Multi-line chain awareness — look ahead for terminal `.save(...)`, `.saveAsTable(...)`, `.load(...)` within the same logical statement.
- **Repro:** Same file as A2.

### A4. Dependency updater ignores sbt and nested build files
- **Location:** [`skills/parquet_to_iceberg/deps.py:11-27`](../skills/parquet_to_iceberg/deps.py)
- **Symptom:** `build.sbt` not recognized. Only project root is scanned — nested modules' `pom.xml` / `build.gradle` also ignored. CLI still prints "Updated dependencies."
- **Expected:**
  - (a) sbt support — append `libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.5.0"`.
  - (b) `rglob` for build files instead of top-level glob.
  - (c) Report what was actually touched; print "no build file found" when 0 updates.
- **Repro:** Run CLI on any repo whose only build files are nested `build.sbt` (e.g. LearningSparkV2, 4 nested modules).

### A5. Duplicate `from pyiceberg.catalog import load_catalog` injected
- **Location:** [`skills/parquet_to_iceberg/transformers/pandas.py:72-75`](../skills/parquet_to_iceberg/transformers/pandas.py) and [`skills/parquet_to_iceberg/transformers/pyarrow.py:75-81`](../skills/parquet_to_iceberg/transformers/pyarrow.py)
- **Symptom:** Both transformers unconditionally inject pyiceberg import even when (a) there are no pandas/pyarrow calls in the file and (b) the import already exists. [`train.py:10-11`](../../test-parquet-iceberg/LearningSparkV2/mlflow-project-example/train.py) now has two identical imports; neither is used (actual transformation was pyspark's `spark.table(...)`).
- **Expected:**
  - (a) Skip injection when `unique_fqns` is empty.
  - (b) De-dupe: `if "pyiceberg.catalog import load_catalog" in source: skip`.
- **Repro:** Any `.py` file with a pyspark read/write and no pandas/pyarrow — pyspark_transform runs, then pandas_transform + pyarrow_transform each append an unused import.

### A6. Multi-line Iceberg config comment breaks indentation
- **Location:** [`skills/parquet_to_iceberg/transformers/pyspark.py:6-10,79-81`](../skills/parquet_to_iceberg/transformers/pyspark.py)
- **Symptom:** `_ICEBERG_CONF_COMMENT` is 3 lines; only the first gets the indent prefix `sp`. Lines 2–3 land at column 0 inside an indented function body. Parses (they're comments) but looks broken.
- **Expected:** Prefix every line of the multi-line comment with the detected indent.
- **Repro:** Any Python file where the first matched read/write is inside an indented block.

### A7. Agent doc does not warn about sbt / nested build files
- **Location:** [`.claude/agents/parquet-to-iceberg-migrator.md`](../.claude/agents/parquet-to-iceberg-migrator.md), steps 1 & 7
- **Symptom:** Agent workflow says "check for `pom.xml` / `build.gradle`" but never mentions sbt or nested layouts. Agent was lulled into trusting "Updated dependencies" output.
- **Expected:**
  - Step 1 should enumerate build tools the skill supports and warn when sbt / nested layout is detected.
  - Step 7 should double-check the build file was actually modified (diff).

### A8. CLI prints "Updated dependencies" unconditionally
- **Location:** [`skills/parquet_to_iceberg/cli.py:69`](../skills/parquet_to_iceberg/cli.py)
- **Symptom:** Summary line says "Updated dependencies." even when `update_dependencies` touched 0 files.
- **Expected:** Track and report which files were actually updated, or print "no build file found".
- **Repro:** Run on any project with no `pom.xml` / `build.gradle` / `requirements.txt` / `pyproject.toml` at root.

### A9. `PatternMatch` has no `direction` attribute despite agent doc referencing it
- **Location:** Agent doc ([line 31](../.claude/agents/parquet-to-iceberg-migrator.md)) says "breakdown by direction (read / write / schema)"; [`detector.py:8-14`](../skills/parquet_to_iceberg/detector.py) `PatternMatch` has no such field.
- **Symptom:** Accessing `m.direction` raises `AttributeError`. Direction is derived inside `analyzer.build_report`.
- **Expected:** Either expose `direction` on `PatternMatch`, or fix the agent doc to say it's derived by the analyzer.

### A10. Detector doesn't flag SQL DDL referencing already-mapped tables
- **Location:** [`skills/parquet_to_iceberg/detector.py:99-111`](../skills/parquet_to_iceberg/detector.py) `_HIVE_PATTERNS`
- **Symptom:** `spark.sql("DROP TABLE IF EXISTS UsersTbl")` / `CACHE TABLE UsersTbl` in [`SortMergeJoinBucketed_7_6.scala:53,68,69`](../../test-parquet-iceberg/LearningSparkV2/chapter7/scala/src/main/scala/chapter7/SortMergeJoinBucketed_7_6.scala) reference the same tables being migrated but don't appear in the report. DROP still works on Iceberg; `CACHE TABLE` behaves differently — user should be warned.
- **Expected:** Optional second pass flagging same-file SQL strings that reference `path_arg` values already mapped to Iceberg targets.

## Confidence by component

| Component | Confidence | Notes |
|---|---|---|
| Detector (Python/Scala/Java source) | **High** | Found all real hits in LearningSparkV2, no false positives in source. |
| Python transformers (pandas/pyarrow/pyspark) | **Medium** | Rewrite is correct; cosmetic issues (A5, A6). |
| JVM transformer (Java/Scala) | **Low** | Multi-line chain = silent no-op (A2, A3). |
| Dependency updater | **Low** | sbt unsupported, no nested-module support, false success message (A4, A8). |
| CLI reporting | **Low** | Claims success without verifying transformer output (A1, A8). |
| Agent doc | **Medium** | Missing sbt warnings, `direction` typo (A7, A9). |

## Single highest-value fix

**A2.** Make `transform_jvm_file` either handle multi-line builder chains or emit `// TODO(iceberg)` on every unhandled `.write.format("parquet"|"orc")` / `.saveAsTable(` line. Alone this turns silent failure into honest partial migration with actionable TODOs.

## Non-source blind spot (not a skill bug)

[`notebooks/LearningSparkv2.dbc`](../../test-parquet-iceberg/LearningSparkV2/notebooks/LearningSparkv2.dbc) is a binary Databricks archive that likely contains many more parquet operations. The detector can't scan it — user must extract first.
