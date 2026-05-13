# `--dry-run` Flag — Design

**Date:** 2026-05-08
**Status:** Approved for implementation planning
**Branch:** `feat/dry-run-flag`
**Scope ref:** improvement 5.1 from the migrator roadmap

## Goal

Add a `--dry-run` CLI flag that runs the full migration pipeline (detection, cross-reference, worklist building, prepass planning, dependency-update planning) but writes **nothing** to disk. Output goes to stdout in four structured sections: a summary, the worklist JSON that would be written, a unified diff of prepass changes, and a unified diff of build-file changes. This is a hard requirement for change-review workflows in regulated environments (banking compliance): the user runs `--dry-run`, pastes output to the change ticket, gets approval, then runs for real.

## Non-goals

- No sub-flags (`--dry-run-format json`, `--dry-run-only-prepass`, etc.). Single mode.
- No coloured output. Plain text only, suitable for piping into files or comments.
- No separate output-file flag (`--dry-run-output ./preview.txt`). Users redirect stdout themselves.
- No validation mode (non-zero exit if changes detected). Dry-run is preview only; always returns 0 on success.
- No diff for code rewrites driven by the worklist. The migrator no longer rewrites code itself — that's the agent's job through `Edit` against the worklist hints. We have no way to predict the agent's output.
- No interactive prompts. The flag is non-interactive.

## CLI surface

```bash
PYTHONPATH=. python3 -m skills.open_table_migrator <project_path> [--dry-run] [other flags]
```

Composition with existing flags:
- `--dry-run --no-deps` — valid; `--no-deps` is redundant but does not error.
- `--dry-run --mapping foo.json` — valid; mapping is read normally.
- `--dry-run --table X --namespace Y` — valid; same validation as today.

Existing validation rules unchanged: `--table` without `--namespace` (or vice versa) still errors with exit code 2, even in dry-run mode.

## What is suppressed

When `--dry-run=True`, `convert_project` suppresses three side effects:

1. `write_worklist(...)` — no `lakehouse-worklist.json` written to project root.
2. `run_prepass(...)` — no source-file modifications.
3. `update_dependencies(...)` — no build-file modifications.

All read-only operations (`detect_parquet_usage`, `scan_sql_files`, `scan_sql_table_references`, `detect_dynamic_sql_loaders`, `cross_reference_sql`, `cross_reference_dynamic_sql`, `annotate_partition_mismatch`, `find_ddl_references`) execute as normal.

## Output format

Four sections to stdout, in this order:

```
=== DRY RUN — no files will be modified ===

--- Summary ---
Detected 64 I/O operation(s), 12 SQL table definition(s).
Would write: lakehouse-worklist.json (47 entries)
Would prepass: 8 file(s) with 14 marker(s)
Would update: pyproject.toml (add pyiceberg dependency)

--- Worklist preview (lakehouse-worklist.json) ---
{
  "version": 1,
  "count": 47,
  "entries": [...],
  "dynamic_sql_loaders": [...]
}

--- Prepass diff preview ---
=== src/jobs/legacy.py (2 markers) ===
@@ -41,3 +41,4 @@
     df.to_csv(out_path)
+    # iceberg: skipped by mapping
     pd.read_parquet(LEGACY_PATH)

=== src/jobs/main.py (1 marker, pyspark conf) ===
@@ -1,3 +1,5 @@
+# iceberg: see SparkSession conf below for Iceberg catalog
+# spark.conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
 import pyspark.sql
 ...

--- Build-file updates ---
pyproject.toml:
@@ -15,3 +15,4 @@
 dependencies = [
   "pandas>=2.0.0",
+  "pyiceberg[sql-sqlite]>=0.7.0",
 ]
```

**Delimiters:**
- `=== ` for top-level blocks and per-file diff headers.
- `--- ` for subsection titles.

**What is NOT output:**
- The pre-existing human-readable summary lines (SQL cross-references, secondary references) — these are printed by the normal CLI path; dry-run does not duplicate them. The dry-run summary section is its own thing.
- Stderr is unused for the preview content. Errors go to stderr as today.

**Empty sections:** if a section has nothing to show (e.g., no prepass edits planned), it's omitted entirely rather than printed as empty. The Summary section always appears.

## Architecture

Each of the three side-effect functions is refactored to separate planning from application. Existing public signatures are preserved for backward compatibility; new `plan_*` / `serialize_*` functions are added alongside.

### `skills/open_table_migrator/prepass.py`

```python
@dataclass(frozen=True)
class PrepassPlan:
    """What prepass would do for one file."""
    file: Path
    original: str
    modified: str
    marker_count: int
    pyspark_conf_added: bool

    @property
    def diff(self) -> str:
        """Unified diff between original and modified."""


def plan_prepass(matches, resolver) -> list[PrepassPlan]:
    """Compute what prepass would change. No filesystem writes."""


def run_prepass(matches, resolver) -> dict[Path, int]:
    """Apply plans. Same signature/return as today — wraps plan_prepass + writes."""
```

### `skills/open_table_migrator/deps.py`

```python
@dataclass(frozen=True)
class BuildFileUpdate:
    file: Path
    original: str
    modified: str

    @property
    def diff(self) -> str:
        """Unified diff."""


def plan_dependencies_update(project_root) -> list[BuildFileUpdate]:
    """Compute build-file changes. No filesystem writes."""


def update_dependencies(project_root) -> list[Path]:
    """Apply plans. Same signature/return as today."""
```

### `skills/open_table_migrator/worklist.py`

```python
def serialize_worklist(entries, *, dyn_cross=None, project_root=None) -> str:
    """Build the JSON string that would be written. No filesystem writes."""


def write_worklist(entries, project_root, *, dyn_cross=None) -> Path:
    """Apply: write the JSON to project_root. Same signature/return as today."""
```

### `skills/open_table_migrator/cli.py`

`convert_project` gains a `dry_run: bool = False` parameter. When True, after detection/cross-ref/annotation, the function dispatches to a new `_run_dry()` helper:

```python
def _run_dry(project_root, matches, sql_defs, resolver, dyn_cross, update_deps_flag) -> int:
    entries = build_worklist(matches, project_root, resolver)
    prepass_plans = plan_prepass(matches, resolver)
    build_plans = plan_dependencies_update(project_root) if update_deps_flag else []

    print("=== DRY RUN — no files will be modified ===\n")
    _print_summary_section(entries, prepass_plans, build_plans)
    _print_worklist_preview(entries, dyn_cross, project_root)
    _print_prepass_diff(prepass_plans)
    _print_build_diff(build_plans)
    return 0
```

`main()` argparse gets `--dry-run` flag passed through to `convert_project`.

## Diff generation

Both `PrepassPlan.diff` and `BuildFileUpdate.diff` use `difflib.unified_diff(original.splitlines(keepends=True), modified.splitlines(keepends=True), fromfile=str(file), tofile=str(file))` and join the result into a single string.

Conventional unified diff format is used (`@@ -X,Y +X,Y @@`, `-` / `+` / space prefixes). Banking change-review tooling is universally familiar with this format.

## Backward compatibility

All existing callers of `run_prepass`, `update_dependencies`, `write_worklist` continue to work unchanged. Their signatures and return values are preserved. The new `plan_*` / `serialize_*` functions are net-new additions.

`convert_project`'s new `dry_run` parameter defaults to `False`, so all existing callers (the CLI's `main()`, any tests) see no behavior change unless they explicitly opt in.

## Testing strategy

### `tests/test_prepass.py` — `plan_prepass` (3 new cases)

- File with skip-markers planned → `PrepassPlan.modified` contains markers, file on disk unchanged
- Python file with pyspark match → `pyspark_conf_added=True`, marker_count=0
- File with no changes → not returned in plan list (or `modified == original`)

### `tests/test_deps.py` — `plan_dependencies_update` (2 new cases)

- `pyproject.toml` without pyiceberg → `BuildFileUpdate.modified` adds pyiceberg, file unchanged
- Already-updated project → empty plan (idempotent)

### `tests/test_worklist.py` — `serialize_worklist` (2 new cases)

- Without `dyn_cross` → valid JSON matching what `write_worklist` would produce
- With `dyn_cross` → JSON contains `dynamic_sql_loaders` key

### `tests/test_cli.py` — end-to-end `--dry-run` (5 new cases)

- `convert_project(dry_run=True)` on a parquet-using fixture → returns 0, stdout contains `=== DRY RUN ===` marker
- `convert_project(dry_run=True)` does NOT create `lakehouse-worklist.json` in project_root
- `convert_project(dry_run=True)` does NOT modify source files (checked via SHA-256 of file contents before/after)
- `convert_project(dry_run=True)` does NOT modify `pyproject.toml` (same check)
- stdout contains the 4 expected sections: Summary, Worklist preview, Prepass diff preview, Build-file updates (when each has content)

### argparse parsing test

- `--dry-run` flag is accepted by `main()` and threaded into `convert_project(dry_run=True)`

### Out of scope for tests

- Color/format options (no such feature)
- Performance benchmarks
- Concurrent dry-run + non-dry-run on same project

## Risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Refactoring `run_prepass` / `update_dependencies` / `write_worklist` accidentally changes existing behavior | medium | Backward-compat tests (existing test suite) must continue to pass. The plan keeps the old signatures and just delegates internally |
| Large prepass diff output overwhelms terminal/log | low | Diff is bounded by the number of detected matches and is small per file (1-3 lines added per match). For 1000-match projects the diff is ~3000 lines — large but parseable |
| `serialize_worklist` JSON diverges from `write_worklist` JSON over time | low | `write_worklist` is implemented in terms of `serialize_worklist` (single source of truth) |
| Diff format changes break downstream change-review tooling | very low | `difflib.unified_diff` is stable across Python versions; format matches git/patch conventions |
| User runs `--dry-run` then forgets to run for real | low | Out of scope (user's responsibility). The DRY RUN header is prominent enough |

## Estimated size

- Production: ~80 LOC refactor (3 functions + dataclasses for plans) + ~120 LOC `_run_dry` and helpers in `cli.py`
- Tests: ~150 LOC across `test_prepass.py`, `test_deps.py`, `test_worklist.py`, `test_cli.py`
- Documentation: ~25 lines in SKILL.md (new "Dry run" section)
- Total: ~375 LOC. Two implementation sessions.

## Open questions deferred to implementation

- Exact text of the `=== DRY RUN — no files will be modified ===` header (cosmetic).
- Sorting order of `prepass_plans` in the output (alphabetic file path for stable output across runs).
- `difflib.unified_diff` vs `difflib.context_diff` — choosing `unified_diff` as more familiar for change-review.
- Whether empty `dynamic_sql_loaders` is rendered in the preview JSON or omitted — match what `write_worklist` does (omit when empty for cleanliness).
