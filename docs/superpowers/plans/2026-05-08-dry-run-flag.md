# `--dry-run` Flag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `--dry-run` CLI flag that runs the full migration pipeline with zero filesystem writes. Output to stdout in four sections: Summary, Worklist JSON preview, Prepass unified diff, Build-file unified diff.

**Architecture:** Refactor three side-effect functions (`run_prepass`, `update_dependencies`, `write_worklist`) into plan + apply pairs. `plan_*` and `serialize_worklist` are pure functions returning data; existing `run_*` / `update_*` / `write_*` are kept as thin wrappers that call the plan + apply to disk (backward compat preserved). `convert_project` gets a `dry_run: bool` parameter; when True it dispatches to a new `_run_dry()` helper that renders the four sections via `difflib.unified_diff`.

**Tech Stack:** Python 3.11+, stdlib `difflib`, `json`, `dataclasses`. No new dependencies.

**Spec:** [docs/superpowers/specs/2026-05-08-dry-run-flag-design.md](../specs/2026-05-08-dry-run-flag-design.md)

---

## File Structure

Modified files:
- `skills/open_table_migrator/prepass.py` — add `PrepassPlan` + `plan_prepass`; `run_prepass` becomes thin wrapper
- `skills/open_table_migrator/deps.py` — add `BuildFileUpdate` + `plan_dependencies_update`; `update_dependencies` becomes thin wrapper
- `skills/open_table_migrator/worklist.py` — add `serialize_worklist`; `write_worklist` becomes thin wrapper
- `skills/open_table_migrator/cli.py` — add `dry_run: bool = False` to `convert_project`, new `_run_dry` helper + 4 print helpers, `--dry-run` argparse flag in `main()`
- `skills/open_table_migrator/SKILL.md` — new "Dry run" section

New tests:
- `tests/test_prepass.py` — extend with 3 `plan_prepass` cases (file may not exist yet, create if needed)
- `tests/test_deps.py` — extend with 2 `plan_dependencies_update` cases
- `tests/test_worklist.py` — extend with 2 `serialize_worklist` cases
- `tests/test_cli.py` — extend (or create) with 5 end-to-end `--dry-run` cases + 1 argparse case

---

## Task 1: prepass.py — PrepassPlan + plan_prepass

**Files:**
- Modify: `skills/open_table_migrator/prepass.py`
- Modify (or create): `tests/test_prepass.py`

### Step 1: Check if test file exists, create if not

Run: `ls tests/test_prepass.py 2>&1`. If "No such file", create with:
```python
# tests/test_prepass.py
"""Tests for prepass module."""
```

### Step 2: Failing tests

Append to `tests/test_prepass.py`:
```python
from pathlib import Path
from textwrap import dedent

from skills.open_table_migrator.detector import PatternMatch
from skills.open_table_migrator.prepass import PrepassPlan, plan_prepass
from skills.open_table_migrator.targets import Target, constant_resolver


def test_plan_prepass_for_skip_match_does_not_write_disk(tmp_path):
    """plan_prepass must NOT modify files. It returns a plan only."""
    src = tmp_path / "job.py"
    src.write_text(dedent('''
        import pandas as pd
        df = pd.read_parquet("s3://legacy/x.parquet")
    '''))
    original_content = src.read_text()
    original_mtime = src.stat().st_mtime

    from skills.open_table_migrator.targets import Mapping, MappingEntry
    mapping = Mapping(entries=[
        MappingEntry(path_glob="s3://legacy/*", skip=True, target=None),
    ])
    from skills.open_table_migrator.targets import build_resolver
    resolver = build_resolver(mapping, fallback=None, project_root=tmp_path)

    m = PatternMatch(
        file=src,
        line=3,
        pattern_type="pandas_read_parquet",
        original_code='df = pd.read_parquet("s3://legacy/x.parquet")',
        path_arg="s3://legacy/x.parquet",
    )
    plans = plan_prepass([m], resolver)

    # File on disk MUST NOT change
    assert src.read_text() == original_content
    # Plan reflects what would be added
    assert len(plans) == 1
    plan = plans[0]
    assert plan.file == src
    assert plan.original == original_content
    assert "iceberg: skipped by mapping" in plan.modified
    assert plan.marker_count == 1


def test_plan_prepass_for_pyspark_match_adds_conf_block(tmp_path):
    """A pyspark match (non-skip) should result in a plan that adds the
    Iceberg conf comment block."""
    src = tmp_path / "job.py"
    src.write_text(dedent('''
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.parquet("s3://prod/users")
    '''))
    original = src.read_text()

    from skills.open_table_migrator.targets import Mapping, build_resolver
    fallback = Target(namespace="ns", table="t")
    resolver = build_resolver(Mapping(), fallback=fallback, project_root=tmp_path)

    m = PatternMatch(
        file=src,
        line=4,
        pattern_type="pyspark_read_parquet",
        original_code='df = spark.read.parquet("s3://prod/users")',
        path_arg="s3://prod/users",
    )
    plans = plan_prepass([m], resolver)

    # Still must not write
    assert src.read_text() == original
    assert len(plans) == 1
    plan = plans[0]
    assert plan.pyspark_conf_added is True
    assert "iceberg: see SparkSession conf" in plan.modified or "spark.sql.catalog" in plan.modified


def test_plan_prepass_for_file_with_no_changes_returns_no_plan(tmp_path):
    """When a match's resolver decision is neither skip nor pyspark-conf-eligible,
    plan_prepass should not return a plan for that file (or return one where
    modified == original)."""
    src = tmp_path / "job.py"
    src.write_text("# already migrated\n")
    original = src.read_text()

    from skills.open_table_migrator.targets import Mapping, build_resolver
    fallback = Target(namespace="ns", table="t")
    resolver = build_resolver(Mapping(), fallback=fallback, project_root=tmp_path)

    m = PatternMatch(
        file=src,
        line=1,
        pattern_type="pandas_read_csv",  # non-skip, non-pyspark
        original_code='df = pd.read_csv("foo.csv")',
        path_arg="foo.csv",
    )
    plans = plan_prepass([m], resolver)

    # Either no plan returned, or plan where modified == original
    assert src.read_text() == original
    for plan in plans:
        if plan.file == src:
            assert plan.modified == plan.original
```

### Step 3: Run, see fail

Run: `PYTHONPATH=. python3 -m pytest tests/test_prepass.py -v`
Expected: `ImportError: cannot import name 'PrepassPlan'` or `cannot import name 'plan_prepass'`.

### Step 4: Refactor prepass.py

Read the current `prepass.py` end-to-end. The current `run_prepass` function has a loop body that:
1. Computes skip-marker line numbers
2. Inserts markers
3. Conditionally adds pyspark conf block
4. Writes the file if `new_source != original`
5. Records edit count in `edits_per_file`

Refactor: extract steps 1-3 into a pure function `_plan_prepass_for_file(matches, resolver, src_file, source) -> PrepassPlan | None`. Make `plan_prepass(matches, resolver)` orchestrate the per-file calls. Make `run_prepass(matches, resolver)` call `plan_prepass` and write each plan to disk.

Add at the top of the file (after existing imports):
```python
import difflib
from dataclasses import dataclass


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
        """Unified diff between original and modified content."""
        return "".join(difflib.unified_diff(
            self.original.splitlines(keepends=True),
            self.modified.splitlines(keepends=True),
            fromfile=str(self.file),
            tofile=str(self.file),
        ))

    @property
    def is_changed(self) -> bool:
        return self.modified != self.original
```

Add `plan_prepass`:
```python
def plan_prepass(
    matches: list[PatternMatch],
    resolver: Resolver,
) -> list[PrepassPlan]:
    """Compute what prepass would change. No filesystem writes."""
    by_file: dict[Path, list[PatternMatch]] = {}
    for m in matches:
        by_file.setdefault(m.file, []).append(m)

    plans: list[PrepassPlan] = []
    for src_file, file_matches in sorted(by_file.items(), key=lambda kv: kv[0]):
        try:
            source = src_file.read_text(errors="replace")
        except OSError:
            continue
        plan = _plan_one_file(src_file, source, file_matches, resolver)
        if plan is not None and plan.is_changed:
            plans.append(plan)
    return plans


def _plan_one_file(
    src_file: Path,
    source: str,
    file_matches: list[PatternMatch],
    resolver: Resolver,
) -> PrepassPlan | None:
    """Return a PrepassPlan for this file, or None if no changes."""
    original = source
    lines = source.splitlines(keepends=True)

    # ── Skip markers ─────────────────────────────────────────────
    skip_lines: list[int] = []
    for m in file_matches:
        direction = direction_of(m.pattern_type)
        d = resolver(m.path_arg, direction if direction in ("read", "write") else "any")
        if d.skip:
            skip_lines.append(m.line)

    unique_skip_lines = sorted(set(skip_lines), reverse=True)
    for line_no in unique_skip_lines:
        idx = line_no - 1
        if idx < 0 or idx >= len(lines):
            continue
        prev = lines[idx - 1] if idx > 0 else ""
        if _SKIP_MARKER_TEXT in prev:
            continue
        indent = _indent_of(lines[idx])
        marker = f"{indent}{_SKIP_MARKER_TEXT}\n"
        lines.insert(idx, marker)

    # ── Pyspark conf comment ─────────────────────────────────────
    pyspark_conf_added = False
    current_source = "".join(lines)
    if src_file.suffix.lower() == ".py" and not _ICEBERG_CONF_MARKER_RE.search(current_source):
        first: PatternMatch | None = None
        for m in sorted(file_matches, key=lambda m: m.line):
            if not _is_pyspark_pattern(m.pattern_type):
                continue
            direction = direction_of(m.pattern_type)
            d = resolver(m.path_arg, direction if direction in ("read", "write") else "any")
            if d.skip:
                continue
            first = m
            break

        if first is not None:
            target_idx = first.line - 1
            inserted_before = sum(
                1 for sl in unique_skip_lines if (sl - 1) <= target_idx
            )
            target_idx += inserted_before
            if 0 <= target_idx < len(lines):
                indent = _indent_of(lines[target_idx])
                block = [f"{indent}{text}\n" for text in _ICEBERG_CONF_LINES]
                for i, blk_line in enumerate(block):
                    lines.insert(target_idx + i, blk_line)
                pyspark_conf_added = True

    new_source = "".join(lines)
    marker_count = len(unique_skip_lines)
    return PrepassPlan(
        file=src_file,
        original=original,
        modified=new_source,
        marker_count=marker_count,
        pyspark_conf_added=pyspark_conf_added,
    )
```

Replace `run_prepass` with a thin wrapper:
```python
def run_prepass(
    matches: list[PatternMatch],
    resolver: Resolver,
) -> dict[Path, int]:
    """Apply skip markers and pyspark conf comments in place.

    Returns ``{file: number_of_edits}`` for files actually touched.
    """
    edits_per_file: dict[Path, int] = {}
    for plan in plan_prepass(matches, resolver):
        plan.file.write_text(plan.modified)
        edits_per_file[plan.file] = plan.marker_count + (1 if plan.pyspark_conf_added else 0)
    return edits_per_file
```

### Step 5: Run, see all tests pass

Run: `PYTHONPATH=. python3 -m pytest tests/test_prepass.py -v`
Expected: 3 new tests pass.

Run full suite to confirm `run_prepass` backward compat:
Run: `PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: no regressions (any existing test that uses `run_prepass` still passes).

### Step 6: Commit

```bash
git add skills/open_table_migrator/prepass.py tests/test_prepass.py
git commit -m "feat(prepass): PrepassPlan + plan_prepass; run_prepass delegates to plan+apply"
```

---

## Task 2: deps.py — BuildFileUpdate + plan_dependencies_update

**Files:**
- Modify: `skills/open_table_migrator/deps.py`
- Modify (or create): `tests/test_deps.py`

### Step 1: Failing tests

Append to `tests/test_deps.py`:
```python
from pathlib import Path
from textwrap import dedent

from skills.open_table_migrator.deps import (
    BuildFileUpdate, plan_dependencies_update,
)


def test_plan_dependencies_update_for_pyproject_without_pyiceberg(tmp_path):
    """plan_dependencies_update returns a BuildFileUpdate that adds pyiceberg
    to pyproject.toml. File on disk MUST NOT be changed."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(dedent('''
        [project]
        name = "x"
        version = "0.1.0"
        dependencies = [
          "pandas>=2.0.0",
        ]
    '''))
    original_content = pyproject.read_text()

    plans = plan_dependencies_update(tmp_path)

    # File on disk unchanged
    assert pyproject.read_text() == original_content
    # Plan reflects added dependency
    assert len(plans) == 1
    plan = plans[0]
    assert plan.file == pyproject
    assert plan.original == original_content
    assert "pyiceberg" in plan.modified


def test_plan_dependencies_update_for_already_updated_pyproject(tmp_path):
    """If pyiceberg is already present, plan is empty (idempotent)."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(dedent('''
        [project]
        name = "x"
        dependencies = [
          "pandas>=2.0.0",
          "pyiceberg[sql-sqlite]>=0.7.0",
        ]
    '''))

    plans = plan_dependencies_update(tmp_path)
    assert plans == []
```

### Step 2: Run, see fail

Run: `PYTHONPATH=. python3 -m pytest tests/test_deps.py -v -k plan_dependencies_update`
Expected: `ImportError: cannot import name 'BuildFileUpdate'`.

### Step 3: Refactor deps.py

Replace `update_dependencies` and each `_update_*` helper. Each helper becomes `_plan_*` returning `str | None` (the new content, or None if unchanged):

```python
import difflib
import re
from dataclasses import dataclass
from pathlib import Path


PYICEBERG_REQ = 'pyiceberg[sql-sqlite]>=0.7.0'

ICEBERG_GROUP = 'org.apache.iceberg'
ICEBERG_ARTIFACT = 'iceberg-spark-runtime-3.5_2.12'
ICEBERG_ARTIFACT_SBT = 'iceberg-spark-runtime-3.5'
ICEBERG_VERSION = '1.5.0'


@dataclass(frozen=True)
class BuildFileUpdate:
    file: Path
    original: str
    modified: str

    @property
    def diff(self) -> str:
        return "".join(difflib.unified_diff(
            self.original.splitlines(keepends=True),
            self.modified.splitlines(keepends=True),
            fromfile=str(self.file),
            tofile=str(self.file),
        ))


def plan_dependencies_update(project_root: Path) -> list[BuildFileUpdate]:
    """Compute build-file changes. No filesystem writes."""
    plans: list[BuildFileUpdate] = []

    req_file = project_root / "requirements.txt"
    if req_file.exists():
        original = req_file.read_text()
        modified = _plan_requirements_txt(original)
        if modified is not None and modified != original:
            plans.append(BuildFileUpdate(file=req_file, original=original, modified=modified))

    pyproject = project_root / "pyproject.toml"
    if pyproject.exists():
        original = pyproject.read_text()
        modified = _plan_pyproject_toml(original)
        if modified is not None and modified != original:
            plans.append(BuildFileUpdate(file=pyproject, original=original, modified=modified))

    for build_file in sorted(project_root.rglob("pom.xml")):
        original = build_file.read_text()
        modified = _plan_pom_xml(original)
        if modified is not None and modified != original:
            plans.append(BuildFileUpdate(file=build_file, original=original, modified=modified))

    for name in ("build.gradle", "build.gradle.kts"):
        for build_file in sorted(project_root.rglob(name)):
            original = build_file.read_text()
            modified = _plan_build_gradle(original)
            if modified is not None and modified != original:
                plans.append(BuildFileUpdate(file=build_file, original=original, modified=modified))

    for build_file in sorted(project_root.rglob("build.sbt")):
        original = build_file.read_text()
        modified = _plan_build_sbt(original)
        if modified is not None and modified != original:
            plans.append(BuildFileUpdate(file=build_file, original=original, modified=modified))

    return plans


def update_dependencies(project_root: Path) -> list[Path]:
    """Apply plans. Returns paths actually modified."""
    updated: list[Path] = []
    for plan in plan_dependencies_update(project_root):
        plan.file.write_text(plan.modified)
        updated.append(plan.file)
    return updated


def _plan_requirements_txt(content: str) -> str | None:
    if "pyiceberg" in content:
        return None
    return content.rstrip() + f"\n{PYICEBERG_REQ}\n"


def _plan_pyproject_toml(content: str) -> str | None:
    if "pyiceberg" in content:
        return None
    new_content = re.sub(
        r'(dependencies\s*=\s*\[)(.*?)(\])',
        lambda m: m.group(1) + m.group(2).rstrip() + f'\n  "{PYICEBERG_REQ}",\n' + m.group(3),
        content,
        flags=re.DOTALL,
    )
    if new_content == content:
        return None
    return new_content


def _plan_pom_xml(content: str) -> str | None:
    if ICEBERG_ARTIFACT in content:
        return None
    iceberg_dep = (
        f'        <dependency>\n'
        f'            <groupId>{ICEBERG_GROUP}</groupId>\n'
        f'            <artifactId>{ICEBERG_ARTIFACT}</artifactId>\n'
        f'            <version>{ICEBERG_VERSION}</version>\n'
        f'        </dependency>\n'
    )
    if '</dependencies>' in content:
        return content.replace('</dependencies>', iceberg_dep + '    </dependencies>', 1)
    block = f'    <dependencies>\n{iceberg_dep}    </dependencies>\n'
    new_content = content.replace('</project>', block + '</project>', 1)
    return new_content if new_content != content else None


def _plan_build_gradle(content: str) -> str | None:
    if ICEBERG_ARTIFACT in content:
        return None
    coordinate = f"'{ICEBERG_GROUP}:{ICEBERG_ARTIFACT}:{ICEBERG_VERSION}'"
    line = f"    implementation {coordinate}\n"
    if re.search(r'dependencies\s*\{', content):
        return re.sub(
            r'(dependencies\s*\{)',
            lambda m: m.group(1) + "\n" + line,
            content,
            count=1,
        )
    return content.rstrip() + f"\n\ndependencies {{\n{line}}}\n"


def _plan_build_sbt(content: str) -> str | None:
    if "iceberg-spark-runtime" in content:
        return None
    dep_line = (
        f'libraryDependencies += "{ICEBERG_GROUP}" %% "{ICEBERG_ARTIFACT_SBT}" % "{ICEBERG_VERSION}"\n'
    )
    return content.rstrip() + "\n\n" + dep_line
```

### Step 4: Run, see all tests pass

Run: `PYTHONPATH=. python3 -m pytest tests/test_deps.py -v`
Expected: existing + 2 new pass.

Run full suite: `PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: no regressions.

### Step 5: Commit

```bash
git add skills/open_table_migrator/deps.py tests/test_deps.py
git commit -m "feat(deps): BuildFileUpdate + plan_dependencies_update; update_dependencies delegates"
```

---

## Task 3: worklist.py — serialize_worklist

**Files:**
- Modify: `skills/open_table_migrator/worklist.py`
- Modify: `tests/test_worklist.py`

### Step 1: Failing tests

Append to `tests/test_worklist.py`:
```python
def test_serialize_worklist_returns_valid_json_no_disk_write(tmp_path):
    """serialize_worklist produces the JSON string without writing to disk."""
    import json
    from skills.open_table_migrator.targets import Target, constant_resolver
    from skills.open_table_migrator.worklist import (
        build_worklist, serialize_worklist,
    )
    from skills.open_table_migrator.detector import detect_all_io

    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x")\n'
    )
    matches = detect_all_io(tmp_path)
    resolver = constant_resolver(Target(namespace="ns", table="t"))
    entries = build_worklist(matches, tmp_path, resolver)

    json_str = serialize_worklist(entries, project_root=tmp_path)

    # No worklist file on disk
    assert not (tmp_path / "lakehouse-worklist.json").exists()
    # JSON is valid and matches version
    blob = json.loads(json_str)
    assert blob["version"] == 1
    assert blob["count"] == len(entries)
    assert "entries" in blob


def test_serialize_worklist_with_dyn_cross_includes_loaders_key(tmp_path):
    """serialize_worklist with dyn_cross adds dynamic_sql_loaders key."""
    import json
    from skills.open_table_migrator.targets import Target, constant_resolver
    from skills.open_table_migrator.worklist import (
        build_worklist, serialize_worklist,
    )
    from skills.open_table_migrator.detector import detect_all_io
    from skills.open_table_migrator.dynamic_sql import detect_dynamic_sql_loaders
    from skills.open_table_migrator.sql_registry import (
        scan_sql_files, scan_sql_table_references,
    )
    from skills.open_table_migrator.analyzer import cross_reference_dynamic_sql

    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "events.sql").write_text(
        "CREATE TABLE events (id INT) STORED AS PARQUET;"
    )
    (tmp_path / "job.py").write_text(
        'sql = open("queries/events.sql").read()\n'
    )
    matches = detect_all_io(tmp_path)
    resolver = constant_resolver(Target(namespace="ns", table="t"))
    entries = build_worklist(matches, tmp_path, resolver)
    loaders = detect_dynamic_sql_loaders(tmp_path)
    defs = scan_sql_files(tmp_path)
    refs = scan_sql_table_references(tmp_path)
    dyn_cross = cross_reference_dynamic_sql(loaders, defs, refs, tmp_path)

    json_str = serialize_worklist(entries, project_root=tmp_path, dyn_cross=dyn_cross)
    blob = json.loads(json_str)
    assert "dynamic_sql_loaders" in blob
    assert len(blob["dynamic_sql_loaders"]) >= 1
```

### Step 2: Run, see fail

Run: `PYTHONPATH=. python3 -m pytest tests/test_worklist.py -v -k serialize_worklist`
Expected: `ImportError: cannot import name 'serialize_worklist'`.

### Step 3: Refactor worklist.py

Read the current `write_worklist` function. Extract the JSON-building logic into a new `serialize_worklist`. Keep `write_worklist` as a thin wrapper that calls `serialize_worklist` and writes to disk.

Find the existing `write_worklist` (around line 172). Replace with:

```python
def serialize_worklist(
    entries: list[WorklistEntry],
    project_root: Path,
    *,
    dyn_cross: list | None = None,
) -> str:
    """Build the lakehouse-worklist.json content string. No filesystem writes."""
    payload: dict = {
        "version": 1,
        "count": len(entries),
        "entries": [e.to_dict() for e in entries],
    }
    if dyn_cross:
        payload["dynamic_sql_loaders"] = [
            _dyn_cross_to_dict(c, project_root) for c in dyn_cross
        ]
    return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"


def write_worklist(
    entries: list[WorklistEntry],
    project_root: Path,
    *,
    dyn_cross: list | None = None,
) -> Path:
    """Write lakehouse-worklist.json at project root. Returns the path."""
    out = project_root / "lakehouse-worklist.json"
    out.write_text(serialize_worklist(entries, project_root, dyn_cross=dyn_cross))
    return out
```

The `_dyn_cross_to_dict` helper already exists in worklist.py (or is inlined). If inlined, extract to a module-level helper so both `serialize_worklist` and any pre-existing code can call it. Read the file to confirm.

If you can't find a `_dyn_cross_to_dict` helper, find the inline dict construction inside the old `write_worklist`'s dynamic_sql_loaders block and extract it. The exact shape is in spec section "Worklist effect":
```python
def _dyn_cross_to_dict(c, project_root: Path) -> dict:
    return {
        "file": _rel(c.loader.file, project_root),
        "line": c.loader.line,
        "pattern": c.loader.pattern,
        "sql_filename": c.loader.sql_filename,
        "confidence": c.loader.confidence,
        "resolved_to": _rel(c.sql_file, project_root.resolve()),
        "match_kind": c.match_kind,
        "tables": [
            {
                "name": t.table_name,
                "format": t.format,
                "ddl_file": _rel(t.file, project_root.resolve()),
                "ddl_line": t.line,
            }
            for t in c.tables
        ],
    }
```

Use whatever the existing `_rel` helper is in worklist.py.

### Step 4: Run, see tests pass

Run: `PYTHONPATH=. python3 -m pytest tests/test_worklist.py -v`
Expected: existing + 2 new pass.

### Step 5: Commit

```bash
git add skills/open_table_migrator/worklist.py tests/test_worklist.py
git commit -m "feat(worklist): serialize_worklist (no I/O); write_worklist delegates"
```

---

## Task 4: cli.py — dry_run parameter + _run_dry helper + argparse

**Files:**
- Modify: `skills/open_table_migrator/cli.py`
- Create or modify: `tests/test_cli.py`

### Step 1: Failing tests

Run `ls tests/test_cli.py 2>&1` to check if file exists.

Create or append:
```python
import hashlib
from pathlib import Path
from textwrap import dedent

from skills.open_table_migrator.cli import convert_project


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else "MISSING"


def test_dry_run_returns_zero_and_prints_header(tmp_path, capsys):
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x.parquet")\n'
    )
    rc = convert_project(
        tmp_path,
        table_name="x", namespace="analytics",
        mapping=None,
        update_deps=True,
        dry_run=True,
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out


def test_dry_run_does_not_create_worklist(tmp_path, capsys):
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x.parquet")\n'
    )
    convert_project(
        tmp_path,
        table_name="x", namespace="analytics",
        mapping=None,
        update_deps=True,
        dry_run=True,
    )
    assert not (tmp_path / "lakehouse-worklist.json").exists()


def test_dry_run_does_not_modify_source_files(tmp_path, capsys):
    src = tmp_path / "job.py"
    src.write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x.parquet")\n'
    )
    before = _sha(src)
    convert_project(
        tmp_path,
        table_name="x", namespace="analytics",
        mapping=None,
        update_deps=True,
        dry_run=True,
    )
    after = _sha(src)
    assert before == after


def test_dry_run_does_not_modify_pyproject_toml(tmp_path, capsys):
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x.parquet")\n'
    )
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(dedent('''
        [project]
        name = "x"
        dependencies = ["pandas>=2.0.0"]
    '''))
    before = _sha(pyproject)
    convert_project(
        tmp_path,
        table_name="x", namespace="analytics",
        mapping=None,
        update_deps=True,
        dry_run=True,
    )
    after = _sha(pyproject)
    assert before == after


def test_dry_run_outputs_four_sections(tmp_path, capsys):
    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x.parquet")\n'
    )
    (tmp_path / "pyproject.toml").write_text(dedent('''
        [project]
        name = "x"
        dependencies = ["pandas>=2.0.0"]
    '''))
    convert_project(
        tmp_path,
        table_name="x", namespace="analytics",
        mapping=None,
        update_deps=True,
        dry_run=True,
    )
    out = capsys.readouterr().out
    assert "--- Summary ---" in out
    assert "--- Worklist preview" in out
    # Build-file section appears because pyproject.toml will get pyiceberg added
    assert "--- Build-file updates" in out
```

### Step 2: Run, see fail

Run: `PYTHONPATH=. python3 -m pytest tests/test_cli.py -v -k dry_run`
Expected: `TypeError: convert_project() got an unexpected keyword argument 'dry_run'`.

### Step 3: Add dry_run + _run_dry to cli.py

Read current `cli.py` to find `convert_project`. Modify the signature:

```python
def convert_project(
    project_root: Path,
    *,
    table_name: str | None = None,
    namespace: str | None = None,
    mapping: Mapping | None = None,
    update_deps: bool = True,
    dry_run: bool = False,
) -> int:
```

After `resolver = build_resolver(...)` and before `_run_hybrid(...)` (or whatever the apply path is), branch:

```python
    if dry_run:
        return _run_dry(
            project_root, matches, sql_defs, resolver,
            dyn_cross=dyn_cross,
            update_deps_flag=update_deps,
        )

    # Existing apply-path continues here
    _run_hybrid(project_root, matches, resolver, dyn_cross=dyn_cross)
    # ... existing code ...
```

Add `_run_dry` and four print helpers. Place them after `_run_hybrid` (or wherever existing helpers live):

```python
def _run_dry(
    project_root: Path,
    matches: list,
    sql_defs: list,
    resolver,
    *,
    dyn_cross: list | None,
    update_deps_flag: bool,
) -> int:
    """Render the dry-run preview to stdout. No file I/O for writes."""
    from .deps import plan_dependencies_update
    from .prepass import plan_prepass
    from .worklist import build_worklist, serialize_worklist

    entries = build_worklist(matches, project_root, resolver)
    prepass_plans = plan_prepass(matches, resolver)
    build_plans = plan_dependencies_update(project_root) if update_deps_flag else []
    worklist_json = serialize_worklist(entries, project_root=project_root, dyn_cross=dyn_cross)

    print("=== DRY RUN — no files will be modified ===\n")
    _print_dry_summary(entries, prepass_plans, build_plans, dyn_cross)
    _print_dry_worklist(worklist_json)
    _print_dry_prepass(prepass_plans)
    _print_dry_build(build_plans)
    return 0


def _print_dry_summary(entries, prepass_plans, build_plans, dyn_cross):
    print("--- Summary ---")
    print(f"Would write: lakehouse-worklist.json ({len(entries)} entries)")
    if dyn_cross:
        print(f"  with {len(dyn_cross)} dynamic SQL loader cross-references")
    total_markers = sum(p.marker_count for p in prepass_plans)
    pyspark_files = sum(1 for p in prepass_plans if p.pyspark_conf_added)
    if prepass_plans:
        print(f"Would prepass: {len(prepass_plans)} file(s) "
              f"with {total_markers} marker(s)"
              + (f", pyspark conf added in {pyspark_files} file(s)" if pyspark_files else ""))
    if build_plans:
        names = ", ".join(p.file.name for p in build_plans)
        print(f"Would update: {names}")
    print()


def _print_dry_worklist(worklist_json: str):
    print("--- Worklist preview (lakehouse-worklist.json) ---")
    print(worklist_json)


def _print_dry_prepass(prepass_plans):
    if not prepass_plans:
        return
    print("--- Prepass diff preview ---")
    for plan in prepass_plans:
        label = f"{plan.marker_count} marker(s)"
        if plan.pyspark_conf_added:
            label += ", pyspark conf"
        print(f"=== {plan.file} ({label}) ===")
        print(plan.diff)


def _print_dry_build(build_plans):
    if not build_plans:
        return
    print("--- Build-file updates ---")
    for plan in build_plans:
        print(f"{plan.file.name}:")
        print(plan.diff)
```

### Step 4: Add --dry-run argparse flag in main()

Find the existing `main()` in `cli.py`. Add the flag right after `--no-deps`:
```python
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="run pipeline without writing to disk; print worklist JSON + unified diffs to stdout",
    )
```

Thread to `convert_project`:
```python
    sys.exit(convert_project(
        args.project,
        table_name=table,
        namespace=ns,
        mapping=mapping,
        update_deps=not args.no_deps,
        dry_run=args.dry_run,
    ))
```

### Step 5: Run, see all 5 dry_run tests pass

Run: `PYTHONPATH=. python3 -m pytest tests/test_cli.py -v -k dry_run`
Expected: 5 passed.

### Step 6: Run full suite

Run: `PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: no regressions.

### Step 7: Commit

```bash
git add skills/open_table_migrator/cli.py tests/test_cli.py
git commit -m "feat(cli): --dry-run flag with 4-section stdout preview"
```

---

## Task 5: argparse parsing test + SKILL.md + smoke run

**Files:**
- Modify: `tests/test_cli.py` (or test_argparse if existing)
- Modify: `skills/open_table_migrator/SKILL.md`

### Step 1: Argparse parsing test

Append to `tests/test_cli.py`:
```python
def test_main_accepts_dry_run_flag(tmp_path, monkeypatch):
    """The --dry-run flag is accepted by argparse."""
    import sys
    from skills.open_table_migrator import cli

    (tmp_path / "job.py").write_text(
        'import pandas as pd\n'
        'df = pd.read_parquet("s3://bucket/x")\n'
    )

    argv = [
        "prog", str(tmp_path),
        "--table", "x", "--namespace", "ns",
        "--no-deps",
        "--dry-run",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    # main() calls sys.exit; capture
    try:
        cli.main()
    except SystemExit as e:
        assert e.code == 0
    else:
        raise AssertionError("main() should call sys.exit")
```

### Step 2: Run

Run: `PYTHONPATH=. python3 -m pytest tests/test_cli.py::test_main_accepts_dry_run_flag -v`
Expected: pass.

### Step 3: Add SKILL.md section

Find an existing section like "## Dynamic SQL loading" in `skills/open_table_migrator/SKILL.md`. Insert "## Dry run" just before "## Known Limitations" (or wherever sections end before that).

```markdown
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

### Composition with other flags

- `--dry-run --no-deps` — valid; `--no-deps` is redundant.
- `--dry-run --mapping foo.json` — valid; mapping is read normally.
- `--dry-run --table X --namespace Y` — valid.

Validation rules (e.g., requiring `--table` with `--namespace`) still apply in dry-run mode.

### Exit code

Always 0 on successful dry-run. Pre-existing argument-validation errors still return exit code 2.
```

### Step 4: Smoke run

```bash
mkdir -p /tmp/dry-run-smoke
cat > /tmp/dry-run-smoke/job.py <<'EOF'
import pandas as pd
df = pd.read_parquet("s3://bucket/events.parquet")
EOF
cat > /tmp/dry-run-smoke/pyproject.toml <<'EOF'
[project]
name = "x"
dependencies = ["pandas>=2.0.0"]
EOF

PYTHONPATH=. python3 -m skills.open_table_migrator.cli /tmp/dry-run-smoke \
    --table events --namespace analytics --dry-run 2>&1 | head -50
ls /tmp/dry-run-smoke/lakehouse-worklist.json 2>&1 | head -1
```

Expected:
- Output contains "DRY RUN" header, Summary, Worklist preview JSON, Prepass diff, Build-file diff
- `ls` reports "No such file" — worklist NOT written

Clean up:
```bash
rm -rf /tmp/dry-run-smoke
```

### Step 5: Full suite

Run: `PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures --ignore=tests/data_lineage/fixtures -q`
Expected: pre-existing + new tests, no regressions.

### Step 6: Commit

```bash
git add skills/open_table_migrator/SKILL.md tests/test_cli.py
git commit -m "docs(cli): --dry-run section in SKILL.md + argparse test"
```

---
