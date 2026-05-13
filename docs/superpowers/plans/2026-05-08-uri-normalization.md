# URI Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `skills/open_table_migrator/targets.py` URI-aware so mapping globs match across sub-scheme variants (`s3` ≡ `s3a` ≡ `s3n`, `abfs` ≡ `abfss`, `hdfs` ≡ `webhdfs`), and bare local paths match `file://` patterns.

**Architecture:** One new module `skills/open_table_migrator/uri.py` exporting `parse()` and `matches_glob()`. `targets.build_resolver` switches from raw `fnmatch` to `uri.matches_glob`. `cli.convert_project` passes `project_root` down. Everything else (extract.py, worklist.py, transformers) is untouched — `path_arg` is preserved as-is, normalization happens only inside matching.

**Tech Stack:** Python 3.11+, stdlib `urllib.parse` + `fnmatch`. No new deps.

**Spec:** [docs/superpowers/specs/2026-05-08-uri-normalization-design.md](../specs/2026-05-08-uri-normalization-design.md)

---

## File Structure

New file:
- `skills/open_table_migrator/uri.py` — `URI` dataclass, `parse()`, `matches_glob()`

Modified:
- `skills/open_table_migrator/targets.py` — `build_resolver` uses `uri.matches_glob`, signature gains `project_root: Path | None = None`
- `skills/open_table_migrator/cli.py` — pass `project_root=project_root` to `build_resolver`
- `skills/open_table_migrator/SKILL.md` — new section "Path schemes"

New tests:
- `tests/test_uri.py` — module unit tests (parsing + matching)
- `tests/test_multi_table.py` — three new scenarios (HDFS, abfs, viewfs)

---

## Task 1: URI parsing — module skeleton + parse() for fully qualified URIs

**Files:**
- Create: `skills/open_table_migrator/uri.py`
- Create: `tests/test_uri.py`

- [ ] **Step 1: Write the failing tests for fully qualified URIs**

`tests/test_uri.py`:
```python
from pathlib import Path

from skills.open_table_migrator.uri import URI, parse


def test_parse_s3():
    assert parse("s3://bucket/key") == URI(
        scheme="s3", raw_scheme="s3", authority="bucket", path="/key",
    )


def test_parse_s3a_canonicalises_to_s3():
    assert parse("s3a://bucket/key") == URI(
        scheme="s3", raw_scheme="s3a", authority="bucket", path="/key",
    )


def test_parse_s3n_canonicalises_to_s3():
    assert parse("s3n://bucket/key") == URI(
        scheme="s3", raw_scheme="s3n", authority="bucket", path="/key",
    )


def test_parse_webhdfs_canonicalises_to_hdfs():
    assert parse("webhdfs://ns/x") == URI(
        scheme="hdfs", raw_scheme="webhdfs", authority="ns", path="/x",
    )


def test_parse_hdfs_empty_authority():
    assert parse("hdfs:///warehouse") == URI(
        scheme="hdfs", raw_scheme="hdfs", authority="", path="/warehouse",
    )


def test_parse_abfss_canonicalises_to_abfs():
    assert parse("abfss://container@account.dfs/x") == URI(
        scheme="abfs", raw_scheme="abfss",
        authority="container@account.dfs", path="/x",
    )


def test_parse_viewfs_kept_distinct_from_hdfs():
    assert parse("viewfs://ns/x") == URI(
        scheme="viewfs", raw_scheme="viewfs", authority="ns", path="/x",
    )


def test_parse_gs():
    assert parse("gs://bucket/x") == URI(
        scheme="gs", raw_scheme="gs", authority="bucket", path="/x",
    )
```

- [ ] **Step 2: Run, see fail**

Run: `PYTHONPATH=. python3 -m pytest tests/test_uri.py -v`
Expected: `ModuleNotFoundError: No module named 'skills.open_table_migrator.uri'`

- [ ] **Step 3: Implement minimal `uri.py`**

`skills/open_table_migrator/uri.py`:
```python
"""URI parsing and glob matching for table-routing maps.

Sub-scheme equivalence (s3 ≡ s3a ≡ s3n, hdfs ≡ webhdfs, abfs ≡ abfss) is
folded into a canonical `scheme`; the original token is preserved in
`raw_scheme` so user-facing artifacts (path_arg in worklist) remain
verbatim.

viewfs is kept distinct from hdfs by design — mount-point resolution
needs cluster config we do not read.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


_CANONICAL = {
    "s3": "s3", "s3a": "s3", "s3n": "s3",
    "abfs": "abfs", "abfss": "abfs",
    "hdfs": "hdfs", "webhdfs": "hdfs",
    "gs": "gs",
    "viewfs": "viewfs",
    "file": "file",
    "": "file",
}

_UNKNOWN_WARNED: set[str] = set()


@dataclass(frozen=True)
class URI:
    scheme: str
    raw_scheme: str
    authority: str
    path: str


def parse(s: str, *, project_root: Path | None = None) -> URI:
    parsed = urlparse(s)
    raw_scheme = parsed.scheme
    if raw_scheme in _CANONICAL:
        canonical = _CANONICAL[raw_scheme]
    else:
        if raw_scheme not in _UNKNOWN_WARNED:
            print(f"open_table_migrator: unknown URI scheme '{raw_scheme}' in {s!r}",
                  file=sys.stderr)
            _UNKNOWN_WARNED.add(raw_scheme)
        canonical = "<unknown>"
    authority = parsed.netloc
    path = parsed.path
    return URI(
        scheme=canonical,
        raw_scheme=raw_scheme,
        authority=authority,
        path=path,
    )
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. python3 -m pytest tests/test_uri.py -v`
Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/open_table_migrator/uri.py tests/test_uri.py
git commit -m "feat(uri): parse() for fully qualified URIs with sub-scheme equivalence"
```

---

## Task 2: parse() bare paths + project_root resolution + unknown scheme

**Files:**
- Modify: `skills/open_table_migrator/uri.py`
- Modify: `tests/test_uri.py`

- [ ] **Step 1: Append failing tests for bare paths and unknown scheme**

Append to `tests/test_uri.py`:
```python
def test_parse_absolute_bare_path_is_file():
    assert parse("/tmp/x.parquet") == URI(
        scheme="file", raw_scheme="", authority="", path="/tmp/x.parquet",
    )


def test_parse_relative_path_resolved_against_project_root():
    assert parse("./data/x", project_root=Path("/proj")) == URI(
        scheme="file", raw_scheme="", authority="", path="/proj/data/x",
    )


def test_parse_relative_path_without_project_root_kept_literal():
    assert parse("./data/x") == URI(
        scheme="file", raw_scheme="", authority="", path="./data/x",
    )


def test_parse_empty_string_returns_empty_file_uri():
    assert parse("") == URI(
        scheme="file", raw_scheme="", authority="", path="",
    )


def test_parse_unknown_scheme_warns_and_returns_unknown(capsys):
    from skills.open_table_migrator import uri as uri_module
    uri_module._UNKNOWN_WARNED.clear()
    result = parse("ftp://host.example/x")
    assert result == URI(
        scheme="<unknown>", raw_scheme="ftp",
        authority="host.example", path="/x",
    )
    captured = capsys.readouterr()
    assert "ftp" in captured.err
    assert "unknown URI scheme" in captured.err
```

- [ ] **Step 2: Run the new tests, see them fail**

Run: `PYTHONPATH=. python3 -m pytest tests/test_uri.py -v`
Expected: 5 new tests fail. The bare-path case currently parses the path correctly but `_CANONICAL[""]` is already wired — so `test_parse_absolute_bare_path_is_file` actually passes. The relative-path tests fail because `project_root` is not yet honored. The empty-string test currently produces `path=""` already — passes incidentally.

- [ ] **Step 3: Extend `parse()` to handle relative paths via project_root**

Replace `parse` in `skills/open_table_migrator/uri.py`:
```python
def parse(s: str, *, project_root: Path | None = None) -> URI:
    parsed = urlparse(s)
    raw_scheme = parsed.scheme
    if raw_scheme in _CANONICAL:
        canonical = _CANONICAL[raw_scheme]
    else:
        if raw_scheme not in _UNKNOWN_WARNED:
            print(f"open_table_migrator: unknown URI scheme '{raw_scheme}' in {s!r}",
                  file=sys.stderr)
            _UNKNOWN_WARNED.add(raw_scheme)
        canonical = "<unknown>"
    authority = parsed.netloc
    path = parsed.path

    if canonical == "file" and raw_scheme == "" and project_root is not None:
        if path and not path.startswith("/"):
            path = str((project_root / path).resolve(strict=False))

    return URI(
        scheme=canonical,
        raw_scheme=raw_scheme,
        authority=authority,
        path=path,
    )
```

- [ ] **Step 4: Run all parse tests, see pass**

Run: `PYTHONPATH=. python3 -m pytest tests/test_uri.py -v`
Expected: 13 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/open_table_migrator/uri.py tests/test_uri.py
git commit -m "feat(uri): parse() handles bare paths, project_root resolution, unknown scheme warning"
```

---

## Task 3: matches_glob() — scheme equivalence + authority + path globs

**Files:**
- Modify: `skills/open_table_migrator/uri.py`
- Modify: `tests/test_uri.py`

- [ ] **Step 1: Append failing tests for matching**

Append to `tests/test_uri.py`:
```python
from skills.open_table_migrator.uri import matches_glob


def _u(s, project_root=None):
    return parse(s, project_root=project_root)


def test_match_s3_pattern_matches_s3a_uri():
    assert matches_glob(_u("s3a://bucket/users/x"), "s3://bucket/users/*")


def test_match_s3_pattern_does_not_match_different_path():
    assert not matches_glob(_u("s3://bucket/orders/x"), "s3://bucket/users/*")


def test_match_hdfs_pattern_matches_webhdfs_uri():
    assert matches_glob(_u("webhdfs://ns/warehouse/x"), "hdfs://ns/warehouse/*")


def test_match_viewfs_does_not_match_hdfs():
    assert not matches_glob(_u("hdfs://ns/x"), "viewfs://ns/x")


def test_match_double_star_crosses_segments():
    assert matches_glob(
        _u("s3://bucket/users/2024/01/data.parquet"),
        "s3://bucket/users/**",
    )


def test_match_question_mark_matches_single_char():
    assert matches_glob(_u("s3://bucket/users/x.parquet"),
                        "s3://bucket/users/?.parquet")


def test_match_authority_glob():
    assert matches_glob(_u("s3://my-prod-bucket/x"), "s3://my-*-bucket/x")


def test_match_bare_path_matches_file_pattern():
    assert matches_glob(_u("/tmp/fixtures/x.parquet"),
                        "file:///tmp/fixtures/*")


def test_match_file_pattern_matches_bare_path_uri():
    assert matches_glob(_u("file:///tmp/fixtures/x.parquet"),
                        "/tmp/fixtures/*")


def test_match_trailing_slash_distinguishes():
    assert not matches_glob(_u("s3://bucket/users"), "s3://bucket/users/")
```

- [ ] **Step 2: Run, see fail**

Run: `PYTHONPATH=. python3 -m pytest tests/test_uri.py -v`
Expected: 10 new tests fail with `ImportError: cannot import name 'matches_glob'`.

- [ ] **Step 3: Implement `matches_glob` and the path glob helper**

Append to `skills/open_table_migrator/uri.py`:
```python
import fnmatch
import re


def _path_match(uri_path: str, pattern_path: str) -> bool:
    """Path glob with `**` (multi-segment), `*` (single segment), `?` (single char)."""
    if "**" not in pattern_path:
        return fnmatch.fnmatchcase(uri_path, pattern_path)

    out: list[str] = []
    i = 0
    while i < len(pattern_path):
        c = pattern_path[i]
        if pattern_path[i:i + 2] == "**":
            out.append(".*")
            i += 2
        elif c == "*":
            out.append("[^/]*")
            i += 1
        elif c == "?":
            out.append(".")
            i += 1
        else:
            out.append(re.escape(c))
            i += 1
    return re.fullmatch("".join(out), uri_path) is not None


def matches_glob(uri: URI, pattern: str) -> bool:
    pat = parse(pattern)
    if uri.scheme != pat.scheme:
        return False
    if not fnmatch.fnmatchcase(uri.authority, pat.authority):
        return False
    return _path_match(uri.path, pat.path)
```

- [ ] **Step 4: Run, see all match tests pass**

Run: `PYTHONPATH=. python3 -m pytest tests/test_uri.py -v`
Expected: 23 passed (13 parse + 10 match).

- [ ] **Step 5: Commit**

```bash
git add skills/open_table_migrator/uri.py tests/test_uri.py
git commit -m "feat(uri): matches_glob() with sub-scheme equivalence + path globs"
```

---

## Task 4: targets.py integration — replace fnmatch with uri.matches_glob

**Files:**
- Modify: `skills/open_table_migrator/targets.py`
- Modify: `skills/open_table_migrator/cli.py`

- [ ] **Step 1: Modify `build_resolver` signature to accept `project_root`**

Edit `skills/open_table_migrator/targets.py`:

Replace the `import fnmatch` line with:
```python
from pathlib import Path

from . import uri
```

Replace the `build_resolver` function body:
```python
def build_resolver(
    mapping: Mapping | None,
    fallback: Target | None,
    *,
    project_root: Path | None = None,
) -> Resolver:
    """Return a resolver: (path_arg, direction) → Decision.

    Priority: first matching entry (glob + direction) → mapping default →
    caller-provided fallback. Returns Decision.unresolved() when everything fails.
    """
    entries: list[MappingEntry] = mapping.entries if mapping else []
    mapping_default: Target | None = mapping.default if mapping else None

    def resolve(path_arg: str | None, direction: Direction = "any") -> Decision:
        if path_arg is not None:
            parsed = uri.parse(path_arg, project_root=project_root)
            for entry in entries:
                if entry.direction != "any" and direction != "any" and entry.direction != direction:
                    continue
                if uri.matches_glob(parsed, entry.path_glob):
                    if entry.skip:
                        return Decision.skipped()
                    assert entry.target is not None
                    return Decision.migrate(entry.target)
        target = mapping_default or fallback
        if target is None:
            return Decision.unresolved()
        return Decision.migrate(target)

    return resolve
```

Drop the now-unused `import fnmatch` and the corresponding line if it remains.

- [ ] **Step 2: Wire `project_root` from `cli.py`**

Edit `skills/open_table_migrator/cli.py` line 51 (the `build_resolver(mapping, fallback)` call).

Replace:
```python
    resolver = build_resolver(mapping, fallback)
```
with:
```python
    resolver = build_resolver(mapping, fallback, project_root=project_root)
```

- [ ] **Step 3: Run the existing test suite, see nothing broke**

Run: `PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures -q`
Expected: 236 passed (existing migrator suite continues green; uri tests run separately).

- [ ] **Step 4: Commit**

```bash
git add skills/open_table_migrator/targets.py skills/open_table_migrator/cli.py
git commit -m "feat(targets): build_resolver uses uri.matches_glob, accepts project_root"
```

---

## Task 5: Multi-table integration tests — HDFS, abfs, viewfs

**Files:**
- Modify: `tests/test_multi_table.py`

- [ ] **Step 1: Read the existing test file to understand its conventions**

Run: `head -60 tests/test_multi_table.py`
Expected: see how existing tests build a fixture project and call the resolver.

- [ ] **Step 2: Add failing test for HDFS mapping**

Append to `tests/test_multi_table.py`:
```python
def test_hdfs_mapping_matches_webhdfs_paths_in_code(tmp_path):
    """Mapping uses hdfs://; code uses webhdfs://. Sub-scheme equivalence
    means the entry resolves both."""
    from skills.open_table_migrator.targets import (
        Mapping, MappingEntry, Target, build_resolver,
    )

    mapping = Mapping(
        entries=[
            MappingEntry(
                path_glob="hdfs://nameservice/warehouse/users/*",
                target=Target(namespace="analytics", table="users"),
            ),
        ],
    )
    resolver = build_resolver(mapping, fallback=None, project_root=tmp_path)

    decision = resolver("webhdfs://nameservice/warehouse/users/2024/01.parquet")
    assert decision.migrate_to == Target(namespace="analytics", table="users")
    assert decision.skip is False


def test_abfs_mapping_matches_abfss_paths(tmp_path):
    from skills.open_table_migrator.targets import (
        Mapping, MappingEntry, Target, build_resolver,
    )

    mapping = Mapping(
        entries=[
            MappingEntry(
                path_glob="abfs://container@account.dfs.core.windows.net/data/*",
                target=Target(namespace="cloud", table="events"),
            ),
        ],
    )
    resolver = build_resolver(mapping, fallback=None, project_root=tmp_path)

    decision = resolver(
        "abfss://container@account.dfs.core.windows.net/data/event.parquet",
    )
    assert decision.migrate_to == Target(namespace="cloud", table="events")


def test_viewfs_requires_separate_mapping_from_hdfs(tmp_path):
    """viewfs is NOT equivalent to hdfs. A user with both schemes in code
    must list both in the mapping."""
    from skills.open_table_migrator.targets import (
        Decision, Mapping, MappingEntry, Target, build_resolver,
    )

    mapping = Mapping(
        entries=[
            MappingEntry(
                path_glob="hdfs://ns/data/users/*",
                target=Target(namespace="analytics", table="users"),
            ),
            MappingEntry(
                path_glob="viewfs://ns/data/users/*",
                target=Target(namespace="analytics", table="users"),
            ),
        ],
    )
    resolver = build_resolver(mapping, fallback=None, project_root=tmp_path)

    hdfs_decision = resolver("hdfs://ns/data/users/x.parquet")
    viewfs_decision = resolver("viewfs://ns/data/users/x.parquet")

    assert hdfs_decision.migrate_to == Target(namespace="analytics", table="users")
    assert viewfs_decision.migrate_to == Target(namespace="analytics", table="users")

    only_hdfs = Mapping(
        entries=[
            MappingEntry(
                path_glob="hdfs://ns/data/users/*",
                target=Target(namespace="analytics", table="users"),
            ),
        ],
    )
    resolver2 = build_resolver(only_hdfs, fallback=None, project_root=tmp_path)
    assert resolver2("viewfs://ns/data/users/x.parquet") == Decision.unresolved()
```

- [ ] **Step 3: Run new tests, expect pass (Task 4 already wired the resolver)**

Run: `PYTHONPATH=. python3 -m pytest tests/test_multi_table.py::test_hdfs_mapping_matches_webhdfs_paths_in_code tests/test_multi_table.py::test_abfs_mapping_matches_abfss_paths tests/test_multi_table.py::test_viewfs_requires_separate_mapping_from_hdfs -v`
Expected: 3 passed.

- [ ] **Step 4: Run the full multi-table suite, ensure no regressions**

Run: `PYTHONPATH=. python3 -m pytest tests/test_multi_table.py -v`
Expected: all multi-table tests green (3 new + however many existed).

- [ ] **Step 5: Commit**

```bash
git add tests/test_multi_table.py
git commit -m "test(uri): multi-table mapping with HDFS, abfs, viewfs"
```

---

## Task 6: SKILL.md "Path schemes" section

**Files:**
- Modify: `skills/open_table_migrator/SKILL.md`

- [ ] **Step 1: Locate the existing structure of SKILL.md**

Run: `grep -n "^##" skills/open_table_migrator/SKILL.md`
Expected: list of `##` section headings. We'll insert the new section after "Multi-table projects" (or before "Known Limitations" — whichever comes first as referenced by README's `#known-limitations` anchor).

- [ ] **Step 2: Add the "Path schemes" section**

In `skills/open_table_migrator/SKILL.md`, insert this section just before the "Known Limitations" section (so the README anchor `#known-limitations` is not affected):

```markdown
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

### Unknown schemes

Unknown schemes (e.g., `ftp://`, custom enterprise schemes) emit a one-time stderr warning per scheme. They participate in matching only via exact `raw_scheme` equality — no aliasing applied.
```

- [ ] **Step 3: Verify the file renders cleanly**

Run: `head -200 skills/open_table_migrator/SKILL.md | tail -80`
Expected: see the new "Path schemes" section followed by "Known Limitations" (the existing one).

- [ ] **Step 4: Commit**

```bash
git add skills/open_table_migrator/SKILL.md
git commit -m "docs(uri): path schemes section in SKILL.md"
```

---

## Task 7: Final full-suite run

- [ ] **Step 1: Run the full project test suite**

Run: `PYTHONPATH=. python3 -m pytest tests/ --ignore=tests/fixtures -q`
Expected: 236 migrator tests + 23 uri tests + 3 multi-table additions = 262 passed.

- [ ] **Step 2: Smoke run the CLI on the existing migrator fixture with a viewfs-style mapping**

```bash
mkdir -p /tmp/uri-smoke
cat > /tmp/uri-smoke/mapping.json <<'EOF'
{
  "tables": [
    { "path_glob": "s3://bucket/users/*", "namespace": "analytics", "table": "users" }
  ]
}
EOF
PYTHONPATH=. python3 -m skills.open_table_migrator.cli . --mapping /tmp/uri-smoke/mapping.json --no-deps 2>&1 | head -5
```
Expected: command runs without throwing; output is the existing "No Parquet/ORC usage found." (because the migrator repo has no parquet code) or similar. Anything other than a stack trace is acceptable — this just verifies the wiring works.

- [ ] **Step 3: If any unexpected output, commit a fixup**

If a fixup is needed: stage, commit `chore(uri): post-suite fixup`. Otherwise no additional commit.

---
