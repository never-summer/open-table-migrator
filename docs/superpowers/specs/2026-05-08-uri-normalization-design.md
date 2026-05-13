# URI Normalization — Design

**Date:** 2026-05-08
**Status:** Approved for implementation planning
**Branch:** `feat/uri-normalization`
**Scope ref:** improvement 3.2 from the migrator roadmap

## Goal

Make `open_table_migrator` first-class for non-S3 storage. Today all examples use `s3://`; mapping globs match raw strings via `fnmatch`. A user with HDFS/abfs/gs/viewfs paths must write exact-form patterns and gets no scheme equivalence. The goal is that mapping `s3://bucket/users/*` matches code references `s3a://bucket/users/...` (and similar for Hadoop and Azure schemes) without configuration, while keeping bare local paths usable for tests and dev fixtures.

## Non-goals

- No resolution of viewfs mount-points. viewfs and hdfs remain distinct canonical schemes; users with both must list both in the mapping.
- No configurable scheme aliases. The canonical equivalence map is hardcoded — small, stable, intentional.
- No URI credential or query-string handling. `s3://key:secret@bucket/x?versionId=...` parses authority as `key:secret@bucket`; we make no effort to strip or interpret credentials.
- No transformation of `path_arg` in the worklist. The original code text is preserved end-to-end so `git diff` and user trust remain intact. Normalization happens only inside matching.

## Architecture

Single new module `skills/open_table_migrator/uri.py`, isolated from the rest of the codebase. Two exports:

- `parse(s, *, project_root=None) -> URI` — string to canonical form
- `matches_glob(uri, pattern) -> bool` — uri-aware glob match with sub-scheme equivalence

`URI` is a frozen dataclass:

```python
@dataclass(frozen=True)
class URI:
    scheme: str        # canonical: "s3", "hdfs", "abfs", "gs", "viewfs", "file", "<unknown>"
    raw_scheme: str    # original token: "s3a", "webhdfs", "abfss", "" (bare)
    authority: str     # bucket / host / nameservice; "" for bare paths and root-relative
    path: str          # always starts with "/" (after normalization)
```

### Canonical scheme map (hardcoded)

```python
_CANONICAL = {
    "s3": "s3", "s3a": "s3", "s3n": "s3",
    "abfs": "abfs", "abfss": "abfs",
    "hdfs": "hdfs", "webhdfs": "hdfs",
    "gs": "gs",
    "viewfs": "viewfs",
    "file": "file",
    "": "file",        # bare paths are local files
}
```

Unknown schemes (`ftp://`, `http://`, custom enterprise schemes) emit a one-time stderr warning per parsed value and produce `canonical=raw_scheme="<unknown>"`. Matching against them works only on exact `raw_scheme` equality.

### Parsing rules

- **Fully qualified URIs** (`<scheme>://<authority>/<path>`) — parsed via `urllib.parse.urlparse`. Path is normalized to start with `/` (so `s3://bucket` → path `/`, `s3://bucket/` → path `/`, `s3://bucket/x` → path `/x`).
- **Bare absolute paths** (`/tmp/x.parquet`) — canonical `file`, authority `""`, path as-is.
- **Bare relative paths** (`./data/x`, `data/x`) — if `project_root` is supplied, resolved to absolute and parsed as `file://`. Without `project_root`, kept literally — caller's responsibility.
- **Empty string** — parses as `URI(scheme="file", raw_scheme="", authority="", path="")`. Will not match anything useful but does not raise.

### Matching rules

`matches_glob(uri, pattern)` parses `pattern` with the same `parse()` (no `project_root` — patterns are user-authored, expected to be absolute or `file://`-explicit), then compares:

1. **Scheme:** `uri.scheme == pattern.scheme` (canonical). Sub-scheme variants collapse via the canonical map.
2. **Authority:** `fnmatch(uri.authority, pattern.authority)` — shell-style glob (`*`, `?`).
3. **Path:** custom matcher supporting `*` (single segment, no `/`), `**` (multiple segments), `?` (single char). Implementation is a thin adapter over `fnmatch.fnmatch` after rewriting `**` to a regex.

Trailing slashes are **not** normalized. `s3://bucket/users/` ≠ `s3://bucket/users`. The user is responsible for writing patterns consistently.

## Integration points

| File | Change |
|---|---|
| `skills/open_table_migrator/uri.py` | new file (~80 LOC) |
| `skills/open_table_migrator/targets.py` | `build_resolver` accepts `project_root`, internally calls `uri.matches_glob` instead of `fnmatch.fnmatch` |
| `skills/open_table_migrator/cli.py` | `build_resolver(mapping, project_root=args.project)` call site |
| `skills/open_table_migrator/extract.py` | no change — `path_arg` stays raw |
| `skills/open_table_migrator/worklist.py` | no change — `path_arg` stored as-is |
| `skills/open_table_migrator/SKILL.md` | new section "Path schemes" documenting the canonical map and viewfs limitation |

## User-visible behavior changes

1. Mapping `s3://bucket/users/*` previously matched only literal `s3://...`. Now also matches `s3a://...` and `s3n://...`. **Strictly broadening** — no existing behavior breaks.
2. Mapping `/tmp/fixtures/*` previously did not match `file:///tmp/fixtures/x`. Now does. Again strictly broadening.
3. New canonical schemes (`abfs`, `gs`, `hdfs`, `viewfs`) work in mappings the same way `s3://` did.
4. Patterns with unknown schemes still work via exact-match — existing user-defined extensions continue to function.

## Testing strategy

### `tests/test_uri.py` — module unit tests

**Parsing (8 cases):**
- `s3://bucket/key` → `URI("s3", "s3", "bucket", "/key")`
- `s3a://bucket/key` → `URI("s3", "s3a", "bucket", "/key")`
- `webhdfs://ns/x` → `URI("hdfs", "webhdfs", "ns", "/x")`
- `hdfs:///warehouse` → `URI("hdfs", "hdfs", "", "/warehouse")`
- `viewfs://ns/x` → `URI("viewfs", "viewfs", "ns", "/x")`
- `/tmp/x.parquet` → `URI("file", "", "", "/tmp/x.parquet")`
- `./data/x` with `project_root=Path("/proj")` → `URI("file", "", "", "/proj/data/x")`
- `ftp://unknown.com/x` → `URI("<unknown>", "ftp", "unknown.com", "/x")` + stderr warning

**Matching (10 cases):**
- `s3://bucket/users/*` matches `s3a://bucket/users/x`
- `s3://bucket/users/*` does not match `s3://bucket/orders/x`
- `hdfs://ns/warehouse/*` matches `webhdfs://ns/warehouse/x`
- `viewfs://ns/x` does not match `hdfs://ns/x`
- `s3://bucket/users/**` matches `s3://bucket/users/2024/01/data.parquet`
- `s3://bucket/users/?.parquet` matches `s3://bucket/users/x.parquet`
- `s3://my-*-bucket/x` matches `s3://my-prod-bucket/x`
- `/tmp/fixtures/*` matches `file:///tmp/fixtures/x` and bare `/tmp/fixtures/x`
- `file:///tmp/fixtures/*` matches bare `/tmp/fixtures/x`
- `s3://bucket/users/` does not match `s3://bucket/users` (trailing slash matters)

### `tests/test_multi_table.py` — extended

Three new scenarios alongside existing tests:
- HDFS mapping resolves table references in a synthetic Hadoop project
- Azure abfs mapping resolves table references
- viewfs and hdfs both listed in mapping for the same logical table (documents the workaround for the no-mount-resolution limitation)

### Snapshot tests

None. URI parsing is too granular for snapshots; per-case assertions are clearer.

## Risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| `urllib.parse.urlparse` quirks across Python versions (path normalization, scheme extraction edge cases) | low | Pin behavior to dataclass assertions; tests will surface differences. Project already requires Python 3.11+ which has stable behavior |
| Unknown scheme matching becomes a silent foot-gun | low | Stderr warning on first occurrence per parsed value. Documented in SKILL.md |
| Sub-scheme equivalence breaks a user who *wants* to distinguish `s3://` from `s3a://` (e.g., different IAM roles per scheme) | very low | Acknowledged as out-of-scope; user can fork canonical map if needed. No realistic production case where this distinction is the intended discriminator |
| Performance: O(n*m) over n mapping entries and m call sites | low | `n` and `m` are both small (tens to low hundreds in typical projects); no benchmark needed pre-shipping |

## Estimated size

- Production: ~80 LOC (`uri.py`) + ~50 LOC integration in `targets.py`
- Tests: ~150 LOC across `test_uri.py` + 3 cases in `test_multi_table.py`
- Documentation: ~30 lines in SKILL.md
- Total: ~310 LOC. Two short implementation sessions.

## Open questions deferred to implementation

These do **not** block the design:

- Exact text of the unknown-scheme warning message (stylistic).
- Whether to wrap `**` matching as a small regex helper or inline it (taste, single call site).
- Whether `parse("")` is a hard error or returns the empty `URI` shown above (currently lean toward soft-return for robustness in the detector pipeline).
