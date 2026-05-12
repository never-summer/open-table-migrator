"""URI parsing and glob matching for table-routing maps.

Sub-scheme equivalence (s3 ≡ s3a ≡ s3n, hdfs ≡ webhdfs, abfs ≡ abfss) is
folded into a canonical `scheme`; the original token is preserved in
`raw_scheme` so user-facing artifacts (path_arg in worklist) remain
verbatim.

viewfs is kept distinct from hdfs by design — mount-point resolution
needs cluster config we do not read.
"""
from __future__ import annotations

import fnmatch
import posixpath
import re
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

# NUL is not valid in URIs or POSIX paths, so it is safe as a sentinel to
# prevent urlparse from treating '?' as a query-string delimiter.
_SENTINEL = "\x00"


@dataclass(frozen=True)
class URI:
    scheme: str
    raw_scheme: str
    authority: str
    path: str


def parse(s: str, *, project_root: Path | None = None) -> URI:
    # Pre-substitute '?' so urlparse doesn't treat it as a query delimiter.
    s_safe = s.replace("?", _SENTINEL)
    parsed = urlparse(s_safe)
    raw_scheme = parsed.scheme
    if raw_scheme in _CANONICAL:
        canonical = _CANONICAL[raw_scheme]
    else:
        if raw_scheme not in _UNKNOWN_WARNED:
            print(f"open_table_migrator: unknown URI scheme '{raw_scheme}' in {s!r}",
                  file=sys.stderr)
            _UNKNOWN_WARNED.add(raw_scheme)
        canonical = "<unknown>"
    authority = parsed.netloc.replace(_SENTINEL, "?")
    path = parsed.path.replace(_SENTINEL, "?")

    if canonical == "file" and raw_scheme == "" and project_root is not None:
        if path and not path.startswith("/"):
            path = posixpath.normpath(str(project_root / path))

    return URI(
        scheme=canonical,
        raw_scheme=raw_scheme,
        authority=authority,
        path=path,
    )


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


def _parse_glob_pattern(pattern: str) -> URI:
    """Parse a glob pattern string into a URI.

    urlparse treats `?` as a query-string delimiter, which breaks glob
    patterns that use `?` as a single-character wildcard.  We work around
    this by temporarily replacing `?` with a sentinel before parsing, then
    restoring it in both the path and authority components.
    """
    sanitised = pattern.replace("?", _SENTINEL)
    parsed = urlparse(sanitised)
    raw_scheme = parsed.scheme
    canonical = _CANONICAL.get(raw_scheme, "<unknown>")
    authority = parsed.netloc.replace(_SENTINEL, "?")
    path = parsed.path.replace(_SENTINEL, "?")
    return URI(scheme=canonical, raw_scheme=raw_scheme, authority=authority, path=path)


def matches_glob(uri: URI, pattern: str) -> bool:
    pat = _parse_glob_pattern(pattern)
    if uri.scheme != pat.scheme:
        return False
    # For unknown schemes, exact raw_scheme equality is required (no aliasing).
    if uri.scheme == "<unknown>" and uri.raw_scheme != pat.raw_scheme:
        return False
    if not fnmatch.fnmatchcase(uri.authority, pat.authority):
        return False
    return _path_match(uri.path, pat.path)
