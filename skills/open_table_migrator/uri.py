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
