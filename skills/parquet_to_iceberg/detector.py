import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PatternMatch:
    file: Path
    line: int
    pattern_type: str
    original_code: str


# File extensions to scan, grouped by language family
_PY_EXTS = {".py"}
_JVM_EXTS = {".java", ".scala"}

# Python patterns (pandas / pyspark / pyarrow)
_PY_PATTERNS: list[tuple[str, str]] = [
    ("pandas_read",    r"pd\.read_parquet\s*\("),
    ("pandas_write",   r"\.to_parquet\s*\("),
    ("pyspark_read",   r"\.read\.parquet\s*\("),
    ("pyspark_write",  r"\.write(?:\.\w+\([^)]*\))*\.parquet\s*\("),
    ("pyarrow_read",   r"pq\.read_table\s*\("),
    ("pyarrow_write",  r"pq\.write_table\s*\("),
]

# Java Spark patterns — Java uses .read().parquet() with parens
_JAVA_SPARK_PATTERNS: list[tuple[str, str]] = [
    ("java_spark_read",   r"\.read\(\)\.parquet\s*\("),
    ("java_spark_write",  r"\.write\(\)(?:\.\w+\([^)]*\))*\.parquet\s*\("),
]

# Scala Spark patterns — Scala omits parens: .read.parquet
_SCALA_SPARK_PATTERNS: list[tuple[str, str]] = [
    ("scala_spark_read",   r"\.read\.parquet\s*\("),
    ("scala_spark_write",  r"\.write(?:\.\w+\([^)]*\))*\.parquet\s*\("),
]

# Hive/SparkSQL patterns — look inside SQL string literals and API calls
_HIVE_PATTERNS: list[tuple[str, str]] = [
    ("hive_create_parquet",    r'"[^"]*\bSTORED\s+AS\s+PARQUET\b[^"]*"'),
    ("hive_save_as_table",     r"\.saveAsTable\s*\("),
    ("hive_insert_overwrite",  r'"[^"]*\bINSERT\s+OVERWRITE\s+TABLE\b[^"]*"'),
]

_COMPILED_PY = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _PY_PATTERNS]
_COMPILED_JAVA = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _JAVA_SPARK_PATTERNS + _HIVE_PATTERNS]
_COMPILED_SCALA = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _SCALA_SPARK_PATTERNS + _HIVE_PATTERNS]


def _patterns_for_file(path: Path) -> list[tuple[str, re.Pattern]]:
    suffix = path.suffix.lower()
    if suffix == ".py":
        return _COMPILED_PY
    if suffix == ".java":
        return _COMPILED_JAVA
    if suffix == ".scala":
        return _COMPILED_SCALA
    return []


def detect_parquet_usage(project_root: Path) -> list[PatternMatch]:
    matches: list[PatternMatch] = []
    for src_file in sorted(project_root.rglob("*")):
        if not src_file.is_file():
            continue
        if src_file.suffix.lower() not in (_PY_EXTS | _JVM_EXTS):
            continue
        compiled = _patterns_for_file(src_file)
        for lineno, line in enumerate(src_file.read_text(errors="replace").splitlines(), 1):
            for pattern_type, regex in compiled:
                if regex.search(line):
                    matches.append(PatternMatch(
                        file=src_file,
                        line=lineno,
                        pattern_type=pattern_type,
                        original_code=line.strip(),
                    ))
    return matches
