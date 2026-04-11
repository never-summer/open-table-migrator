import re
from dataclasses import dataclass
from pathlib import Path

from .extract import extract_path_arg
from .folding import fold_chains


@dataclass
class PatternMatch:
    file: Path
    line: int
    pattern_type: str
    original_code: str
    path_arg: str | None = None
    end_line: int | None = None  # last physical line of the logical statement

    @property
    def direction(self) -> str:
        # Derived from pattern_type; lazy import avoids a detector ↔ analyzer cycle.
        from .analyzer import direction_of
        return direction_of(self.pattern_type)


_PY_EXTS = {".py"}
_JVM_EXTS = {".java", ".scala"}

# ─── Python patterns ──────────────────────────────────────────────────
# pandas / pyspark / pyarrow / streaming
_PY_PATTERNS: list[tuple[str, str]] = [
    # pandas — Parquet
    ("pandas_read",             r"pd\.read_parquet\s*\("),
    ("pandas_write",            r"\.to_parquet\s*\("),
    # pandas — ORC
    ("pandas_orc_read",         r"pd\.read_orc\s*\("),
    ("pandas_orc_write",        r"\.to_orc\s*\("),

    # PySpark — streaming (warn-only, detected but transformers skip)
    ("pyspark_stream_read",     r"\.readStream(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\("),
    ("pyspark_stream_read_fmt", r'\.readStream(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\'](?:parquet|orc)["\']\s*\)'),
    ("pyspark_stream_write",    r"\.writeStream(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\("),
    ("pyspark_stream_write_fmt",r'\.writeStream(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\'](?:parquet|orc)["\']\s*\)'),

    # PySpark — batch Parquet
    ("pyspark_read",            r"\.read\.parquet\s*\("),
    ("pyspark_write",           r"\.write(?:\.\w+\([^)]*\))*\.parquet\s*\("),
    # PySpark — batch ORC
    ("pyspark_orc_read",        r"\.read\.orc\s*\("),
    ("pyspark_orc_write",       r"\.write(?:\.\w+\([^)]*\))*\.orc\s*\("),
    # PySpark — generic format("parquet"/"orc")
    ("pyspark_read_fmt",        r'\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\'](?:parquet|orc)["\']\s*\)'),
    ("pyspark_write_fmt",       r'\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\'](?:parquet|orc)["\']\s*\)'),

    # pyarrow — classic
    ("pyarrow_read",            r"pq\.read_table\s*\("),
    ("pyarrow_write",           r"pq\.write_table\s*\("),
    # pyarrow — dataset / ParquetFile / ParquetDataset / ORC
    ("pyarrow_parquet_file",    r"pq\.ParquetFile\s*\("),
    ("pyarrow_parquet_dataset", r"pq\.ParquetDataset\s*\("),
    ("pyarrow_dataset_read",    r'(?:pa|pyarrow)\.dataset\.dataset\s*\('),
    ("pyarrow_dataset_write",   r'(?:pa|pyarrow)\.dataset\.write_dataset\s*\('),
    ("pyarrow_orc_read",        r"(?:orc|po)\.read_table\s*\("),
    ("pyarrow_orc_write",       r"(?:orc|po)\.write_table\s*\("),
]

# ─── Java Spark patterns ─────────────────────────────────────────────
# Java uses .read() / .write() with parentheses
_JAVA_SPARK_PATTERNS: list[tuple[str, str]] = [
    # Streaming (warn-only)
    ("java_spark_stream_read",     r"\.readStream\(\)(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\("),
    ("java_spark_stream_read_fmt", r'\.readStream\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'),
    ("java_spark_stream_write",    r"\.writeStream\(\)(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\("),
    ("java_spark_stream_write_fmt",r'\.writeStream\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'),

    # Batch Parquet
    ("java_spark_read",            r"\.read\(\)\.parquet\s*\("),
    ("java_spark_write",           r"\.write\(\)(?:\.\w+\([^)]*\))*\.parquet\s*\("),
    # Batch ORC
    ("java_spark_orc_read",        r"\.read\(\)\.orc\s*\("),
    ("java_spark_orc_write",       r"\.write\(\)(?:\.\w+\([^)]*\))*\.orc\s*\("),
    # Generic format("parquet"/"orc")
    ("java_spark_read_fmt",        r'\.read\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'),
    ("java_spark_write_fmt",       r'\.write\(\)(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'),
]

# ─── Scala Spark patterns ────────────────────────────────────────────
# Scala omits parens on read/write
_SCALA_SPARK_PATTERNS: list[tuple[str, str]] = [
    # Streaming
    ("scala_spark_stream_read",     r"\.readStream(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\("),
    ("scala_spark_stream_read_fmt", r'\.readStream(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'),
    ("scala_spark_stream_write",    r"\.writeStream(?:\.\w+\([^)]*\))*\.(?:parquet|orc)\s*\("),
    ("scala_spark_stream_write_fmt",r'\.writeStream(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'),

    # Batch Parquet
    ("scala_spark_read",            r"\.read\.parquet\s*\("),
    ("scala_spark_write",           r"\.write(?:\.\w+\([^)]*\))*\.parquet\s*\("),
    # Batch ORC
    ("scala_spark_orc_read",        r"\.read\.orc\s*\("),
    ("scala_spark_orc_write",       r"\.write(?:\.\w+\([^)]*\))*\.orc\s*\("),
    # Generic format
    ("scala_spark_read_fmt",        r'\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'),
    ("scala_spark_write_fmt",       r'\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*"(?:parquet|orc)"\s*\)'),
]

# ─── Hive / SparkSQL patterns (shared across JVM + Python SparkSQL) ──
_HIVE_PATTERNS: list[tuple[str, str]] = [
    # Legacy Hive DDL
    ("hive_create_parquet",     r'"[^"]*\bSTORED\s+AS\s+PARQUET\b[^"]*"'),
    ("hive_create_orc",         r'"[^"]*\bSTORED\s+AS\s+ORC\b[^"]*"'),
    # Modern Spark SQL DDL
    ("sql_using_parquet",       r'"[^"]*\bUSING\s+parquet\b[^"]*"'),
    ("sql_using_orc",           r'"[^"]*\bUSING\s+orc\b[^"]*"'),
    # DML
    ("hive_insert_overwrite",   r'"[^"]*\bINSERT\s+OVERWRITE\s+TABLE\b[^"]*"'),
    ("hive_insert_into",        r'"[^"]*\bINSERT\s+INTO\s+(?:TABLE\s+)?\w[^"]*"'),
    # API
    ("hive_save_as_table",      r"\.saveAsTable\s*\("),
]

_COMPILED_PY = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _PY_PATTERNS + _HIVE_PATTERNS]
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
        source = src_file.read_text(errors="replace")
        for logical in fold_chains(source):
            text = logical.folded_text
            seen: set[str] = set()
            for pattern_type, regex in compiled:
                if pattern_type in seen:
                    continue
                if regex.search(text):
                    matches.append(PatternMatch(
                        file=src_file,
                        line=logical.start_line,
                        pattern_type=pattern_type,
                        original_code=text.strip(),
                        path_arg=extract_path_arg(text),
                        end_line=logical.end_line,
                    ))
                    seen.add(pattern_type)
    return matches
