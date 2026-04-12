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

# ─── Broad I/O patterns (CSV, JSON, Avro, Delta, text, JDBC, etc.) ───

_IO_EXTRA_PY: list[tuple[str, str]] = [
    # pandas CSV/JSON/Excel
    ("pandas_csv_read",     r"pd\.read_csv\s*\("),
    ("pandas_csv_write",    r"\.to_csv\s*\("),
    ("pandas_json_read",    r"pd\.read_json\s*\("),
    ("pandas_json_write",   r"\.to_json\s*\("),
    ("pandas_excel_read",   r"pd\.read_excel\s*\("),
    ("pandas_excel_write",  r"\.to_excel\s*\("),

    # PySpark CSV/JSON/text/Avro/Delta
    ("pyspark_csv_read",    r"\.read\.csv\s*\("),
    ("pyspark_csv_write",   r"\.write(?:\.\w+\([^)]*\))*\.csv\s*\("),
    ("pyspark_json_read",   r"\.read\.json\s*\("),
    ("pyspark_json_write",  r"\.write(?:\.\w+\([^)]*\))*\.json\s*\("),
    ("pyspark_text_read",   r"\.read\.text\s*\("),
    ("pyspark_text_write",  r"\.write(?:\.\w+\([^)]*\))*\.text\s*\("),
    ("pyspark_csv_read_fmt",  r'\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']csv["\']\s*\)'),
    ("pyspark_csv_write_fmt", r'\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']csv["\']\s*\)'),
    ("pyspark_json_read_fmt", r'\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']json["\']\s*\)'),
    ("pyspark_json_write_fmt",r'\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']json["\']\s*\)'),
    ("pyspark_avro_read_fmt", r'\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']avro["\']\s*\)'),
    ("pyspark_avro_write_fmt",r'\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']avro["\']\s*\)'),
    ("pyspark_delta_read_fmt",r'\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']delta["\']\s*\)'),
    ("pyspark_delta_write_fmt",r'\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']delta["\']\s*\)'),
    ("pyspark_text_read_fmt", r'\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']text["\']\s*\)'),
    ("pyspark_text_write_fmt",r'\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']text["\']\s*\)'),
    ("pyspark_jdbc_read_fmt", r'\.read(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']jdbc["\']\s*\)'),
    ("pyspark_jdbc_write_fmt",r'\.write(?:\.\w+\([^)]*\))*\.format\s*\(\s*["\']jdbc["\']\s*\)'),
    ("pyspark_jdbc_read",   r"\.read\.jdbc\s*\("),

    # spark.table()
    ("spark_table_read",    r"spark\.table\s*\("),

    # Python csv module
    ("python_csv_writer",   r"csv\.(?:writer|DictWriter)\s*\("),
    ("python_csv_reader",   r"csv\.(?:reader|DictReader)\s*\("),
]

_IO_EXTRA_JVM: list[tuple[str, str]] = [
    # Java/Scala Spark CSV/JSON/text/Avro/Delta
    ("jvm_csv_read",        r"\.read\(?\.?\)?\.csv\s*\("),
    ("jvm_csv_write",       r"\.write\(?\.?\)?(?:\.\w+\([^)]*\))*\.csv\s*\("),
    ("jvm_json_read",       r"\.read\(?\.?\)?\.json\s*\("),
    ("jvm_json_write",      r"\.write\(?\.?\)?(?:\.\w+\([^)]*\))*\.json\s*\("),
    ("jvm_text_read",       r"\.read\(?\.?\)?\.text\s*\("),
    ("jvm_text_write",      r"\.write\(?\.?\)?(?:\.\w+\([^)]*\))*\.text\s*\("),
    ("jvm_csv_read_fmt",    r'\.read\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"csv"\s*\)'),
    ("jvm_csv_write_fmt",   r'\.write\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"csv"\s*\)'),
    ("jvm_json_read_fmt",   r'\.read\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"json"\s*\)'),
    ("jvm_json_write_fmt",  r'\.write\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"json"\s*\)'),
    ("jvm_avro_read_fmt",   r'\.read\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"avro"\s*\)'),
    ("jvm_avro_write_fmt",  r'\.write\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"avro"\s*\)'),
    ("jvm_delta_read_fmt",  r'\.read\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"delta"\s*\)'),
    ("jvm_delta_write_fmt", r'\.write\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"delta"\s*\)'),
    ("jvm_text_read_fmt",   r'\.read\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"text"\s*\)'),
    ("jvm_text_write_fmt",  r'\.write\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"text"\s*\)'),
    ("jvm_jdbc_read_fmt",   r'\.read\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"jdbc"\s*\)'),
    ("jvm_jdbc_write_fmt",  r'\.write\(?\.?\)?(?:\.\w+\([^)]*\))*\.format\s*\(\s*"jdbc"\s*\)'),
    ("jvm_jdbc_read",       r"\.read\(\)\.jdbc\s*\("),

    # spark.table()
    ("spark_table_read",    r"spark\.table\s*\("),

    # Java FileWriter / BufferedWriter (local file I/O)
    ("java_file_writer",    r"(?:FileWriter|BufferedWriter|PrintWriter)\s*\("),
]


_COMPILED_PY = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _PY_PATTERNS + _HIVE_PATTERNS]
_COMPILED_JAVA = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _JAVA_SPARK_PATTERNS + _HIVE_PATTERNS]
_COMPILED_SCALA = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _SCALA_SPARK_PATTERNS + _HIVE_PATTERNS]

_COMPILED_ALL_PY = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _PY_PATTERNS + _HIVE_PATTERNS + _IO_EXTRA_PY]
_COMPILED_ALL_JAVA = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _JAVA_SPARK_PATTERNS + _HIVE_PATTERNS + _IO_EXTRA_JVM]
_COMPILED_ALL_SCALA = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _SCALA_SPARK_PATTERNS + _HIVE_PATTERNS + _IO_EXTRA_JVM]


def _patterns_for_file(path: Path, *, all_io: bool = False) -> list[tuple[str, re.Pattern]]:
    suffix = path.suffix.lower()
    if all_io:
        if suffix == ".py":
            return _COMPILED_ALL_PY
        if suffix == ".java":
            return _COMPILED_ALL_JAVA
        if suffix == ".scala":
            return _COMPILED_ALL_SCALA
    else:
        if suffix == ".py":
            return _COMPILED_PY
        if suffix == ".java":
            return _COMPILED_JAVA
        if suffix == ".scala":
            return _COMPILED_SCALA
    return []


def _detect(project_root: Path, *, all_io: bool = False) -> list[PatternMatch]:
    matches: list[PatternMatch] = []
    for src_file in sorted(project_root.rglob("*")):
        if not src_file.is_file():
            continue
        if src_file.suffix.lower() not in (_PY_EXTS | _JVM_EXTS):
            continue
        compiled = _patterns_for_file(src_file, all_io=all_io)
        if not compiled:
            continue
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


def detect_parquet_usage(project_root: Path) -> list[PatternMatch]:
    """Detect parquet/ORC operations only (migration candidates)."""
    return _detect(project_root, all_io=False)


def detect_all_io(project_root: Path) -> list[PatternMatch]:
    """Detect ALL data I/O operations — parquet, ORC, CSV, JSON, Avro,
    Delta, text, JDBC, spark.table(), etc. Used for the full inventory
    table the agent shows the user."""
    return _detect(project_root, all_io=True)
