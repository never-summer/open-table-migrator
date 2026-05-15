"""Extract the target path / table identifier from a matched parquet/orc call site.

Best-effort, literal-only: returns the string argument when it's a string literal,
`None` when the argument is a variable, expression, or otherwise not extractable.
"""
import re

_STR = r'["\']([^"\']+)["\']'

_EXTRACTORS = [
    # write_table(first_arg, "path") — second arg is the path
    re.compile(rf'(?:pq|orc|po)\.write_table\s*\(\s*\w+\s*,\s*{_STR}'),
    re.compile(rf'(?:pa|pyarrow)\.dataset\.write_dataset\s*\(\s*\w+\s*,\s*{_STR}'),

    # First-arg readers
    re.compile(
        rf'(?:pd\.read_parquet|pd\.read_orc|(?:pq|orc|po)\.read_table'
        rf'|pq\.ParquetFile|pq\.ParquetDataset'
        rf'|(?:pa|pyarrow)\.dataset\.dataset)\s*\(\s*{_STR}'
    ),

    # .saveAsTable("ns.t")  — the string IS the target table identifier
    re.compile(rf'\.saveAsTable\s*\(\s*{_STR}'),

    # Spark chain terminators: .parquet / .orc / .load / .save / .to_parquet / .to_orc
    re.compile(rf'\.(?:parquet|orc|load|save|to_parquet|to_orc)\s*\(\s*{_STR}'),

    # SQL DDL / DML — take the table identifier
    re.compile(r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.`]+)', re.IGNORECASE),
    re.compile(r'INSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?([\w.`]+)', re.IGNORECASE),
]


def extract_path_arg(line: str) -> str | None:
    for rx in _EXTRACTORS:
        m = rx.search(line)
        if m:
            return m.group(1).strip("`")
    return None


# ─── Subject (DataFrame / variable being operated on) ────────────────

_SUBJECT_PATTERNS = [
    # Assignment target: `val df = spark.read...` / `df = pd.read...`
    re.compile(r'(?:val\s+|var\s+)?(\w+)\s*=\s*(?:spark|pd|pq|orc|po|pa|pyarrow)\b'),
    # spark.read → subject is the assignment target before `=`
    re.compile(r'(?:val\s+|var\s+)?(\w+)\s*=\s*.*\.read[.(]'),
    # Method chain with possible intermediaries:
    # `usersDF.orderBy(...).write...` / `df.write.format(...)` / `df.to_parquet(...)`
    re.compile(r'\b(\w+)\.(?:\w+\([^)]*\)\.)*(?:write|writeTo|to_parquet|to_orc)\b'),
    # Simpler fallback: first identifier before `.write` anywhere in the line
    re.compile(r'\b(\w+)\.\w+.*\.write\b'),
]


def extract_subject(code: str) -> str | None:
    """Extract the DataFrame/variable name that is the subject of the operation.

    Strips leading comments (``//`` and ``#``) so that folded chains like
    ``// comment text.bucketBy(8, "uid").saveAsTable("T")`` still find the
    subject from the non-comment portion.
    """
    # Strip leading single-line comment prefix(es) to handle folded chains
    # where a comment line was joined with a continuation line.
    stripped = re.sub(r'^(?://|#)[^\n]*\.', '.', code)
    for rx in _SUBJECT_PATTERNS:
        m = rx.search(code)
        if m:
            name = m.group(1)
            if name not in ("val", "var", "return", "def", "object", "class"):
                return name
        # Try on stripped version too
        m2 = rx.search(stripped)
        if m2:
            name = m2.group(1)
            if name not in ("val", "var", "return", "def", "object", "class"):
                return name
    return None


# ─── Human-readable summary ──────────────────────────────────────────

def summarize_operation(
    code: str,
    pattern_type: str,
    path_arg: str | None,
    subject_override: str | None = None,
) -> str:
    """Return a short (1-line) human-readable description of the operation."""
    subject = subject_override or extract_subject(code) or "?"
    target = path_arg or "(variable/expression)"
    fmt = _format_of(pattern_type) or "data"

    # Special cases first
    if pattern_type in ("spark_table_read", "spark_read_table"):
        return f"{subject} — reads table via spark.table({target})"
    if pattern_type in ("java_file_writer", "stdlib_write_file"):
        return f"{subject} — writes to local file via FileWriter/BufferedWriter"
    if pattern_type in ("hive_save_as_table", "hive_save_table"):
        return f"{subject} — saves as Hive table '{target}' (saveAsTable)"
    if "hive_create" in pattern_type or "sql_using" in pattern_type:
        return f"DDL — creates table {target} with {fmt} storage"
    if "hive_insert" in pattern_type:
        return f"DML — inserts into table {target}"

    if "stream" in pattern_type:
        direction = "reads stream from" if "read" in pattern_type else "writes stream to"
        return f"{subject} — {direction} {target} (Structured Streaming, {fmt})"

    if pattern_type.startswith("pyarrow"):
        if "dataset" in pattern_type or "parquet_file" in pattern_type:
            return f"{subject} — uses pyarrow dataset/ParquetFile API on {target}"
        if "read" in pattern_type:
            return f"{subject} — reads {fmt} into Arrow table from {target}"
        return f"{subject} — writes Arrow table to {fmt} at {target}"

    # Spark / pandas / pyarrow — generic path with format name
    extras = []
    if ".bucketBy(" in code:
        m = re.search(r'\.bucketBy\(([^)]*)\)', code)
        extras.append(f"bucketBy({m.group(1)})" if m else "bucketBy")
    if ".partitionBy(" in code:
        m = re.search(r'\.partitionBy\(([^)]*)\)', code)
        extras.append(f"partitionBy({m.group(1)})" if m else "partitionBy")
    if ".sortBy(" in code or ".orderBy(" in code:
        extras.append("sorted")
    extra_str = f" [{', '.join(extras)}]" if extras else ""

    if "read" in pattern_type or "reader" in pattern_type:
        return f"{subject} — reads {fmt} from {target}"
    if "write" in pattern_type or "writer" in pattern_type:
        return f"{subject} — writes {fmt} to {target}{extra_str}"

    return f"{subject} — {fmt} I/O on {target}"


def _format_of(pattern_type: str) -> str | None:
    """Return the human-readable data format name from a pattern_type, or None."""
    for fmt in ("csv", "json", "avro", "delta", "text", "jdbc", "excel", "parquet", "orc"):
        if fmt in pattern_type:
            return fmt.upper() if fmt in ("csv", "jdbc") else fmt.capitalize()
    return None
