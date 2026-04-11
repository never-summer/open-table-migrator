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
