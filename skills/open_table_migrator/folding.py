"""Fold fluent-chain continuation lines into single logical statements.

Spark DataFrame builder chains often span multiple physical lines:

    usersDF.write
      .format("parquet")
      .bucketBy(8, "uid")
      .mode(SaveMode.OverWrite)
      .saveAsTable("UsersTbl")

Line-by-line regex scanning misses these because `.write` and `.saveAsTable`
live on different physical lines. This helper folds every sequence of lines
where each continuation line (after leading whitespace) starts with `.` into
one logical line. The detector and transformers operate on logical lines so
they can match and rewrite the entire chain.
"""
from dataclasses import dataclass


@dataclass
class LogicalLine:
    start_line: int              # 1-indexed physical line of the first element
    end_line: int                # 1-indexed physical line of the last element (inclusive)
    physical_lines: list[str]    # raw lines WITH their original line endings
    folded_text: str             # concatenated, whitespace-stripped at boundaries
    indent: str                  # leading whitespace of the first physical line


def fold_chains(source: str) -> list[LogicalLine]:
    raw_lines = source.splitlines(keepends=True)
    result: list[LogicalLine] = []
    n = len(raw_lines)
    i = 0
    while i < n:
        first = raw_lines[i]
        indent_len = len(first) - len(first.lstrip(" \t"))
        indent = first[:indent_len]

        buf: list[str] = [first]
        j = i + 1
        while j < n and raw_lines[j].lstrip(" \t").startswith("."):
            buf.append(raw_lines[j])
            j += 1

        folded = buf[0].rstrip()
        for cont in buf[1:]:
            folded += cont.strip()

        result.append(LogicalLine(
            start_line=i + 1,
            end_line=j,
            physical_lines=buf,
            folded_text=folded,
            indent=indent,
        ))
        i = j
    return result
