"""Dynamic SQL loading detection.

Finds call-sites in Python/Java/Scala that load .sql files at runtime, so
the migrator can cross-reference them with parquet/orc tables defined in
those files (or elsewhere in the SQL registry).

Patterns covered:
  - py_open: open("*.sql")
  - py_path_read_text: Path("*.sql").read_text() / .read_bytes()
  - py_pkgutil_get_data: pkgutil.get_data(<pkg>, "*.sql")
  - java_files_read: Files.readAllBytes/readString(Path.of/Paths.get("*.sql"))
  - java_resource_stream: any.getResourceAsStream("*.sql")
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .ts_parser import parse, language_for_file


_EXTS = {".py", ".java", ".scala"}
_IGNORED_DIRS = {".git", "__pycache__", "node_modules", "target", "build", ".gradle", "venv", ".venv"}
_SQL_SUFFIXES = (".sql", ".hql", ".ddl")


@dataclass(frozen=True)
class DynamicSqlLoader:
    file: Path
    line: int
    pattern: str
    sql_filename: str
    confidence: str  # "high" | "medium"


def detect_dynamic_sql_loaders(
    project_root: Path,
    *,
    const_table_for_file: Callable[[Path], object] | None = None,
) -> list[DynamicSqlLoader]:
    """Walk .py/.java/.scala in project_root, detect loader patterns."""
    out: list[DynamicSqlLoader] = []
    for src in project_root.rglob("*"):
        if not src.is_file() or src.suffix.lower() not in _EXTS:
            continue
        if any(part in _IGNORED_DIRS for part in src.parts):
            continue
        lang = language_for_file(src)
        if lang is None:
            continue
        source = src.read_bytes()
        const_table = const_table_for_file(src) if const_table_for_file else None
        if lang == "python":
            out.extend(_detect_python_loaders(source, src, const_table))
    return out


def _filename_is_sql(name: str) -> bool:
    return name.lower().endswith(_SQL_SUFFIXES)


def _extract_string_literal_python(node, source: bytes) -> str | None:
    if node.type != "string":
        return None
    for child in node.children:
        if child.type == "interpolation":
            return None
    for child in node.children:
        if child.type == "string_content":
            return source[child.start_byte:child.end_byte].decode()
    return ""


def _detect_python_loaders(source: bytes, src_path: Path, const_table) -> list[DynamicSqlLoader]:
    tree = parse(source, "python")
    out: list[DynamicSqlLoader] = []
    stack = [tree.root_node]
    while stack:
        node = stack.pop()
        if node.type == "call":
            loader = _try_py_open(node, source, src_path, const_table)
            if loader is not None:
                out.append(loader)
        for child in reversed(node.children):
            stack.append(child)
    return out


def _try_py_open(call_node, source: bytes, src_path: Path, const_table):
    func = call_node.child_by_field_name("function")
    if func is None or func.type != "identifier":
        return None
    func_name = source[func.start_byte:func.end_byte].decode()
    if func_name != "open":
        return None
    args = call_node.child_by_field_name("arguments")
    if args is None or args.named_child_count < 1:
        return None
    first_arg = args.named_children[0]
    filename = _extract_string_literal_python(first_arg, source)
    if filename is None:
        # Identifier — placeholder for Task 2 (const_table integration)
        return None
    if not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="py_open",
        sql_filename=filename,
        confidence="high",
    )
