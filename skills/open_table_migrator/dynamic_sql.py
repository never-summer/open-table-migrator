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
        elif lang == "java":
            out.extend(_detect_java_loaders(source, src, const_table))
        elif lang == "scala":
            out.extend(_detect_scala_loaders(source, src, const_table))
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
            for attempt in (_try_py_open, _try_py_path_read_text, _try_py_pkgutil_get_data):
                loader = attempt(node, source, src_path, const_table)
                if loader is not None:
                    out.append(loader)
                    break
        for child in reversed(node.children):
            stack.append(child)
    return out


def _resolve_string_or_identifier_python(node, source: bytes, const_table) -> tuple[str | None, str]:
    """Return (value, confidence). confidence in {"high", "medium", "skip"}."""
    literal = _extract_string_literal_python(node, source)
    if literal is not None:
        return literal, "high"
    if node.type == "identifier" and const_table is not None:
        name = source[node.start_byte:node.end_byte].decode()
        binding = const_table.resolve(name)
        if binding is not None and binding.value is not None:
            return binding.value, "medium"
    return None, "skip"


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
    filename, confidence = _resolve_string_or_identifier_python(
        args.named_children[0], source, const_table,
    )
    if filename is None or not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="py_open",
        sql_filename=filename,
        confidence=confidence,
    )


def _try_py_path_read_text(call_node, source: bytes, src_path: Path, const_table):
    func = call_node.child_by_field_name("function")
    if func is None or func.type != "attribute":
        return None
    method_name_node = func.child_by_field_name("attribute")
    if method_name_node is None:
        return None
    method_name = source[method_name_node.start_byte:method_name_node.end_byte].decode()
    if method_name not in ("read_text", "read_bytes"):
        return None
    obj = func.child_by_field_name("object")
    if obj is None or obj.type != "call":
        return None
    inner_func = obj.child_by_field_name("function")
    if inner_func is None or inner_func.type != "identifier":
        return None
    if source[inner_func.start_byte:inner_func.end_byte].decode() != "Path":
        return None
    inner_args = obj.child_by_field_name("arguments")
    if inner_args is None or inner_args.named_child_count < 1:
        return None
    filename, confidence = _resolve_string_or_identifier_python(
        inner_args.named_children[0], source, const_table,
    )
    if filename is None or not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="py_path_read_text",
        sql_filename=filename,
        confidence=confidence,
    )


def _try_py_pkgutil_get_data(call_node, source: bytes, src_path: Path, const_table):
    func = call_node.child_by_field_name("function")
    if func is None or func.type != "attribute":
        return None
    method_name_node = func.child_by_field_name("attribute")
    if method_name_node is None:
        return None
    if source[method_name_node.start_byte:method_name_node.end_byte].decode() != "get_data":
        return None
    obj = func.child_by_field_name("object")
    if obj is None or obj.type != "identifier":
        return None
    if source[obj.start_byte:obj.end_byte].decode() != "pkgutil":
        return None
    args = call_node.child_by_field_name("arguments")
    if args is None or args.named_child_count < 2:
        return None
    second_arg = args.named_children[1]
    filename, confidence = _resolve_string_or_identifier_python(
        second_arg, source, const_table,
    )
    if filename is None or not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="py_pkgutil_get_data",
        sql_filename=filename,
        confidence=confidence,
    )


def _extract_string_literal_java(node, source: bytes) -> str | None:
    if node.type != "string_literal":
        return None
    for child in node.children:
        if child.type == "string_fragment":
            return source[child.start_byte:child.end_byte].decode()
    return ""


def _extract_string_literal_scala(node, source: bytes) -> str | None:
    if node.type != "string":
        return None
    text = source[node.start_byte:node.end_byte].decode()
    if text.startswith('"') and text.endswith('"'):
        return text[1:-1]
    return None


def _detect_java_loaders(source: bytes, src_path: Path, const_table) -> list[DynamicSqlLoader]:
    tree = parse(source, "java")
    out: list[DynamicSqlLoader] = []
    stack = [tree.root_node]
    while stack:
        node = stack.pop()
        if node.type == "method_invocation":
            for attempt in (_try_java_files_read, _try_java_resource_stream):
                loader = attempt(node, source, src_path, "java")
                if loader is not None:
                    out.append(loader)
                    break
        for child in reversed(node.children):
            stack.append(child)
    return out


def _detect_scala_loaders(source: bytes, src_path: Path, const_table) -> list[DynamicSqlLoader]:
    tree = parse(source, "scala")
    out: list[DynamicSqlLoader] = []
    stack = [tree.root_node]
    while stack:
        node = stack.pop()
        if node.type == "call_expression":
            loader = _try_java_resource_stream(node, source, src_path, "scala")
            if loader is not None:
                out.append(loader)
        for child in reversed(node.children):
            stack.append(child)
    return out


def _try_java_files_read(call_node, source: bytes, src_path: Path, lang: str):
    name_n = call_node.child_by_field_name("name")
    if name_n is None:
        return None
    method_name = source[name_n.start_byte:name_n.end_byte].decode()
    if method_name not in ("readAllBytes", "readString"):
        return None
    obj_n = call_node.child_by_field_name("object")
    if obj_n is None:
        return None
    if source[obj_n.start_byte:obj_n.end_byte].decode() != "Files":
        return None
    args = call_node.child_by_field_name("arguments")
    if args is None or args.named_child_count < 1:
        return None
    first_arg = args.named_children[0]
    if first_arg.type != "method_invocation":
        return None
    inner_obj = first_arg.child_by_field_name("object")
    inner_name = first_arg.child_by_field_name("name")
    inner_args = first_arg.child_by_field_name("arguments")
    if inner_obj is None or inner_name is None or inner_args is None:
        return None
    obj_text = source[inner_obj.start_byte:inner_obj.end_byte].decode()
    name_text = source[inner_name.start_byte:inner_name.end_byte].decode()
    if not ((obj_text == "Path" and name_text == "of") or (obj_text == "Paths" and name_text == "get")):
        return None
    if inner_args.named_child_count < 1:
        return None
    filename = _extract_string_literal_java(inner_args.named_children[0], source)
    if filename is None or not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="java_files_read",
        sql_filename=filename,
        confidence="high",
    )


def _try_java_resource_stream(call_node, source: bytes, src_path: Path, lang: str):
    if lang == "java":
        name_n = call_node.child_by_field_name("name")
        args_n = call_node.child_by_field_name("arguments")
        if name_n is None or args_n is None:
            return None
        name_text = source[name_n.start_byte:name_n.end_byte].decode()
        if name_text != "getResourceAsStream":
            return None
        if args_n.named_child_count < 1:
            return None
        filename = _extract_string_literal_java(args_n.named_children[0], source)
    elif lang == "scala":
        func = call_node.child_by_field_name("function")
        args = call_node.child_by_field_name("arguments")
        if func is None or args is None:
            return None
        if func.type != "field_expression":
            return None
        method_name_node = func.child_by_field_name("field")
        if method_name_node is None:
            return None
        method_name = source[method_name_node.start_byte:method_name_node.end_byte].decode()
        if method_name != "getResourceAsStream":
            return None
        if args.named_child_count < 1:
            return None
        filename = _extract_string_literal_scala(args.named_children[0], source)
    else:
        return None
    if filename is None or not _filename_is_sql(filename):
        return None
    return DynamicSqlLoader(
        file=src_path,
        line=call_node.start_point[0] + 1,
        pattern="java_resource_stream",
        sql_filename=filename,
        confidence="high",
    )
