"""Tree-sitter AST-based detector for all data I/O operations.

Replaces the regex detector with precise AST analysis across Python, Java,
and Scala.  Uses programmatic tree walking (no .scm query files).

Public API
----------
ts_detect(project_root) -> list[PatternMatch]
"""
from __future__ import annotations

import re
from pathlib import Path

from .detector import PatternMatch
from .ts_parser import language_for_file, parse

# ---------------------------------------------------------------------------
# File extensions we scan
# ---------------------------------------------------------------------------
_EXTS = {".py", ".java", ".scala"}


# ---------------------------------------------------------------------------
# Helpers: extract string values from AST nodes per language
# ---------------------------------------------------------------------------

def _string_value_python(node) -> str | None:
    """Extract the text of a Python string literal."""
    if node.type == "string":
        for child in node.children:
            if child.type == "string_content":
                return child.text.decode()
    return None


def _string_value_java(node) -> str | None:
    """Extract the text of a Java string literal."""
    if node.type == "string_literal":
        for child in node.children:
            if child.type == "string_fragment":
                return child.text.decode()
    return None


def _string_value_scala(node) -> str | None:
    """Extract the text of a Scala string literal."""
    if node.type == "string":
        return node.text.decode().strip('"')
    return None


_STRING_EXTRACTORS = {
    "python": _string_value_python,
    "java": _string_value_java,
    "scala": _string_value_scala,
}


def _extract_string(node, lang: str) -> str | None:
    return _STRING_EXTRACTORS[lang](node)


# ---------------------------------------------------------------------------
# Helpers: walk tree, find nodes, resolve identifiers
# ---------------------------------------------------------------------------

def _walk(node):
    """Yield every node in the subtree (pre-order)."""
    yield node
    for child in node.children:
        yield from _walk(child)


def _first_string_arg(args_node, lang: str) -> str | None:
    """Return the string value of the first string-literal argument."""
    for child in args_node.children:
        val = _extract_string(child, lang)
        if val is not None:
            return val
    return None


def _nth_positional_arg_string(args_node, lang: str, n: int) -> str | None:
    """Return the string value of the nth (0-indexed) positional argument.

    Counts all positional arguments (skipping commas, parens, keyword args),
    then extracts the string value of the nth one.
    """
    # Collect positional argument nodes (skip punctuation like '(', ')', ',')
    _SKIP = {"(", ")", ",", "="}
    positional = []
    for child in args_node.children:
        if child.type in _SKIP:
            continue
        # Skip keyword arguments (Python: keyword_argument, Java/Scala: named args are rare)
        if child.type in ("keyword_argument",):
            continue
        positional.append(child)
    if n < len(positional):
        return _extract_string(positional[n], lang)
    return None


def _get_args_node(call_node, lang: str):
    """Get the arguments node from a call."""
    if lang == "scala":
        for child in call_node.children:
            if child.type == "arguments":
                return child
    else:
        for child in call_node.children:
            if child.type == "argument_list":
                return child
    return None


def _get_method_name(call_node, lang: str) -> str | None:
    """Extract the method/function name from a call node."""
    if lang == "python":
        # call -> function (attribute) -> identifier (last child)
        func = call_node.child_by_field_name("function")
        if func is None:
            for c in call_node.children:
                if c.type in ("attribute", "identifier"):
                    func = c
                    break
        if func is not None:
            if func.type == "attribute":
                # last identifier child is the method name
                for child in reversed(func.children):
                    if child.type == "identifier":
                        return child.text.decode()
            elif func.type == "identifier":
                return func.text.decode()
    elif lang == "java":
        # method_invocation -> name identifier
        name_node = call_node.child_by_field_name("name")
        if name_node is not None:
            return name_node.text.decode()
        # fallback: find identifier child
        for child in call_node.children:
            if child.type == "identifier":
                return child.text.decode()
    elif lang == "scala":
        # call_expression -> function (field_expression) -> field identifier
        func = call_node.child_by_field_name("function")
        if func is None:
            for c in call_node.children:
                if c.type in ("field_expression", "identifier"):
                    func = c
                    break
        if func is not None:
            if func.type == "field_expression":
                for child in reversed(func.children):
                    if child.type in ("identifier", "property_identifier"):
                        return child.text.decode()
            elif func.type == "identifier":
                return func.text.decode()
    return None


def _get_object_text(call_node, lang: str) -> str | None:
    """Get the full text of the object part of a method call."""
    if lang == "python":
        func = call_node.child_by_field_name("function")
        if func is None:
            for c in call_node.children:
                if c.type == "attribute":
                    func = c
                    break
        if func is not None and func.type == "attribute":
            # The object is the first child (before the dot)
            obj = func.child_by_field_name("object")
            if obj is not None:
                return obj.text.decode()
            # fallback: first child
            if func.children:
                return func.children[0].text.decode()
    elif lang == "java":
        obj = call_node.child_by_field_name("object")
        if obj is not None:
            return obj.text.decode()
    elif lang == "scala":
        func = call_node.child_by_field_name("function")
        if func is None:
            for c in call_node.children:
                if c.type == "field_expression":
                    func = c
                    break
        if func is not None and func.type == "field_expression":
            # first child is the object
            if func.children:
                return func.children[0].text.decode()
    return None


def _call_node_type(lang: str) -> str:
    """The AST node type for function calls in each language."""
    return {"python": "call", "java": "method_invocation", "scala": "call_expression"}[lang]


def _is_inside_comment(node, lang: str) -> bool:
    """Check if a node is inside a comment."""
    current = node.parent
    while current is not None:
        if current.type in ("comment", "line_comment", "block_comment"):
            return True
        current = current.parent
    return False


def _is_inside_string_only(node, lang: str) -> bool:
    """Check if a call node is inside a string literal (not as code).

    Returns True when the call text appears inside a string but is not
    itself a real call node.  We detect this by checking if any ancestor
    is a string type.
    """
    current = node.parent
    while current is not None:
        if current.type in ("string", "string_literal"):
            return True
        current = current.parent
    return False


def _ancestor_chain_text(node) -> str:
    """Walk up to the outermost expression-statement and return its text."""
    current = node
    while current.parent is not None:
        pt = current.parent.type
        if pt in (
            "expression_statement", "local_variable_declaration",
            "val_definition", "var_definition",
            "assignment", "assignment_expression",
            "module", "program", "compilation_unit",
            "class_body", "template_body", "block",
        ):
            break
        current = current.parent
    return current.text.decode()


def _find_chain_keywords(node) -> set[str]:
    """Walk up from a call node and collect identifier names in the chain.

    This finds things like 'read', 'write', 'readStream', 'writeStream',
    'format', etc. by inspecting the text of ancestor nodes.
    """
    text = _ancestor_chain_text(node)
    keywords = set()
    for kw in ("readStream", "writeStream", "read", "write"):
        if kw in text:
            keywords.add(kw)
    return keywords


def _find_format_in_chain(node, lang: str) -> str | None:
    """Look for .format("X") in the method chain of a call node.

    Walk up to find the top-level expression, then search all call nodes
    within it for a `format(...)` call and extract its string argument.
    """
    # Walk up to the statement level
    top = node
    while top.parent is not None:
        if top.parent.type in (
            "expression_statement", "local_variable_declaration",
            "val_definition", "var_definition",
            "module", "program", "compilation_unit",
        ):
            break
        top = top.parent

    call_type = _call_node_type(lang)
    for n in _walk(top):
        if n.type == call_type:
            mname = _get_method_name(n, lang)
            if mname == "format":
                args = _get_args_node(n, lang)
                if args is not None:
                    return _first_string_arg(args, lang)
    return None


def _top_statement(node):
    """Walk up to the top-level statement containing this node."""
    top = node
    while top.parent is not None:
        if top.parent.type in (
            "expression_statement", "local_variable_declaration",
            "val_definition", "var_definition",
            "module", "program", "compilation_unit",
            "class_body", "template_body", "block",
        ):
            break
        top = top.parent
    return top


# ---------------------------------------------------------------------------
# Hive SQL detection in string literals
# ---------------------------------------------------------------------------

_STORED_AS_RE = re.compile(r"\bSTORED\s+AS\s+(\w+)", re.IGNORECASE)
_USING_RE = re.compile(r"\bUSING\s+(\w+)", re.IGNORECASE)
_INSERT_OVERWRITE_RE = re.compile(r"\bINSERT\s+OVERWRITE\s+TABLE\s+(\w+)", re.IGNORECASE)
_INSERT_INTO_RE = re.compile(r"\bINSERT\s+INTO\s+(?:TABLE\s+)?(\w+)", re.IGNORECASE)


def _detect_hive_sql(string_text: str) -> list[tuple[str, str | None]]:
    """Detect Hive SQL patterns in a string literal.

    Returns list of (pattern_type, path_arg) tuples.
    """
    results = []
    m = _STORED_AS_RE.search(string_text)
    if m:
        fmt = m.group(1).lower()
        results.append((f"hive_create_{fmt}", None))

    m = _USING_RE.search(string_text)
    if m:
        fmt = m.group(1).lower()
        results.append((f"hive_create_{fmt}", None))

    m = _INSERT_OVERWRITE_RE.search(string_text)
    if m:
        table_name = m.group(1)
        results.append((f"hive_insert_{table_name}", table_name))

    m = _INSERT_INTO_RE.search(string_text)
    if m:
        table_name = m.group(1)
        results.append((f"hive_insert_{table_name}", table_name))

    return results


# ---------------------------------------------------------------------------
# String node types per language
# ---------------------------------------------------------------------------
_STRING_NODE_TYPES = {
    "python": {"string"},
    "java": {"string_literal"},
    "scala": {"string"},
}


# ---------------------------------------------------------------------------
# Per-language pattern detectors
# ---------------------------------------------------------------------------

def _detect_calls_in_tree(root, lang: str, file_path: Path, source_bytes: bytes) -> list[PatternMatch]:
    """Walk the AST and detect all I/O call patterns."""
    matches: list[PatternMatch] = []
    call_type = _call_node_type(lang)
    string_types = _STRING_NODE_TYPES[lang]

    for node in _walk(root):
        # --- Hive SQL in string literals ---
        if node.type in string_types:
            val = _extract_string(node, lang)
            if val:
                for pattern_type, path_arg in _detect_hive_sql(val):
                    top = _top_statement(node)
                    matches.append(PatternMatch(
                        file=file_path,
                        line=node.start_point[0] + 1,
                        pattern_type=pattern_type,
                        original_code=top.text.decode().strip(),
                        path_arg=path_arg,
                        end_line=top.end_point[0] + 1,
                    ))

        # --- Java "new" object creation (FileWriter, etc.) ---
        if lang == "java" and node.type == "object_creation_expression":
            type_node = node.child_by_field_name("type")
            if type_node is not None:
                type_name = type_node.text.decode()
                if type_name in ("FileWriter", "BufferedWriter", "PrintWriter"):
                    args = None
                    for child in node.children:
                        if child.type == "argument_list":
                            args = child
                            break
                    path_arg = _first_string_arg(args, lang) if args else None
                    top = _top_statement(node)
                    matches.append(PatternMatch(
                        file=file_path,
                        line=node.start_point[0] + 1,
                        pattern_type="stdlib_write_file",
                        original_code=top.text.decode().strip(),
                        path_arg=path_arg,
                        end_line=top.end_point[0] + 1,
                    ))

        if node.type != call_type:
            continue

        # Skip calls inside comments or string-only contexts
        if _is_inside_comment(node, lang):
            continue
        if _is_inside_string_only(node, lang):
            continue

        method_name = _get_method_name(node, lang)
        if method_name is None:
            continue

        obj_text = _get_object_text(node, lang) or ""
        args = _get_args_node(node, lang)
        first_str = _first_string_arg(args, lang) if args else None

        line = node.start_point[0] + 1
        top = _top_statement(node)
        end_line = top.end_point[0] + 1
        original_code = top.text.decode().strip()

        # --- pandas: pd.read_FORMAT / .to_FORMAT ---
        if lang == "python":
            if method_name.startswith("read_") and ("pd" in obj_text or "pandas" in obj_text):
                fmt = method_name[5:]  # strip "read_"
                matches.append(PatternMatch(
                    file=file_path, line=line,
                    pattern_type=f"pandas_read_{fmt}",
                    original_code=original_code,
                    path_arg=first_str,
                    end_line=end_line,
                    format=fmt,
                ))
                continue

            if method_name.startswith("to_") and method_name != "to_":
                fmt = method_name[3:]  # strip "to_"
                # Verify it looks like a pandas write (not a random .to_X call)
                # pandas to_ methods: to_parquet, to_csv, to_json, to_orc, to_excel, etc.
                if fmt in (
                    "parquet", "csv", "json", "orc", "excel", "feather", "hdf",
                    "stata", "pickle", "html", "xml", "latex", "markdown",
                    "clipboard", "sql", "gbq",
                ):
                    matches.append(PatternMatch(
                        file=file_path, line=line,
                        pattern_type=f"pandas_write_{fmt}",
                        original_code=original_code,
                        path_arg=first_str,
                        end_line=end_line,
                        format=fmt,
                    ))
                    continue

        # --- pyarrow: pq.read_table, pq.write_table, pq.ParquetFile, etc. ---
        if lang == "python":
            # pq.read_table / orc.read_table / po.read_table
            if method_name == "read_table":
                if any(x in obj_text for x in ("pq", "parquet")):
                    matches.append(PatternMatch(
                        file=file_path, line=line,
                        pattern_type="pyarrow_read_parquet",
                        original_code=original_code,
                        path_arg=first_str,
                        end_line=end_line,
                        format="parquet",
                    ))
                    continue
                if any(x in obj_text for x in ("orc", "po")):
                    matches.append(PatternMatch(
                        file=file_path, line=line,
                        pattern_type="pyarrow_read_orc",
                        original_code=original_code,
                        path_arg=first_str,
                        end_line=end_line,
                        format="orc",
                    ))
                    continue

            # pq.write_table / orc.write_table / po.write_table
            if method_name == "write_table":
                path_arg = _nth_positional_arg_string(args, lang, 1) if args else None
                if any(x in obj_text for x in ("pq", "parquet")):
                    matches.append(PatternMatch(
                        file=file_path, line=line,
                        pattern_type="pyarrow_write_parquet",
                        original_code=original_code,
                        path_arg=path_arg,
                        end_line=end_line,
                        format="parquet",
                    ))
                    continue
                if any(x in obj_text for x in ("orc", "po")):
                    matches.append(PatternMatch(
                        file=file_path, line=line,
                        pattern_type="pyarrow_write_orc",
                        original_code=original_code,
                        path_arg=path_arg,
                        end_line=end_line,
                        format="orc",
                    ))
                    continue

            # pq.ParquetFile / pq.ParquetDataset
            if method_name in ("ParquetFile", "ParquetDataset"):
                if any(x in obj_text for x in ("pq", "parquet")):
                    matches.append(PatternMatch(
                        file=file_path, line=line,
                        pattern_type="pyarrow_read_dataset",
                        original_code=original_code,
                        path_arg=first_str,
                        end_line=end_line,
                        format="parquet",
                    ))
                    continue

            # pa.dataset.dataset / pyarrow.dataset.dataset
            if method_name == "dataset" and any(x in obj_text for x in ("pa.dataset", "pyarrow.dataset", "dataset")):
                matches.append(PatternMatch(
                    file=file_path, line=line,
                    pattern_type="pyarrow_read_dataset",
                    original_code=original_code,
                    path_arg=first_str,
                    end_line=end_line,
                    format="dataset",
                ))
                continue

            # pa.dataset.write_dataset
            if method_name == "write_dataset" and any(x in obj_text for x in ("pa.dataset", "pyarrow.dataset", "dataset")):
                path_arg = _nth_positional_arg_string(args, lang, 1) if args else None
                matches.append(PatternMatch(
                    file=file_path, line=line,
                    pattern_type="pyarrow_write_dataset",
                    original_code=original_code,
                    path_arg=path_arg,
                    end_line=end_line,
                    format="dataset",
                ))
                continue

        # --- stdlib: csv.reader/writer, csv.DictReader/DictWriter ---
        if lang == "python":
            if method_name in ("writer", "DictWriter") and "csv" in obj_text:
                matches.append(PatternMatch(
                    file=file_path, line=line,
                    pattern_type="stdlib_write_csv",
                    original_code=original_code,
                    path_arg=first_str,
                    end_line=end_line,
                    format="csv",
                ))
                continue
            if method_name in ("reader", "DictReader") and "csv" in obj_text:
                matches.append(PatternMatch(
                    file=file_path, line=line,
                    pattern_type="stdlib_read_csv",
                    original_code=original_code,
                    path_arg=first_str,
                    end_line=end_line,
                    format="csv",
                ))
                continue

        # --- spark.table("name") ---
        if method_name == "table" and "spark" in obj_text:
            matches.append(PatternMatch(
                file=file_path, line=line,
                pattern_type="spark_read_table",
                original_code=original_code,
                path_arg=first_str,
                end_line=end_line,
                format="table",
            ))
            continue

        # --- saveAsTable("name") ---
        if method_name == "saveAsTable":
            matches.append(PatternMatch(
                file=file_path, line=line,
                pattern_type="hive_save_table",
                original_code=original_code,
                path_arg=first_str,
                end_line=end_line,
            ))
            continue

        # --- Spark batch/stream: .read/.write/.readStream/.writeStream chain ---
        # Detect terminal methods like .parquet(), .orc(), .csv(), .json(), .load(), .save(), .start()
        chain_kws = _find_chain_keywords(node)

        # Terminal format methods: parquet, orc, csv, json, text, avro, delta
        _FORMAT_TERMINALS = {
            "parquet", "orc", "csv", "json", "text", "avro", "delta",
        }

        if method_name in _FORMAT_TERMINALS and chain_kws:
            is_stream = "readStream" in chain_kws or "writeStream" in chain_kws
            if "writeStream" in chain_kws or ("write" in chain_kws and "writeStream" not in chain_kws and "readStream" not in chain_kws):
                direction = "stream_write" if "writeStream" in chain_kws else "write"
            elif "readStream" in chain_kws:
                direction = "stream_read"
            else:
                direction = "read"
            matches.append(PatternMatch(
                file=file_path, line=line,
                pattern_type=f"spark_{direction}_{method_name}",
                original_code=original_code,
                path_arg=first_str,
                end_line=end_line,
                format=method_name,
            ))
            continue

        # .load() / .save() / .start() with .format("X") in chain
        if method_name in ("load", "save", "start") and chain_kws:
            fmt = _find_format_in_chain(node, lang)
            if fmt:
                if method_name == "load":
                    if "readStream" in chain_kws:
                        direction = "stream_read"
                    else:
                        direction = "read"
                elif method_name in ("save", "start"):
                    if "writeStream" in chain_kws:
                        direction = "stream_write"
                    else:
                        direction = "write"
                else:
                    direction = "read"
                matches.append(PatternMatch(
                    file=file_path, line=line,
                    pattern_type=f"spark_{direction}_{fmt}",
                    original_code=original_code,
                    path_arg=first_str,
                    end_line=end_line,
                    format=fmt,
                ))
                continue

    return matches


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ts_detect(project_root: Path) -> list[PatternMatch]:
    """Detect all data I/O operations using tree-sitter AST analysis."""
    matches: list[PatternMatch] = []

    for src_file in sorted(project_root.rglob("*")):
        if not src_file.is_file():
            continue
        if src_file.suffix.lower() not in _EXTS:
            continue

        lang = language_for_file(src_file)
        if lang is None:
            continue

        try:
            source = src_file.read_bytes()
        except (OSError, UnicodeDecodeError):
            continue

        tree = parse(source, lang)
        matches.extend(_detect_calls_in_tree(tree.root_node, lang, src_file, source))

    return matches
