"""Intraprocedural constant folding for path-argument resolution.

Builds a per-file `ConstTable` keyed by (scope_key, name) so that the
detector can resolve `pd.read_parquet(MODULE_PATH)` to a literal value
when `MODULE_PATH = "s3://..."` is in scope.

Two-pass algorithm per language:
  Pass 1: collect assignments whose RHS is a single string literal.
  Pass 2: collect assignments whose RHS is `<literal_or_known> + <literal_or_known>`.

True-const policy: any reassignment in the same (scope, name) marks the
binding as unresolvable. Concat with 3+ operands or unsupported operators
is skipped with a reason.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .ts_parser import parse


@dataclass(frozen=True)
class ConstBinding:
    name: str
    value: str | None
    file: str
    line: int
    scope: str
    reason: str | None


@dataclass
class ConstTable:
    bindings: dict[tuple[str, str], ConstBinding] = field(default_factory=dict)

    def resolve(self, name: str, scope_hint: str | None = None) -> ConstBinding | None:
        if scope_hint is not None:
            binding = self.bindings.get((scope_hint, name))
            if binding is not None:
                return binding
        return self.bindings.get(("module", name))


def build_const_table(source: bytes, language: str, file: str) -> ConstTable:
    if not source:
        return ConstTable()
    tree = parse(source, language)
    collector = _COLLECTORS.get(language)
    if collector is None:
        return ConstTable()
    return collector(tree.root_node, source, file)


_COLLECTORS: dict[str, callable] = {}


def _python_extract_string_literal(node, source: bytes) -> str | None:
    """Return the string value of a Python `string` node, or None.

    Returns None for f-strings (contain `interpolation` children).
    """
    if node.type != "string":
        return None
    for child in node.children:
        if child.type == "interpolation":
            return None
    for child in node.children:
        if child.type == "string_content":
            return source[child.start_byte:child.end_byte].decode()
    # Empty string "" — no string_content child
    return ""


def _python_assign_target_name(assignment_node, source: bytes) -> str | None:
    left = assignment_node.child_by_field_name("left")
    if left is None:
        for child in assignment_node.children:
            if child.type == "identifier":
                return source[child.start_byte:child.end_byte].decode()
        return None
    if left.type == "identifier":
        return source[left.start_byte:left.end_byte].decode()
    return None


def _python_scope_key(node) -> str:
    """Walk up to the enclosing function_definition. Return its name or 'module'."""
    parent = node.parent
    while parent is not None:
        if parent.type == "function_definition":
            for child in parent.children:
                if child.type == "identifier":
                    return child.text.decode()
            return "anonymous"
        parent = parent.parent
    return "module"


def _python_iter_assignments(root):
    """Yield every assignment node in the file (pre-order)."""
    stack = [root]
    while stack:
        node = stack.pop()
        if node.type == "assignment":
            yield node
        for child in reversed(node.children):
            stack.append(child)


def _python_collector(root, source: bytes, file: str) -> ConstTable:
    table = ConstTable()
    seen: dict[tuple[str, str], int] = {}

    def _add_or_reassign(scope: str, name: str, value: str | None,
                         line: int, reason: str | None) -> None:
        key = (scope, name)
        if key in seen:
            table.bindings[key] = ConstBinding(
                name=name, value=None, file=file, line=line,
                scope=scope, reason="reassigned",
            )
            seen[key] += 1
            return
        seen[key] = 1
        table.bindings[key] = ConstBinding(
            name=name, value=value, file=file, line=line,
            scope=scope, reason=reason,
        )

    # Pass 1: pure literals
    for assign in _python_iter_assignments(root):
        name = _python_assign_target_name(assign, source)
        if name is None:
            continue
        right = assign.child_by_field_name("right")
        if right is None:
            continue
        literal = _python_extract_string_literal(right, source)
        if literal is None:
            continue
        scope = _python_scope_key(assign)
        line = assign.start_point[0] + 1
        _add_or_reassign(scope, name, literal, line, reason=None)

    # Pass 2: 1-level concat `+`
    for assign in _python_iter_assignments(root):
        name = _python_assign_target_name(assign, source)
        if name is None:
            continue
        right = assign.child_by_field_name("right")
        if right is None or right.type != "binary_operator":
            continue
        scope = _python_scope_key(assign)
        line = assign.start_point[0] + 1

        # Check the operator is `+`
        op_node = None
        for child in right.children:
            txt = source[child.start_byte:child.end_byte]
            if txt == b"+":
                op_node = child
                break
        if op_node is None:
            continue

        left_n = right.child_by_field_name("left")
        right_n = right.child_by_field_name("right")
        if left_n is None or right_n is None:
            continue

        def _resolve_operand(operand) -> str | None:
            lit = _python_extract_string_literal(operand, source)
            if lit is not None:
                return lit
            if operand.type == "identifier":
                op_name = source[operand.start_byte:operand.end_byte].decode()
                lookup = table.resolve(op_name, scope_hint=scope)
                if lookup is not None and lookup.value is not None:
                    return lookup.value
            return None

        l_val = _resolve_operand(left_n)
        r_val = _resolve_operand(right_n)
        if l_val is None or r_val is None:
            _add_or_reassign(scope, name, value=None, line=line,
                             reason="dependency_unresolved")
            continue
        _add_or_reassign(scope, name, value=l_val + r_val, line=line,
                         reason=None)

    return table


_COLLECTORS["python"] = _python_collector


def _java_extract_string_literal(node, source: bytes) -> str | None:
    """Return the string value of a Java `string_literal` node, or None."""
    if node.type != "string_literal":
        return None
    for child in node.children:
        if child.type == "string_fragment":
            return source[child.start_byte:child.end_byte].decode()
    return ""


def _java_has_modifier(decl_node, modifier_name: str) -> bool:
    """Check if the declaration has the named modifier (e.g. 'static', 'final')."""
    for child in decl_node.children:
        if child.type == "modifiers":
            for mod in child.children:
                if mod.type == modifier_name:
                    return True
    return False


def _java_scope_key(node, source: bytes) -> str:
    """Walk up to enclosing method_declaration. Return its name or 'module'."""
    parent = node.parent
    while parent is not None:
        if parent.type == "method_declaration":
            name_n = parent.child_by_field_name("name")
            if name_n is not None:
                return source[name_n.start_byte:name_n.end_byte].decode()
            return "anonymous"
        parent = parent.parent
    return "module"


def _java_iter_declarations(root):
    """Yield every field_declaration and local_variable_declaration node."""
    stack = [root]
    while stack:
        node = stack.pop()
        if node.type in ("field_declaration", "local_variable_declaration"):
            yield node
        for child in reversed(node.children):
            stack.append(child)


def _java_collector(root, source: bytes, file: str) -> ConstTable:
    table = ConstTable()
    seen: dict[tuple[str, str], int] = {}

    def _add_or_reassign(scope: str, name: str, value: str | None,
                         line: int, reason: str | None) -> None:
        key = (scope, name)
        if key in seen:
            table.bindings[key] = ConstBinding(
                name=name, value=None, file=file, line=line,
                scope=scope, reason="reassigned",
            )
            seen[key] += 1
            return
        seen[key] = 1
        table.bindings[key] = ConstBinding(
            name=name, value=value, file=file, line=line,
            scope=scope, reason=reason,
        )

    def _is_eligible(decl) -> bool:
        return _java_has_modifier(decl, "final")

    def _iter_declarators(decl):
        for child in decl.children:
            if child.type == "variable_declarator":
                yield child

    # Pass 1: pure literals
    for decl in _java_iter_declarations(root):
        if not _is_eligible(decl):
            continue
        scope = _java_scope_key(decl, source)
        for declarator in _iter_declarators(decl):
            name_n = declarator.child_by_field_name("name")
            value_n = declarator.child_by_field_name("value")
            if name_n is None or value_n is None:
                continue
            name = source[name_n.start_byte:name_n.end_byte].decode()
            literal = _java_extract_string_literal(value_n, source)
            if literal is None:
                continue
            line = declarator.start_point[0] + 1
            _add_or_reassign(scope, name, literal, line, reason=None)

    # Pass 2: 1-level concat
    for decl in _java_iter_declarations(root):
        if not _is_eligible(decl):
            continue
        scope = _java_scope_key(decl, source)
        for declarator in _iter_declarators(decl):
            name_n = declarator.child_by_field_name("name")
            value_n = declarator.child_by_field_name("value")
            if name_n is None or value_n is None:
                continue
            if value_n.type != "binary_expression":
                continue
            name = source[name_n.start_byte:name_n.end_byte].decode()
            line = declarator.start_point[0] + 1
            op_n = value_n.child_by_field_name("operator")
            if op_n is None or source[op_n.start_byte:op_n.end_byte] != b"+":
                continue
            left_n = value_n.child_by_field_name("left")
            right_n = value_n.child_by_field_name("right")
            if left_n is None or right_n is None:
                continue

            def _resolve_operand(operand) -> str | None:
                lit = _java_extract_string_literal(operand, source)
                if lit is not None:
                    return lit
                if operand.type == "identifier":
                    op_name = source[operand.start_byte:operand.end_byte].decode()
                    lookup = table.resolve(op_name, scope_hint=scope)
                    if lookup is not None and lookup.value is not None:
                        return lookup.value
                return None

            l_val = _resolve_operand(left_n)
            r_val = _resolve_operand(right_n)
            if l_val is None or r_val is None:
                _add_or_reassign(scope, name, value=None, line=line,
                                 reason="dependency_unresolved")
                continue
            _add_or_reassign(scope, name, value=l_val + r_val, line=line,
                             reason=None)

    return table


_COLLECTORS["java"] = _java_collector
