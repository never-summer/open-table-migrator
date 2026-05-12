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
