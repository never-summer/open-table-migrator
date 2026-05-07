"""Pass 1 — collect classes, fields, methods, DTO shapes via tree-sitter."""
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class JavaClass:
    fqn: str
    file: str
    fields: tuple[str, ...]
    getter_to_field: dict[str, str]
    annotations: tuple[str, ...]


@dataclass
class SymbolTable:
    classes: dict[str, JavaClass] = field(default_factory=dict)


def run(project_root: Path) -> SymbolTable:
    return SymbolTable()
