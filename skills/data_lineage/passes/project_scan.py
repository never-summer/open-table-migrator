"""Pass 1 — scan project for class shapes / DTO definitions / getters.

Output is a SymbolTable keyed by FQN. Used by sql_extract for jOOQ-record
dispatch and by java_dfa for getter→field resolution.
"""
from dataclasses import dataclass, field
from pathlib import Path

from skills.data_lineage.extractors.dto import Dto, extract as extract_dtos


_IGNORED_DIRS = {"generated", "build", "target", ".gradle", "node_modules", ".git"}


@dataclass
class SymbolTable:
    classes: dict[str, Dto] = field(default_factory=dict)


def run(project_root: Path) -> SymbolTable:
    table = SymbolTable()
    for path in project_root.rglob("*.java"):
        if any(part in _IGNORED_DIRS for part in path.parts):
            continue
        rel = str(path.relative_to(project_root))
        for dto in extract_dtos(path.read_bytes(), file=rel):
            table.classes[dto.fqn] = dto
    return table
