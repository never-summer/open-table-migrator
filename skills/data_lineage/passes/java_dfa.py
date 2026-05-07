"""Pass 4 — heuristic Java data-flow tracking."""
from pathlib import Path

from skills.data_lineage.model import Edge

from .project_scan import SymbolTable
from .sql_extract import SqlUnit


def run(project_root: Path, symbols: SymbolTable, units: list[SqlUnit]) -> list[Edge]:
    return []
