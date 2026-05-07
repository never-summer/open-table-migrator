"""Pass 2 — fan out SQL extractors across all .java files in the project."""
from pathlib import Path

from skills.data_lineage.extractors import jdbc_template, jooq, spring_data

from .project_scan import SymbolTable
from .sql_unit import SqlUnit  # re-exported for consumers that import from here

__all__ = ["SqlUnit", "run"]


_IGNORED_DIRS = {"generated", "build", "target", ".gradle", "node_modules", ".git"}


def run(project_root: Path, symbols: SymbolTable) -> list[SqlUnit]:
    out: list[SqlUnit] = []
    for path in _iter_java_files(project_root):
        rel = str(path.relative_to(project_root))
        source = path.read_bytes()
        out.extend(jdbc_template.extract(source, file=rel))
        out.extend(spring_data.extract(source, file=rel))
        out.extend(jooq.extract(source, file=rel))
    return out


def _iter_java_files(root: Path):
    for path in root.rglob("*.java"):
        if any(part in _IGNORED_DIRS for part in path.parts):
            continue
        yield path
