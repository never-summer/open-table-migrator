"""Java-only tree-sitter wrapper, local to data_lineage.

Kept separate from open_table_migrator's ts_parser by design: this skill
only needs Java, and we want the two skills to evolve independently.
"""
import tree_sitter_java as tsjava
from tree_sitter import Language, Parser, Tree

_PARSER: Parser | None = None
_LANGUAGE: Language | None = None


def _get_parser() -> Parser:
    global _PARSER, _LANGUAGE
    if _PARSER is None:
        _LANGUAGE = Language(tsjava.language())
        _PARSER = Parser(_LANGUAGE)
    return _PARSER


def parse_java(source: bytes) -> Tree:
    return _get_parser().parse(source)


def java_language() -> Language:
    _get_parser()
    assert _LANGUAGE is not None
    return _LANGUAGE
