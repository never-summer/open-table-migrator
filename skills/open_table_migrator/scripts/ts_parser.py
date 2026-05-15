# skills/open_table_migrator/ts_parser.py
"""Thin wrapper around tree-sitter: parse source code into AST trees.

Caches Language and Parser objects per language (created once per process).
"""
import tree_sitter_java as tsjava
import tree_sitter_python as tspython
import tree_sitter_scala as tsscala
from pathlib import Path
from tree_sitter import Language, Parser, Tree

_LANGUAGES: dict[str, Language] = {}
_PARSERS: dict[str, Parser] = {}

_LANG_MODULES = {
    "python": tspython,
    "java": tsjava,
    "scala": tsscala,
}

_EXT_TO_LANG = {
    ".py": "python",
    ".java": "java",
    ".scala": "scala",
}


def _get_parser(lang: str) -> Parser:
    if lang not in _PARSERS:
        mod = _LANG_MODULES[lang]
        language = Language(mod.language())
        _LANGUAGES[lang] = language
        _PARSERS[lang] = Parser(language)
    return _PARSERS[lang]


def get_language(lang: str) -> Language:
    """Return the tree-sitter Language object, initializing if needed."""
    if lang not in _LANGUAGES:
        _get_parser(lang)
    return _LANGUAGES[lang]


def parse(source: bytes, language: str) -> Tree:
    """Parse source bytes into a tree-sitter AST."""
    return _get_parser(language).parse(source)


def language_for_file(path: Path) -> str | None:
    """Return language name by file extension, or None if unsupported."""
    return _EXT_TO_LANG.get(path.suffix.lower())
