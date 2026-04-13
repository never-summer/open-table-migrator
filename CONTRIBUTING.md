# Contributing to open-table-migrator

Thank you for your interest in contributing!

## Getting Started

```bash
git clone https://github.com/YOUR_USERNAME/open-table-migrator.git
cd open-table-migrator
pip install -e ".[test]"
```

## Running Tests

```bash
PYTHONPATH=. pytest tests/ --ignore=tests/fixtures -v
```

Test fixtures in `tests/fixtures/` are sample projects used as input data.

## Project Structure

- `skills/open_table_migrator/` — core skill code (detector, analyzer, worklist, CLI)
- `tests/` — test suite (179 tests)
- `.claude/agents/` — Claude Code subagent definition

## How to Contribute

### Bug Reports

Open an issue with:
- What you expected
- What happened instead
- Minimal reproduction (a small `.py`/`.java`/`.scala` snippet the detector mishandles)

### Adding a New Format Detector

The tree-sitter detector (`ts_detector.py`) extracts formats dynamically from AST nodes. If a format is missed:

1. Add a test case in `tests/test_ts_queries.py`
2. Update detection logic in `ts_detector.py`
3. Run the full test suite

### Adding a New Target Format (beyond Iceberg)

The architecture supports any-to-any migration. To add a new target (e.g. Delta, Paimon):

1. Extend `worklist.py` to emit target-specific rewrite hints
2. Update `SKILL.md` with conversion reference tables the LLM rewriter follows

### Adding a New Language

1. Add the tree-sitter grammar to `pyproject.toml` dependencies
2. Register it in `ts_parser.py`
3. Add language-specific detection in `ts_detector.py`
4. Add test fixtures and tests

## Code Style

- No comments unless the WHY is non-obvious
- No docstrings longer than one line
- Test names: `test_<what>_<scenario>`
- Pattern types follow `{runtime}_{direction}_{format}` taxonomy

## Pull Requests

1. Fork and create a feature branch
2. Add tests for new functionality
3. Ensure all 179+ tests pass
4. Submit a PR with a clear description

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
