# Intraprocedural Constant Folding — Design

**Date:** 2026-05-08
**Status:** Approved for implementation planning
**Branch:** `feat/const-folding`
**Scope ref:** improvement 1.1 from the migrator roadmap

## Goal

Make `open_table_migrator` resolve string-literal constants when a detected I/O call uses a variable instead of a literal path. Today the detector requires the path argument to be a literal in place; in real projects, paths are typically named constants at module/class level or local variables in a function. This causes path_arg to be `None` and a `TODO(iceberg): could not resolve target table` comment in the rewritten code. Resolving simple cases (`PATH = "s3://..."` then `pd.read_parquet(PATH)`) covers an estimated 50–70% of the unresolved sites in typical projects.

## Non-goals

- No cross-module imports. `from config import EVENTS_PATH` is not followed.
- No f-string / format-string / `%`-format / `.format()` resolution. Only `+` concatenation.
- No recursive concat chains. `A = B + "x"` resolves only if `B` is in the const table with a literal value (one level of indirection).
- No dataflow analysis. Reassignment of a name within a scope marks it as unresolvable, never approximates "value at use-site."
- No constructor-body analysis for Java instance `final` fields. Only inline initializers are resolved.
- No interpretation of computed values (`os.getenv`, `Path` constructors, etc.).

## Scope

Three languages, two scope levels each:

| Language | Module/class scope | Function/method scope |
|---|---|---|
| Python | `MODULE_CONST = "..."` (top-level) | `def f(): path = "..."` |
| Java | `static final String X = "..."` or `final String X = "..."` (inline-initialized field) | `final String x = "..."` (local) |
| Scala | `val X = "..."` (object/class top-level) | `def f(): { val x = "..." }` |

Class-level constants in Python (`class C: X = "..."`) are out of scope — uncommon in data-job code.

## Architecture

New module `skills/open_table_migrator/scope.py`. Isolated, depends only on `ts_parser` from the same package.

### Public API

```python
@dataclass(frozen=True)
class ConstBinding:
    name: str
    value: str | None        # resolved literal, or None if unresolvable
    file: str
    line: int
    scope: str               # "module" or fully-qualified function/method name
    reason: str | None       # populated when value is None


@dataclass
class ConstTable:
    bindings: dict[tuple[str, str], ConstBinding]  # (scope_key, name) → binding

    def resolve(self, name: str, scope_hint: str | None = None) -> ConstBinding | None:
        """Try scope_hint first (local), then 'module'. None if not found."""


def build_const_table(source: bytes, language: str, file: str) -> ConstTable:
    """Two-pass scan: pass 1 = pure literals, pass 2 = 1-level concats."""
```

### Two-pass algorithm

Const-table construction is **two passes over the AST per file**:

1. **Pass 1 — literals.** Walk the AST, collect every assignment whose RHS is a single `string_literal`. Populate the table with `(scope_key, name) → ConstBinding(value=literal_text, ...)`. Reassignment within the same `(scope_key, name)` marks the binding as `value=None, reason="reassigned"`.

2. **Pass 2 — one-level concats.** Walk the AST again. For each assignment whose RHS is a `binary_operator/binary_expression` with `+`, where both sides are either `string_literal` or `identifier` resolvable through pass-1 bindings, compute the concatenated value. If any side cannot be resolved, mark as `value=None, reason="dependency_unresolved"`. Reassignment still wins: if a name was already in the table from pass 1, the pass-2 binding does not overwrite — instead it's treated as a reassignment.

The two-pass design avoids the topological-sort or fixpoint-iteration complexity of full recursion. Skipping deeper chains (`A = B + "x"` where `B = C + "y"`) is explicit: pass 2 sees `B` was set in pass 2 (not pass 1), so it treats it as unavailable.

### Integration with the existing pipeline

In `ts_detector.detect_all_io`, per `.py`/`.java`/`.scala` file:

```
1. parse via ts_parser                       (existing)
2. build_const_table(source, language, file)   ← NEW (this design)
3. walk AST, detect I/O call sites             (existing)
4. for each match, when extracting path_arg:
     - if path_arg is a string literal → as before
     - if path_arg is an identifier:
         binding = const_table.resolve(name, scope_hint=current_function_qualname)
         if binding and binding.value is not None:
             match.path_arg = binding.value
             match.attrs["resolved_from"] = f"{name}@{binding.file}:{binding.line}"
         elif binding and binding.value is None:
             match.path_arg = None
             match.attrs["skipped_reason"] = binding.reason
         else:
             match.path_arg = None  # unchanged
```

`extract_path_arg` in `extract.py` receives two new optional parameters: `const_table` and `scope_hint`. When path is a literal, behavior is identical to today.

## Per-language behavior

### Python

| Pattern | Behavior |
|---|---|
| `X = "literal"` at module top-level | Resolve (`scope="module"`) |
| `X: str = "literal"` at module top-level | Resolve (annotation is ignored) |
| `def f(): x = "literal"` | Resolve (`scope="f"`) |
| `def f(): x = "literal"; x = "other"` | Mark unresolvable, `reason="reassigned"` |
| `X = f"s3://{bucket}"` | Skip — `reason="f_string"` |
| `X = "a" + "b"` (two literals) | Resolve via pass 2 |
| `X = BASE + "/events"` (BASE in pass 1) | Resolve via pass 2 |
| `X = "a" + os.getenv("X")` | Skip — `reason="non_literal_operand"` |

AST shapes:
- Module assignment: `module > expression_statement > assignment > [identifier, "=", expression]`
- Function-local: `function_definition > block > expression_statement > assignment > ...`
- Concat: `expression` is `binary_operator` with `operator "+"` and children `[string|identifier, string|identifier]`

### Java

| Pattern | Behavior |
|---|---|
| `private static final String X = "literal"` | Resolve (`scope=class_name`) |
| `private final String X = "literal"` (inline init) | Resolve (`scope=class_name`) |
| `private final String X;` + constructor sets it | Skip — `reason="non_inline_init"` |
| `String X = "literal"` (no `final`) | Skip — `reason="not_final"` |
| `void f(): final String x = "literal"` | Resolve (`scope=class_name.f`) |
| `void f(): String x = "literal"` | Skip — `reason="not_final"` |
| `static final String X = BASE + "/events"` | Resolve via pass 2 |

AST shapes:
- Field declaration: `class_body > field_declaration > [modifiers, type, variable_declarator]`
- Local variable: `block > local_variable_declaration > [modifiers, type, variable_declarator]`
- Modifiers check: walk children for `static` and `final` tokens
- Concat: `binary_expression` with `operator "+"`

### Scala

| Pattern | Behavior |
|---|---|
| `object X { val Y = "literal" }` | Resolve (`scope="X"`) |
| `class X { val Y = "literal" }` | Resolve (`scope="X"`) |
| `var Y = "literal"` | Skip — `reason="not_immutable"` |
| `def f() = { val y = "literal" }` | Resolve (`scope=enclosing.f`) |
| `val Y = BASE + "/events"` (BASE in pass 1) | Resolve via pass 2 |

AST shapes:
- Object/class member: `template_body > val_definition > [pattern, type?, "=", expression]`
- Method-local: `function_definition > block > val_definition > ...`
- Concat: `infix_expression` with operator `+`

### Cross-cutting rules

| Case | Behavior |
|---|---|
| Reassignment in same scope | `value=None, reason="reassigned"` |
| Shadowing (local hides module) | Local wins via `resolve(name, scope_hint=local)` |
| Concat `"a" + "b"` | Resolve in pass 2 |
| Concat literal + identifier (identifier has value) | Resolve in pass 2 |
| Concat literal + identifier (identifier unresolved) | `value=None, reason="dependency_unresolved"` |
| Concat 3+ operands (`A + B + C`) | Skip — `reason="multi_operand_concat"` |
| Multiplication, f-string, `.format`, `%` | Skip — `reason="unsupported_operator"` |
| Cross-file imports | Not attempted |

## Worklist effect

The detected match's `path_arg` is mutated as if the literal was at the call site. `attrs.resolved_from = "NAME@file:line"` records the source for audit. Downstream code (mapping resolver, transformers, worklist writer) does not change. The resolved string flows through unchanged.

If resolution fails (binding exists but `value=None`), `match.attrs["skipped_reason"]` records why. The detector emits the same TODO comment as before, plus the reason in the attrs.

## Testing strategy

### Level 1 — `tests/test_scope.py` (~30 cases)

**Python (8):** module literal, module concat both literals, module concat using another const, function-local literal, function-local shadowing module-level, reassignment, f-string skip, non-literal RHS.

**Java (8):** class static-final literal, class static-final concat, class non-static final inline, class non-static final constructor-only (skip), method-local final, method-local non-final (skip), reassignment in method, concat across two static final fields.

**Scala (5):** object val, object val concat, var skip, def-local val, val concat.

**Cross-cutting (5+):** dependency depth-2 chain stops, circular dependency safe (no infinite loop), forward reference (Python use before declare), empty file empty table, file with no consts empty table.

Each test builds source bytes inline via `textwrap.dedent`, calls `build_const_table`, asserts on `bindings` content. No filesystem, no AST mocking.

### Level 2 — `tests/test_detector.py` extension (~6 cases)

End-to-end: source with const declaration + I/O call → detector resolves identifier → `match.path_arg` is the resolved string and `attrs.resolved_from` is populated.

Covers all three languages and the reassignment-skip case.

### Level 3 — `tests/test_multi_table.py` + `tests/test_transformer_pandas.py` (1 case each)

Multi-table: mapping glob matches the resolved path (proves end-to-end wiring through the resolver).
Pandas transformer: rewrite uses the resolved path correctly.

### Out of scope for tests

Performance benchmarks, fuzz-tests on syntactic variants, cross-language fixtures in one project.

## Risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Function qualname construction differs by AST quirk (anonymous functions, lambdas) | medium | Fallback: if no enclosing named function, scope key is `"module"` |
| Reassignment detection misses if the second assignment is conditional (`if cond: X = ...`) | low | The simple "more than one assignment node found" check is conservative — false positives skip rather than mis-resolve |
| Java instance `final` field with constructor-also-assigns might appear resolved when it should not | low | We only mark as resolved if the field has an inline `= ...` initializer; constructor body is not parsed |
| Performance on large files (1000+ const declarations) | low | Two linear passes; even at 10k declarations, runs in milliseconds. No optimization until proven a problem |
| AST shape differs between tree-sitter grammar versions | low | Pinned in `pyproject.toml`; tests would catch breakage |

## Estimated size

- Production: ~250 LOC (`scope.py`) + ~50 LOC integration (extract.py, ts_detector.py)
- Tests: ~280 LOC across `test_scope.py`, `test_detector.py` extensions, `test_multi_table.py`, `test_transformer_pandas.py`
- Documentation: ~30 lines in SKILL.md "Const folding" section
- Total: ~610 LOC. Three to four implementation sessions.

## Open questions deferred to implementation

These do not block the design:

- Exact format of `attrs.resolved_from` string (`"NAME@file:line"` is reasonable; alternatives like dict are possible).
- Behavior on string literals with embedded escape sequences (`"s3://x\\n"` — the literal includes a literal backslash-n; we preserve `string_literal.text` verbatim, which is correct).
- Whether the const-table is cached across detector invocations (initially no — built fresh per file per run, simple to reason about).
