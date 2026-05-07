# Data Lineage Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a sibling skill `skills/data_lineage/` that produces column-level data lineage graphs for Spring Boot / jOOQ / JdbcTemplate / Spring Kafka / Spring Web (RestClient) projects. Output: text report + `lineage-graph.json` + `lineage.mmd`.

**Architecture:** Six-pass pipeline. tree-sitter scans `.java` for symbols, SQL literals, jOOQ DSL chains, Kafka/REST sites; sqlglot parses extracted SQL into column edges; a heuristic Java data-flow pass connects sources to sinks via name conventions; a graph-build pass merges everything into a frozen `Graph`; three renderers emit text/JSON/Mermaid.

**Tech Stack:** Python 3.11+, `tree-sitter` (Java), `tree-sitter-java`, `sqlglot` (Oracle dialect), `pytest`. Pure Python — no JVM, no `dot` binary.

**Spec:** [docs/superpowers/specs/2026-05-07-data-lineage-design.md](../specs/2026-05-07-data-lineage-design.md)

---

## File Structure

New files to be created:

```
skills/data_lineage/
├── __init__.py
├── __main__.py
├── SKILL.md
├── cli.py
├── model.py                  # Node, Edge, Evidence, Graph, Unresolved
├── ts_parser.py              # tree-sitter Java wrapper (local copy, not shared with migrator)
├── filters.py                # --only-table / --only-topic / --max-depth
├── passes/
│   ├── __init__.py
│   ├── project_scan.py
│   ├── sql_extract.py
│   ├── sql_parse.py
│   ├── java_dfa.py
│   └── graph_build.py
├── extractors/
│   ├── __init__.py
│   ├── jooq.py
│   ├── jdbc_template.py
│   ├── spring_data.py
│   ├── kafka.py
│   ├── rest.py
│   └── dto.py
└── renderers/
    ├── __init__.py
    ├── text.py
    ├── json.py
    └── mermaid.py

tests/data_lineage/
├── __init__.py
├── conftest.py
├── test_model.py
├── test_ts_parser.py
├── test_filters.py
├── test_extractors/
│   ├── __init__.py
│   ├── test_jooq.py
│   ├── test_jdbc_template.py
│   ├── test_spring_data.py
│   ├── test_kafka.py
│   ├── test_rest.py
│   └── test_dto.py
├── test_passes/
│   ├── __init__.py
│   ├── test_project_scan.py
│   ├── test_sql_extract.py
│   ├── test_sql_parse.py
│   ├── test_java_dfa.py
│   └── test_graph_build.py
├── test_renderers/
│   ├── __init__.py
│   ├── test_text.py
│   ├── test_json.py
│   └── test_mermaid.py
├── test_integration.py
└── fixtures/
    └── synthetic-spring-app/   (built incrementally during phases 1–3)

.claude/agents/
└── data-lineage.md          # subagent (final task)
```

Files modified:
- `pyproject.toml` — add `sqlglot` dependency.
- `README.md` — add link to the new skill.

---

# Phase 0 — Skeleton

Goal: empty pipeline runs end-to-end on any path, emits empty artifacts. All renderers and the CLI plumbed.

## Task 0.1: Create directory layout and dependencies

**Files:**
- Create: `skills/data_lineage/__init__.py`
- Create: `skills/data_lineage/__main__.py`
- Create: `skills/data_lineage/passes/__init__.py`
- Create: `skills/data_lineage/extractors/__init__.py`
- Create: `skills/data_lineage/renderers/__init__.py`
- Create: `tests/data_lineage/__init__.py`
- Create: `tests/data_lineage/test_extractors/__init__.py`
- Create: `tests/data_lineage/test_passes/__init__.py`
- Create: `tests/data_lineage/test_renderers/__init__.py`
- Modify: `pyproject.toml` — add `sqlglot>=25.0.0`

- [ ] **Step 1: Create empty `__init__.py` files**

```bash
mkdir -p skills/data_lineage/passes skills/data_lineage/extractors skills/data_lineage/renderers
mkdir -p tests/data_lineage/test_extractors tests/data_lineage/test_passes tests/data_lineage/test_renderers
touch skills/data_lineage/__init__.py
touch skills/data_lineage/passes/__init__.py
touch skills/data_lineage/extractors/__init__.py
touch skills/data_lineage/renderers/__init__.py
touch tests/data_lineage/__init__.py
touch tests/data_lineage/test_extractors/__init__.py
touch tests/data_lineage/test_passes/__init__.py
touch tests/data_lineage/test_renderers/__init__.py
```

- [ ] **Step 2: Create `__main__.py` shim**

`skills/data_lineage/__main__.py`:
```python
from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 3: Add sqlglot to pyproject.toml**

Edit `pyproject.toml` `dependencies` array. Insert `"sqlglot>=25.0.0"` after the existing `tree-sitter-scala` entry.

- [ ] **Step 4: Install the new dependency**

Run: `pip install -e ".[test]"`
Expected: `sqlglot` installed, no errors.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage tests/data_lineage pyproject.toml
git commit -m "feat(data-lineage): scaffold skill module + sqlglot dep"
```

## Task 0.2: Data model

**Files:**
- Create: `skills/data_lineage/model.py`
- Create: `tests/data_lineage/test_model.py`

- [ ] **Step 1: Write the failing test**

`tests/data_lineage/test_model.py`:
```python
import json
from dataclasses import asdict
from skills.data_lineage.model import Node, Edge, Evidence, Graph, Unresolved


def test_node_construct_minimal():
    n = Node(id="db.users.email", kind="db.column", name="users.email",
             parent_id="db.users", attrs={})
    assert n.id == "db.users.email"
    assert n.parent_id == "db.users"


def test_edge_with_evidence():
    ev = Evidence(file="Foo.java", line=10, pattern="jooq_select",
                  snippet="dsl.select(USERS.EMAIL)")
    e = Edge(src_id="db.users.email", dst_id="code.var.email",
             kind="read", evidence=[ev], confidence="high")
    assert e.evidence[0].pattern == "jooq_select"


def test_graph_serializes_to_json_via_asdict():
    n = Node(id="db.users", kind="db.table", name="users", parent_id=None, attrs={})
    g = Graph(nodes={"db.users": n}, edges=[], unresolved=[])
    blob = json.dumps(asdict(g))
    assert "db.users" in blob


def test_unresolved_record():
    u = Unresolved(file="Foo.java", line=42,
                   reason="string concat sql", context="sb.append(...)")
    assert u.reason == "string concat sql"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_model.py -v`
Expected: `ModuleNotFoundError: No module named 'skills.data_lineage.model'`

- [ ] **Step 3: Write `model.py`**

`skills/data_lineage/model.py`:
```python
"""Read-only data classes for the lineage graph.

No logic lives here — only shapes. Every pass returns frozen instances
of these types so each stage is independently testable and serializable.
"""
from dataclasses import dataclass, field
from typing import Literal


NodeKind = Literal[
    "db.table",
    "db.column",
    "kafka.topic",
    "kafka.field",
    "http.endpoint",
    "http.field",
    "code.method",
]

EdgeKind = Literal["read", "write", "transform"]
Confidence = Literal["high", "medium", "low"]


@dataclass(frozen=True)
class Evidence:
    file: str
    line: int
    pattern: str
    snippet: str


@dataclass(frozen=True)
class Node:
    id: str
    kind: NodeKind
    name: str
    parent_id: str | None
    attrs: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Edge:
    src_id: str
    dst_id: str
    kind: EdgeKind
    evidence: tuple[Evidence, ...]
    confidence: Confidence


@dataclass(frozen=True)
class Unresolved:
    file: str
    line: int
    reason: str
    context: str


@dataclass
class Graph:
    nodes: dict[str, Node] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)
    unresolved: list[Unresolved] = field(default_factory=list)

    def has_edge(self, src_id: str, dst_id: str) -> bool:
        return any(e.src_id == src_id and e.dst_id == dst_id for e in self.edges)
```

Note: `Edge.evidence` is a tuple (not list) because `Edge` is frozen — lists aren't hashable but tuples are, and frozen dataclasses must be hashable to live in sets.

- [ ] **Step 4: Adjust the test to use a tuple**

In the same test file, change `evidence=[ev]` to `evidence=(ev,)` in `test_edge_with_evidence`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_model.py -v`
Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add skills/data_lineage/model.py tests/data_lineage/test_model.py
git commit -m "feat(data-lineage): data model (Node, Edge, Evidence, Graph, Unresolved)"
```

## Task 0.3: tree-sitter Java wrapper

**Files:**
- Create: `skills/data_lineage/ts_parser.py`
- Create: `tests/data_lineage/test_ts_parser.py`

- [ ] **Step 1: Write the failing test**

`tests/data_lineage/test_ts_parser.py`:
```python
from skills.data_lineage.ts_parser import parse_java


def test_parse_java_returns_tree_with_root():
    tree = parse_java(b"class Foo { int x; }")
    assert tree.root_node.type == "program"
    assert tree.root_node.start_byte == 0


def test_parse_java_caches_parser_across_calls():
    t1 = parse_java(b"class A {}")
    t2 = parse_java(b"class B {}")
    assert t1.root_node.type == "program"
    assert t2.root_node.type == "program"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_ts_parser.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Write `ts_parser.py`**

`skills/data_lineage/ts_parser.py`:
```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_ts_parser.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/ts_parser.py tests/data_lineage/test_ts_parser.py
git commit -m "feat(data-lineage): tree-sitter Java wrapper"
```

## Task 0.4: Empty passes returning a stub Graph

**Files:**
- Create: `skills/data_lineage/passes/project_scan.py`
- Create: `skills/data_lineage/passes/sql_extract.py`
- Create: `skills/data_lineage/passes/sql_parse.py`
- Create: `skills/data_lineage/passes/java_dfa.py`
- Create: `skills/data_lineage/passes/graph_build.py`

- [ ] **Step 1: Stub the five passes**

Each pass file is the same shape: a `run(...)` function that returns the next stage. For Phase 0 they return empty values.

`skills/data_lineage/passes/project_scan.py`:
```python
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
```

`skills/data_lineage/passes/sql_extract.py`:
```python
"""Pass 2 — find SQL literals (@Query, JdbcTemplate) and jOOQ DSL chains."""
from dataclasses import dataclass
from pathlib import Path

from .project_scan import SymbolTable


@dataclass(frozen=True)
class SqlUnit:
    file: str
    line: int
    kind: str            # "jdbc_query", "jpa_query", "jooq_select", ...
    sql: str | None      # raw SQL string, None for jOOQ DSL
    jooq_columns: tuple[str, ...] = ()
    jooq_tables: tuple[str, ...] = ()


def run(project_root: Path, symbols: SymbolTable) -> list[SqlUnit]:
    return []
```

`skills/data_lineage/passes/sql_parse.py`:
```python
"""Pass 3 — sqlglot parses each SqlUnit, emits column-level edges."""
from skills.data_lineage.model import Edge

from .sql_extract import SqlUnit


def run(units: list[SqlUnit]) -> list[Edge]:
    return []
```

`skills/data_lineage/passes/java_dfa.py`:
```python
"""Pass 4 — heuristic Java data-flow tracking."""
from pathlib import Path

from skills.data_lineage.model import Edge

from .project_scan import SymbolTable
from .sql_extract import SqlUnit


def run(project_root: Path, symbols: SymbolTable, units: list[SqlUnit]) -> list[Edge]:
    return []
```

`skills/data_lineage/passes/graph_build.py`:
```python
"""Pass 5 — merge all edges into a unified Graph."""
from skills.data_lineage.model import Edge, Graph, Unresolved

from .project_scan import SymbolTable


def run(
    symbols: SymbolTable,
    sql_edges: list[Edge],
    java_edges: list[Edge],
    unresolved: list[Unresolved] | None = None,
) -> Graph:
    g = Graph()
    for e in sql_edges + java_edges:
        g.edges.append(e)
    g.unresolved.extend(unresolved or [])
    return g
```

- [ ] **Step 2: Smoke test the empty pipeline**

`tests/data_lineage/test_passes/test_smoke.py`:
```python
from pathlib import Path

from skills.data_lineage.passes import project_scan, sql_extract, sql_parse, java_dfa, graph_build


def test_empty_pipeline_returns_empty_graph(tmp_path: Path):
    symbols = project_scan.run(tmp_path)
    units = sql_extract.run(tmp_path, symbols)
    sql_edges = sql_parse.run(units)
    java_edges = java_dfa.run(tmp_path, symbols, units)
    graph = graph_build.run(symbols, sql_edges, java_edges)
    assert graph.nodes == {}
    assert graph.edges == []
    assert graph.unresolved == []
```

- [ ] **Step 3: Run the test**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_passes/test_smoke.py -v`
Expected: 1 passed.

- [ ] **Step 4: Commit**

```bash
git add skills/data_lineage/passes tests/data_lineage/test_passes/test_smoke.py
git commit -m "feat(data-lineage): empty passes wired into a smoke pipeline"
```

## Task 0.5: Text renderer (empty graph)

**Files:**
- Create: `skills/data_lineage/renderers/text.py`
- Create: `tests/data_lineage/test_renderers/test_text.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_renderers/test_text.py`:
```python
from skills.data_lineage.model import Graph, Node, Edge, Evidence
from skills.data_lineage.renderers.text import render


def test_render_empty_graph():
    out = render(Graph())
    assert "0 nodes" in out
    assert "0 edges" in out


def test_render_single_edge_high_confidence():
    n1 = Node(id="db.users.email", kind="db.column", name="users.email",
              parent_id="db.users", attrs={})
    n2 = Node(id="kafka.client-updates.email", kind="kafka.field",
              name="client-updates.email",
              parent_id="kafka.client-updates", attrs={})
    ev = Evidence(file="X.java", line=12, pattern="jooq_select", snippet="dsl.select(USERS.EMAIL)")
    e = Edge(src_id=n1.id, dst_id=n2.id, kind="write",
             evidence=(ev,), confidence="high")
    g = Graph(nodes={n1.id: n1, n2.id: n2}, edges=[e])
    out = render(g)
    assert "db.users.email" in out
    assert "kafka.client-updates.email" in out
    assert "X.java:12" in out
```

- [ ] **Step 2: Run, see fail**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_renderers/test_text.py -v`

- [ ] **Step 3: Implement**

`skills/data_lineage/renderers/text.py`:
```python
"""Plain-text lineage report. Three sections: summary, edges by confidence, unresolved."""
from io import StringIO

from skills.data_lineage.model import Graph


def render(graph: Graph) -> str:
    out = StringIO()
    out.write(f"Lineage report — {len(graph.nodes)} nodes, {len(graph.edges)} edges, "
              f"{len(graph.unresolved)} unresolved\n\n")

    by_conf: dict[str, list] = {"high": [], "medium": [], "low": []}
    for e in graph.edges:
        by_conf[e.confidence].append(e)

    for conf in ("high", "medium", "low"):
        edges = by_conf[conf]
        if not edges:
            continue
        out.write(f"=== {conf.upper()} confidence ({len(edges)}) ===\n")
        for e in edges:
            ev = e.evidence[0] if e.evidence else None
            site = f"{ev.file}:{ev.line}" if ev else "—"
            out.write(f"  {e.src_id}  --{e.kind}-->  {e.dst_id}    [{site}]\n")
        out.write("\n")

    if graph.unresolved:
        out.write(f"=== Unresolved ({len(graph.unresolved)}) ===\n")
        for u in graph.unresolved:
            out.write(f"  {u.file}:{u.line}  {u.reason}  — {u.context}\n")

    return out.getvalue()
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_renderers/test_text.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/renderers/text.py tests/data_lineage/test_renderers/test_text.py
git commit -m "feat(data-lineage): text renderer"
```

## Task 0.6: JSON renderer

**Files:**
- Create: `skills/data_lineage/renderers/json.py`
- Create: `tests/data_lineage/test_renderers/test_json.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_renderers/test_json.py`:
```python
import json

from skills.data_lineage.model import Graph, Node, Edge, Evidence
from skills.data_lineage.renderers.json import render


def test_render_empty():
    out = render(Graph())
    blob = json.loads(out)
    assert blob == {"nodes": [], "edges": [], "unresolved": []}


def test_render_node_and_edge_round_trip():
    n = Node(id="db.users", kind="db.table", name="users", parent_id=None, attrs={})
    ev = Evidence(file="X.java", line=1, pattern="p", snippet="s")
    e = Edge(src_id="a", dst_id="b", kind="read",
             evidence=(ev,), confidence="medium")
    g = Graph(nodes={n.id: n}, edges=[e])
    out = render(g)
    blob = json.loads(out)
    assert blob["nodes"] == [
        {"id": "db.users", "kind": "db.table", "name": "users",
         "parent_id": None, "attrs": {}},
    ]
    assert blob["edges"][0]["evidence"][0]["file"] == "X.java"
    assert blob["edges"][0]["confidence"] == "medium"
```

- [ ] **Step 2: Run, see fail**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_renderers/test_json.py -v`

- [ ] **Step 3: Implement**

`skills/data_lineage/renderers/json.py`:
```python
"""JSON serialization for the lineage Graph.

Edges are emitted as a list (Graph.edges list is ordered). Evidence is
unrolled into nested objects, not flattened.
"""
import json
from dataclasses import asdict

from skills.data_lineage.model import Graph


def render(graph: Graph) -> str:
    payload = {
        "nodes": [asdict(n) for n in graph.nodes.values()],
        "edges": [
            {
                "src_id": e.src_id,
                "dst_id": e.dst_id,
                "kind": e.kind,
                "confidence": e.confidence,
                "evidence": [asdict(ev) for ev in e.evidence],
            }
            for e in graph.edges
        ],
        "unresolved": [asdict(u) for u in graph.unresolved],
    }
    return json.dumps(payload, indent=2, sort_keys=False)
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_renderers/test_json.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/renderers/json.py tests/data_lineage/test_renderers/test_json.py
git commit -m "feat(data-lineage): json renderer"
```

## Task 0.7: Mermaid renderer

**Files:**
- Create: `skills/data_lineage/renderers/mermaid.py`
- Create: `tests/data_lineage/test_renderers/test_mermaid.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_renderers/test_mermaid.py`:
```python
from skills.data_lineage.model import Graph, Node, Edge, Evidence
from skills.data_lineage.renderers.mermaid import render


def test_render_empty():
    out = render(Graph())
    assert out.startswith("flowchart LR")
    assert "%% 0 nodes, 0 edges" in out


def test_render_one_edge():
    n1 = Node(id="db.users.email", kind="db.column", name="users.email",
              parent_id="db.users", attrs={})
    n2 = Node(id="kafka.client-updates.email", kind="kafka.field",
              name="client-updates.email",
              parent_id="kafka.client-updates", attrs={})
    ev = Evidence(file="X.java", line=1, pattern="p", snippet="s")
    e = Edge(src_id=n1.id, dst_id=n2.id, kind="write",
             evidence=(ev,), confidence="high")
    g = Graph(nodes={n1.id: n1, n2.id: n2}, edges=[e])
    out = render(g)
    assert "db_users_email" in out
    assert "kafka_client_updates_email" in out
    assert "-->" in out
```

- [ ] **Step 2: Run, see fail**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_renderers/test_mermaid.py -v`

- [ ] **Step 3: Implement**

`skills/data_lineage/renderers/mermaid.py`:
```python
"""Mermaid `flowchart LR` renderer.

Mermaid IDs disallow dots/dashes; we replace with underscores. The original
ID is kept as the node label so the diagram remains readable.
"""
import re
from io import StringIO

from skills.data_lineage.model import Graph

_ID_SAFE = re.compile(r"[^A-Za-z0-9]")


def _mid(node_id: str) -> str:
    return _ID_SAFE.sub("_", node_id)


def render(graph: Graph) -> str:
    out = StringIO()
    out.write("flowchart LR\n")
    out.write(f"  %% {len(graph.nodes)} nodes, {len(graph.edges)} edges\n")

    for node in graph.nodes.values():
        out.write(f'  {_mid(node.id)}["{node.id}"]\n')

    for e in graph.edges:
        arrow = {"read": "-->", "write": "==>", "transform": "-.->"}[e.kind]
        out.write(f"  {_mid(e.src_id)} {arrow} {_mid(e.dst_id)}\n")

    return out.getvalue()
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_renderers/test_mermaid.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/renderers/mermaid.py tests/data_lineage/test_renderers/test_mermaid.py
git commit -m "feat(data-lineage): mermaid renderer"
```

## Task 0.8: CLI wiring + filters stub

**Files:**
- Create: `skills/data_lineage/cli.py`
- Create: `skills/data_lineage/filters.py`
- Create: `tests/data_lineage/test_integration.py`

- [ ] **Step 1: Failing integration test**

`tests/data_lineage/test_integration.py`:
```python
from pathlib import Path

from skills.data_lineage.cli import run_pipeline


def test_empty_project_produces_empty_artifacts(tmp_path: Path):
    out = tmp_path / "out"
    out.mkdir()
    code = run_pipeline(tmp_path, output_dir=out, formats=("text", "json", "mermaid"))
    assert code == 0
    assert (out / "lineage-report.txt").exists()
    assert (out / "lineage-graph.json").exists()
    assert (out / "lineage.mmd").exists()
    assert "0 nodes" in (out / "lineage-report.txt").read_text()
```

- [ ] **Step 2: Run, see fail**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_integration.py -v`

- [ ] **Step 3: Implement filters stub**

`skills/data_lineage/filters.py`:
```python
"""Subgraph filters. For Phase 0 these are no-ops; populated in Phase 4."""
from dataclasses import dataclass

from skills.data_lineage.model import Graph


@dataclass(frozen=True)
class FilterSpec:
    only_table: str | None = None
    only_topic: str | None = None
    max_depth: int | None = None


def apply(graph: Graph, spec: FilterSpec) -> Graph:
    return graph  # filled in during Phase 4
```

- [ ] **Step 4: Implement CLI**

`skills/data_lineage/cli.py`:
```python
"""Data lineage CLI entry point.

Runs the 5 pipeline passes, applies filters, and emits the requested formats.
"""
import argparse
import sys
from pathlib import Path

from .filters import FilterSpec, apply as apply_filters
from .passes import project_scan, sql_extract, sql_parse, java_dfa, graph_build
from .renderers import text as render_text
from .renderers import json as render_json
from .renderers import mermaid as render_mermaid


_FORMAT_FILES = {
    "text": ("lineage-report.txt", render_text.render),
    "json": ("lineage-graph.json", render_json.render),
    "mermaid": ("lineage.mmd", render_mermaid.render),
}


def run_pipeline(
    project_root: Path,
    *,
    output_dir: Path,
    formats: tuple[str, ...] = ("text", "json", "mermaid"),
    filter_spec: FilterSpec | None = None,
    quiet: bool = False,
) -> int:
    symbols = project_scan.run(project_root)
    units = sql_extract.run(project_root, symbols)
    sql_edges = sql_parse.run(units)
    java_edges = java_dfa.run(project_root, symbols, units)
    graph = graph_build.run(symbols, sql_edges, java_edges)

    if filter_spec is not None:
        graph = apply_filters(graph, filter_spec)

    output_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        filename, fn = _FORMAT_FILES[fmt]
        (output_dir / filename).write_text(fn(graph))

    if not quiet and "text" in formats:
        sys.stdout.write((output_dir / "lineage-report.txt").read_text())

    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="data_lineage")
    p.add_argument("project_path", type=Path)
    p.add_argument("--output", type=Path, default=Path.cwd())
    p.add_argument("--formats", default="text,json,mermaid")
    p.add_argument("--only-table", default=None)
    p.add_argument("--only-topic", default=None)
    p.add_argument("--max-depth", type=int, default=None)
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--strict", action="store_true")
    args = p.parse_args(argv)

    formats = tuple(s.strip() for s in args.formats.split(",") if s.strip())
    spec = FilterSpec(
        only_table=args.only_table,
        only_topic=args.only_topic,
        max_depth=args.max_depth,
    )
    return run_pipeline(
        args.project_path,
        output_dir=args.output,
        formats=formats,
        filter_spec=spec,
        quiet=args.quiet,
    )
```

Note: `--debug` and `--strict` are accepted for forward-compatibility but currently unused — wired in Phase 4.

- [ ] **Step 5: Run integration test**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_integration.py -v`
Expected: 1 passed.

- [ ] **Step 6: Smoke-test the actual CLI**

Run: `PYTHONPATH=. python -m skills.data_lineage . --output /tmp/dl-smoke --quiet`
Expected: exit 0; `/tmp/dl-smoke/lineage-report.txt` contains `0 nodes`.

- [ ] **Step 7: Commit**

```bash
git add skills/data_lineage/cli.py skills/data_lineage/filters.py tests/data_lineage/test_integration.py
git commit -m "feat(data-lineage): CLI + filter stub + end-to-end smoke"
```

---

# Phase 1 — SQL side (column-lineage within statements)

Goal: extract SQL from `@Query`, JdbcTemplate, and jOOQ DSL; parse it with sqlglot; emit column-level edges. After this phase, the report shows "column X is read in these N files" without any Java DFA.

## Task 1.1: JdbcTemplate extractor

**Files:**
- Create: `skills/data_lineage/extractors/jdbc_template.py`
- Create: `tests/data_lineage/test_extractors/test_jdbc_template.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_extractors/test_jdbc_template.py`:
```python
from textwrap import dedent

from skills.data_lineage.extractors.jdbc_template import extract


def test_extracts_query_for_object_string_literal():
    code = dedent('''
        public Device load(int id) {
            return jdbc.queryForObject(
                "SELECT id, mac, owner_id FROM devices WHERE id = ?",
                new Object[]{id},
                deviceMapper);
        }
    ''').encode()
    units = extract(code, file="DeviceRepo.java")
    assert len(units) == 1
    assert units[0].kind == "jdbc_query"
    assert "SELECT id" in units[0].sql
    assert units[0].file == "DeviceRepo.java"


def test_extracts_update_statement():
    code = dedent('''
        public void touch(int id) {
            jdbc.update("UPDATE devices SET last_seen = NOW() WHERE id = ?", id);
        }
    ''').encode()
    units = extract(code, file="DeviceRepo.java")
    assert units[0].kind == "jdbc_query"
    assert "UPDATE devices" in units[0].sql


def test_ignores_non_string_first_argument():
    code = dedent('''
        public List<X> dynamic(String sqlVar) {
            return jdbc.query(sqlVar, mapper);
        }
    ''').encode()
    units = extract(code, file="X.java")
    assert units == []


def test_concatenated_string_marked_unresolved_via_kind():
    code = dedent('''
        public List<X> bad() {
            return jdbc.query("SELECT * FROM t WHERE " + cond, mapper);
        }
    ''').encode()
    units = extract(code, file="X.java")
    assert units == []  # concat doesn't make it through; pass logs unresolved separately


def test_named_parameter_jdbc_template_too():
    code = dedent('''
        public Foo load() {
            return namedJdbc.queryForObject(
                "SELECT name FROM users WHERE id = :id",
                Map.of("id", 1), mapper);
        }
    ''').encode()
    units = extract(code, file="X.java")
    assert len(units) == 1
    assert "SELECT name" in units[0].sql
```

- [ ] **Step 2: Run, see fail**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_extractors/test_jdbc_template.py -v`

- [ ] **Step 3: Implement**

`skills/data_lineage/extractors/jdbc_template.py`:
```python
"""Find JdbcTemplate / NamedParameterJdbcTemplate calls with literal SQL.

Recognized methods: query, queryForObject, queryForList, queryForMap,
queryForRowSet, update, batchUpdate, execute. The first argument must be
a single string literal; concatenations and variables are skipped (they
become Unresolved entries elsewhere).
"""
from skills.data_lineage.passes.sql_extract import SqlUnit
from skills.data_lineage.ts_parser import parse_java


_METHODS = {
    "query", "queryForObject", "queryForList", "queryForMap",
    "queryForRowSet", "update", "batchUpdate", "execute",
}


def extract(source: bytes, file: str) -> list[SqlUnit]:
    tree = parse_java(source)
    out: list[SqlUnit] = []
    _walk(tree.root_node, source, file, out)
    return out


def _walk(node, source: bytes, file: str, out: list[SqlUnit]) -> None:
    if node.type == "method_invocation":
        name_node = node.child_by_field_name("name")
        if name_node is not None and source[name_node.start_byte:name_node.end_byte].decode() in _METHODS:
            args = node.child_by_field_name("arguments")
            if args is not None and args.named_child_count >= 1:
                first = args.named_children[0]
                if first.type == "string_literal":
                    sql = source[first.start_byte:first.end_byte].decode().strip('"')
                    out.append(SqlUnit(
                        file=file,
                        line=node.start_point[0] + 1,
                        kind="jdbc_query",
                        sql=sql,
                    ))
    for child in node.children:
        _walk(child, source, file, out)
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_extractors/test_jdbc_template.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/extractors/jdbc_template.py tests/data_lineage/test_extractors/test_jdbc_template.py
git commit -m "feat(data-lineage): JdbcTemplate SQL extractor"
```

## Task 1.2: Spring Data extractor (`@Query` + derived methods)

**Files:**
- Create: `skills/data_lineage/extractors/spring_data.py`
- Create: `tests/data_lineage/test_extractors/test_spring_data.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_extractors/test_spring_data.py`:
```python
from textwrap import dedent

from skills.data_lineage.extractors.spring_data import extract


def test_extracts_query_annotation_value():
    code = dedent('''
        public interface ClientRepository {
            @Query("SELECT c FROM Client c WHERE c.email = :email")
            Optional<Client> findByEmail(String email);
        }
    ''').encode()
    units = extract(code, file="ClientRepository.java")
    assert len(units) >= 1
    annotated = [u for u in units if u.kind == "jpa_query"]
    assert len(annotated) == 1
    assert "SELECT c FROM Client" in annotated[0].sql


def test_extracts_native_query():
    code = dedent('''
        public interface DeviceRepo {
            @Query(value = "SELECT * FROM devices", nativeQuery = true)
            List<Device> all();
        }
    ''').encode()
    units = extract(code, file="X.java")
    assert any(u.kind == "jpa_query_native" and "SELECT *" in u.sql for u in units)


def test_derived_method_findBy_extracts_column():
    code = dedent('''
        public interface ClientRepository {
            Optional<Client> findByEmail(String email);
            List<Client> findByEmailAndStatus(String email, String status);
        }
    ''').encode()
    units = extract(code, file="ClientRepository.java")
    derived = [u for u in units if u.kind == "spring_data_derived"]
    assert any("email" in u.jooq_columns for u in derived)
    assert any({"email", "status"} <= set(u.jooq_columns) for u in derived)


def test_skips_methods_without_findBy_prefix():
    code = dedent('''
        public interface X { void doStuff(String x); }
    ''').encode()
    units = extract(code, file="X.java")
    assert all(u.kind != "spring_data_derived" for u in units)
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement**

`skills/data_lineage/extractors/spring_data.py`:
```python
"""Spring Data sources of SQL/JPQL.

Two paths:
1. @Query("...") annotations — both JPQL and nativeQuery=true.
2. Derived query methods (findBy*, readBy*, queryBy*, getBy*) — name-parsed
   into column references. Heuristic; column names are camelCase → snake_case.
"""
import re

from skills.data_lineage.passes.sql_extract import SqlUnit
from skills.data_lineage.ts_parser import parse_java


_DERIVED_PREFIX = re.compile(r"^(findBy|readBy|queryBy|getBy|countBy|existsBy)(.+)$")
_AND_OR = re.compile(r"And|Or")


def _camel_to_snake(s: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


def extract(source: bytes, file: str) -> list[SqlUnit]:
    tree = parse_java(source)
    out: list[SqlUnit] = []
    _walk(tree.root_node, source, file, out)
    return out


def _walk(node, source: bytes, file: str, out: list[SqlUnit]) -> None:
    if node.type == "annotation" or node.type == "marker_annotation":
        _handle_annotation(node, source, file, out)
    if node.type == "method_declaration":
        _handle_derived(node, source, file, out)
    for child in node.children:
        _walk(child, source, file, out)


def _handle_annotation(node, source: bytes, file: str, out: list[SqlUnit]) -> None:
    name_node = node.child_by_field_name("name")
    if name_node is None:
        return
    name = source[name_node.start_byte:name_node.end_byte].decode()
    if name != "Query":
        return
    args = node.child_by_field_name("arguments")
    if args is None:
        return
    sql = None
    is_native = False
    for child in args.named_children:
        if child.type == "string_literal":
            sql = source[child.start_byte:child.end_byte].decode().strip('"')
        elif child.type == "element_value_pair":
            key_n = child.child_by_field_name("key")
            val_n = child.child_by_field_name("value")
            if key_n is None or val_n is None:
                continue
            key = source[key_n.start_byte:key_n.end_byte].decode()
            if key == "value" and val_n.type == "string_literal":
                sql = source[val_n.start_byte:val_n.end_byte].decode().strip('"')
            elif key == "nativeQuery":
                is_native = source[val_n.start_byte:val_n.end_byte].decode() == "true"
    if sql is None:
        return
    out.append(SqlUnit(
        file=file,
        line=node.start_point[0] + 1,
        kind="jpa_query_native" if is_native else "jpa_query",
        sql=sql,
    ))


def _handle_derived(node, source: bytes, file: str, out: list[SqlUnit]) -> None:
    name_n = node.child_by_field_name("name")
    if name_n is None:
        return
    method_name = source[name_n.start_byte:name_n.end_byte].decode()
    m = _DERIVED_PREFIX.match(method_name)
    if not m:
        return
    suffix = m.group(2)
    parts = _AND_OR.split(suffix)
    columns = tuple(_camel_to_snake(p) for p in parts if p)
    out.append(SqlUnit(
        file=file,
        line=node.start_point[0] + 1,
        kind="spring_data_derived",
        sql=None,
        jooq_columns=columns,
    ))
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_extractors/test_spring_data.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/extractors/spring_data.py tests/data_lineage/test_extractors/test_spring_data.py
git commit -m "feat(data-lineage): Spring Data @Query + derived method extractor"
```

## Task 1.3: jOOQ DSL extractor

**Files:**
- Create: `skills/data_lineage/extractors/jooq.py`
- Create: `tests/data_lineage/test_extractors/test_jooq.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_extractors/test_jooq.py`:
```python
from textwrap import dedent

from skills.data_lineage.extractors.jooq import extract


def test_extracts_select_typed_columns():
    code = dedent('''
        public List<User> findActive() {
            return dsl.select(USERS.ID, USERS.EMAIL)
                      .from(USERS)
                      .where(USERS.ACTIVE.eq(true))
                      .fetchInto(User.class);
        }
    ''').encode()
    units = extract(code, file="UserRepo.java")
    select = [u for u in units if u.kind == "jooq_select"]
    assert len(select) == 1
    assert "users" in select[0].jooq_tables
    assert {"id", "email"} <= set(select[0].jooq_columns)


def test_extracts_insert_into():
    code = dedent('''
        public void add(String email) {
            dsl.insertInto(USERS, USERS.EMAIL).values(email).execute();
        }
    ''').encode()
    units = extract(code, file="X.java")
    insert = [u for u in units if u.kind == "jooq_insert"]
    assert len(insert) == 1
    assert insert[0].jooq_tables == ("users",)
    assert insert[0].jooq_columns == ("email",)


def test_extracts_update():
    code = dedent('''
        public void touch(int id) {
            dsl.update(USERS).set(USERS.LAST_SEEN, now())
               .where(USERS.ID.eq(id)).execute();
        }
    ''').encode()
    units = extract(code, file="X.java")
    update = [u for u in units if u.kind == "jooq_update"]
    assert len(update) == 1
    assert "last_seen" in update[0].jooq_columns


def test_extracts_delete():
    code = dedent('''
        public void purge(int id) {
            dsl.deleteFrom(USERS).where(USERS.ID.eq(id)).execute();
        }
    ''').encode()
    units = extract(code, file="X.java")
    assert any(u.kind == "jooq_delete" and u.jooq_tables == ("users",) for u in units)


def test_select_star_no_columns_listed():
    code = dedent('''
        public List<UsersRecord> all() { return dsl.selectFrom(USERS).fetch(); }
    ''').encode()
    units = extract(code, file="X.java")
    assert any(u.kind == "jooq_select" and u.jooq_tables == ("users",) for u in units)


def test_ignores_non_jooq_method_calls():
    code = dedent('''
        public int x() { return Math.max(1, 2); }
    ''').encode()
    units = extract(code, file="X.java")
    assert units == []
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement**

`skills/data_lineage/extractors/jooq.py`:
```python
"""jOOQ DSL extractor.

Recognizes jOOQ's DSLContext fluent chains:
- dsl.select(TABLE.COL, ...).from(TABLE)
- dsl.selectFrom(TABLE)
- dsl.insertInto(TABLE, TABLE.COL...).values(...)
- dsl.update(TABLE).set(TABLE.COL, ...)
- dsl.deleteFrom(TABLE)

Identifiers are detected by convention: SCREAMING_SNAKE field accesses
mapped to lowercase. Generated jOOQ classes follow this convention.
"""
from skills.data_lineage.passes.sql_extract import SqlUnit
from skills.data_lineage.ts_parser import parse_java


def extract(source: bytes, file: str) -> list[SqlUnit]:
    tree = parse_java(source)
    out: list[SqlUnit] = []
    _walk(tree.root_node, source, file, out)
    return out


def _walk(node, source: bytes, file: str, out: list[SqlUnit]) -> None:
    if node.type == "method_invocation":
        unit = _try_jooq_chain(node, source, file)
        if unit is not None:
            out.append(unit)
            return  # don't recurse into the chain we just consumed
    for child in node.children:
        _walk(child, source, file, out)


_ENTRY_METHODS = {
    "select": "jooq_select",
    "selectFrom": "jooq_select",
    "insertInto": "jooq_insert",
    "update": "jooq_update",
    "deleteFrom": "jooq_delete",
}


def _try_jooq_chain(node, source: bytes, file: str) -> SqlUnit | None:
    head_name, head_call, head_args = _walk_to_chain_head(node, source)
    if head_name is None or head_name not in _ENTRY_METHODS:
        return None
    kind = _ENTRY_METHODS[head_name]

    tables: list[str] = []
    columns: list[str] = []

    if head_name in ("selectFrom", "deleteFrom", "update"):
        if head_args:
            tables.extend(_collect_table_names(head_args, source))
    elif head_name == "select":
        if head_args:
            columns.extend(_collect_column_names(head_args, source))
        # `from(TABLE)` somewhere in the outer chain
        tables.extend(_chain_from_clause_tables(node, source))
    elif head_name == "insertInto":
        if head_args:
            args_named = head_args.named_children
            if args_named:
                tables.extend(_collect_table_names(args_named[0:1], source))
                columns.extend(_collect_column_names(args_named[1:], source))
    if head_name == "update":
        columns.extend(_chain_set_columns(node, source))

    if not tables and not columns:
        return None

    return SqlUnit(
        file=file,
        line=node.start_point[0] + 1,
        kind=kind,
        sql=None,
        jooq_tables=tuple(dict.fromkeys(tables)),
        jooq_columns=tuple(dict.fromkeys(columns)),
    )


def _walk_to_chain_head(node, source: bytes):
    """Walk down `.method(...)` chain to the leftmost method invocation."""
    cur = node
    while True:
        obj = cur.child_by_field_name("object")
        if obj is not None and obj.type == "method_invocation":
            cur = obj
            continue
        name_n = cur.child_by_field_name("name")
        args_n = cur.child_by_field_name("arguments")
        if name_n is None:
            return None, None, None
        head_name = source[name_n.start_byte:name_n.end_byte].decode()
        args_children = args_n.named_children if args_n is not None else []
        return head_name, cur, args_children


def _collect_table_names(arg_nodes, source: bytes) -> list[str]:
    names: list[str] = []
    for n in arg_nodes:
        if n.type == "identifier":
            text = source[n.start_byte:n.end_byte].decode()
            if text.isupper() or "_" in text:
                names.append(text.lower())
    return names


def _collect_column_names(arg_nodes, source: bytes) -> list[str]:
    """Args are typically `TABLE.COLUMN` field accesses."""
    cols: list[str] = []
    for n in arg_nodes:
        if n.type == "field_access":
            field_n = n.child_by_field_name("field")
            if field_n is not None:
                cols.append(source[field_n.start_byte:field_n.end_byte].decode().lower())
    return cols


def _chain_from_clause_tables(node, source: bytes) -> list[str]:
    tables: list[str] = []
    cur = node
    while cur is not None and cur.type == "method_invocation":
        name_n = cur.child_by_field_name("name")
        if name_n is not None:
            name = source[name_n.start_byte:name_n.end_byte].decode()
            if name == "from":
                args_n = cur.child_by_field_name("arguments")
                if args_n is not None:
                    tables.extend(_collect_table_names(args_n.named_children, source))
        cur = cur.child_by_field_name("object")
    return tables


def _chain_set_columns(node, source: bytes) -> list[str]:
    cols: list[str] = []
    cur = node
    while cur is not None and cur.type == "method_invocation":
        name_n = cur.child_by_field_name("name")
        if name_n is not None and source[name_n.start_byte:name_n.end_byte].decode() == "set":
            args_n = cur.child_by_field_name("arguments")
            if args_n is not None and args_n.named_child_count >= 1:
                cols.extend(_collect_column_names(args_n.named_children[:1], source))
        cur = cur.child_by_field_name("object")
    return cols
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_extractors/test_jooq.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/extractors/jooq.py tests/data_lineage/test_extractors/test_jooq.py
git commit -m "feat(data-lineage): jOOQ DSL extractor"
```

## Task 1.4: sql_extract pass

**Files:**
- Modify: `skills/data_lineage/passes/sql_extract.py`
- Create: `tests/data_lineage/test_passes/test_sql_extract.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_passes/test_sql_extract.py`:
```python
from pathlib import Path

from skills.data_lineage.passes import sql_extract
from skills.data_lineage.passes.project_scan import SymbolTable


def test_combines_extractors_across_files(tmp_path: Path):
    (tmp_path / "Repo.java").write_text(
        'public class Repo {\n'
        '  public Foo a() { return jdbc.queryForObject("SELECT * FROM users", null); }\n'
        '  public Bar b() { return dsl.selectFrom(USERS).fetch(); }\n'
        '}\n'
    )
    (tmp_path / "Other.java").write_text(
        'public interface I {\n'
        '  @Query("SELECT 1") String x();\n'
        '}\n'
    )
    units = sql_extract.run(tmp_path, SymbolTable())
    kinds = [u.kind for u in units]
    assert "jdbc_query" in kinds
    assert "jooq_select" in kinds
    assert "jpa_query" in kinds


def test_skips_generated_directories(tmp_path: Path):
    (tmp_path / "generated").mkdir()
    (tmp_path / "generated" / "X.java").write_text(
        'class X { Foo f() { return jdbc.queryForObject("SELECT 1", null); } }\n'
    )
    units = sql_extract.run(tmp_path, SymbolTable())
    assert all("generated" not in u.file for u in units)
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Replace `passes/sql_extract.py`**

```python
"""Pass 2 — fan out SQL extractors across all .java files in the project."""
from dataclasses import dataclass
from pathlib import Path

from skills.data_lineage.extractors import jdbc_template, jooq, spring_data

from .project_scan import SymbolTable


_IGNORED_DIRS = {"generated", "build", "target", ".gradle", "node_modules", ".git"}


@dataclass(frozen=True)
class SqlUnit:
    file: str
    line: int
    kind: str
    sql: str | None
    jooq_columns: tuple[str, ...] = ()
    jooq_tables: tuple[str, ...] = ()


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
```

Note: `SqlUnit` definition stays in `sql_extract.py` — extractors already import it from there in earlier tasks.

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_passes/test_sql_extract.py -v`
Expected: 2 passed.

- [ ] **Step 5: Re-run all data_lineage tests, ensure nothing else broke**

Run: `PYTHONPATH=. pytest tests/data_lineage -v`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add skills/data_lineage/passes/sql_extract.py tests/data_lineage/test_passes/test_sql_extract.py
git commit -m "feat(data-lineage): sql_extract pass aggregating extractors"
```

## Task 1.5: sql_parse pass via sqlglot

**Files:**
- Modify: `skills/data_lineage/passes/sql_parse.py`
- Create: `tests/data_lineage/test_passes/test_sql_parse.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_passes/test_sql_parse.py`:
```python
from skills.data_lineage.model import Edge
from skills.data_lineage.passes import sql_parse
from skills.data_lineage.passes.sql_extract import SqlUnit


def test_select_emits_table_to_column_read_edges():
    units = [SqlUnit(
        file="R.java", line=1, kind="jdbc_query",
        sql="SELECT id, email FROM users WHERE active = true",
    )]
    edges = sql_parse.run(units)
    pairs = {(e.src_id, e.dst_id, e.kind) for e in edges}
    assert ("db.users.id", "code.var.id", "read") in pairs
    assert ("db.users.email", "code.var.email", "read") in pairs


def test_insert_emits_write_edges_into_columns():
    units = [SqlUnit(
        file="R.java", line=1, kind="jdbc_query",
        sql="INSERT INTO devices (mac, owner_id) VALUES (?, ?)",
    )]
    edges = sql_parse.run(units)
    written = {(e.src_id, e.dst_id, e.kind) for e in edges if e.kind == "write"}
    assert ("code.var.mac", "db.devices.mac", "write") in written
    assert ("code.var.owner_id", "db.devices.owner_id", "write") in written


def test_update_emits_write_edges():
    units = [SqlUnit(
        file="R.java", line=1, kind="jdbc_query",
        sql="UPDATE devices SET last_seen = NOW() WHERE id = ?",
    )]
    edges = sql_parse.run(units)
    assert any(e.dst_id == "db.devices.last_seen" and e.kind == "write" for e in edges)


def test_jooq_select_with_typed_columns_emits_reads():
    units = [SqlUnit(
        file="R.java", line=1, kind="jooq_select",
        sql=None, jooq_tables=("users",), jooq_columns=("id", "email"),
    )]
    edges = sql_parse.run(units)
    assert any(e.src_id == "db.users.email" and e.kind == "read" for e in edges)


def test_jooq_insert_emits_write():
    units = [SqlUnit(
        file="R.java", line=1, kind="jooq_insert",
        sql=None, jooq_tables=("users",), jooq_columns=("email",),
    )]
    edges = sql_parse.run(units)
    assert any(e.dst_id == "db.users.email" and e.kind == "write" for e in edges)


def test_unparseable_sql_yields_no_edges_no_exception():
    units = [SqlUnit(file="R.java", line=1, kind="jdbc_query",
                    sql="THIS IS NOT SQL ;;")]
    edges = sql_parse.run(units)
    assert edges == []
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement**

`skills/data_lineage/passes/sql_parse.py`:
```python
"""Pass 3 — parse each SqlUnit via sqlglot, emit column-level edges.

For SELECT: each output column becomes a read edge from db.<table>.<col>
to code.var.<col>.

For INSERT/UPDATE: each target column becomes a write edge from
code.var.<col> to db.<table>.<col>.

Tables and columns are normalized to lowercase. Multi-table SELECTs without
aliases or with `*` use the first table; ambiguous columns are skipped.
"""
import sqlglot
from sqlglot import exp

from skills.data_lineage.model import Edge, Evidence

from .sql_extract import SqlUnit


def run(units: list[SqlUnit]) -> list[Edge]:
    out: list[Edge] = []
    for u in units:
        try:
            out.extend(_unit_edges(u))
        except Exception:
            continue
    return out


def _unit_edges(u: SqlUnit) -> list[Edge]:
    if u.sql is not None:
        return _from_sql(u)
    return _from_jooq(u)


def _evidence(u: SqlUnit, pattern: str) -> tuple[Evidence, ...]:
    snippet = (u.sql or " ".join(u.jooq_tables + u.jooq_columns))[:120]
    return (Evidence(file=u.file, line=u.line, pattern=pattern, snippet=snippet),)


def _from_sql(u: SqlUnit) -> list[Edge]:
    parsed = sqlglot.parse_one(u.sql, read="oracle")
    if isinstance(parsed, exp.Select):
        return _select_edges(u, parsed)
    if isinstance(parsed, exp.Insert):
        return _insert_edges(u, parsed)
    if isinstance(parsed, exp.Update):
        return _update_edges(u, parsed)
    return []


def _select_edges(u: SqlUnit, sel: exp.Select) -> list[Edge]:
    tables = [t.name.lower() for t in sel.find_all(exp.Table)]
    if not tables:
        return []
    primary = tables[0]
    edges: list[Edge] = []
    for col in sel.expressions:
        if isinstance(col, exp.Column):
            name = col.name.lower()
            edges.append(Edge(
                src_id=f"db.{primary}.{name}",
                dst_id=f"code.var.{name}",
                kind="read",
                evidence=_evidence(u, "sql_select"),
                confidence="high",
            ))
        elif isinstance(col, exp.Alias):
            inner = col.this
            if isinstance(inner, exp.Column):
                name = inner.name.lower()
                edges.append(Edge(
                    src_id=f"db.{primary}.{name}",
                    dst_id=f"code.var.{col.alias.lower()}",
                    kind="read",
                    evidence=_evidence(u, "sql_select"),
                    confidence="high",
                ))
    return edges


def _insert_edges(u: SqlUnit, ins: exp.Insert) -> list[Edge]:
    table = ins.this.find(exp.Table)
    if table is None:
        return []
    table_name = table.name.lower()
    columns = []
    schema = ins.this if isinstance(ins.this, exp.Schema) else None
    if schema is not None:
        for col in schema.expressions:
            if isinstance(col, exp.Column):
                columns.append(col.name.lower())
    edges: list[Edge] = []
    for c in columns:
        edges.append(Edge(
            src_id=f"code.var.{c}",
            dst_id=f"db.{table_name}.{c}",
            kind="write",
            evidence=_evidence(u, "sql_insert"),
            confidence="high",
        ))
    return edges


def _update_edges(u: SqlUnit, upd: exp.Update) -> list[Edge]:
    table = upd.this if isinstance(upd.this, exp.Table) else upd.find(exp.Table)
    if table is None:
        return []
    table_name = table.name.lower()
    edges: list[Edge] = []
    for setter in upd.expressions:
        if isinstance(setter, exp.EQ) and isinstance(setter.this, exp.Column):
            c = setter.this.name.lower()
            edges.append(Edge(
                src_id=f"code.var.{c}",
                dst_id=f"db.{table_name}.{c}",
                kind="write",
                evidence=_evidence(u, "sql_update"),
                confidence="high",
            ))
    return edges


def _from_jooq(u: SqlUnit) -> list[Edge]:
    if not u.jooq_tables:
        return []
    table = u.jooq_tables[0]
    edges: list[Edge] = []
    is_write = u.kind in ("jooq_insert", "jooq_update")
    for c in u.jooq_columns:
        if is_write:
            edges.append(Edge(
                src_id=f"code.var.{c}",
                dst_id=f"db.{table}.{c}",
                kind="write",
                evidence=_evidence(u, u.kind),
                confidence="high",
            ))
        else:
            edges.append(Edge(
                src_id=f"db.{table}.{c}",
                dst_id=f"code.var.{c}",
                kind="read",
                evidence=_evidence(u, u.kind),
                confidence="high",
            ))
    return edges
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_passes/test_sql_parse.py -v`
Expected: 6 passed.

- [ ] **Step 5: Phase-1 integration probe**

Add to `tests/data_lineage/test_integration.py`:
```python
def test_phase1_jdbc_sql_emits_high_confidence_edges(tmp_path: Path):
    (tmp_path / "Repo.java").write_text(
        'public class Repo {\n'
        '  public Foo a() { return jdbc.queryForObject(\n'
        '    "SELECT id, email FROM users WHERE active = true", null); }\n'
        '}\n'
    )
    out = tmp_path / "out"
    code = run_pipeline(tmp_path, output_dir=out, formats=("json",), quiet=True)
    assert code == 0
    import json
    blob = json.loads((out / "lineage-graph.json").read_text())
    edges = blob["edges"]
    pairs = {(e["src_id"], e["dst_id"]) for e in edges}
    assert ("db.users.id", "code.var.id") in pairs
    assert ("db.users.email", "code.var.email") in pairs
```

Run: `PYTHONPATH=. pytest tests/data_lineage/test_integration.py -v`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add skills/data_lineage/passes/sql_parse.py tests/data_lineage/test_passes/test_sql_parse.py tests/data_lineage/test_integration.py
git commit -m "feat(data-lineage): sql_parse via sqlglot — column-level edges"
```

---

# Phase 2 — Java DFA + DTO

Goal: extract DTO shapes (POJO + Lombok + Jackson rename); build a project symbol table; track values within methods so a SELECT result can be linked to a DTO field. After this phase the graph has new node types `code.method.*` and edges connecting `code.var.X → code.dto.Foo.X`.

## Task 2.1: DTO extractor

**Files:**
- Create: `skills/data_lineage/extractors/dto.py`
- Create: `tests/data_lineage/test_extractors/test_dto.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_extractors/test_dto.py`:
```python
from textwrap import dedent

from skills.data_lineage.extractors.dto import extract


def test_plain_pojo_fields_and_getters():
    code = dedent('''
        public class Client {
            private String email;
            private Long id;
            public String getEmail() { return email; }
            public Long getId() { return id; }
        }
    ''').encode()
    dtos = extract(code, file="Client.java")
    assert len(dtos) == 1
    dto = dtos[0]
    assert dto.fqn.endswith("Client")
    assert {"email", "id"} <= set(dto.fields.keys())
    assert dto.getter_to_field["getEmail"] == "email"


def test_lombok_data_class_has_implicit_getters():
    code = dedent('''
        @lombok.Data
        public class Client {
            private String email;
            private Long id;
        }
    ''').encode()
    dtos = extract(code, file="Client.java")
    dto = dtos[0]
    assert dto.getter_to_field["getEmail"] == "email"
    assert dto.getter_to_field["getId"] == "id"


def test_jackson_property_renames_serialized_name():
    code = dedent('''
        public class Event {
            @JsonProperty("user_id")
            private Long userId;
        }
    ''').encode()
    dto = extract(code, file="Event.java")[0]
    assert dto.fields["userId"].serialized_as == "user_id"


def test_jackson_ignore_excludes_field_from_serialization():
    code = dedent('''
        public class Event {
            private Long id;
            @JsonIgnore private String secret;
        }
    ''').encode()
    dto = extract(code, file="Event.java")[0]
    assert dto.fields["secret"].ignored
    assert not dto.fields["id"].ignored
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement**

`skills/data_lineage/extractors/dto.py`:
```python
"""DTO shape extractor: classes, fields, Lombok-implicit getters, Jackson rename/ignore."""
from dataclasses import dataclass

from skills.data_lineage.ts_parser import parse_java


@dataclass(frozen=True)
class DtoField:
    name: str
    type: str
    serialized_as: str           # Jackson @JsonProperty value, default = name
    ignored: bool                # @JsonIgnore


@dataclass(frozen=True)
class Dto:
    fqn: str
    file: str
    fields: dict[str, DtoField]
    getter_to_field: dict[str, str]
    annotations: tuple[str, ...]


def extract(source: bytes, file: str) -> list[Dto]:
    tree = parse_java(source)
    pkg = _package(tree.root_node, source)
    out: list[Dto] = []
    for cls in _iter_classes(tree.root_node):
        out.append(_class_to_dto(cls, source, file, pkg))
    return out


def _package(root, source: bytes) -> str:
    for child in root.children:
        if child.type == "package_declaration":
            for sub in child.named_children:
                if sub.type in ("scoped_identifier", "identifier"):
                    return source[sub.start_byte:sub.end_byte].decode()
    return ""


def _iter_classes(node):
    if node.type == "class_declaration":
        yield node
    for child in node.children:
        yield from _iter_classes(child)


def _class_to_dto(cls, source: bytes, file: str, pkg: str) -> Dto:
    name_n = cls.child_by_field_name("name")
    name = source[name_n.start_byte:name_n.end_byte].decode()
    fqn = f"{pkg}.{name}" if pkg else name

    annotations = _class_annotations(cls, source)
    body = cls.child_by_field_name("body")
    fields: dict[str, DtoField] = {}
    explicit_getters: dict[str, str] = {}

    if body is not None:
        for member in body.named_children:
            if member.type == "field_declaration":
                f = _parse_field(member, source)
                if f is not None:
                    fields[f.name] = f
            elif member.type == "method_declaration":
                pair = _parse_getter(member, source)
                if pair is not None:
                    explicit_getters[pair[0]] = pair[1]

    if any(a in {"Data", "Getter"} for a in annotations):
        for fname in fields:
            getter = "get" + fname[:1].upper() + fname[1:]
            explicit_getters.setdefault(getter, fname)

    return Dto(
        fqn=fqn, file=file, fields=fields,
        getter_to_field=explicit_getters,
        annotations=annotations,
    )


def _class_annotations(cls, source: bytes) -> tuple[str, ...]:
    out: list[str] = []
    for child in cls.children:
        if child.type in ("modifiers", "marker_annotation", "annotation"):
            for sub in child.children if child.type == "modifiers" else (child,):
                if sub.type in ("marker_annotation", "annotation"):
                    name_n = sub.child_by_field_name("name")
                    if name_n is not None:
                        text = source[name_n.start_byte:name_n.end_byte].decode()
                        out.append(text.split(".")[-1])
    return tuple(out)


def _parse_field(node, source: bytes):
    type_n = node.child_by_field_name("type")
    if type_n is None:
        return None
    type_text = source[type_n.start_byte:type_n.end_byte].decode()
    decl = next((c for c in node.named_children if c.type == "variable_declarator"), None)
    if decl is None:
        return None
    name_n = decl.child_by_field_name("name")
    if name_n is None:
        return None
    fname = source[name_n.start_byte:name_n.end_byte].decode()

    serialized_as = fname
    ignored = False
    for child in node.children:
        if child.type == "modifiers":
            for sub in child.children:
                if sub.type in ("marker_annotation", "annotation"):
                    n = sub.child_by_field_name("name")
                    if n is None:
                        continue
                    aname = source[n.start_byte:n.end_byte].decode().split(".")[-1]
                    if aname == "JsonIgnore":
                        ignored = True
                    elif aname == "JsonProperty":
                        args = sub.child_by_field_name("arguments")
                        if args is not None:
                            for a in args.named_children:
                                if a.type == "string_literal":
                                    serialized_as = source[a.start_byte:a.end_byte].decode().strip('"')
                                elif a.type == "element_value_pair":
                                    key_n = a.child_by_field_name("key")
                                    val_n = a.child_by_field_name("value")
                                    if (
                                        key_n is not None
                                        and val_n is not None
                                        and source[key_n.start_byte:key_n.end_byte].decode() == "value"
                                        and val_n.type == "string_literal"
                                    ):
                                        serialized_as = source[val_n.start_byte:val_n.end_byte].decode().strip('"')

    from .dto import DtoField  # noqa: E402  (self-ref for forward type)
    return DtoField(name=fname, type=type_text, serialized_as=serialized_as, ignored=ignored)


def _parse_getter(node, source: bytes):
    name_n = node.child_by_field_name("name")
    if name_n is None:
        return None
    method_name = source[name_n.start_byte:name_n.end_byte].decode()
    if not method_name.startswith("get") or len(method_name) < 4:
        return None
    body = node.child_by_field_name("body")
    if body is None:
        return None
    field_name = method_name[3].lower() + method_name[4:]
    return method_name, field_name
```

Note: the inner `from .dto import DtoField` in `_parse_field` can be replaced by direct construction once the type is defined above. Use `DtoField(...)` directly — drop the inner import.

- [ ] **Step 4: Clean up the inner import**

Edit `_parse_field`: remove the `from .dto import DtoField` line; the symbol is already in module scope.

- [ ] **Step 5: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_extractors/test_dto.py -v`
Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add skills/data_lineage/extractors/dto.py tests/data_lineage/test_extractors/test_dto.py
git commit -m "feat(data-lineage): DTO extractor (POJO + Lombok + Jackson)"
```

## Task 2.2: project_scan pass — full symbol table

**Files:**
- Modify: `skills/data_lineage/passes/project_scan.py`
- Create: `tests/data_lineage/test_passes/test_project_scan.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_passes/test_project_scan.py`:
```python
from pathlib import Path

from skills.data_lineage.passes import project_scan


def test_collects_dto_classes(tmp_path: Path):
    (tmp_path / "Client.java").write_text(
        'package com.x;\n'
        '@lombok.Data\n'
        'public class Client { private String email; }\n'
    )
    table = project_scan.run(tmp_path)
    assert "com.x.Client" in table.classes
    cls = table.classes["com.x.Client"]
    assert "email" in cls.fields
    assert cls.getter_to_field == {"getEmail": "email"}


def test_skips_generated_directories(tmp_path: Path):
    (tmp_path / "generated").mkdir()
    (tmp_path / "generated" / "Gen.java").write_text(
        'package g; public class Gen { private int x; }\n'
    )
    table = project_scan.run(tmp_path)
    assert "g.Gen" not in table.classes
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Replace `passes/project_scan.py`**

```python
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
```

Note: We're replacing the placeholder `JavaClass` from Phase 0. Anything that imported `JavaClass` (only `project_scan.py` itself) must be updated. Other passes import `SymbolTable` only — they remain unchanged.

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_passes/test_project_scan.py tests/data_lineage/test_passes/test_smoke.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/passes/project_scan.py tests/data_lineage/test_passes/test_project_scan.py
git commit -m "feat(data-lineage): project_scan pass returns full SymbolTable"
```

## Task 2.3: java_dfa — variable assignment tracking

**Files:**
- Modify: `skills/data_lineage/passes/java_dfa.py`
- Create: `tests/data_lineage/test_passes/test_java_dfa.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_passes/test_java_dfa.py`:
```python
from pathlib import Path
from textwrap import dedent

from skills.data_lineage.passes import java_dfa
from skills.data_lineage.passes.project_scan import run as scan_project


def test_local_var_assignment_creates_transform_edge(tmp_path: Path):
    (tmp_path / "Svc.java").write_text(dedent('''
        public class Svc {
            public void send() {
                String email = repo.findEmail();
                String forwarded = email;
            }
        }
    '''))
    edges = java_dfa.run(tmp_path, scan_project(tmp_path), [])
    pairs = {(e.src_id, e.dst_id) for e in edges}
    assert ("code.var.email", "code.var.forwarded") in pairs


def test_dto_field_assignment_emits_edge(tmp_path: Path):
    (tmp_path / "Svc.java").write_text(dedent('''
        public class Svc {
            public void send() {
                Event ev = new Event();
                ev.email = userEmail;
            }
        }
    '''))
    edges = java_dfa.run(tmp_path, scan_project(tmp_path), [])
    assert any(e.src_id == "code.var.userEmail" and e.dst_id.endswith("ev.email") for e in edges)
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement**

`skills/data_lineage/passes/java_dfa.py`:
```python
"""Pass 4 — heuristic Java data-flow tracking.

Scope: per method body, track local variable assignments and DTO field
writes. Emits 'transform' edges between code.var.* nodes and code.var.<obj>.<field>
nodes. No type resolution — assumes the AST shape is local enough for
useful signal.

Confidence: every edge from this pass is at most 'medium'. graph_build
upgrades to 'high' when sql_parse already produced a high-confidence
read/write into the same code.var.
"""
from pathlib import Path

from skills.data_lineage.model import Edge, Evidence
from skills.data_lineage.ts_parser import parse_java

from .project_scan import SymbolTable
from .sql_extract import SqlUnit


_IGNORED_DIRS = {"generated", "build", "target", ".gradle", "node_modules", ".git"}


def run(project_root: Path, symbols: SymbolTable, units: list[SqlUnit]) -> list[Edge]:
    edges: list[Edge] = []
    for path in project_root.rglob("*.java"):
        if any(part in _IGNORED_DIRS for part in path.parts):
            continue
        rel = str(path.relative_to(project_root))
        source = path.read_bytes()
        tree = parse_java(source)
        for method in _iter_methods(tree.root_node):
            edges.extend(_method_edges(method, source, rel))
    return edges


def _iter_methods(node):
    if node.type in ("method_declaration", "constructor_declaration"):
        yield node
    for child in node.children:
        yield from _iter_methods(child)


def _method_edges(method, source: bytes, file: str) -> list[Edge]:
    body = method.child_by_field_name("body")
    if body is None:
        return []
    edges: list[Edge] = []

    for stmt in _iter_statements(body):
        # Local var declaration:  Type name = expr;
        if stmt.type == "local_variable_declaration":
            for decl in stmt.named_children:
                if decl.type != "variable_declarator":
                    continue
                target_n = decl.child_by_field_name("name")
                value_n = decl.child_by_field_name("value")
                if target_n is None or value_n is None:
                    continue
                tgt = source[target_n.start_byte:target_n.end_byte].decode()
                src = _expr_source(value_n, source)
                if src is None:
                    continue
                edges.append(_edge(file, stmt, src, f"code.var.{tgt}"))

        # Plain assignment:  x = expr;  ev.email = expr;
        elif stmt.type == "expression_statement":
            inner = stmt.named_children[0] if stmt.named_children else None
            if inner is not None and inner.type == "assignment_expression":
                left_n = inner.child_by_field_name("left")
                right_n = inner.child_by_field_name("right")
                if left_n is None or right_n is None:
                    continue
                tgt = _lhs_id(left_n, source)
                src = _expr_source(right_n, source)
                if tgt is None or src is None:
                    continue
                edges.append(_edge(file, stmt, src, tgt))

    return edges


def _iter_statements(node):
    yield node
    for child in node.children:
        yield from _iter_statements(child)


def _lhs_id(node, source: bytes) -> str | None:
    if node.type == "identifier":
        return f"code.var.{source[node.start_byte:node.end_byte].decode()}"
    if node.type == "field_access":
        obj_n = node.child_by_field_name("object")
        field_n = node.child_by_field_name("field")
        if obj_n is None or field_n is None:
            return None
        obj = source[obj_n.start_byte:obj_n.end_byte].decode()
        field = source[field_n.start_byte:field_n.end_byte].decode()
        return f"code.var.{obj}.{field}"
    return None


def _expr_source(node, source: bytes) -> str | None:
    if node.type == "identifier":
        return f"code.var.{source[node.start_byte:node.end_byte].decode()}"
    if node.type == "field_access":
        obj_n = node.child_by_field_name("object")
        field_n = node.child_by_field_name("field")
        if obj_n is not None and field_n is not None:
            obj = source[obj_n.start_byte:obj_n.end_byte].decode()
            field = source[field_n.start_byte:field_n.end_byte].decode()
            return f"code.var.{obj}.{field}"
    return None


def _edge(file: str, stmt, src: str, dst: str) -> Edge:
    return Edge(
        src_id=src,
        dst_id=dst,
        kind="transform",
        evidence=(Evidence(file=file, line=stmt.start_point[0] + 1,
                           pattern="dfa_assign", snippet=""),),
        confidence="medium",
    )
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_passes/test_java_dfa.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/passes/java_dfa.py tests/data_lineage/test_passes/test_java_dfa.py
git commit -m "feat(data-lineage): java_dfa — local var + field assignment tracking"
```

## Task 2.4: java_dfa — getter calls resolved via SymbolTable

**Files:**
- Modify: `skills/data_lineage/passes/java_dfa.py`
- Modify: `tests/data_lineage/test_passes/test_java_dfa.py`

- [ ] **Step 1: Failing test (append to existing test file)**

```python
def test_getter_call_resolves_to_dto_field(tmp_path: Path):
    (tmp_path / "Client.java").write_text(dedent('''
        package c;
        @lombok.Data public class Client { private String email; }
    '''))
    (tmp_path / "Svc.java").write_text(dedent('''
        package c;
        public class Svc {
            public void run(Client client) {
                String e = client.getEmail();
            }
        }
    '''))
    edges = java_dfa.run(tmp_path, scan_project(tmp_path), [])
    assert any(e.src_id == "code.var.client.email" and e.dst_id == "code.var.e" for e in edges)
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Extend `_expr_source` and pass `symbols` through**

Modify the `run` function to thread `symbols` down into helpers, and extend `_expr_source` to handle `method_invocation` whose name is a known getter:

Replace `_method_edges` signature and `_expr_source` body:

```python
def _method_edges(method, source: bytes, file: str, symbols: SymbolTable) -> list[Edge]:
    body = method.child_by_field_name("body")
    if body is None:
        return []
    edges: list[Edge] = []
    getter_index = _build_getter_index(symbols)
    for stmt in _iter_statements(body):
        if stmt.type == "local_variable_declaration":
            for decl in stmt.named_children:
                if decl.type != "variable_declarator":
                    continue
                target_n = decl.child_by_field_name("name")
                value_n = decl.child_by_field_name("value")
                if target_n is None or value_n is None:
                    continue
                tgt = source[target_n.start_byte:target_n.end_byte].decode()
                src = _expr_source(value_n, source, getter_index)
                if src is None:
                    continue
                edges.append(_edge(file, stmt, src, f"code.var.{tgt}"))
        elif stmt.type == "expression_statement":
            inner = stmt.named_children[0] if stmt.named_children else None
            if inner is not None and inner.type == "assignment_expression":
                left_n = inner.child_by_field_name("left")
                right_n = inner.child_by_field_name("right")
                if left_n is None or right_n is None:
                    continue
                tgt = _lhs_id(left_n, source)
                src = _expr_source(right_n, source, getter_index)
                if tgt is None or src is None:
                    continue
                edges.append(_edge(file, stmt, src, tgt))
    return edges


def _build_getter_index(symbols: SymbolTable) -> dict[str, str]:
    """Return getter-name → field-name across all known DTOs (last writer wins)."""
    idx: dict[str, str] = {}
    for dto in symbols.classes.values():
        for getter, field in dto.getter_to_field.items():
            idx[getter] = field
    return idx


def _expr_source(node, source: bytes, getter_index: dict[str, str]) -> str | None:
    if node.type == "identifier":
        return f"code.var.{source[node.start_byte:node.end_byte].decode()}"
    if node.type == "field_access":
        obj_n = node.child_by_field_name("object")
        field_n = node.child_by_field_name("field")
        if obj_n is not None and field_n is not None:
            obj = source[obj_n.start_byte:obj_n.end_byte].decode()
            field = source[field_n.start_byte:field_n.end_byte].decode()
            return f"code.var.{obj}.{field}"
    if node.type == "method_invocation":
        obj_n = node.child_by_field_name("object")
        name_n = node.child_by_field_name("name")
        if obj_n is None or name_n is None:
            return None
        method = source[name_n.start_byte:name_n.end_byte].decode()
        if method in getter_index:
            obj = source[obj_n.start_byte:obj_n.end_byte].decode()
            return f"code.var.{obj}.{getter_index[method]}"
    return None
```

Update the outer `run` to pass `symbols` into `_method_edges`:
```python
        for method in _iter_methods(tree.root_node):
            edges.extend(_method_edges(method, source, rel, symbols))
```

- [ ] **Step 4: Run all DFA tests**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_passes/test_java_dfa.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/passes/java_dfa.py tests/data_lineage/test_passes/test_java_dfa.py
git commit -m "feat(data-lineage): java_dfa resolves getter calls via SymbolTable"
```

---

# Phase 3 — Kafka + REST + graph_build

Goal: detect KafkaTemplate.send / @KafkaListener / @RestController / RestClient calls; resolve payloads via SymbolTable; finalize graph_build to merge edges, register nodes, and assign confidence.

## Task 3.1: Kafka producer extractor

**Files:**
- Create: `skills/data_lineage/extractors/kafka.py`
- Create: `tests/data_lineage/test_extractors/test_kafka.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_extractors/test_kafka.py`:
```python
from textwrap import dedent

from skills.data_lineage.extractors.kafka import extract


def test_kafka_template_send_with_string_topic():
    code = dedent('''
        public class Pub {
            public void emit(ClientUpdateEvent ev) {
                kafkaTemplate.send("client-updates", ev);
            }
        }
    ''').encode()
    sites = extract(code, file="Pub.java")
    sends = [s for s in sites if s.kind == "kafka_send"]
    assert len(sends) == 1
    assert sends[0].topic == "client-updates"
    assert sends[0].payload_var == "ev"


def test_kafka_listener_with_topics_attribute():
    code = dedent('''
        public class L {
            @KafkaListener(topics = "client-updates")
            public void onMsg(ClientUpdateEvent ev) {}
        }
    ''').encode()
    sites = extract(code, file="L.java")
    listens = [s for s in sites if s.kind == "kafka_listen"]
    assert len(listens) == 1
    assert listens[0].topic == "client-updates"
    assert listens[0].payload_type == "ClientUpdateEvent"


def test_send_with_non_string_topic_skipped():
    code = dedent('''
        public class P {
            public void emit(String topic, Foo ev) {
                kafkaTemplate.send(topic, ev);
            }
        }
    ''').encode()
    sites = extract(code, file="P.java")
    assert all(s.kind != "kafka_send" for s in sites)
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement**

`skills/data_lineage/extractors/kafka.py`:
```python
"""Spring Kafka detector: KafkaTemplate.send sites + @KafkaListener methods."""
from dataclasses import dataclass

from skills.data_lineage.ts_parser import parse_java


@dataclass(frozen=True)
class KafkaSite:
    file: str
    line: int
    kind: str             # "kafka_send" | "kafka_listen"
    topic: str | None
    payload_var: str | None    # for kafka_send
    payload_type: str | None   # for kafka_listen


def extract(source: bytes, file: str) -> list[KafkaSite]:
    tree = parse_java(source)
    out: list[KafkaSite] = []
    _walk(tree.root_node, source, file, out)
    return out


def _walk(node, source: bytes, file: str, out: list[KafkaSite]) -> None:
    if node.type == "method_invocation":
        site = _try_send(node, source, file)
        if site is not None:
            out.append(site)
    if node.type == "method_declaration":
        site = _try_listener(node, source, file)
        if site is not None:
            out.append(site)
    for child in node.children:
        _walk(child, source, file, out)


def _try_send(node, source: bytes, file: str) -> KafkaSite | None:
    name_n = node.child_by_field_name("name")
    if name_n is None or source[name_n.start_byte:name_n.end_byte].decode() != "send":
        return None
    args = node.child_by_field_name("arguments")
    if args is None or args.named_child_count < 2:
        return None
    topic_n = args.named_children[0]
    if topic_n.type != "string_literal":
        return None
    topic = source[topic_n.start_byte:topic_n.end_byte].decode().strip('"')
    payload_n = args.named_children[-1]
    if payload_n.type != "identifier":
        return None
    payload_var = source[payload_n.start_byte:payload_n.end_byte].decode()
    return KafkaSite(
        file=file,
        line=node.start_point[0] + 1,
        kind="kafka_send",
        topic=topic,
        payload_var=payload_var,
        payload_type=None,
    )


def _try_listener(method, source: bytes, file: str) -> KafkaSite | None:
    topic = _listener_topic(method, source)
    if topic is None:
        return None
    params = method.child_by_field_name("parameters")
    payload_type: str | None = None
    if params is not None:
        for p in params.named_children:
            if p.type == "formal_parameter":
                type_n = p.child_by_field_name("type")
                if type_n is not None:
                    payload_type = source[type_n.start_byte:type_n.end_byte].decode()
                    break
    return KafkaSite(
        file=file,
        line=method.start_point[0] + 1,
        kind="kafka_listen",
        topic=topic,
        payload_var=None,
        payload_type=payload_type,
    )


def _listener_topic(method, source: bytes) -> str | None:
    for child in method.children:
        if child.type != "modifiers":
            continue
        for sub in child.children:
            if sub.type not in ("annotation", "marker_annotation"):
                continue
            n = sub.child_by_field_name("name")
            if n is None:
                continue
            if source[n.start_byte:n.end_byte].decode().split(".")[-1] != "KafkaListener":
                continue
            args = sub.child_by_field_name("arguments")
            if args is None:
                continue
            for a in args.named_children:
                if a.type == "element_value_pair":
                    key_n = a.child_by_field_name("key")
                    val_n = a.child_by_field_name("value")
                    if key_n is None or val_n is None:
                        continue
                    if source[key_n.start_byte:key_n.end_byte].decode() == "topics":
                        if val_n.type == "string_literal":
                            return source[val_n.start_byte:val_n.end_byte].decode().strip('"')
                elif a.type == "string_literal":
                    return source[a.start_byte:a.end_byte].decode().strip('"')
    return None
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_extractors/test_kafka.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/extractors/kafka.py tests/data_lineage/test_extractors/test_kafka.py
git commit -m "feat(data-lineage): Kafka producer/consumer extractor"
```

## Task 3.2: REST extractor

**Files:**
- Create: `skills/data_lineage/extractors/rest.py`
- Create: `tests/data_lineage/test_extractors/test_rest.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_extractors/test_rest.py`:
```python
from textwrap import dedent

from skills.data_lineage.extractors.rest import extract


def test_get_mapping_with_response_dto():
    code = dedent('''
        @RestController
        public class ClientApi {
            @GetMapping("/clients/{id}")
            public ClientDto get(@PathVariable Long id) { return null; }
        }
    ''').encode()
    sites = extract(code, file="ClientApi.java")
    endpoints = [s for s in sites if s.kind == "rest_endpoint"]
    assert len(endpoints) == 1
    assert endpoints[0].http_method == "GET"
    assert endpoints[0].path == "/clients/{id}"
    assert endpoints[0].response_type == "ClientDto"


def test_post_mapping_with_request_body():
    code = dedent('''
        @RestController
        public class ClientApi {
            @PostMapping("/clients")
            public ClientDto create(@RequestBody ClientCreateDto in) { return null; }
        }
    ''').encode()
    sites = extract(code, file="ClientApi.java")
    ep = [s for s in sites if s.kind == "rest_endpoint"][0]
    assert ep.http_method == "POST"
    assert ep.request_body_type == "ClientCreateDto"
    assert ep.response_type == "ClientDto"


def test_rest_client_post_call():
    code = dedent('''
        public class Svc {
            public void notifyExt(NotifyDto dto) {
                restClient.post().uri("/notify").body(dto).retrieve();
            }
        }
    ''').encode()
    sites = extract(code, file="Svc.java")
    calls = [s for s in sites if s.kind == "rest_client_call"]
    assert len(calls) == 1
    assert calls[0].http_method == "POST"
    assert calls[0].path == "/notify"
    assert calls[0].request_body_var == "dto"
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement**

`skills/data_lineage/extractors/rest.py`:
```python
"""Spring Web detector: @RestController endpoints and RestClient outbound calls."""
from dataclasses import dataclass

from skills.data_lineage.ts_parser import parse_java


_HTTP_BY_ANNOTATION = {
    "GetMapping": "GET",
    "PostMapping": "POST",
    "PutMapping": "PUT",
    "DeleteMapping": "DELETE",
    "PatchMapping": "PATCH",
}


@dataclass(frozen=True)
class RestSite:
    file: str
    line: int
    kind: str                       # "rest_endpoint" | "rest_client_call"
    http_method: str
    path: str | None
    request_body_type: str | None    # for endpoints
    response_type: str | None        # for endpoints
    request_body_var: str | None     # for client calls


def extract(source: bytes, file: str) -> list[RestSite]:
    tree = parse_java(source)
    out: list[RestSite] = []
    _walk(tree.root_node, source, file, out)
    return out


def _walk(node, source: bytes, file: str, out: list[RestSite]) -> None:
    if node.type == "method_declaration":
        ep = _try_endpoint(node, source, file)
        if ep is not None:
            out.append(ep)
    if node.type == "method_invocation":
        call = _try_client_call(node, source, file)
        if call is not None:
            out.append(call)
    for child in node.children:
        _walk(child, source, file, out)


def _try_endpoint(method, source: bytes, file: str) -> RestSite | None:
    http_method, path = _endpoint_annotation(method, source)
    if http_method is None:
        return None
    response_type = None
    type_n = method.child_by_field_name("type")
    if type_n is not None:
        response_type = source[type_n.start_byte:type_n.end_byte].decode()
    request_body_type = None
    params = method.child_by_field_name("parameters")
    if params is not None:
        for p in params.named_children:
            if p.type == "formal_parameter" and _has_request_body(p, source):
                t = p.child_by_field_name("type")
                if t is not None:
                    request_body_type = source[t.start_byte:t.end_byte].decode()
                    break
    return RestSite(
        file=file, line=method.start_point[0] + 1,
        kind="rest_endpoint", http_method=http_method, path=path,
        request_body_type=request_body_type,
        response_type=response_type,
        request_body_var=None,
    )


def _endpoint_annotation(method, source: bytes):
    for child in method.children:
        if child.type != "modifiers":
            continue
        for sub in child.children:
            if sub.type not in ("annotation", "marker_annotation"):
                continue
            n = sub.child_by_field_name("name")
            if n is None:
                continue
            ann = source[n.start_byte:n.end_byte].decode().split(".")[-1]
            if ann not in _HTTP_BY_ANNOTATION:
                continue
            http_method = _HTTP_BY_ANNOTATION[ann]
            path = None
            args = sub.child_by_field_name("arguments")
            if args is not None:
                for a in args.named_children:
                    if a.type == "string_literal":
                        path = source[a.start_byte:a.end_byte].decode().strip('"')
                        break
                    if a.type == "element_value_pair":
                        key_n = a.child_by_field_name("key")
                        val_n = a.child_by_field_name("value")
                        if key_n is None or val_n is None:
                            continue
                        if source[key_n.start_byte:key_n.end_byte].decode() in ("value", "path"):
                            if val_n.type == "string_literal":
                                path = source[val_n.start_byte:val_n.end_byte].decode().strip('"')
                                break
            return http_method, path
    return None, None


def _has_request_body(param, source: bytes) -> bool:
    for child in param.children:
        if child.type == "modifiers":
            for sub in child.children:
                if sub.type in ("annotation", "marker_annotation"):
                    n = sub.child_by_field_name("name")
                    if n is None:
                        continue
                    if source[n.start_byte:n.end_byte].decode().split(".")[-1] == "RequestBody":
                        return True
    return False


def _try_client_call(node, source: bytes, file: str) -> RestSite | None:
    http_method = _restclient_method(node, source)
    if http_method is None:
        return None
    path = _chain_uri(node, source)
    body_var = _chain_body(node, source)
    return RestSite(
        file=file, line=node.start_point[0] + 1,
        kind="rest_client_call", http_method=http_method, path=path,
        request_body_type=None, response_type=None,
        request_body_var=body_var,
    )


def _restclient_method(node, source: bytes) -> str | None:
    cur = node
    while cur is not None and cur.type == "method_invocation":
        name_n = cur.child_by_field_name("name")
        if name_n is None:
            break
        m = source[name_n.start_byte:name_n.end_byte].decode()
        if m in {"get", "post", "put", "delete", "patch"}:
            obj_n = cur.child_by_field_name("object")
            if obj_n is not None and obj_n.type == "identifier":
                ident = source[obj_n.start_byte:obj_n.end_byte].decode().lower()
                if "client" in ident:
                    return m.upper()
        cur = cur.child_by_field_name("object")
    return None


def _chain_uri(node, source: bytes) -> str | None:
    cur = node
    while cur is not None and cur.type == "method_invocation":
        name_n = cur.child_by_field_name("name")
        if name_n is not None and source[name_n.start_byte:name_n.end_byte].decode() == "uri":
            args = cur.child_by_field_name("arguments")
            if args is not None and args.named_child_count >= 1:
                first = args.named_children[0]
                if first.type == "string_literal":
                    return source[first.start_byte:first.end_byte].decode().strip('"')
        cur = cur.child_by_field_name("object")
    return None


def _chain_body(node, source: bytes) -> str | None:
    cur = node
    while cur is not None and cur.type == "method_invocation":
        name_n = cur.child_by_field_name("name")
        if name_n is not None and source[name_n.start_byte:name_n.end_byte].decode() == "body":
            args = cur.child_by_field_name("arguments")
            if args is not None and args.named_child_count >= 1:
                first = args.named_children[0]
                if first.type == "identifier":
                    return source[first.start_byte:first.end_byte].decode()
        cur = cur.child_by_field_name("object")
    return None
```

- [ ] **Step 4: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_extractors/test_rest.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/extractors/rest.py tests/data_lineage/test_extractors/test_rest.py
git commit -m "feat(data-lineage): REST endpoint + RestClient extractor"
```

## Task 3.3: graph_build — node registration + Kafka/REST edges

**Files:**
- Modify: `skills/data_lineage/passes/graph_build.py`
- Modify: `skills/data_lineage/passes/java_dfa.py` (no — graph_build owns this)
- Modify: `skills/data_lineage/passes/sql_extract.py` to also surface Kafka/REST sites
- Create: `tests/data_lineage/test_passes/test_graph_build.py`

Decision: Kafka/REST detection runs inside `java_dfa` (it already walks every `.java` file). Then `graph_build` consumes the resulting edges plus a list of `KafkaSite`/`RestSite` records returned alongside.

To avoid threading new types through every pass signature, extend `java_dfa.run` to return a tuple `(edges, kafka_sites, rest_sites)`. CLI passes them to `graph_build.run`.

- [ ] **Step 1: Update `java_dfa.run` signature**

Modify `passes/java_dfa.py`:

```python
from skills.data_lineage.extractors import kafka as kafka_extract, rest as rest_extract
from skills.data_lineage.extractors.kafka import KafkaSite
from skills.data_lineage.extractors.rest import RestSite


def run(
    project_root: Path, symbols: SymbolTable, units: list[SqlUnit]
) -> tuple[list[Edge], list[KafkaSite], list[RestSite]]:
    edges: list[Edge] = []
    kafka_sites: list[KafkaSite] = []
    rest_sites: list[RestSite] = []
    for path in project_root.rglob("*.java"):
        if any(part in _IGNORED_DIRS for part in path.parts):
            continue
        rel = str(path.relative_to(project_root))
        source = path.read_bytes()
        tree = parse_java(source)
        for method in _iter_methods(tree.root_node):
            edges.extend(_method_edges(method, source, rel, symbols))
        kafka_sites.extend(kafka_extract.extract(source, file=rel))
        rest_sites.extend(rest_extract.extract(source, file=rel))
    return edges, kafka_sites, rest_sites
```

Update `cli.py`:
```python
java_edges, kafka_sites, rest_sites = java_dfa.run(project_root, symbols, units)
graph = graph_build.run(symbols, sql_edges, java_edges,
                        kafka_sites=kafka_sites, rest_sites=rest_sites)
```

Update `tests/data_lineage/test_passes/test_smoke.py` and `test_java_dfa.py` to unpack the tuple — change `edges = java_dfa.run(...)` to `edges, _, _ = java_dfa.run(...)`.

- [ ] **Step 2: Failing test**

`tests/data_lineage/test_passes/test_graph_build.py`:
```python
from skills.data_lineage.extractors.kafka import KafkaSite
from skills.data_lineage.extractors.rest import RestSite
from skills.data_lineage.extractors.dto import Dto, DtoField
from skills.data_lineage.model import Edge, Evidence
from skills.data_lineage.passes import graph_build
from skills.data_lineage.passes.project_scan import SymbolTable


def _ev(): return (Evidence("X.java", 1, "p", "s"),)


def test_graph_registers_db_nodes_for_sql_edges():
    sql_edges = [Edge("db.users.email", "code.var.email", "read", _ev(), "high")]
    g = graph_build.run(SymbolTable(), sql_edges, [], kafka_sites=[], rest_sites=[])
    assert "db.users" in g.nodes
    assert "db.users.email" in g.nodes
    assert g.nodes["db.users.email"].parent_id == "db.users"


def test_graph_kafka_send_produces_topic_and_field_nodes():
    dto = Dto(fqn="x.Event", file="X.java",
              fields={"email": DtoField("email", "String", "user_email", False)},
              getter_to_field={"getEmail": "email"}, annotations=())
    syms = SymbolTable(classes={"x.Event": dto})
    sites = [KafkaSite(file="P.java", line=2, kind="kafka_send",
                       topic="client-updates", payload_var="ev", payload_type=None)]
    g = graph_build.run(syms, [], [], kafka_sites=sites, rest_sites=[])
    assert "kafka.client-updates" in g.nodes
    assert "kafka.client-updates.user_email" in g.nodes


def test_rest_endpoint_creates_endpoint_and_response_field_nodes():
    dto = Dto(fqn="x.ClientDto", file="X.java",
              fields={"id": DtoField("id", "Long", "id", False)},
              getter_to_field={"getId": "id"}, annotations=())
    syms = SymbolTable(classes={"x.ClientDto": dto})
    sites = [RestSite(file="A.java", line=3, kind="rest_endpoint",
                      http_method="GET", path="/clients/{id}",
                      request_body_type=None, response_type="ClientDto",
                      request_body_var=None)]
    g = graph_build.run(syms, [], [], kafka_sites=[], rest_sites=sites)
    assert "http.GET:/clients/{id}" in g.nodes
    assert "http.GET:/clients/{id}.response.id" in g.nodes
```

- [ ] **Step 3: Run, see fail**

- [ ] **Step 4: Implement `graph_build`**

`skills/data_lineage/passes/graph_build.py`:
```python
"""Pass 5 — assemble the unified Graph.

Inputs:
- SymbolTable (DTOs, getters)
- SQL edges from sql_parse (always 'high' confidence)
- Java DFA edges (always 'medium' / 'low')
- Kafka sites and REST sites (turned into nodes + write/read edges to DTO fields)

Output: Graph with all referenced nodes registered. Confidence semantics:
- high  — sourced from sqlglot/jOOQ-typed DSL
- medium — DTO-resolved Java edge OR Kafka/REST site connected to a known DTO
- low   — Java edge whose target is unresolved
"""
from skills.data_lineage.extractors.dto import Dto
from skills.data_lineage.extractors.kafka import KafkaSite
from skills.data_lineage.extractors.rest import RestSite
from skills.data_lineage.model import Edge, Evidence, Graph, Node, Unresolved

from .project_scan import SymbolTable


def run(
    symbols: SymbolTable,
    sql_edges: list[Edge],
    java_edges: list[Edge],
    *,
    kafka_sites: list[KafkaSite],
    rest_sites: list[RestSite],
    unresolved: list[Unresolved] | None = None,
) -> Graph:
    g = Graph()
    g.unresolved.extend(unresolved or [])

    for e in sql_edges:
        _register_for_edge(g, e)
        g.edges.append(e)

    for e in java_edges:
        _register_for_edge(g, e)
        g.edges.append(e)

    for site in kafka_sites:
        _register_kafka(g, site, symbols)

    for site in rest_sites:
        _register_rest(g, site, symbols)

    return g


def _register_for_edge(g: Graph, e: Edge) -> None:
    for nid in (e.src_id, e.dst_id):
        if nid in g.nodes:
            continue
        g.nodes[nid] = _node_for_id(nid)
    parent = g.nodes[e.src_id].parent_id
    if parent and parent not in g.nodes:
        g.nodes[parent] = _node_for_id(parent)
    parent = g.nodes[e.dst_id].parent_id
    if parent and parent not in g.nodes:
        g.nodes[parent] = _node_for_id(parent)


def _node_for_id(node_id: str) -> Node:
    parts = node_id.split(".")
    if node_id.startswith("db."):
        if len(parts) == 3:
            return Node(id=node_id, kind="db.column", name=f"{parts[1]}.{parts[2]}",
                        parent_id=f"db.{parts[1]}", attrs={})
        return Node(id=node_id, kind="db.table", name=parts[1], parent_id=None, attrs={})
    if node_id.startswith("kafka."):
        if len(parts) == 3:
            return Node(id=node_id, kind="kafka.field",
                        name=f"{parts[1]}.{parts[2]}",
                        parent_id=f"kafka.{parts[1]}", attrs={})
        return Node(id=node_id, kind="kafka.topic", name=parts[1],
                    parent_id=None, attrs={})
    if node_id.startswith("http."):
        prefix, _, suffix = node_id.partition(".")
        # endpoint: "http.<METHOD>:<path>"
        # field:    "http.<METHOD>:<path>.{request,response}.<field>"
        # heuristic: if last segment after the last '.' is a known field part, treat as field
        if ".response." in node_id or ".request." in node_id:
            base, sep, field = node_id.rpartition(".")
            return Node(id=node_id, kind="http.field", name=field,
                        parent_id=base.rsplit(".", 1)[0], attrs={})
        return Node(id=node_id, kind="http.endpoint", name=suffix, parent_id=None,
                    attrs={})
    if node_id.startswith("code."):
        return Node(id=node_id, kind="code.method",
                    name=node_id, parent_id=None, attrs={})
    return Node(id=node_id, kind="code.method", name=node_id, parent_id=None, attrs={})


def _register_kafka(g: Graph, site: KafkaSite, symbols: SymbolTable) -> None:
    topic_id = f"kafka.{site.topic}"
    if topic_id not in g.nodes:
        g.nodes[topic_id] = Node(id=topic_id, kind="kafka.topic",
                                 name=site.topic or "", parent_id=None, attrs={})

    dto = _dto_for_kafka(site, symbols)
    ev = Evidence(file=site.file, line=site.line,
                  pattern=site.kind, snippet=f"kafka {site.kind} {site.topic}")
    if dto is None:
        # No payload resolved — single coarse edge to topic.
        if site.kind == "kafka_send" and site.payload_var:
            src = f"code.var.{site.payload_var}"
            if src not in g.nodes:
                g.nodes[src] = _node_for_id(src)
            g.edges.append(Edge(src_id=src, dst_id=topic_id,
                                kind="write", evidence=(ev,), confidence="low"))
        return

    direction = "write" if site.kind == "kafka_send" else "read"
    for f in dto.fields.values():
        if f.ignored:
            continue
        field_id = f"{topic_id}.{f.serialized_as}"
        if field_id not in g.nodes:
            g.nodes[field_id] = Node(id=field_id, kind="kafka.field",
                                     name=f"{site.topic}.{f.serialized_as}",
                                     parent_id=topic_id, attrs={})
        if direction == "write":
            src = f"code.var.{site.payload_var}.{f.name}" if site.payload_var else f"code.dto.{dto.fqn}.{f.name}"
        else:
            src = field_id
        if direction == "write":
            if src not in g.nodes:
                g.nodes[src] = _node_for_id(src)
            g.edges.append(Edge(src_id=src, dst_id=field_id,
                                kind="write", evidence=(ev,), confidence="medium"))
        else:
            dst = f"code.dto.{dto.fqn}.{f.name}"
            if dst not in g.nodes:
                g.nodes[dst] = _node_for_id(dst)
            g.edges.append(Edge(src_id=field_id, dst_id=dst,
                                kind="read", evidence=(ev,), confidence="medium"))


def _register_rest(g: Graph, site: RestSite, symbols: SymbolTable) -> None:
    ev = Evidence(file=site.file, line=site.line,
                  pattern=site.kind, snippet=f"{site.http_method} {site.path}")

    if site.kind == "rest_endpoint":
        endpoint_id = f"http.{site.http_method}:{site.path}"
        if endpoint_id not in g.nodes:
            g.nodes[endpoint_id] = Node(id=endpoint_id, kind="http.endpoint",
                                        name=f"{site.http_method} {site.path}",
                                        parent_id=None,
                                        attrs={"method": site.http_method,
                                               "path": site.path or ""})
        if site.response_type:
            _add_dto_fields(g, endpoint_id, site.response_type, "response", symbols, ev,
                            direction="read")
        if site.request_body_type:
            _add_dto_fields(g, endpoint_id, site.request_body_type, "request", symbols, ev,
                            direction="write")
        return

    # rest_client_call
    endpoint_id = f"http.{site.http_method}:{site.path or ''}"
    if endpoint_id not in g.nodes:
        g.nodes[endpoint_id] = Node(id=endpoint_id, kind="http.endpoint",
                                    name=f"{site.http_method} {site.path or ''}",
                                    parent_id=None,
                                    attrs={"method": site.http_method,
                                           "path": site.path or "",
                                           "outbound": "true"})
    if site.request_body_var:
        src = f"code.var.{site.request_body_var}"
        if src not in g.nodes:
            g.nodes[src] = _node_for_id(src)
        g.edges.append(Edge(src_id=src, dst_id=endpoint_id,
                            kind="write", evidence=(ev,), confidence="low"))


def _dto_for_kafka(site: KafkaSite, symbols: SymbolTable) -> Dto | None:
    if site.payload_type:
        for fqn, dto in symbols.classes.items():
            if fqn.endswith("." + site.payload_type) or fqn == site.payload_type:
                return dto
    return None


def _add_dto_fields(g: Graph, endpoint_id: str, type_name: str, side: str,
                    symbols: SymbolTable, ev: Evidence, *, direction: str) -> None:
    dto = None
    for fqn, candidate in symbols.classes.items():
        if fqn.endswith("." + type_name) or fqn == type_name:
            dto = candidate
            break
    if dto is None:
        return
    for f in dto.fields.values():
        if f.ignored:
            continue
        field_id = f"{endpoint_id}.{side}.{f.serialized_as}"
        g.nodes[field_id] = Node(id=field_id, kind="http.field",
                                 name=f.serialized_as, parent_id=endpoint_id, attrs={})
        if direction == "read":
            dst = f"code.dto.{dto.fqn}.{f.name}"
            if dst not in g.nodes:
                g.nodes[dst] = _node_for_id(dst)
            g.edges.append(Edge(src_id=field_id, dst_id=dst,
                                kind="read", evidence=(ev,), confidence="medium"))
        else:
            src = f"code.dto.{dto.fqn}.{f.name}"
            if src not in g.nodes:
                g.nodes[src] = _node_for_id(src)
            g.edges.append(Edge(src_id=src, dst_id=field_id,
                                kind="write", evidence=(ev,), confidence="medium"))
```

- [ ] **Step 5: Run, see pass**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_passes/test_graph_build.py -v`
Expected: 3 passed.

- [ ] **Step 6: Run all data_lineage tests**

Run: `PYTHONPATH=. pytest tests/data_lineage -v`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add skills/data_lineage/passes/graph_build.py skills/data_lineage/passes/java_dfa.py \
        skills/data_lineage/cli.py tests/data_lineage/test_passes/test_graph_build.py \
        tests/data_lineage/test_passes/test_java_dfa.py tests/data_lineage/test_passes/test_smoke.py
git commit -m "feat(data-lineage): graph_build merges SQL/DFA/Kafka/REST into unified graph"
```

## Task 3.4: Synthetic Spring Boot fixture

**Files:**
- Create: `tests/data_lineage/fixtures/synthetic-spring-app/` (~10 source files)

- [ ] **Step 1: Create fixture directory tree**

```bash
mkdir -p tests/data_lineage/fixtures/synthetic-spring-app/{api,app,db}/src/main/java/com/example
mkdir -p tests/data_lineage/fixtures/synthetic-spring-app/db/src/main/java/com/example/generated/tables
```

- [ ] **Step 2: Write `Clients.java` (jOOQ generated table stub)**

`tests/data_lineage/fixtures/synthetic-spring-app/db/src/main/java/com/example/generated/tables/Clients.java`:
```java
package com.example.generated.tables;

public final class Clients {
    public static final Clients CLIENTS = new Clients();
    public final String EMAIL = "email";
    public final String ID = "id";
    public final String STATUS = "status";
}
```

The fixture isn't compiled — only parsed; constants don't need to be valid jOOQ.

- [ ] **Step 3: Write `Devices.java`**

`tests/data_lineage/fixtures/synthetic-spring-app/db/src/main/java/com/example/generated/tables/Devices.java`:
```java
package com.example.generated.tables;

public final class Devices {
    public static final Devices DEVICES = new Devices();
    public final String ID = "id";
    public final String MAC = "mac";
    public final String OWNER_ID = "owner_id";
    public final String LAST_SEEN = "last_seen";
}
```

- [ ] **Step 4: Write `ClientRepository.java`**

`tests/data_lineage/fixtures/synthetic-spring-app/db/src/main/java/com/example/ClientRepository.java`:
```java
package com.example;

import com.example.generated.tables.Clients;

public class ClientRepository {
    public Object findActive() {
        return dsl.select(CLIENTS.ID, CLIENTS.EMAIL).from(CLIENTS).fetch();
    }
    public void insertClient(String email) {
        dsl.insertInto(CLIENTS, CLIENTS.EMAIL).values(email).execute();
    }
    @Query("SELECT c FROM Client c WHERE c.email = :email")
    public Object findByEmail(String email) { return null; }
}
```

- [ ] **Step 5: Write `DeviceRepository.java`**

`tests/data_lineage/fixtures/synthetic-spring-app/db/src/main/java/com/example/DeviceRepository.java`:
```java
package com.example;

public class DeviceRepository {
    public Object load(int id) {
        return jdbc.queryForObject(
            "SELECT id, mac, owner_id FROM devices WHERE id = ?",
            new Object[]{id}, deviceMapper);
    }
    public void touch(int id) {
        jdbc.update("UPDATE devices SET last_seen = NOW() WHERE id = ?", id);
    }
    public Object dynamic(String filter) {
        StringBuilder sb = new StringBuilder("SELECT * FROM devices WHERE ");
        sb.append(filter);
        return jdbc.query(sb.toString(), deviceMapper);
    }
}
```

- [ ] **Step 6: Write `ClientDto.java`**

`tests/data_lineage/fixtures/synthetic-spring-app/app/src/main/java/com/example/ClientDto.java`:
```java
package com.example;

@lombok.Data
public class ClientDto {
    private Long id;
    @JsonProperty("user_email") private String email;
    private String status;
}
```

- [ ] **Step 7: Write `ClientUpdateEvent.java`**

`tests/data_lineage/fixtures/synthetic-spring-app/app/src/main/java/com/example/ClientUpdateEvent.java`:
```java
package com.example;

@lombok.Data
public class ClientUpdateEvent {
    private Long id;
    @JsonProperty("user_email") private String email;
}
```

- [ ] **Step 8: Write `ClientService.java`**

`tests/data_lineage/fixtures/synthetic-spring-app/app/src/main/java/com/example/ClientService.java`:
```java
package com.example;

public class ClientService {
    public void publishUpdate(ClientDto dto) {
        ClientUpdateEvent ev = new ClientUpdateEvent();
        ev.id = dto.getId();
        ev.email = dto.getEmail();
        kafkaTemplate.send("client-updates", ev);
    }
    public void notifyExternal(ClientDto dto) {
        restClient.post().uri("/notify").body(dto).retrieve();
    }
}
```

- [ ] **Step 9: Write `ClientUpdateListener.java`**

`tests/data_lineage/fixtures/synthetic-spring-app/app/src/main/java/com/example/ClientUpdateListener.java`:
```java
package com.example;

public class ClientUpdateListener {
    @KafkaListener(topics = "client-updates")
    public void onMsg(ClientUpdateEvent ev) {
        // consumer body
    }
}
```

- [ ] **Step 10: Write `ClientApi.java`**

`tests/data_lineage/fixtures/synthetic-spring-app/api/src/main/java/com/example/ClientApi.java`:
```java
package com.example;

@RestController
public class ClientApi {
    @GetMapping("/clients/{id}")
    public ClientDto get(@PathVariable Long id) { return null; }

    @PostMapping("/clients")
    public ClientDto create(@RequestBody ClientDto in) { return null; }
}
```

- [ ] **Step 11: Commit**

```bash
git add tests/data_lineage/fixtures/synthetic-spring-app
git commit -m "test(data-lineage): synthetic spring-boot fixture"
```

## Task 3.5: Integration test on synthetic fixture

**Files:**
- Modify: `tests/data_lineage/test_integration.py`

- [ ] **Step 1: Add the integration test**

Append to `tests/data_lineage/test_integration.py`:
```python
import json
from pathlib import Path

FIXTURE = Path(__file__).parent / "fixtures" / "synthetic-spring-app"


def test_synthetic_spring_app_end_to_end(tmp_path: Path):
    out = tmp_path / "out"
    code = run_pipeline(FIXTURE, output_dir=out, formats=("json",), quiet=True)
    assert code == 0
    blob = json.loads((out / "lineage-graph.json").read_text())

    pairs = {(e["src_id"], e["dst_id"]) for e in blob["edges"]}

    # SQL → variable read
    assert ("db.clients.id", "code.var.id") in pairs
    assert ("db.clients.email", "code.var.email") in pairs

    # JdbcTemplate read
    assert ("db.devices.mac", "code.var.mac") in pairs

    # Kafka topic node exists
    assert any(n["id"] == "kafka.client-updates" for n in blob["nodes"])
    assert any(n["id"] == "kafka.client-updates.user_email" for n in blob["nodes"])

    # REST endpoint
    assert any(n["id"] == "http.GET:/clients/{id}" for n in blob["nodes"])

    # Range bounds — sanity check
    assert 20 <= len(blob["nodes"]) <= 100
    assert 10 <= len(blob["edges"]) <= 100
```

- [ ] **Step 2: Run, see pass (or fail and debug)**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_integration.py::test_synthetic_spring_app_end_to_end -v`
Expected: pass.

If anything fails, inspect:
```bash
PYTHONPATH=. python -m skills.data_lineage tests/data_lineage/fixtures/synthetic-spring-app --output /tmp/dl --quiet
cat /tmp/dl/lineage-report.txt
```
Fix the offending pass/extractor. Tests for that pass should be augmented with the missing case before re-running integration.

- [ ] **Step 3: Commit**

```bash
git add tests/data_lineage/test_integration.py
git commit -m "test(data-lineage): integration on synthetic spring app"
```

---

# Phase 4 — Polish, debug flag, filters, docs

## Task 4.1: Filters (`--only-table`, `--only-topic`, `--max-depth`)

**Files:**
- Modify: `skills/data_lineage/filters.py`
- Create: `tests/data_lineage/test_filters.py`

- [ ] **Step 1: Failing test**

`tests/data_lineage/test_filters.py`:
```python
from skills.data_lineage.filters import FilterSpec, apply
from skills.data_lineage.model import Edge, Evidence, Graph, Node


def _ev(): return (Evidence("X", 1, "p", "s"),)


def _build():
    g = Graph()
    for nid, kind in [
        ("db.clients", "db.table"),
        ("db.clients.email", "db.column"),
        ("code.var.email", "code.method"),
        ("kafka.client-updates", "kafka.topic"),
        ("kafka.client-updates.email", "kafka.field"),
        ("db.devices", "db.table"),
        ("db.devices.id", "db.column"),
    ]:
        g.nodes[nid] = Node(id=nid, kind=kind, name=nid, parent_id=None, attrs={})
    g.edges = [
        Edge("db.clients.email", "code.var.email", "read", _ev(), "high"),
        Edge("code.var.email", "kafka.client-updates.email", "write", _ev(), "medium"),
        Edge("db.devices.id", "code.var.dev_id", "read", _ev(), "high"),
    ]
    return g


def test_only_table_keeps_edges_touching_table():
    g = _build()
    out = apply(g, FilterSpec(only_table="clients"))
    src_ids = {e.src_id for e in out.edges}
    assert "db.clients.email" in src_ids
    assert "db.devices.id" not in src_ids


def test_only_topic_keeps_edges_touching_topic():
    g = _build()
    out = apply(g, FilterSpec(only_topic="client-updates"))
    assert any(e.dst_id == "kafka.client-updates.email" for e in out.edges)
    assert all("devices" not in e.src_id for e in out.edges)


def test_max_depth_limits_chain_from_seeds():
    g = _build()
    out = apply(g, FilterSpec(only_table="clients", max_depth=1))
    # depth 1 keeps edges starting from the seed, not the next hop
    src_ids = {e.src_id for e in out.edges}
    dst_ids = {e.dst_id for e in out.edges}
    assert "db.clients.email" in src_ids
    assert "kafka.client-updates.email" not in dst_ids
```

- [ ] **Step 2: Implement**

Replace `filters.py`:

```python
"""Subgraph filters for the lineage Graph."""
from collections import deque
from dataclasses import dataclass

from skills.data_lineage.model import Graph


@dataclass(frozen=True)
class FilterSpec:
    only_table: str | None = None
    only_topic: str | None = None
    max_depth: int | None = None


def apply(graph: Graph, spec: FilterSpec) -> Graph:
    if spec.only_table is None and spec.only_topic is None and spec.max_depth is None:
        return graph

    seeds = _seeds(graph, spec)
    if not seeds:
        return Graph()

    keep_node_ids = _bfs(graph, seeds, max_depth=spec.max_depth)
    new = Graph()
    for nid in keep_node_ids:
        if nid in graph.nodes:
            new.nodes[nid] = graph.nodes[nid]
    for e in graph.edges:
        if e.src_id in keep_node_ids and e.dst_id in keep_node_ids:
            new.edges.append(e)
    new.unresolved.extend(graph.unresolved)
    return new


def _seeds(graph: Graph, spec: FilterSpec) -> set[str]:
    seeds: set[str] = set()
    if spec.only_table:
        prefix = f"db.{spec.only_table}"
        seeds |= {nid for nid in graph.nodes if nid == prefix or nid.startswith(prefix + ".")}
    if spec.only_topic:
        prefix = f"kafka.{spec.only_topic}"
        seeds |= {nid for nid in graph.nodes if nid == prefix or nid.startswith(prefix + ".")}
    if not spec.only_table and not spec.only_topic:
        seeds = set(graph.nodes.keys())
    return seeds


def _bfs(graph: Graph, seeds: set[str], *, max_depth: int | None) -> set[str]:
    adj_out: dict[str, list[str]] = {}
    adj_in: dict[str, list[str]] = {}
    for e in graph.edges:
        adj_out.setdefault(e.src_id, []).append(e.dst_id)
        adj_in.setdefault(e.dst_id, []).append(e.src_id)

    visited = set(seeds)
    queue = deque((s, 0) for s in seeds)
    while queue:
        node, depth = queue.popleft()
        if max_depth is not None and depth >= max_depth:
            continue
        for nbr in adj_out.get(node, []) + adj_in.get(node, []):
            if nbr in visited:
                continue
            visited.add(nbr)
            queue.append((nbr, depth + 1))
    return visited
```

- [ ] **Step 3: Run filter tests**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_filters.py -v`
Expected: 3 passed.

- [ ] **Step 4: Smoke through CLI**

```bash
PYTHONPATH=. python -m skills.data_lineage tests/data_lineage/fixtures/synthetic-spring-app \
    --output /tmp/dl --formats json --quiet --only-table clients
python -c "import json; b=json.load(open('/tmp/dl/lineage-graph.json')); \
    assert all('devices' not in n['id'] for n in b['nodes']); print('ok')"
```
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/filters.py tests/data_lineage/test_filters.py
git commit -m "feat(data-lineage): filters — only-table/only-topic/max-depth"
```

## Task 4.2: `--debug` writes intermediate stage artifacts

**Files:**
- Modify: `skills/data_lineage/cli.py`

- [ ] **Step 1: Test (append to test_integration.py)**

```python
def test_debug_flag_writes_intermediate_artifacts(tmp_path: Path):
    out = tmp_path / "out"
    code = run_pipeline(FIXTURE, output_dir=out, formats=("json",), quiet=True, debug=True)
    assert code == 0
    debug_dir = out / "debug"
    assert (debug_dir / "01-symbol-table.json").exists()
    assert (debug_dir / "02-sql-units.json").exists()
    assert (debug_dir / "03-sql-edges.json").exists()
    assert (debug_dir / "04-java-edges.json").exists()
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement — modify `cli.py`**

Add `debug: bool = False` parameter to `run_pipeline`. After each pass, when `debug=True`, write the intermediate artifact:

```python
import json
from dataclasses import asdict


def _write_debug_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if hasattr(data, "__dict__") or isinstance(data, list):
        try:
            blob = [asdict(x) for x in data] if isinstance(data, list) else asdict(data)
        except TypeError:
            blob = repr(data)
    else:
        blob = repr(data)
    path.write_text(json.dumps(blob, indent=2, default=str))


def run_pipeline(
    project_root: Path,
    *,
    output_dir: Path,
    formats: tuple[str, ...] = ("text", "json", "mermaid"),
    filter_spec: FilterSpec | None = None,
    quiet: bool = False,
    debug: bool = False,
) -> int:
    debug_dir = output_dir / "debug"
    symbols = project_scan.run(project_root)
    if debug:
        _write_debug_json(debug_dir / "01-symbol-table.json",
                          {fqn: asdict(d) for fqn, d in symbols.classes.items()})
    units = sql_extract.run(project_root, symbols)
    if debug:
        _write_debug_json(debug_dir / "02-sql-units.json", units)
    sql_edges = sql_parse.run(units)
    if debug:
        _write_debug_json(debug_dir / "03-sql-edges.json", sql_edges)
    java_edges, kafka_sites, rest_sites = java_dfa.run(project_root, symbols, units)
    if debug:
        _write_debug_json(debug_dir / "04-java-edges.json", java_edges)
    graph = graph_build.run(symbols, sql_edges, java_edges,
                            kafka_sites=kafka_sites, rest_sites=rest_sites)
    if filter_spec is not None:
        graph = apply_filters(graph, filter_spec)
    output_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        filename, fn = _FORMAT_FILES[fmt]
        (output_dir / filename).write_text(fn(graph))
    if not quiet and "text" in formats:
        sys.stdout.write((output_dir / "lineage-report.txt").read_text())
    return 0
```

Wire the CLI flag through:
```python
return run_pipeline(args.project_path, output_dir=args.output, formats=formats,
                    filter_spec=spec, quiet=args.quiet, debug=args.debug)
```

- [ ] **Step 4: Run integration test**

Run: `PYTHONPATH=. pytest tests/data_lineage/test_integration.py::test_debug_flag_writes_intermediate_artifacts -v`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add skills/data_lineage/cli.py tests/data_lineage/test_integration.py
git commit -m "feat(data-lineage): --debug writes intermediate stage JSON"
```

## Task 4.3: SKILL.md and README link

**Files:**
- Create: `skills/data_lineage/SKILL.md`
- Modify: `README.md`

- [ ] **Step 1: Write `SKILL.md`**

`skills/data_lineage/SKILL.md`:
```markdown
# data_lineage

Skill for column-level data lineage on Spring Boot / jOOQ / JdbcTemplate / Spring Kafka / Spring Web (RestClient) projects.

Sibling to `open_table_migrator` — different purpose (lineage vs migration), different stack (OLTP vs OLAP). Pure Python via tree-sitter (Java) and sqlglot.

## What it does

Scans a Java project and emits three artifacts:

- `lineage-report.txt` — human-readable report grouped by confidence
- `lineage-graph.json` — full graph (nodes + edges + unresolved)
- `lineage.mmd` — Mermaid diagram for embedding in Markdown / GitHub

Coverage:

| Sink/Source | Detection |
|---|---|
| jOOQ DSL | typed `TABLE.COLUMN` chains: select / selectFrom / insertInto / update / deleteFrom |
| JdbcTemplate | string-literal SQL in query/queryForObject/update/etc. |
| Spring Data | `@Query` and derived `findByX` methods |
| Spring Kafka | `KafkaTemplate.send(...)` + `@KafkaListener(topics=...)` |
| Spring Web | `@RestController` + `@GetMapping`/etc. + `@RequestBody` + RestClient calls |
| DTOs | POJO fields, Lombok `@Data`, Jackson `@JsonProperty(name=...)` rename, `@JsonIgnore` |

Out of scope today: Oracle stored procedures, Spring Cache (Ehcache), Avro/Protobuf schemas. Tracked in the design doc as Phase 5.

## CLI

```bash
PYTHONPATH=. python -m skills.data_lineage <project_path> [options]

Options:
  --output DIR             Where to put artifacts (default: cwd)
  --formats LIST           Comma-separated subset of {text,json,mermaid}
  --only-table NAME        Filter to a table's neighborhood
  --only-topic NAME        Filter to a Kafka topic's neighborhood
  --max-depth N            BFS depth limit from filter seeds
  --quiet                  Don't print the text report to stdout
  --debug                  Write intermediate stage JSON in <output>/debug/
  --strict                 (reserved) non-zero exit if unresolved > 0
```

## Confidence levels

- `high` — column edges from sqlglot or typed jOOQ DSL
- `medium` — convention-based heuristics (Lombok getter ↔ field, Spring Data `findByX`, Jackson rename)
- `low` — Java DFA edge whose target couldn't be resolved into a DTO

`low` edges live in their own section of the text report so the user sees the boundary of the automation.

## Architecture

Six-pass pipeline:
```
project_scan → sql_extract → sql_parse → java_dfa → graph_build → render
```

Each pass returns frozen data; renderers are read-only consumers. See [docs/superpowers/specs/2026-05-07-data-lineage-design.md](../../docs/superpowers/specs/2026-05-07-data-lineage-design.md) for the full design.

## Limitations

Hard mismatches will land in `unresolved`:
- SQL built via `StringBuilder` / `String.format` / `String.join`
- MapStruct mappers, custom Jackson serializers
- Reflection-based serialization
- Stored procedures (Phase 5)
```

- [ ] **Step 2: Add a section in `README.md`**

Append to `README.md` after the migrator's "Структура" section, before "Детектор: tree-sitter AST":

```markdown
---

## Sibling skill: data_lineage

Repo также содержит [`skills/data_lineage/`](skills/data_lineage/SKILL.md) — column-level lineage для Spring Boot / jOOQ / JdbcTemplate / Spring Kafka / Spring Web проектов. Принцип тот же (tree-sitter + чистый Python), цель другая (граф потоков данных вместо миграции).

```bash
PYTHONPATH=. python -m skills.data_lineage <project_path>
```
```

- [ ] **Step 3: Commit**

```bash
git add skills/data_lineage/SKILL.md README.md
git commit -m "docs(data-lineage): SKILL.md + README pointer"
```

## Task 4.4: Subagent definition

**Files:**
- Create: `.claude/agents/data-lineage.md`

- [ ] **Step 1: Write the subagent file**

`.claude/agents/data-lineage.md`:
```markdown
---
name: data-lineage
description: Use this agent when the user wants to build column-level data lineage for a Spring Boot / jOOQ / JdbcTemplate / Spring Kafka / Spring Web project. The agent runs the data_lineage CLI, summarizes the resulting graph, surfaces low-confidence edges and unresolved sites, and can render filtered subgraphs as Mermaid on request. Examples:

<example>
Context: User has a Spring Boot service and wants to understand data flow.
user: "проанализируй data lineage в проекте"
assistant: "Запускаю data-lineage агента — он построит column-level граф потоков."
<commentary>Direct trigger phrase.</commentary>
</example>

<example>
Context: English request.
user: "build data lineage for this project"
assistant: "I'll run the data-lineage agent to produce the column-level graph."
<commentary>Same agent, English trigger.</commentary>
</example>

(Tools: Bash, Read, Edit, Write)
---

# data-lineage subagent

Operate the `skills/data_lineage` CLI on the user's Java project, then interpret the result for the human in chat.

## Default flow

1. Run `PYTHONPATH=. python -m skills.data_lineage <project_path> --output /tmp/dl-<run_id> --quiet`.
2. Parse `/tmp/dl-<run_id>/lineage-report.txt`. Show the user:
   - node count, edge count, breakdown by confidence
   - top 10 `low`-confidence edges with their evidence
   - count of `unresolved` and a sample of 5
3. Ask: do they want a filtered subgraph for a specific table or topic?
4. If yes, re-run with `--only-table X` or `--only-topic Y`, parse `lineage.mmd`, embed it inline in the chat.

## Constraints

- The skill is read-only; never propose code edits as part of lineage interpretation.
- If the user reports edges that look wrong, do not write a hint file — by design the skill has none. Instead, capture the disagreement in plain prose so the user can fold it into a future skill upgrade.
- Don't run on huge projects without `--only-table` first; offer the filter if `--debug` shows >5000 SQL units.
```

- [ ] **Step 2: Verify it loads**

Run: `ls .claude/agents/data-lineage.md`
Expected: file exists.

- [ ] **Step 3: Commit**

```bash
git add .claude/agents/data-lineage.md
git commit -m "feat(data-lineage): subagent definition"
```

## Task 4.5: Final full-suite run + cleanup

- [ ] **Step 1: Run the full data_lineage test suite**

Run: `PYTHONPATH=. pytest tests/data_lineage -v`
Expected: all green.

- [ ] **Step 2: Run the existing migrator suite to ensure no regression**

Run: `PYTHONPATH=. pytest tests/ --ignore=tests/fixtures -v`
Expected: 236 (migrator) + new lineage tests, all green.

- [ ] **Step 3: Smoke run on the synthetic fixture**

```bash
PYTHONPATH=. python -m skills.data_lineage \
    tests/data_lineage/fixtures/synthetic-spring-app \
    --output /tmp/dl-final
```
Expected: text report prints; `/tmp/dl-final/lineage-graph.json` and `lineage.mmd` exist.

- [ ] **Step 4: Final commit if any cleanup needed**

If steps surfaced trailing fixes:
```bash
git add -A
git commit -m "chore(data-lineage): final suite cleanup"
```

If no changes — skip.

---

# Phase 5 — Deferred

Out of scope for this plan. Triggered by feedback from the real-project run (the user's "Client Profile" project):

- Oracle Stored Procedures: `extractors/oracle_sp.py` reads `.sql` files / DDL dumps; sqlglot in `read="oracle"` parses PL/SQL bodies; new pass `pl_sql_parse.py` walks procedures and emits column-level edges within them.
- Spring Cache / Ehcache: `extractors/cache.py` for `@Cacheable` / `@CachePut` — usually side-channel rather than data flow; will likely become a separate node kind `cache.region`.
- Avro / Protobuf: `extractors/avro.py` and `extractors/protobuf.py` parse `.avsc` and `.proto`; consumed by `kafka.py` extractor when payload type matches a registered schema.

Each is a separate spec + plan once Phase 4 is in real-world use.
