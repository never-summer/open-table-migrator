# Data Lineage Skill — Design

**Date:** 2026-05-07
**Status:** Approved for implementation planning
**Branch:** `feat/data-lineage`

## Goal

Add a sibling skill `skills/data_lineage/` to this repo that builds **column-level data lineage** for Spring Boot / jOOQ / JdbcTemplate / Spring Kafka / Spring Web (RestClient) projects. Output: text report, JSON graph, Mermaid diagram. No external system ingestion.

The skill is independent from `open_table_migrator` — different purpose (lineage vs migration), different stack (OLTP vs OLAP). Shares no code in MVP; both skills happen to use tree-sitter and follow the same report-style conventions, but each keeps its own `ts_parser.py`. Extracting a shared module is explicitly out of scope.

## Non-goals

- No code rewriting (lineage is read-only).
- No runtime / OpenLineage agent. Pure static analysis.
- No external system ingestion (DataHub, Marquez, OpenLineage receivers). If needed later — separate `export_*` module.
- No JVM dependency. Pure Python skill, like `open_table_migrator`.
- No hint/config file. User-provided overrides are out of scope; the analysis runs without configuration.

## Target stack

Reference architecture (from user-provided photo of the system being analyzed):

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3.5.x |
| Build | Gradle 8.x |
| Primary DB | PostgreSQL via HikariCP + jOOQ |
| Secondary DB | Oracle via JdbcTemplate + Stored Procedures |
| Cache | Spring Cache + Ehcache |
| Messaging | Spring Kafka |
| HTTP | Spring Web + RestClient |
| Build layout | Multi-module: `api`, `app`, `db` |

## Scope

### MVP (sources / sinks covered)

1. **jOOQ DSL** — typed Java code (`dsl.selectFrom(USERS).where(...)`, `USERS.EMAIL`).
2. **JdbcTemplate / NamedParameterJdbcTemplate** — SQL string literals.
3. **Spring Data `@Query`** — SQL/JPQL in annotations + derived query methods (`findByEmail`).
4. **Spring Kafka** — `KafkaTemplate.send(...)`, `@KafkaListener`, payload DTO.
5. **Spring Web / RestClient** — `@RestController`, `@RequestMapping` family, `restClient.post(...)`, request/response DTO.

### Phase 5 (deferred, after real-project validation)

- Oracle Stored Procedures (PL/SQL bodies parsed via sqlglot).
- Spring Cache / Ehcache (`@Cacheable`, `@CachePut`).
- Avro `.avsc` and Protobuf `.proto` schema files for Kafka payloads.

### Granularity

**Column-level.** Edges connect specific columns / fields, not just tables / topics.

### Output

Text report + `lineage-graph.json` + `lineage.mmd` (Mermaid). Generated in one CLI invocation.

## Architecture

### Pipeline

Six sequential passes; each produces a frozen value consumed by the next.

```
1. project-scan      → tree-sitter walks .java; collects classes, fields, methods, DTO shapes → SymbolTable
2. sql-extract       → tree-sitter finds SQL literals (@Query, JdbcTemplate args) and jOOQ DSL chains → list[SqlUnit]
3. sql-parse         → sqlglot parses each SqlUnit; emits column-level edges within statements → list[Edge]
4. java-dfa          → tree-sitter heuristic: track variable assignments, DTO field flows, KafkaTemplate.send / RestClient.post sites → list[Edge]
5. graph-build       → merge sql edges + java edges + symbol table into unified Graph; resolve via name conventions
6. render            → text + json + mermaid (all three from the same Graph)
```

### Heuristic resolution (the key design choice)

In pass 5, edges are stitched together by **name convention**, not by type resolution:

- jOOQ getter `record.getEmail()` ↔ column `email`
- Lombok `@Data` field ↔ getter `getX()` / setter `setX(...)`
- Spring Data method-name parsing (`findByEmail` → reads column `email`)
- Jackson `@JsonProperty(name = "...")` for DTO field renaming when serialized

This is an **explicit tradeoff**: ~80% correct edges on typical Spring code, with no JVM dependency and no user configuration. The rest is exposed as `Unresolved` records in the graph (e.g. MapStruct mappers, custom Jackson serializers, `StringBuilder`-built SQL, reflection).

### Data model

All dataclasses, JSON-serializable, deterministic IDs (so `lineage-graph.json` diffs cleanly between runs). ID convention: `{kind_root}.{parent_name}.{name}` for leaves (`db.users.email`, `kafka.client-updates.email`), `{kind_root}.{name}` for containers (`db.users`, `kafka.client-updates`). HTTP endpoints encode method and path (`http.GET:/clients/{id}`).

```python
@dataclass
class Node:
    id: str                    # "db.users.email", "kafka.client-updates.email"
    kind: Literal["db.table", "db.column", "kafka.topic", "kafka.field",
                  "http.endpoint", "http.field", "code.method"]
    name: str
    parent_id: str | None      # column → table, field → topic
    attrs: dict[str, str]      # dialect, schema, http_method, ...

@dataclass
class Evidence:
    file: str                  # relative path
    line: int
    pattern: str               # "jooq_select", "kafka_send", "rest_post", "dfa_assign", ...
    snippet: str               # ≤120 chars, single line

@dataclass
class Edge:
    src_id: str
    dst_id: str
    kind: Literal["read", "write", "transform"]
    evidence: list[Evidence]   # one edge can be backed by multiple sites
    confidence: Literal["high", "medium", "low"]

@dataclass
class Unresolved:
    file: str
    line: int
    reason: str                # "mapstruct mapper", "string concat sql", "reflection", ...
    context: str

@dataclass
class Graph:
    nodes: dict[str, Node]
    edges: list[Edge]
    unresolved: list[Unresolved]
```

**Confidence semantics:**

- `high` — extracted directly from SQL/PL-SQL via sqlglot (precise AST), or from typed jOOQ DSL (`USERS.EMAIL`).
- `medium` — built via convention heuristics (Lombok-getter ↔ field, Spring Data method-name parsing, Jackson rename).
- `low` — connected via Java DFA with gaps (opaque method call mid-chain, unresolved generic, unknown DTO).

In the text renderer, `low` edges appear in a separate "approximate edges" section.

### Module layout

```
skills/data_lineage/
├── SKILL.md
├── __init__.py
├── __main__.py               # python -m skills.data_lineage
├── cli.py
│
├── model.py                  # Node, Edge, Evidence, Graph, Unresolved (dataclasses only)
├── ts_parser.py              # tree-sitter Java wrapper, Language/Parser cache
│
├── passes/
│   ├── __init__.py
│   ├── project_scan.py       # pass 1
│   ├── sql_extract.py        # pass 2
│   ├── sql_parse.py          # pass 3 (sqlglot)
│   ├── java_dfa.py           # pass 4
│   └── graph_build.py        # pass 5
│
├── extractors/
│   ├── __init__.py
│   ├── jooq.py
│   ├── jdbc_template.py
│   ├── spring_data.py
│   ├── kafka.py
│   ├── rest.py
│   └── dto.py                # POJO + Lombok @Data + Jackson @JsonProperty/@JsonIgnore
│
├── renderers/
│   ├── __init__.py
│   ├── text.py
│   ├── json.py
│   └── mermaid.py
│
└── filters.py                # --only-table, --only-topic, --max-depth
```

**Layout rules:**

- `passes/` are sequential pipeline stages: `def run(input: PrevStage) -> NextStage`. No cross-talk beyond inputs/outputs.
- `extractors/` encapsulate Spring/jOOQ/Kafka/JDBC API knowledge. Called from `sql_extract` and `java_dfa`.
- `renderers/` are read-only consumers of `Graph`.
- `model.py` contains only dataclasses (no logic, no imports cycles).
- `ts_parser.py` is the only file that imports `tree_sitter`.

**Explicit non-choices:**

- No orchestrator / manager class. `cli.py` calls passes directly.
- No DI / plugin loader for extractors. Their list is hardcoded in pass functions. Phase 5 additions (Oracle SP, Cache) get explicit imports + calls when they land.
- `sql_parse.py` uses `sqlglot` directly. If `sqllineage` is added as a fallback later, an adapter is introduced then — not preemptively.

## CLI

```bash
# Full analysis, default formats (text + json + mermaid)
python -m skills.data_lineage <project_path>

# Filters
python -m skills.data_lineage <project_path> --only-table users
python -m skills.data_lineage <project_path> --only-topic client-updates
python -m skills.data_lineage <project_path> --max-depth 3

# Output control
python -m skills.data_lineage <project_path> --output ./out/
python -m skills.data_lineage <project_path> --formats json,mermaid
python -m skills.data_lineage <project_path> --quiet

# Debug — write intermediate stage artifacts
python -m skills.data_lineage <project_path> --debug
```

**Default artifacts** (when `--output` not given, written to cwd):

```
lineage-report.txt
lineage-graph.json
lineage.mmd
```

**`--debug` artifacts:**

```
out/debug/
├── 01-symbol-table.json
├── 02-sql-units.json
├── 03-sql-edges.json
└── 04-java-edges.json
```

**Exit codes:**

- `0` — analysis completed (even with `unresolved` entries)
- `1` — fatal error (invalid path, tree-sitter failure)
- `2` — `--strict` mode and `unresolved > N` threshold hit (off by default)

**Ignored paths** (hardcoded, like migrator): `generated/`, `target/`, `build/`, `.gradle/`, `node_modules/`.

## Subagent

`.claude/agents/data-lineage.md` — triggers on:

- *"проанализируй data lineage в проекте"*
- *"построй граф потоков данных"*
- *"build data lineage for this project"*

Flow:

1. Run CLI without filters; parse `lineage-report.txt`.
2. Show summary: node count, edge count, breakdown by confidence, unresolved count.
3. Show top 10 `low`-confidence edges; ask user to confirm or skip.
4. Show unresolved section; user can either accept or note manually (no hint file is written — by design).
5. On user request, render filtered subgraph (`--only-table X`) and emit Mermaid into the chat.

## Testing strategy

Three levels.

### Level 1 — extractor unit tests (`tests/data_lineage/test_extractors/`)

One file per extractor (`test_jooq.py`, `test_jdbc_template.py`, etc.). Mini Java snippets inline via `dedent`. No filesystem, no full pipeline.

### Level 2 — pass-level tests (`tests/data_lineage/test_passes/`)

One file per pass. Hand-built input dataclasses → expected output dataclasses. Validates pass logic in isolation; this is the main regression shield.

### Level 3 — integration on synthetic Spring Boot app

`tests/data_lineage/fixtures/synthetic-spring-app/` — minimal real Spring Boot multi-module project mirroring the target stack:

```
synthetic-spring-app/
├── build.gradle.kts
├── settings.gradle.kts
├── api/                                # DTO + REST
│   └── src/main/java/.../ClientApi.java
├── app/                                # business + Kafka + RestClient
│   └── src/main/java/.../
│       ├── ClientService.java
│       ├── dto/
│       │   ├── ClientDto.java          # Lombok @Data + Jackson @JsonProperty
│       │   └── ClientUpdateEvent.java
│       └── kafka/
│           └── ClientUpdateListener.java
└── db/                                 # repositories
    └── src/main/java/.../
        ├── ClientRepository.java       # jOOQ DSL
        ├── DeviceRepository.java       # JdbcTemplate
        └── generated/
            └── tables/
                ├── Clients.java
                └── Devices.java
```

Covers all five MVP patterns. Includes one **deliberately broken** case: a method that builds SQL via `StringBuilder` — must land in `unresolved` with `reason="string concat sql"`.

Tests do not invoke `gradle`; they parse `.java` via tree-sitter directly.

Integration assertions are **point-checks on key edges**, not whole-graph snapshots:

```python
assert graph.has_edge("db.clients.email", "kafka.client-updates.email")
assert graph.has_edge("http.GET:/clients/{id}.response.email", "db.clients.email")
assert 30 <= len(graph.nodes) <= 60
assert any("string concat" in u.reason for u in graph.unresolved)
```

### Snapshot tests

Only for individual renderers (`text.py`, `json.py`, `mermaid.py`) on small hand-built graphs. Never on the full integration graph (would become noise).

### Coverage target

Per-extractor case coverage, not line %. Each extractor: ≥5 unit tests covering typical and edge cases.

## Implementation phases

| Phase | Scope | Outcome |
|---|---|---|
| **0 — skeleton** | `model.py`, `ts_parser.py`, `cli.py` stub, dummy pass returning empty Graph, all 3 renderers on empty input | Repository structure in place; tests on model/render |
| **1 — SQL side** | `extractors/jdbc_template.py`, `spring_data.py`, `jooq.py`, `passes/sql_extract.py`, `passes/sql_parse.py` | Column-lineage *within* SQL statements, no Java DFA. Already useful: "this column is read in these files" |
| **2 — Java DFA + DTO** | `extractors/dto.py`, `passes/project_scan.py`, `passes/java_dfa.py` | Symbol table + intra-method tracking + cross-method via direct calls |
| **3 — Kafka + REST** | `extractors/kafka.py`, `extractors/rest.py`, `passes/graph_build.py` finalized | End-to-end on synthetic-spring-app: column → DTO → Kafka topic → REST endpoint |
| **4 — finish** | `--debug`, `--strict`, filters, `SKILL.md`, README update, `.claude/agents/data-lineage.md`, real-project validation | MVP shippable |
| **5 — deferred** | Oracle SP (sqlglot on PL/SQL bodies), Spring Cache, Avro/Protobuf schemas | After real-project feedback shapes priorities |

Each phase results in a working, growing MVP. Phase 1 is already useful on its own.

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| jOOQ DSL semantics deeper than expected (joins, subqueries) | medium | MVP covers basic select/insert/update/delete; complex chains → `unresolved` with clear reason |
| Java DFA produces many `low`-confidence edges on real code | high | `high/medium/low` separation already in the model; renderer surfaces `low` distinctly |
| Lombok `@Builder` / `@SuperBuilder` / MapStruct mappers / custom Jackson serializers | high | Out of MVP scope; explicitly land in `unresolved`. No hint file by design — user sees the gap |
| sqlglot fails on niche Oracle syntax (`CONNECT BY`, complex `MERGE`) | low (MVP), medium (phase 5) | Wrap each statement; per-statement failure → `unresolved`, rest of graph still builds |
| Slow tree-sitter pass on huge projects (10k+ Java files) | medium | Tree-sitter is fast natively; if hit, add `multiprocessing.Pool` to `project_scan`. Not optimized preemptively |
| Generated jOOQ code dominates the project | medium | Hardcoded ignore list (`generated/`, `target/`, `build/`); not configurable |

## Open questions deferred to implementation

These do **not** block the design:

- Mermaid node ID format with dots (`db.users.email`) — escaping decided in `renderers/mermaid.py`.
- `Edge.evidence` JSON shape (list of dicts vs joined string) — decided in `renderers/json.py`.
- Symlink behavior for `--project_path`: do not follow (matches migrator).

## Dependencies

Single new runtime dependency: `sqlglot`. No JVM, no `dot` binary, no node tooling.

## Estimated size

~3000 lines of Python (migrator is ~3500). ~150–200 tests across three levels. Estimated 4–6 substantive implementation sessions.
