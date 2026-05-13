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
PYTHONPATH=. python3 -m skills.data_lineage <project_path> [options]
```

Options:
- `--output DIR` — Where to put artifacts (default: cwd)
- `--formats LIST` — Comma-separated subset of `{text,json,mermaid}`
- `--only-table NAME` — Filter to a table's neighborhood
- `--only-topic NAME` — Filter to a Kafka topic's neighborhood
- `--max-depth N` — BFS depth limit from filter seeds
- `--quiet` — Don't print the text report to stdout
- `--debug` — Write intermediate stage JSON in `<output>/debug/`
- `--strict` — (reserved) non-zero exit if unresolved > 0

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

Filter cross-table contamination: `--only-table X` walks BFS through `code.var.*` nodes; when two tables share a column variable name (e.g. both have `id`), the subgraph may include both. This is by design — variables are global across the analyzed scope. Use `--max-depth 1` for stricter scoping.
