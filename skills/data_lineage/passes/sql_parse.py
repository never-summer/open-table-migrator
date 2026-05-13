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
    # ins.this is an exp.Schema node wrapping the table + column list
    schema = ins.this if isinstance(ins.this, exp.Schema) else None
    if schema is None:
        return []
    table = schema.this
    if not isinstance(table, exp.Table):
        return []
    table_name = table.name.lower()
    # Schema expressions are Identifier nodes (not Column nodes)
    columns = []
    for node in schema.expressions:
        if isinstance(node, (exp.Column, exp.Identifier)):
            columns.append(node.name.lower())
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
