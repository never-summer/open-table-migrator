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
