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
