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
