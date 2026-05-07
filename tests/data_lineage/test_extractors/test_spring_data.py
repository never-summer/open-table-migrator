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
