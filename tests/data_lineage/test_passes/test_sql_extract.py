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
