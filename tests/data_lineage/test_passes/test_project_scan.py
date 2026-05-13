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
