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
