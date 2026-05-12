from textwrap import dedent

from skills.data_lineage.extractors.dto import extract


def test_plain_pojo_fields_and_getters():
    code = dedent('''
        public class Client {
            private String email;
            private Long id;
            public String getEmail() { return email; }
            public Long getId() { return id; }
        }
    ''').encode()
    dtos = extract(code, file="Client.java")
    assert len(dtos) == 1
    dto = dtos[0]
    assert dto.fqn.endswith("Client")
    assert {"email", "id"} <= set(dto.fields.keys())
    assert dto.getter_to_field["getEmail"] == "email"


def test_lombok_data_class_has_implicit_getters():
    code = dedent('''
        @lombok.Data
        public class Client {
            private String email;
            private Long id;
        }
    ''').encode()
    dtos = extract(code, file="Client.java")
    dto = dtos[0]
    assert dto.getter_to_field["getEmail"] == "email"
    assert dto.getter_to_field["getId"] == "id"


def test_jackson_property_renames_serialized_name():
    code = dedent('''
        public class Event {
            @JsonProperty("user_id")
            private Long userId;
        }
    ''').encode()
    dto = extract(code, file="Event.java")[0]
    assert dto.fields["userId"].serialized_as == "user_id"


def test_jackson_ignore_excludes_field_from_serialization():
    code = dedent('''
        public class Event {
            private Long id;
            @JsonIgnore private String secret;
        }
    ''').encode()
    dto = extract(code, file="Event.java")[0]
    assert dto.fields["secret"].ignored
    assert not dto.fields["id"].ignored
