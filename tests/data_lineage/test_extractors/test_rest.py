from textwrap import dedent

from skills.data_lineage.extractors.rest import extract


def test_get_mapping_with_response_dto():
    code = dedent('''
        @RestController
        public class ClientApi {
            @GetMapping("/clients/{id}")
            public ClientDto get(@PathVariable Long id) { return null; }
        }
    ''').encode()
    sites = extract(code, file="ClientApi.java")
    endpoints = [s for s in sites if s.kind == "rest_endpoint"]
    assert len(endpoints) == 1
    assert endpoints[0].http_method == "GET"
    assert endpoints[0].path == "/clients/{id}"
    assert endpoints[0].response_type == "ClientDto"


def test_post_mapping_with_request_body():
    code = dedent('''
        @RestController
        public class ClientApi {
            @PostMapping("/clients")
            public ClientDto create(@RequestBody ClientCreateDto in) { return null; }
        }
    ''').encode()
    sites = extract(code, file="ClientApi.java")
    ep = [s for s in sites if s.kind == "rest_endpoint"][0]
    assert ep.http_method == "POST"
    assert ep.request_body_type == "ClientCreateDto"
    assert ep.response_type == "ClientDto"


def test_rest_client_post_call():
    code = dedent('''
        public class Svc {
            public void notifyExt(NotifyDto dto) {
                restClient.post().uri("/notify").body(dto).retrieve();
            }
        }
    ''').encode()
    sites = extract(code, file="Svc.java")
    calls = [s for s in sites if s.kind == "rest_client_call"]
    assert len(calls) == 1
    assert calls[0].http_method == "POST"
    assert calls[0].path == "/notify"
    assert calls[0].request_body_var == "dto"
