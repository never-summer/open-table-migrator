"""Spring Web detector: @RestController endpoints and RestClient outbound calls."""
from dataclasses import dataclass

from skills.data_lineage.ts_parser import parse_java


_HTTP_BY_ANNOTATION = {
    "GetMapping": "GET",
    "PostMapping": "POST",
    "PutMapping": "PUT",
    "DeleteMapping": "DELETE",
    "PatchMapping": "PATCH",
}

_HTTP_METHOD_NAMES = {"get", "post", "put", "delete", "patch"}


@dataclass(frozen=True)
class RestSite:
    file: str
    line: int
    kind: str                       # "rest_endpoint" | "rest_client_call"
    http_method: str
    path: str | None
    request_body_type: str | None    # for endpoints
    response_type: str | None        # for endpoints
    request_body_var: str | None     # for client calls


def extract(source: bytes, file: str) -> list[RestSite]:
    tree = parse_java(source)
    out: list[RestSite] = []
    _walk(tree.root_node, source, file, out, parent=None)
    return out


def _walk(node, source: bytes, file: str, out: list[RestSite], parent) -> None:
    if node.type == "method_declaration":
        ep = _try_endpoint(node, source, file)
        if ep is not None:
            out.append(ep)
    if node.type == "method_invocation":
        # Only handle as a potential RestClient call site if this node is NOT
        # the `object` (i.e. inner chain member) of another method_invocation.
        # That means the parent must not be a method_invocation, OR this node
        # is not the value of the parent's "object" field.
        parent_obj = parent.child_by_field_name("object") if (
            parent is not None and parent.type == "method_invocation"
        ) else None
        is_inner_chain = (
            parent_obj is not None
            and parent_obj.start_byte == node.start_byte
            and parent_obj.end_byte == node.end_byte
        )
        if not is_inner_chain:
            call = _try_client_call(node, source, file)
            if call is not None:
                out.append(call)
    for child in node.children:
        _walk(child, source, file, out, parent=node)


def _try_endpoint(method, source: bytes, file: str) -> RestSite | None:
    http_method, path = _endpoint_annotation(method, source)
    if http_method is None:
        return None
    response_type = None
    type_n = method.child_by_field_name("type")
    if type_n is not None:
        response_type = source[type_n.start_byte:type_n.end_byte].decode()
    request_body_type = None
    params = method.child_by_field_name("parameters")
    if params is not None:
        for p in params.named_children:
            if p.type == "formal_parameter" and _has_request_body(p, source):
                t = p.child_by_field_name("type")
                if t is not None:
                    request_body_type = source[t.start_byte:t.end_byte].decode()
                    break
    return RestSite(
        file=file, line=method.start_point[0] + 1,
        kind="rest_endpoint", http_method=http_method, path=path,
        request_body_type=request_body_type,
        response_type=response_type,
        request_body_var=None,
    )


def _endpoint_annotation(method, source: bytes):
    for child in method.children:
        if child.type != "modifiers":
            continue
        for sub in child.children:
            if sub.type not in ("annotation", "marker_annotation"):
                continue
            n = sub.child_by_field_name("name")
            if n is None:
                continue
            ann = source[n.start_byte:n.end_byte].decode().split(".")[-1]
            if ann not in _HTTP_BY_ANNOTATION:
                continue
            http_method = _HTTP_BY_ANNOTATION[ann]
            path = None
            args = sub.child_by_field_name("arguments")
            if args is not None:
                for a in args.named_children:
                    if a.type == "string_literal":
                        path = source[a.start_byte:a.end_byte].decode().strip('"')
                        break
                    if a.type == "element_value_pair":
                        key_n = a.child_by_field_name("key")
                        val_n = a.child_by_field_name("value")
                        if key_n is None or val_n is None:
                            continue
                        if source[key_n.start_byte:key_n.end_byte].decode() in ("value", "path"):
                            if val_n.type == "string_literal":
                                path = source[val_n.start_byte:val_n.end_byte].decode().strip('"')
                                break
            return http_method, path
    return None, None


def _has_request_body(param, source: bytes) -> bool:
    for child in param.children:
        if child.type == "modifiers":
            for sub in child.children:
                if sub.type in ("annotation", "marker_annotation"):
                    n = sub.child_by_field_name("name")
                    if n is None:
                        continue
                    if source[n.start_byte:n.end_byte].decode().split(".")[-1] == "RequestBody":
                        return True
    return False


def _try_client_call(node, source: bytes, file: str) -> RestSite | None:
    http_method = _restclient_method(node, source)
    if http_method is None:
        return None
    path = _chain_uri(node, source)
    body_var = _chain_body(node, source)
    return RestSite(
        file=file, line=node.start_point[0] + 1,
        kind="rest_client_call", http_method=http_method, path=path,
        request_body_type=None, response_type=None,
        request_body_var=body_var,
    )


def _restclient_method(node, source: bytes) -> str | None:
    cur = node
    while cur is not None and cur.type == "method_invocation":
        name_n = cur.child_by_field_name("name")
        if name_n is None:
            break
        m = source[name_n.start_byte:name_n.end_byte].decode()
        if m in _HTTP_METHOD_NAMES:
            obj_n = cur.child_by_field_name("object")
            if obj_n is not None and obj_n.type == "identifier":
                ident = source[obj_n.start_byte:obj_n.end_byte].decode().lower()
                if "client" in ident:
                    return m.upper()
        cur = cur.child_by_field_name("object")
    return None


def _chain_uri(node, source: bytes) -> str | None:
    cur = node
    while cur is not None and cur.type == "method_invocation":
        name_n = cur.child_by_field_name("name")
        if name_n is not None and source[name_n.start_byte:name_n.end_byte].decode() == "uri":
            args = cur.child_by_field_name("arguments")
            if args is not None and args.named_child_count >= 1:
                first = args.named_children[0]
                if first.type == "string_literal":
                    return source[first.start_byte:first.end_byte].decode().strip('"')
        cur = cur.child_by_field_name("object")
    return None


def _chain_body(node, source: bytes) -> str | None:
    cur = node
    while cur is not None and cur.type == "method_invocation":
        name_n = cur.child_by_field_name("name")
        if name_n is not None and source[name_n.start_byte:name_n.end_byte].decode() == "body":
            args = cur.child_by_field_name("arguments")
            if args is not None and args.named_child_count >= 1:
                first = args.named_children[0]
                if first.type == "identifier":
                    return source[first.start_byte:first.end_byte].decode()
        cur = cur.child_by_field_name("object")
    return None
