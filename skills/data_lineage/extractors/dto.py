"""DTO shape extractor: classes, fields, Lombok-implicit getters, Jackson rename/ignore."""
from dataclasses import dataclass

from skills.data_lineage.ts_parser import parse_java


@dataclass(frozen=True)
class DtoField:
    name: str
    type: str
    serialized_as: str
    ignored: bool


@dataclass(frozen=True)
class Dto:
    fqn: str
    file: str
    fields: dict[str, DtoField]
    getter_to_field: dict[str, str]
    annotations: tuple[str, ...]


def extract(source: bytes, file: str) -> list[Dto]:
    tree = parse_java(source)
    pkg = _package(tree.root_node, source)
    out: list[Dto] = []
    for cls in _iter_classes(tree.root_node):
        out.append(_class_to_dto(cls, source, file, pkg))
    return out


def _package(root, source: bytes) -> str:
    for child in root.children:
        if child.type == "package_declaration":
            for sub in child.named_children:
                if sub.type in ("scoped_identifier", "identifier"):
                    return source[sub.start_byte:sub.end_byte].decode()
    return ""


def _iter_classes(node):
    if node.type == "class_declaration":
        yield node
    for child in node.children:
        yield from _iter_classes(child)


def _class_to_dto(cls, source: bytes, file: str, pkg: str) -> Dto:
    name_n = cls.child_by_field_name("name")
    name = source[name_n.start_byte:name_n.end_byte].decode()
    fqn = f"{pkg}.{name}" if pkg else name

    annotations = _class_annotations(cls, source)
    body = cls.child_by_field_name("body")
    fields: dict[str, DtoField] = {}
    explicit_getters: dict[str, str] = {}

    if body is not None:
        for member in body.named_children:
            if member.type == "field_declaration":
                f = _parse_field(member, source)
                if f is not None:
                    fields[f.name] = f
            elif member.type == "method_declaration":
                pair = _parse_getter(member, source)
                if pair is not None:
                    explicit_getters[pair[0]] = pair[1]

    if any(a in {"Data", "Getter"} for a in annotations):
        for fname in fields:
            getter = "get" + fname[:1].upper() + fname[1:]
            explicit_getters.setdefault(getter, fname)

    return Dto(
        fqn=fqn, file=file, fields=fields,
        getter_to_field=explicit_getters,
        annotations=annotations,
    )


def _class_annotations(cls, source: bytes) -> tuple[str, ...]:
    out: list[str] = []
    for child in cls.children:
        if child.type in ("modifiers", "marker_annotation", "annotation"):
            for sub in child.children if child.type == "modifiers" else (child,):
                if sub.type in ("marker_annotation", "annotation"):
                    name_n = sub.child_by_field_name("name")
                    if name_n is not None:
                        text = source[name_n.start_byte:name_n.end_byte].decode()
                        out.append(text.split(".")[-1])
    return tuple(out)


def _parse_field(node, source: bytes):
    type_n = node.child_by_field_name("type")
    if type_n is None:
        return None
    type_text = source[type_n.start_byte:type_n.end_byte].decode()
    decl = next((c for c in node.named_children if c.type == "variable_declarator"), None)
    if decl is None:
        return None
    name_n = decl.child_by_field_name("name")
    if name_n is None:
        return None
    fname = source[name_n.start_byte:name_n.end_byte].decode()

    serialized_as = fname
    ignored = False
    for child in node.children:
        if child.type == "modifiers":
            for sub in child.children:
                if sub.type in ("marker_annotation", "annotation"):
                    n = sub.child_by_field_name("name")
                    if n is None:
                        continue
                    aname = source[n.start_byte:n.end_byte].decode().split(".")[-1]
                    if aname == "JsonIgnore":
                        ignored = True
                    elif aname == "JsonProperty":
                        args = sub.child_by_field_name("arguments")
                        if args is not None:
                            for a in args.named_children:
                                if a.type == "string_literal":
                                    serialized_as = source[a.start_byte:a.end_byte].decode().strip('"')
                                elif a.type == "element_value_pair":
                                    key_n = a.child_by_field_name("key")
                                    val_n = a.child_by_field_name("value")
                                    if (
                                        key_n is not None
                                        and val_n is not None
                                        and source[key_n.start_byte:key_n.end_byte].decode() == "value"
                                        and val_n.type == "string_literal"
                                    ):
                                        serialized_as = source[val_n.start_byte:val_n.end_byte].decode().strip('"')

    return DtoField(name=fname, type=type_text, serialized_as=serialized_as, ignored=ignored)


def _parse_getter(node, source: bytes):
    name_n = node.child_by_field_name("name")
    if name_n is None:
        return None
    method_name = source[name_n.start_byte:name_n.end_byte].decode()
    if not method_name.startswith("get") or len(method_name) < 4:
        return None
    body = node.child_by_field_name("body")
    if body is None:
        return None
    field_name = method_name[3].lower() + method_name[4:]
    return method_name, field_name
