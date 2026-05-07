"""Spring Kafka detector: KafkaTemplate.send sites + @KafkaListener methods."""
from dataclasses import dataclass

from skills.data_lineage.ts_parser import parse_java


@dataclass(frozen=True)
class KafkaSite:
    file: str
    line: int
    kind: str             # "kafka_send" | "kafka_listen"
    topic: str | None
    payload_var: str | None    # for kafka_send
    payload_type: str | None   # for kafka_listen


def extract(source: bytes, file: str) -> list[KafkaSite]:
    tree = parse_java(source)
    out: list[KafkaSite] = []
    _walk(tree.root_node, source, file, out)
    return out


def _walk(node, source: bytes, file: str, out: list[KafkaSite]) -> None:
    if node.type == "method_invocation":
        site = _try_send(node, source, file)
        if site is not None:
            out.append(site)
    if node.type == "method_declaration":
        site = _try_listener(node, source, file)
        if site is not None:
            out.append(site)
    for child in node.children:
        _walk(child, source, file, out)


def _try_send(node, source: bytes, file: str) -> KafkaSite | None:
    name_n = node.child_by_field_name("name")
    if name_n is None or source[name_n.start_byte:name_n.end_byte].decode() != "send":
        return None
    args = node.child_by_field_name("arguments")
    if args is None or args.named_child_count < 2:
        return None
    topic_n = args.named_children[0]
    if topic_n.type != "string_literal":
        return None
    topic = source[topic_n.start_byte:topic_n.end_byte].decode().strip('"')
    payload_n = args.named_children[-1]
    if payload_n.type != "identifier":
        return None
    payload_var = source[payload_n.start_byte:payload_n.end_byte].decode()
    return KafkaSite(
        file=file,
        line=node.start_point[0] + 1,
        kind="kafka_send",
        topic=topic,
        payload_var=payload_var,
        payload_type=None,
    )


def _try_listener(method, source: bytes, file: str) -> KafkaSite | None:
    topic = _listener_topic(method, source)
    if topic is None:
        return None
    params = method.child_by_field_name("parameters")
    payload_type: str | None = None
    if params is not None:
        for p in params.named_children:
            if p.type == "formal_parameter":
                type_n = p.child_by_field_name("type")
                if type_n is not None:
                    payload_type = source[type_n.start_byte:type_n.end_byte].decode()
                    break
    return KafkaSite(
        file=file,
        line=method.start_point[0] + 1,
        kind="kafka_listen",
        topic=topic,
        payload_var=None,
        payload_type=payload_type,
    )


def _listener_topic(method, source: bytes) -> str | None:
    for child in method.children:
        if child.type != "modifiers":
            continue
        for sub in child.children:
            if sub.type not in ("annotation", "marker_annotation"):
                continue
            n = sub.child_by_field_name("name")
            if n is None:
                continue
            if source[n.start_byte:n.end_byte].decode().split(".")[-1] != "KafkaListener":
                continue
            args = sub.child_by_field_name("arguments")
            if args is None:
                continue
            for a in args.named_children:
                if a.type == "element_value_pair":
                    key_n = a.child_by_field_name("key")
                    val_n = a.child_by_field_name("value")
                    if key_n is None or val_n is None:
                        continue
                    if source[key_n.start_byte:key_n.end_byte].decode() == "topics":
                        if val_n.type == "string_literal":
                            return source[val_n.start_byte:val_n.end_byte].decode().strip('"')
                elif a.type == "string_literal":
                    return source[a.start_byte:a.end_byte].decode().strip('"')
    return None
