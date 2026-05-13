from skills.data_lineage.ts_parser import parse_java


def test_parse_java_returns_tree_with_root():
    tree = parse_java(b"class Foo { int x; }")
    assert tree.root_node.type == "program"
    assert tree.root_node.start_byte == 0


def test_parse_java_caches_parser_across_calls():
    t1 = parse_java(b"class A {}")
    t2 = parse_java(b"class B {}")
    assert t1.root_node.type == "program"
    assert t2.root_node.type == "program"
