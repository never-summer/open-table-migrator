from pathlib import Path

from skills.open_table_migrator.uri import URI, parse


def test_parse_s3():
    assert parse("s3://bucket/key") == URI(
        scheme="s3", raw_scheme="s3", authority="bucket", path="/key",
    )


def test_parse_s3a_canonicalises_to_s3():
    assert parse("s3a://bucket/key") == URI(
        scheme="s3", raw_scheme="s3a", authority="bucket", path="/key",
    )


def test_parse_s3n_canonicalises_to_s3():
    assert parse("s3n://bucket/key") == URI(
        scheme="s3", raw_scheme="s3n", authority="bucket", path="/key",
    )


def test_parse_webhdfs_canonicalises_to_hdfs():
    assert parse("webhdfs://ns/x") == URI(
        scheme="hdfs", raw_scheme="webhdfs", authority="ns", path="/x",
    )


def test_parse_hdfs_empty_authority():
    assert parse("hdfs:///warehouse") == URI(
        scheme="hdfs", raw_scheme="hdfs", authority="", path="/warehouse",
    )


def test_parse_abfss_canonicalises_to_abfs():
    assert parse("abfss://container@account.dfs/x") == URI(
        scheme="abfs", raw_scheme="abfss",
        authority="container@account.dfs", path="/x",
    )


def test_parse_viewfs_kept_distinct_from_hdfs():
    assert parse("viewfs://ns/x") == URI(
        scheme="viewfs", raw_scheme="viewfs", authority="ns", path="/x",
    )


def test_parse_gs():
    assert parse("gs://bucket/x") == URI(
        scheme="gs", raw_scheme="gs", authority="bucket", path="/x",
    )


def test_parse_absolute_bare_path_is_file():
    assert parse("/tmp/x.parquet") == URI(
        scheme="file", raw_scheme="", authority="", path="/tmp/x.parquet",
    )


def test_parse_relative_path_resolved_against_project_root():
    assert parse("./data/x", project_root=Path("/proj")) == URI(
        scheme="file", raw_scheme="", authority="", path="/proj/data/x",
    )


def test_parse_relative_path_without_project_root_kept_literal():
    assert parse("./data/x") == URI(
        scheme="file", raw_scheme="", authority="", path="./data/x",
    )


def test_parse_empty_string_returns_empty_file_uri():
    assert parse("") == URI(
        scheme="file", raw_scheme="", authority="", path="",
    )


def test_parse_unknown_scheme_warns_and_returns_unknown(capsys):
    from skills.open_table_migrator import uri as uri_module
    uri_module._UNKNOWN_WARNED.clear()
    result = parse("ftp://host.example/x")
    assert result == URI(
        scheme="<unknown>", raw_scheme="ftp",
        authority="host.example", path="/x",
    )
    captured = capsys.readouterr()
    assert "ftp" in captured.err
    assert "unknown URI scheme" in captured.err
