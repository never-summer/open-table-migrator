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


from skills.open_table_migrator.uri import matches_glob


def _u(s, project_root=None):
    return parse(s, project_root=project_root)


def test_match_s3_pattern_matches_s3a_uri():
    assert matches_glob(_u("s3a://bucket/users/x"), "s3://bucket/users/*")


def test_match_s3_pattern_does_not_match_different_path():
    assert not matches_glob(_u("s3://bucket/orders/x"), "s3://bucket/users/*")


def test_match_hdfs_pattern_matches_webhdfs_uri():
    assert matches_glob(_u("webhdfs://ns/warehouse/x"), "hdfs://ns/warehouse/*")


def test_match_viewfs_does_not_match_hdfs():
    assert not matches_glob(_u("hdfs://ns/x"), "viewfs://ns/x")


def test_match_double_star_crosses_segments():
    assert matches_glob(
        _u("s3://bucket/users/2024/01/data.parquet"),
        "s3://bucket/users/**",
    )


def test_match_question_mark_matches_single_char():
    assert matches_glob(_u("s3://bucket/users/x.parquet"),
                        "s3://bucket/users/?.parquet")


def test_match_authority_glob():
    assert matches_glob(_u("s3://my-prod-bucket/x"), "s3://my-*-bucket/x")


def test_match_bare_path_matches_file_pattern():
    assert matches_glob(_u("/tmp/fixtures/x.parquet"),
                        "file:///tmp/fixtures/*")


def test_match_file_pattern_matches_bare_path_uri():
    assert matches_glob(_u("file:///tmp/fixtures/x.parquet"),
                        "/tmp/fixtures/*")


def test_match_trailing_slash_distinguishes():
    assert not matches_glob(_u("s3://bucket/users"), "s3://bucket/users/")


# Fix 1: authority NUL sentinel restore
def test_match_question_mark_in_authority():
    assert matches_glob(_u("s3://my-A-bucket/x"), "s3://my-?-bucket/x")
    assert not matches_glob(_u("s3://my-AB-bucket/x"), "s3://my-?-bucket/x")


# Fix 2: unknown-scheme matching
def test_match_unknown_scheme_matches_itself_exactly(capsys):
    from skills.open_table_migrator import uri as uri_module
    uri_module._UNKNOWN_WARNED.clear()
    uri = _u("ftp://host.example/x")
    assert matches_glob(uri, "ftp://host.example/x")


def test_match_unknown_scheme_does_not_match_different_scheme():
    uri = _u("ftp://host.example/x")
    assert not matches_glob(uri, "s3://host.example/x")


# Fix 3: parse() bare path with literal '?'
def test_parse_bare_path_with_literal_question_mark_preserved():
    # Rare but valid filename — '?' is legal in POSIX filenames.
    # parse() should not lose it (urlparse treats '?' as query delimiter).
    result = parse("/tmp/weird?.parquet")
    assert result.path == "/tmp/weird?.parquet"


def test_match_single_star_crosses_segments_when_no_double_star():
    """`*` matches across `/` when the pattern has no `**` — this is the
    fnmatch fallback behavior (aligned with aws-cli convention).
    Use `**` only when you need to disambiguate (no practical difference
    in single-pattern matching)."""
    assert matches_glob(_u("s3://bucket/users/2024/01/data.parquet"),
                        "s3://bucket/users/*")


def test_parse_glob_pattern_warns_on_unknown_scheme(capsys):
    from skills.open_table_migrator import uri as uri_module
    uri_module._UNKNOWN_WARNED.clear()
    # Indirectly via matches_glob — it calls _parse_glob_pattern
    matches_glob(_u("s3://bucket/x"), "hfds://typo/x")
    captured = capsys.readouterr()
    assert "hfds" in captured.err
    assert "unknown URI scheme" in captured.err
