import fnmatch
from .detector import PatternMatch
from .analyzer import direction_of


def _matches_any_glob(file_path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(file_path, pat) for pat in patterns)


def filter_matches(
    matches: list[PatternMatch],
    *,
    directions: set[str] | None = None,
    pattern_types: set[str] | None = None,
    include_files: list[str] | None = None,
    exclude_files: list[str] | None = None,
) -> list[PatternMatch]:
    result: list[PatternMatch] = []
    for m in matches:
        if directions is not None and direction_of(m.pattern_type) not in directions:
            continue
        if pattern_types is not None and m.pattern_type not in pattern_types:
            continue
        # Glob filters work against both absolute and relative-looking paths
        file_str = str(m.file)
        if include_files is not None and not _matches_any_glob(file_str, include_files):
            continue
        if exclude_files is not None and _matches_any_glob(file_str, exclude_files):
            continue
        result.append(m)
    return result
