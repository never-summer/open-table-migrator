"""Multi-table routing: map detected paths to Iceberg (namespace, table) targets.

Mapping file format (JSON):

    {
      "default": {"namespace": "default", "table": "events"},
      "tables": [
        {"path_glob": "s3://bucket/events/*", "namespace": "analytics", "table": "events"},
        {"path_glob": "*/users/*",            "namespace": "analytics", "table": "users"}
      ]
    }

`default` is optional. `tables` is matched in order — first glob hit wins.
"""
import fnmatch
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class Target:
    namespace: str
    table: str

    @property
    def fqn(self) -> str:
        return f"{self.namespace}.{self.table}"


@dataclass
class MappingEntry:
    path_glob: str
    target: Target


@dataclass
class Mapping:
    entries: list[MappingEntry] = field(default_factory=list)
    default: Target | None = None


def load_mapping(path: Path) -> Mapping:
    data = json.loads(path.read_text())
    entries = [
        MappingEntry(
            path_glob=e["path_glob"],
            target=Target(namespace=e["namespace"], table=e["table"]),
        )
        for e in data.get("tables", [])
    ]
    default = None
    if "default" in data and data["default"] is not None:
        d = data["default"]
        default = Target(namespace=d["namespace"], table=d["table"])
    return Mapping(entries=entries, default=default)


Resolver = Callable[[str | None], Target | None]


def build_resolver(mapping: Mapping | None, fallback: Target | None) -> Resolver:
    """Return a resolver: path_arg (or None) → Target (or None).

    Priority: glob-matched entry → mapping default → caller-provided fallback.
    Returns None only when everything fails, signalling an unresolvable match.
    """
    entries: list[MappingEntry] = mapping.entries if mapping else []
    mapping_default: Target | None = mapping.default if mapping else None

    def resolve(path_arg: str | None) -> Target | None:
        if path_arg is not None:
            for entry in entries:
                if fnmatch.fnmatchcase(path_arg, entry.path_glob):
                    return entry.target
        return mapping_default or fallback

    return resolve


def constant_resolver(target: Target) -> Resolver:
    """Resolver that always returns `target`, regardless of path_arg."""
    return lambda _path_arg: target
