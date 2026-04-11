"""Multi-table routing: map detected paths to Iceberg (namespace, table) targets.

Mapping file format (JSON):

    {
      "default": {"namespace": "default", "table": "events"},
      "tables": [
        {"path_glob": "s3://bucket/events/*", "namespace": "analytics", "table": "events"},
        {"path_glob": "*/users/*",            "namespace": "analytics", "table": "users"},
        {"path_glob": "s3://legacy/*",        "skip": true},
        {"path_glob": "s3://bucket/logs/*",   "direction": "write", "namespace": "analytics", "table": "logs"}
      ]
    }

- `default` is optional — target used when no entry matches.
- `tables` is matched in order; first glob hit wins.
- `skip: true` — leave matching operations untouched (do NOT migrate).
- `direction: "read"|"write"|"any"` (default "any") — restrict an entry to
  read-only or write-only operations. Useful when the same path is both read
  and written but only one direction should migrate.
"""
import fnmatch
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Literal


Direction = Literal["read", "write", "schema", "any"]


@dataclass(frozen=True)
class Target:
    namespace: str
    table: str

    @property
    def fqn(self) -> str:
        return f"{self.namespace}.{self.table}"


@dataclass(frozen=True)
class Decision:
    """What to do with a single detected operation.

    - migrate_to set, skip False → rewrite the line to target
    - migrate_to None,  skip True  → leave the line untouched
    - migrate_to None,  skip False → unresolved; emit TODO comment
    """
    migrate_to: Target | None = None
    skip: bool = False

    @classmethod
    def migrate(cls, target: Target) -> "Decision":
        return cls(migrate_to=target, skip=False)

    @classmethod
    def skipped(cls) -> "Decision":
        return cls(migrate_to=None, skip=True)

    @classmethod
    def unresolved(cls) -> "Decision":
        return cls(migrate_to=None, skip=False)


@dataclass
class MappingEntry:
    path_glob: str
    target: Target | None = None
    skip: bool = False
    direction: Direction = "any"


@dataclass
class Mapping:
    entries: list[MappingEntry] = field(default_factory=list)
    default: Target | None = None


def load_mapping(path: Path) -> Mapping:
    data = json.loads(path.read_text())
    entries: list[MappingEntry] = []
    for e in data.get("tables", []):
        skip = bool(e.get("skip", False))
        direction: Direction = e.get("direction", "any")
        target: Target | None = None
        if not skip:
            target = Target(namespace=e["namespace"], table=e["table"])
        entries.append(MappingEntry(
            path_glob=e["path_glob"],
            target=target,
            skip=skip,
            direction=direction,
        ))
    default: Target | None = None
    if "default" in data and data["default"] is not None:
        d = data["default"]
        default = Target(namespace=d["namespace"], table=d["table"])
    return Mapping(entries=entries, default=default)


Resolver = Callable[[str | None, Direction], Decision]


def build_resolver(mapping: Mapping | None, fallback: Target | None) -> Resolver:
    """Return a resolver: (path_arg, direction) → Decision.

    Priority: first matching entry (glob + direction) → mapping default →
    caller-provided fallback. Returns Decision.unresolved() when everything fails.
    """
    entries: list[MappingEntry] = mapping.entries if mapping else []
    mapping_default: Target | None = mapping.default if mapping else None

    def resolve(path_arg: str | None, direction: Direction = "any") -> Decision:
        if path_arg is not None:
            for entry in entries:
                if entry.direction != "any" and direction != "any" and entry.direction != direction:
                    continue
                if fnmatch.fnmatchcase(path_arg, entry.path_glob):
                    if entry.skip:
                        return Decision.skipped()
                    assert entry.target is not None  # guaranteed by load_mapping
                    return Decision.migrate(entry.target)
        target = mapping_default or fallback
        if target is None:
            return Decision.unresolved()
        return Decision.migrate(target)

    return resolve


def constant_resolver(target: Target) -> Resolver:
    """Resolver that always returns Decision.migrate(target), regardless of path_arg."""
    decision = Decision.migrate(target)
    return lambda _path_arg, _direction="any": decision
