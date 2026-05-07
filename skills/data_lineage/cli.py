"""Data lineage CLI entry point.

Runs the 5 pipeline passes, applies filters, and emits the requested formats.
"""
import argparse
import sys
from pathlib import Path

from .filters import FilterSpec, apply as apply_filters
from .passes import project_scan, sql_extract, sql_parse, java_dfa, graph_build
from .renderers import text as render_text
from .renderers import json as render_json
from .renderers import mermaid as render_mermaid


_FORMAT_FILES = {
    "text": ("lineage-report.txt", render_text.render),
    "json": ("lineage-graph.json", render_json.render),
    "mermaid": ("lineage.mmd", render_mermaid.render),
}


def run_pipeline(
    project_root: Path,
    *,
    output_dir: Path,
    formats: tuple[str, ...] = ("text", "json", "mermaid"),
    filter_spec: FilterSpec | None = None,
    quiet: bool = False,
) -> int:
    symbols = project_scan.run(project_root)
    units = sql_extract.run(project_root, symbols)
    sql_edges = sql_parse.run(units)
    java_edges, kafka_sites, rest_sites = java_dfa.run(project_root, symbols, units)
    graph = graph_build.run(symbols, sql_edges, java_edges,
                            kafka_sites=kafka_sites, rest_sites=rest_sites)

    if filter_spec is not None:
        graph = apply_filters(graph, filter_spec)

    output_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        filename, fn = _FORMAT_FILES[fmt]
        (output_dir / filename).write_text(fn(graph))

    if not quiet and "text" in formats:
        sys.stdout.write((output_dir / "lineage-report.txt").read_text())

    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="data_lineage")
    p.add_argument("project_path", type=Path)
    p.add_argument("--output", type=Path, default=Path.cwd())
    p.add_argument("--formats", default="text,json,mermaid")
    p.add_argument("--only-table", default=None)
    p.add_argument("--only-topic", default=None)
    p.add_argument("--max-depth", type=int, default=None)
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--strict", action="store_true")
    args = p.parse_args(argv)

    formats = tuple(s.strip() for s in args.formats.split(",") if s.strip())
    spec = FilterSpec(
        only_table=args.only_table,
        only_topic=args.only_topic,
        max_depth=args.max_depth,
    )
    return run_pipeline(
        args.project_path,
        output_dir=args.output,
        formats=formats,
        filter_spec=spec,
        quiet=args.quiet,
    )
