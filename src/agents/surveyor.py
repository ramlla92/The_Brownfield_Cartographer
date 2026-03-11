"""
Agent 1: The Surveyor — Static Structure Analyst.
Builds the module import graph, computes PageRank, extracts git velocity,
and identifies dead code candidates.

Status: STUB — full implementation in Phase 1.
"""

from __future__ import annotations

import json
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Optional

import networkx as nx
from loguru import logger
from tqdm import tqdm

from src.analyzers.tree_sitter_analyzer import analyze_module, detect_language
from src.models.graph import ModuleGraph
from src.models.nodes import Language, ModuleNode
from src.models.edges import ImportEdge

# File extensions to include in analysis
INCLUDE_EXTENSIONS = {".py", ".sql", ".yaml", ".yml", ".js", ".ts", ".ipynb"}
# Directories to always skip
SKIP_DIRS = {
    ".git", "__pycache__", ".venv", "venv", "node_modules",
    ".tox", "dist", "build", ".eggs", ".mypy_cache",
}


class Surveyor:
    """
    Static structure analysis agent.

    Usage:
        surveyor = Surveyor(repo_root=Path("path/to/repo"))
        module_graph = surveyor.run()
    """

    def __init__(self, repo_root: Path, days: int = 30):
        self.repo_root = repo_root.resolve()
        self.days = days
        self._nx_graph: nx.DiGraph = nx.DiGraph()

    # ─── Main Entry Point ─────────────────────────────────────────────────

    def run(self) -> ModuleGraph:
        """Execute the full static analysis pipeline and return a ModuleGraph."""
        logger.info(f"[Surveyor] Scanning: {self.repo_root}")

        files = self._collect_files()
        logger.info(f"[Surveyor] Found {len(files)} files to analyse")

        modules: dict[str, ModuleNode] = {}
        for f in tqdm(files, desc="Surveyor: analysing modules"):
            node = analyze_module(f, self.repo_root)
            if node:
                modules[node.path] = node
                self._nx_graph.add_node(node.path)

        # Build import edges
        import_edges = self._build_import_edges(modules)

        # Git velocity
        velocity = self._extract_git_velocity(self.days)
        for path, count in velocity.items():
            if path in modules:
                modules[path].change_velocity_30d = count

        # Identify high-velocity files (top 20%)
        sorted_by_velocity = sorted(modules.items(), key=lambda kv: kv[1].change_velocity_30d, reverse=True)
        top_n = max(1, len(sorted_by_velocity) // 5)
        top_files = [kv[0] for kv in sorted_by_velocity[:top_n]]

        # PageRank
        pagerank = nx.pagerank(self._nx_graph) if self._nx_graph.number_of_nodes() > 0 else {}

        # Strongly Connected Components (circular deps)
        sccs = [c for c in nx.strongly_connected_components(self._nx_graph) if len(c) > 1]

        # Dead code candidates: exported symbols with no internal references
        for path, node in modules.items():
            in_deg = self._nx_graph.in_degree(path) if path in self._nx_graph else 0
            if in_deg == 0 and node.language == Language.PYTHON:
                node.is_dead_code_candidate = True

        logger.info(f"[Surveyor] PageRank computed. SCCs: {len(sccs)}. Dead code candidates: "
                    f"{sum(1 for n in modules.values() if n.is_dead_code_candidate)}")

        return ModuleGraph(
            modules=modules,
            import_edges=import_edges,
            pagerank_scores=pagerank,
            strongly_connected_components=[list(s) for s in sccs],
        )

    # ─── Helpers ──────────────────────────────────────────────────────────

    def _collect_files(self) -> list[Path]:
        """Walk the repo and collect analysable files."""
        files = []
        for f in self.repo_root.rglob("*"):
            if any(part in SKIP_DIRS for part in f.parts):
                continue
            if f.is_file() and f.suffix in INCLUDE_EXTENSIONS:
                files.append(f)
        return files

    def _build_import_edges(self, modules: dict[str, ModuleNode]) -> list[ImportEdge]:
        """Resolve Python import strings to actual module paths and add edges."""
        edges: list[ImportEdge] = []
        path_index = {m.replace("/", ".").replace("\\", ".").rstrip(".py"): m
                      for m in modules}

        for path, node in modules.items():
            for imp in node.imports:
                # Attempt to match import string to a known module
                target = path_index.get(imp)
                if target and target != path:
                    edge = ImportEdge(source=path, target=target)
                    edges.append(edge)
                    self._nx_graph.add_edge(path, target)

        return edges

    def _extract_git_velocity(self, days: int) -> dict[str, int]:
        """Run git log and count commits per file in the last N days."""
        velocity: dict[str, int] = defaultdict(int)
        try:
            result = subprocess.run(
                [
                    "git", "-C", str(self.repo_root),
                    "log", f"--since={days} days ago",
                    "--name-only", "--pretty=format:",
                ],
                capture_output=True, text=True, timeout=30,
            )
            for line in result.stdout.splitlines():
                line = line.strip()
                if line:
                    velocity[line] += 1
        except Exception as exc:
            logger.warning(f"[Surveyor] git velocity failed: {exc}")
        return dict(velocity)
