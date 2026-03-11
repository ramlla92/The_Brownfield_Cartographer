"""
Knowledge Graph — NetworkX wrapper with serialization.
Provides a unified interface over ModuleGraph + DataLineageGraph.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import networkx as nx
from loguru import logger

from src.models.graph import CartographyResult, DataLineageGraph, ModuleGraph


class KnowledgeGraph:
    """
    Central graph store combining module dependency and data lineage graphs.

    Backed by two NetworkX DiGraphs:
        - module_nx: module import graph
        - lineage_nx: data lineage DAG
    """

    def __init__(self):
        self.module_nx: nx.DiGraph = nx.DiGraph()
        self.lineage_nx: nx.DiGraph = nx.DiGraph()
        self._result: Optional[CartographyResult] = None

    # ─── Loading ──────────────────────────────────────────────────────

    def load(self, result: CartographyResult) -> None:
        """Populate NetworkX graphs from a CartographyResult."""
        self._result = result
        mg = result.module_graph
        lg = result.lineage_graph

        # Module graph
        for path, node in mg.modules.items():
            self.module_nx.add_node(path, **node.model_dump(exclude={"imports", "exports"}))
        for edge in mg.import_edges:
            self.module_nx.add_edge(edge.source, edge.target, weight=edge.import_count)

        # Lineage graph
        for name, dataset in lg.datasets.items():
            self.lineage_nx.add_node(name, node_type="dataset", **dataset.model_dump())
        for tid, transform in lg.transformations.items():
            self.lineage_nx.add_node(tid, node_type="transformation", **transform.model_dump())
        for e in lg.produces_edges:
            self.lineage_nx.add_edge(e.transformation_id, e.dataset_name, rel="PRODUCES")
        for e in lg.consumes_edges:
            self.lineage_nx.add_edge(e.dataset_name, e.transformation_id, rel="CONSUMES")

        logger.info(
            f"[KnowledgeGraph] Loaded: "
            f"{self.module_nx.number_of_nodes()} module nodes, "
            f"{self.lineage_nx.number_of_nodes()} lineage nodes"
        )

    # ─── Serialization ────────────────────────────────────────────────

    def save_module_graph(self, path: Path) -> None:
        data = nx.node_link_data(self.module_nx)
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        logger.info(f"[KnowledgeGraph] module_graph.json → {path}")

    def save_lineage_graph(self, path: Path) -> None:
        data = nx.node_link_data(self.lineage_nx)
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        logger.info(f"[KnowledgeGraph] lineage_graph.json → {path}")

    def load_module_graph(self, path: Path) -> None:
        data = json.loads(path.read_text(encoding="utf-8"))
        self.module_nx = nx.node_link_graph(data)
        logger.info(f"[KnowledgeGraph] Loaded module graph from {path}")

    def load_lineage_graph(self, path: Path) -> None:
        data = json.loads(path.read_text(encoding="utf-8"))
        self.lineage_nx = nx.node_link_graph(data)
        logger.info(f"[KnowledgeGraph] Loaded lineage graph from {path}")

    # ─── Queries ──────────────────────────────────────────────────────

    def pagerank(self) -> dict[str, float]:
        if self.module_nx.number_of_nodes() == 0:
            return {}
        return nx.pagerank(self.module_nx)

    def strongly_connected_components(self) -> list[list[str]]:
        return [list(c) for c in nx.strongly_connected_components(self.module_nx) if len(c) > 1]

    def blast_radius(self, node: str) -> list[str]:
        """Return all nodes reachable from *node* in the lineage graph."""
        if node not in self.lineage_nx:
            return []
        return list(nx.descendants(self.lineage_nx, node))

    def ancestors(self, node: str) -> list[str]:
        """Return all nodes that can reach *node* in the lineage graph."""
        if node not in self.lineage_nx:
            return []
        return list(nx.ancestors(self.lineage_nx, node))
