"""
Agent 2: The Hydrologist — Data Flow & Lineage Analyst.
Constructs the DataLineageGraph by analysing Python, SQL, and YAML files.

Status: STUB — full implementation in Phase 2.
"""

from __future__ import annotations

from pathlib import Path
from collections import defaultdict
from typing import Optional

import networkx as nx
from loguru import logger
from tqdm import tqdm

from src.analyzers.sql_lineage import extract_sql_lineage
from src.analyzers.dag_config_parser import parse_config_file
from src.models.graph import DataLineageGraph
from src.models.nodes import DatasetNode, StorageType, TransformationNode
from src.models.edges import ConsumesEdge, ProducesEdge


class Hydrologist:
    """
    Data flow and lineage analysis agent.

    Usage:
        hydro = Hydrologist(repo_root=Path("path/to/repo"))
        lineage_graph = hydro.run()
    """

    def __init__(self, repo_root: Path):
        self.repo_root = repo_root.resolve()
        self._nx_lineage: nx.DiGraph = nx.DiGraph()

    # ─── Main Entry Point ─────────────────────────────────────────────────

    def run(self) -> DataLineageGraph:
        """Execute the full data lineage analysis pipeline."""
        logger.info(f"[Hydrologist] Scanning: {self.repo_root}")

        transformations: dict[str, TransformationNode] = {}
        datasets: dict[str, DatasetNode] = {}
        produces_edges: list[ProducesEdge] = []
        consumes_edges: list[ConsumesEdge] = []

        sql_files = list(self.repo_root.rglob("*.sql"))
        py_files = list(self.repo_root.rglob("*.py"))
        yaml_files = list(self.repo_root.rglob("*.yml")) + list(self.repo_root.rglob("*.yaml"))

        # ── SQL Lineage ────────────────────────────────────────────────
        for sql_path in tqdm(sql_files, desc="Hydrologist: SQL files"):
            node = extract_sql_lineage(sql_path)
            if node:
                transformations[node.id] = node
                for ds in node.source_datasets:
                    datasets.setdefault(ds, DatasetNode(name=ds))
                    consumes_edges.append(ConsumesEdge(transformation_id=node.id, dataset_name=ds))
                    self._nx_lineage.add_edge(ds, node.id)
                for ds in node.target_datasets:
                    datasets.setdefault(ds, DatasetNode(name=ds))
                    produces_edges.append(ProducesEdge(transformation_id=node.id, dataset_name=ds))
                    self._nx_lineage.add_edge(node.id, ds)

        # ── Python Data Flow ───────────────────────────────────────────
        for py_path in tqdm(py_files, desc="Hydrologist: Python files"):
            nodes = self._analyze_python_dataflow(py_path)
            for node in nodes:
                transformations[node.id] = node
                for ds in node.source_datasets:
                    datasets.setdefault(ds, DatasetNode(name=ds))
                    consumes_edges.append(ConsumesEdge(transformation_id=node.id, dataset_name=ds))
                for ds in node.target_datasets:
                    datasets.setdefault(ds, DatasetNode(name=ds))
                    produces_edges.append(ProducesEdge(transformation_id=node.id, dataset_name=ds))

        # ── YAML / Config ──────────────────────────────────────────────
        for yaml_path in tqdm(yaml_files, desc="Hydrologist: YAML configs"):
            nodes = parse_config_file(yaml_path)
            for node in nodes:
                transformations[node.id] = node

        # ── Sources & Sinks ────────────────────────────────────────────
        sources = [n for n in self._nx_lineage.nodes()
                   if self._nx_lineage.in_degree(n) == 0 and n in datasets]
        sinks = [n for n in self._nx_lineage.nodes()
                 if self._nx_lineage.out_degree(n) == 0 and n in datasets]

        logger.info(f"[Hydrologist] Datasets: {len(datasets)}, "
                    f"Transformations: {len(transformations)}, "
                    f"Sources: {len(sources)}, Sinks: {len(sinks)}")

        return DataLineageGraph(
            datasets=datasets,
            transformations=transformations,
            produces_edges=produces_edges,
            consumes_edges=consumes_edges,
            source_datasets=sources,
            sink_datasets=sinks,
        )

    # ─── Python Data Flow (stub) ───────────────────────────────────────

    def _analyze_python_dataflow(self, py_path: Path) -> list[TransformationNode]:
        """
        TODO (Phase 2): Use tree-sitter to extract:
            - pandas: read_csv, read_sql, to_csv, to_parquet
            - SQLAlchemy: session.execute(), engine.connect()
            - PySpark: spark.read, df.write
        For now: regex-based stub.
        """
        import re

        try:
            source = py_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return []

        nodes: list[TransformationNode] = []

        # Regex hunt for read_csv / read_parquet / read_json paths
        reads = re.findall(r'read_(?:csv|parquet|json|sql)\s*\(\s*["\']([^"\']+)["\']', source)
        writes = re.findall(r'to_(?:csv|parquet|json)\s*\(\s*["\']([^"\']+)["\']', source)

        if reads or writes:
            rel_path = str(py_path.relative_to(self.repo_root))
            nodes.append(TransformationNode(
                id=f"python::{rel_path}",
                source_datasets=reads,
                target_datasets=writes,
                transformation_type="pandas",
                source_file=rel_path,
            ))

        return nodes

    # ─── Blast Radius ──────────────────────────────────────────────────

    def blast_radius(self, node_name: str, lineage_graph: DataLineageGraph) -> list[str]:
        """
        BFS from node_name → return all downstream dependants.

        Args:
            node_name: dataset or transformation ID
            lineage_graph: the constructed DataLineageGraph

        Returns:
            List of downstream node names that would be affected.
        """
        # Rebuild nx graph from lineage_graph if needed
        G = nx.DiGraph()
        for edge in lineage_graph.produces_edges:
            G.add_edge(edge.transformation_id, edge.dataset_name)
        for edge in lineage_graph.consumes_edges:
            G.add_edge(edge.dataset_name, edge.transformation_id)

        if node_name not in G:
            logger.warning(f"[Hydrologist] blast_radius: '{node_name}' not in lineage graph")
            return []

        descendants = list(nx.descendants(G, node_name))
        logger.info(f"[Hydrologist] Blast radius of '{node_name}': {len(descendants)} nodes")
        return descendants
