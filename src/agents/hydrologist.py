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

from src.analyzers import SQLLineageAnalyzer, DAGConfigAnalyzer, PythonDataFlowAnalyzer
from src.models.graph import DataLineageGraph
from src.models.nodes import DatasetNode, StorageType, TransformationNode
from src.models.edges import ConsumesEdge, ProducesEdge


class Hydrologist:
    """
    Data flow and lineage analysis agent.
    
    Orchestrates the extraction of data lineage from SQL, Python, and YAML configs.
    """

    def __init__(self, repo_root: Path):
        self.repo_root = repo_root.resolve()
        self.sql_analyzer = SQLLineageAnalyzer()
        self.dag_analyzer = DAGConfigAnalyzer()
        self.py_analyzer = PythonDataFlowAnalyzer()
        self._lineage_graph = nx.DiGraph()

    def run(self) -> DataLineageGraph:
        """Execute the full data lineage analysis pipeline."""
        logger.info(f"[Hydrologist] Starting lineage analysis on: {self.repo_root}")

        transformations: dict[str, TransformationNode] = {}
        datasets: dict[str, DatasetNode] = {}
        produces_edges: list[ProducesEdge] = []
        consumes_edges: list[ConsumesEdge] = []

        # 1. Collect files (skipping unwanted directories)
        skip_dirs = {".git", ".venv", "dbt_packages", "target", ".cartography", "__pycache__"}
        sql_files: list[Path] = []
        py_files: list[Path] = []
        yaml_files: list[Path] = []

        import os
        for root, dirs, files in os.walk(self.repo_root):
            # Prune directories in-place
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            
            for file in files:
                file_path = Path(root) / file
                if file.endswith(".sql"):
                    sql_files.append(file_path)
                elif file.endswith(".py"):
                    py_files.append(file_path)
                elif file.endswith(".yml") or file.endswith(".yaml"):
                    yaml_files.append(file_path)

        # 2. Process SQL (including dbt)
        for sql_path in tqdm(sql_files, desc="Hydrologist: SQL files"):
            node = self.sql_analyzer.analyze(sql_path)
            if node:
                self._add_transformation(node, transformations, datasets, produces_edges, consumes_edges)

        # 3. Process Python (pandas/sqlalchemy/pyspark AND Airflow DAGs)
        for py_path in tqdm(py_files, desc="Hydrologist: Python files"):
            # Data flows
            df_nodes = self.py_analyzer.analyze(py_path)
            for node in df_nodes:
                self._add_transformation(node, transformations, datasets, produces_edges, consumes_edges)
            
            # DAGs (if applicable)
            dag_nodes = self.dag_analyzer.analyze(py_path)
            for node in dag_nodes:
                self._add_transformation(node, transformations, datasets, produces_edges, consumes_edges)

        # 4. Process Configs (Airflow/dbt YAML)
        for yaml_path in tqdm(yaml_files, desc="Hydrologist: YAML configs"):
            nodes = self.dag_analyzer.analyze(yaml_path)
            for node in nodes:
                self._add_transformation(node, transformations, datasets, produces_edges, consumes_edges)

        # Identify sources and sinks
        sources = self.find_sources(datasets.keys())
        sinks = self.find_sinks(datasets.keys())

        logger.info(f"[Hydrologist] Analysis complete: {len(datasets)} datasets, {len(transformations)} transformations")
        
        return DataLineageGraph(
            datasets=datasets,
            transformations=transformations,
            produces_edges=produces_edges,
            consumes_edges=consumes_edges,
            source_datasets=sources,
            sink_datasets=sinks,
        )

    def _add_transformation(self, node: TransformationNode, transformations, datasets, produces, consumes):
        """Helper to add a transformation and its associated edges/datasets to the graph."""
        transformations[node.id] = node
        self._lineage_graph.add_node(node.id, type="transformation")
        
        for ds in node.source_datasets:
            if ds not in datasets:
                datasets[ds] = DatasetNode(name=ds)
            consumes.append(ConsumesEdge(transformation_id=node.id, dataset_name=ds))
            self._lineage_graph.add_edge(ds, node.id)
            
        for ds in node.target_datasets:
            if ds not in datasets:
                datasets[ds] = DatasetNode(name=ds)
            produces.append(ProducesEdge(transformation_id=node.id, dataset_name=ds))
            self._lineage_graph.add_edge(node.id, ds)

    def blast_radius(self, node_id: str) -> list[str]:
        """BFS from node_id to find all downstream dependent datasets and transformations."""
        if node_id not in self._lineage_graph:
            return []
        return list(nx.descendants(self._lineage_graph, node_id))

    def find_sources(self, dataset_names: list[str]) -> list[str]:
        """Datasets with in-degree=0 in the lineage graph (entry points)."""
        return [ds for ds in dataset_names if self._lineage_graph.in_degree(ds) == 0]

    def find_sinks(self, dataset_names: list[str]) -> list[str]:
        """Datasets with out-degree=0 in the lineage graph (exit points)."""
        return [ds for ds in dataset_names if self._lineage_graph.out_degree(ds) == 0]
