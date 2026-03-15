"""
Agent 2: The Hydrologist — Data Flow & Lineage Analyst.
Constructs the DataLineageGraph by analyzing Python, SQL, and YAML files.
Handles cross-language lineage (e.g. dbt models, Airflow DAGs, Python dataflows).
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Set, Any

import networkx as nx
from loguru import logger
from tqdm import tqdm

from src.analyzers.sql_lineage import SQLLineageAnalyzer
from src.analyzers.dag_config_parser import DAGConfigAnalyzer
from src.analyzers.python_dataflow import PythonDataFlowAnalyzer
from src.models.graph import DataLineageGraph
from src.models.nodes import DatasetNode, StorageType, TransformationNode
from src.models.edges import ConsumesEdge, ProducesEdge


class Hydrologist:
    """
    Data flow and lineage analysis agent.
    
    Orchestrates the extraction of data lineage from SQL, Python, and YAML configs.
    Builds a unified DataLineageGraph with advanced analytics.
    """

    SUPPORTED_EXTENSIONS = {".py", ".sql", ".yml", ".yaml"}

    def __init__(self, repo_root: Path, max_workers: int = 4, include_files: Optional[Set[str]] = None):
        self.repo_root = Path(repo_root).resolve()
        self.sql_analyzer = SQLLineageAnalyzer(repo_root=self.repo_root)
        self.dag_analyzer = DAGConfigAnalyzer()
        self.py_analyzer = PythonDataFlowAnalyzer(repo_root=self.repo_root)
        self.max_workers = max_workers
        self.include_files = include_files # Set of repo-relative paths
        
        self.graph = nx.DiGraph()
        self.datasets: Dict[str, DatasetNode] = {}
        self.transformations: Dict[str, TransformationNode] = {}
        self.failed_files: List[str] = []

    def analyze_file(self, file_path: Path) -> List[TransformationNode]:
        """
        Extract transformations from a single file based on its type.
        """
        results: List[TransformationNode] = []
        rel_path = str(file_path.relative_to(self.repo_root)).replace("\\", "/")
        
        try:
            ext = file_path.suffix.lower()
            if ext == ".sql":
                node = self.sql_analyzer.analyze(file_path)
                if node:
                    results.append(node)
            elif ext == ".py":
                # 1. Data flows (pandas, etc)
                py_nodes = self.py_analyzer.analyze(file_path)
                results.extend(py_nodes)
                # 2. Airflow DAGs
                dag_nodes = self.dag_analyzer.analyze(file_path)
                results.extend(dag_nodes)
            elif ext in {".yml", ".yaml"}:
                # Airflow/dbt YAML
                yaml_nodes = self.dag_analyzer.analyze(file_path)
                results.extend(yaml_nodes)
        except Exception as exc:
            logger.error(f"Error analyzing file {rel_path}: {exc}")
            self.failed_files.append(rel_path)
            
        return results

    def _process_results(self, transformation_nodes: List[TransformationNode]):
        """Deduplicate and integrate results into the graph."""
        for node in transformation_nodes:
            # Standardize source file path
            if node.source_file and Path(node.source_file).is_absolute():
                node.source_file = str(Path(node.source_file).relative_to(self.repo_root)).replace("\\", "/")
            
            self.transformations[node.id] = node
            self.graph.add_node(node.id, type="transformation", **node.model_dump())

            for ds_name in node.source_datasets:
                if ds_name not in self.datasets:
                    self.datasets[ds_name] = DatasetNode(name=ds_name)
                self.graph.add_node(ds_name, type="dataset", **self.datasets[ds_name].model_dump())
                self.graph.add_edge(ds_name, node.id, type="consumes")

            for ds_name in node.target_datasets:
                if ds_name not in self.datasets:
                    self.datasets[ds_name] = DatasetNode(name=ds_name)
                self.graph.add_node(ds_name, type="dataset", **self.datasets[ds_name].model_dump())
                self.graph.add_edge(node.id, ds_name, type="produces")

    def build_lineage_graph(self) -> nx.DiGraph:
        """Walks the repo and builds the full DataLineageGraph."""
        logger.info(f"[Hydrologist] Analyzing lineage in {self.repo_root}...")
        
        skip_dirs = {".git", ".venv", "dbt_packages", "target", ".cartography", "__pycache__", "node_modules"}
        all_files: List[Path] = []
        
        for root, dirs, files in os.walk(self.repo_root):
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for file in files:
                file_path = Path(root) / file
                if file_path.suffix.lower() in self.SUPPORTED_EXTENSIONS:
                    rel_path = str(file_path.relative_to(self.repo_root)).replace("\\", "/")
                    if self.include_files is None or rel_path in self.include_files:
                        all_files.append(file_path)

        # Process files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {executor.submit(self.analyze_file, f): f for f in all_files}
            
            for future in tqdm(as_completed(future_to_file), total=len(all_files), desc="Hydrating lineage"):
                file_path = future_to_file[future]
                try:
                    t_nodes = future.result()
                    self._process_results(t_nodes)
                except Exception as exc:
                    rel_path = str(file_path.relative_to(self.repo_root))
                    logger.error(f"Worker failed for {rel_path}: {exc}")
                    self.failed_files.append(rel_path)

        self._compute_analytics()
        return self.graph

    def _compute_analytics(self):
        """Compute sources, sinks, SCCs, and topological layers."""
        if len(self.graph) == 0:
            return

        # 1. Sources & Sinks (Datasets only)
        for ds_name, node in self.datasets.items():
            in_deg = self.graph.in_degree(ds_name)
            out_deg = self.graph.out_degree(ds_name)
            
            node.upstream_count = in_deg
            node.downstream_count = out_deg
            
            # Update attributes in graph for serialization
            self.graph.nodes[ds_name]["upstream_count"] = in_deg
            self.graph.nodes[ds_name]["downstream_count"] = out_deg

        # 2. Blast Radius (Downstream impact)
        # Often computed on-demand, but we can store importance/degree
        # Centrality on a lineage graph identifies "bottleneck" datasets
        try:
            centrality = nx.betweenness_centrality(self.graph)
            for node_id, score in centrality.items():
                if node_id in self.datasets:
                    self.datasets[node_id].confidence = round(score, 4) # Reuse confidence or add importance field?
                    # node.confidence is used here as a proxy for 'centrality' or 'importance'
                    self.graph.nodes[node_id]["importance_score"] = score
        except Exception as exc:
            logger.warning(f"Centrality computation failed: {exc}")

        # 3. SCCs & Topological Layering
        try:
            sccs = list(nx.strongly_connected_components(self.graph))
            for i, scc in enumerate(sccs):
                if len(scc) > 1:
                    for node_id in scc:
                        self.graph.nodes[node_id]["scc_id"] = i

            # Topological Sort (on condensation graph if cycles exist)
            condensation = nx.condensation(self.graph)
            layers = list(nx.topological_generations(condensation))
            for layer_id, node_set in enumerate(layers):
                for condensed_node in node_set:
                    original_nodes = condensation.nodes[condensed_node]["members"]
                    for node_id in original_nodes:
                        self.graph.nodes[node_id]["topological_layer"] = layer_id
        except Exception as exc:
            logger.warning(f"Topological layering failed: {exc}")

    def run(self) -> DataLineageGraph:
        """Execute and return the Pydantic graph model."""
        self.build_lineage_graph()
        
        # Build edges lists for Pydantic model
        produces: List[ProducesEdge] = []
        consumes: List[ConsumesEdge] = []
        
        for u, v, attrs in self.graph.edges(data=True):
            edge_type = attrs.get("type")
            if edge_type == "produces":
                produces.append(ProducesEdge(transformation_id=u, dataset_name=v))
            elif edge_type == "consumes":
                consumes.append(ConsumesEdge(transformation_id=v, dataset_name=u))

        sources = [n for n, d in self.graph.in_degree() if d == 0 and n in self.datasets]
        sinks = [n for n, d in self.graph.out_degree() if d == 0 and n in self.datasets]

        return DataLineageGraph(
            datasets=self.datasets,
            transformations=self.transformations,
            produces_edges=produces,
            consumes_edges=consumes,
            source_datasets=sources,
            sink_datasets=sinks
        )

    def write_lineage_graph_json(self, output_path: Path) -> None:
        """Serialize the graph for the Archivist."""
        data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "repo_root": str(self.repo_root),
                "dataset_count": len(self.datasets),
                "transformation_count": len(self.transformations),
                "failed_files_count": len(self.failed_files),
                "failed_files": self.failed_files
            },
            "nodes": {
                "datasets": {},
                "transformations": {}
            },
            "edges": []
        }
        
        for node_id, attrs in self.graph.nodes(data=True):
            # Clean up Pydantic-heavy dicts for simple JSON
            node_type = attrs.get("type", "unknown")
            clean_attrs = {k: v for k, v in attrs.items() if k != "type"}
            
            # Map node attributes to the right category
            if node_type == "dataset":
                data["nodes"]["datasets"][node_id] = clean_attrs
            else:
                data["nodes"]["transformations"][node_id] = clean_attrs
                
        for u, v, attrs in self.graph.edges(data=True):
            edge_entry = {"source": u, "target": v}
            edge_entry.update(attrs)
            data["edges"].append(edge_entry)
            
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info(f"[Hydrologist] Lineage graph written to {output_path}")


if __name__ == "__main__":
    # Debug run
    repo = Path(".")
    hydrologist = Hydrologist(repo)
    lineage = hydrologist.run()
    hydrologist.write_lineage_graph_json(Path(".cartography/lineage_graph.json"))
