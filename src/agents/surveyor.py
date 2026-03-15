"""
Agent 1: The Surveyor — Static Structure Analyst.
Performs deep static analysis of the codebase using tree-sitter for AST parsing.
Builds the structural skeleton of the system including the module import graph.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Set, Any

import networkx as nx
from loguru import logger
from tqdm import tqdm

from src.analyzers.tree_sitter_analyzer import analyze_module, TreeSitterAnalyzer, LanguageRouter
from src.analyzers import extract_git_velocity
from src.models.nodes import ModuleNode, Language


class Surveyor:
    """
    Static structure analysis agent.
    
    Orchestrates codebase walking, AST analysis, dependency resolution,
    and graph-based metrics (PageRank, SCCs).
    """

    SUPPORTED_EXTENSIONS = {".py", ".sql", ".yml", ".yaml", ".ipynb"}
    DEFAULT_VELOCITY_THRESHOLD = 50
    DEFAULT_DEAD_CODE_VELOCITY = 2

    def __init__(self, repo_root: Path, velocity_threshold: int = 50, dead_code_velocity: int = 2, include_files: Optional[Set[str]] = None):
        self.repo_root = Path(repo_root).resolve()
        self.router = LanguageRouter()
        self.analyzer = TreeSitterAnalyzer()
        self.velocity_threshold = velocity_threshold
        self.dead_code_velocity = dead_code_velocity
        self.include_files = include_files # Set of repo-relative paths
        
        # Initialise git velocity once
        self.git_velocity = extract_git_velocity(self.repo_root, days=30)
        self.graph = nx.DiGraph()
        self.modules: Dict[str, ModuleNode] = {}
        self.failed_modules: List[str] = []

    def analyze_module(self, file_path: Path) -> Optional[ModuleNode]:
        """
        Extract detailed metadata for a single file using the TreeSitter foundation.
        """
        try:
            # Shared logic in tree_sitter_analyzer
            node = analyze_module(file_path, self.repo_root)
            if node:
                # Add Surveyor-specific metrics
                rel_path = node.path
                
                # Git Velocity integration
                velocity_data = self.git_velocity.get(rel_path, 0)
                if isinstance(velocity_data, dict):
                    node.change_velocity_30d = velocity_data.get("velocity", 0)
                    node.deep_audit_required = velocity_data.get("deep_audit_required", False)
                else:
                    # Fallback for simple integer velocity
                    node.change_velocity_30d = int(velocity_data)
                    node.deep_audit_required = node.change_velocity_30d > self.velocity_threshold

                node.last_modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                
                # Identify if it's an entrypoint
                entrypoints = {"cli.py", "__main__.py", "app.py", "main.py"}
                if any(rel_path.endswith(e) for e in entrypoints) or "src/cli.py" in rel_path:
                    node.is_entrypoint = True
            
            if node and node.parsing_error:
                self.failed_modules.append(str(file_path.relative_to(self.repo_root)))
            return node
        except Exception as exc:
            logger.error(f"Error analyzing module {file_path}: {exc}")
            self.failed_modules.append(str(file_path.relative_to(self.repo_root)))
            return None

    def build_module_graph(self) -> nx.DiGraph:
        """
        Walks the repository, analyzes modules, and builds the connectivity graph.
        """
        logger.info(f"[Surveyor] Building module graph for {self.repo_root}...")
        
        skip_dirs = {".git", ".venv", "dbt_packages", "target", ".cartography", "__pycache__", "node_modules"}
        all_files: List[Path] = []
        
        # 1. Walk and collect all supported files
        for root, dirs, files in os.walk(self.repo_root):
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for file in files:
                file_path = Path(root) / file
                if file_path.suffix.lower() in self.SUPPORTED_EXTENSIONS:
                    all_files.append(file_path)

        # 2. Analyze modules (Deeply for include_files, skeleton for others)
        for file_path in tqdm(all_files, desc="Surveying modules"):
            rel_path = str(file_path.relative_to(self.repo_root)).replace("\\", "/")
            
            # Determine if we should perform deep analysis
            should_analyze = (self.include_files is None or rel_path in self.include_files)
            
            if should_analyze:
                node = self.analyze_module(file_path)
            else:
                # Create skeleton node for edge resolution
                node = ModuleNode(path=rel_path, language=Language.PYTHON) # Language will be refined by router if needed
            
            if node:
                self.modules[node.path] = node
                self.graph.add_node(node.path, **node.model_dump())

        # 3. Add edges from resolved imports
        for source_path, node in self.modules.items():
            for target_path in node.imports:
                # Imports are normalized to repository-relative file paths
                if target_path in self.modules and target_path != source_path:
                    if self.graph.has_edge(source_path, target_path):
                        self.graph[source_path][target_path]["weight"] += 1
                    else:
                        self.graph.add_edge(source_path, target_path, type="IMPORTS", weight=1)

        # 4. Compute Graph Metrics (PageRank, SCCs, Topological Layers)
        self._compute_graph_analytics()

        # 5. Dead Code Detection (SCC-Aware)
        self._detect_dead_code()
                
        return self.graph

    def _compute_graph_analytics(self) -> None:
        """Compute advanced graph metrics and layering."""
        if len(self.graph) == 0:
            return

        # 1. PageRank: Importance based on in-degree and weight
        try:
            pagerank = nx.pagerank(self.graph, weight="weight")
            for node_id, score in pagerank.items():
                self.graph.nodes[node_id]["page_rank"] = score
                if node_id in self.modules:
                    self.modules[node_id].page_rank = score
        except Exception as exc:
            logger.warning(f"PageRank computation failed: {exc}")
        
        # 2. SCCs: Identify circular dependency clusters
        sccs = list(nx.strongly_connected_components(self.graph))
        for i, scc in enumerate(sccs):
            if len(scc) > 1: # Only annotate nodes in meaningful SCCs
                for node_id in scc:
                    self.graph.nodes[node_id]["scc_id"] = i
                    if node_id in self.modules:
                        self.modules[node_id].scc_id = i

        # 3. Topological Layering (on DAG components)
        try:
            # Create a DAG for sorting by removing back-edges (minimal set)
            # or just use the whole graph if it's already a DAG.
            # For brownfield, cycles are common, so we sort the condensation graph or handle cycles.
            condensation = nx.condensation(self.graph)
            layers = list(nx.topological_generations(condensation))
            for layer_id, node_set in enumerate(layers):
                for condensed_node in node_set:
                    # condensed_node represents a set of original nodes (an SCC)
                    original_nodes = condensation.nodes[condensed_node]["members"]
                    for node_id in original_nodes:
                        self.graph.nodes[node_id]["topological_layer"] = layer_id
                        if node_id in self.modules:
                            self.modules[node_id].topological_layer = layer_id
        except Exception as exc:
            logger.warning(f"Topological layering failed: {exc}")

    def _detect_dead_code(self) -> None:
        """
        Sophisticated dead code detection.
        Considers in-degree, entrypoint status, and SCC-aware isolation.
        """
        # 1. Standard in-degree check
        for node_id in self.graph.nodes:
            in_degree = self.graph.in_degree(node_id)
            node_data = self.modules.get(node_id)
            if not node_data:
                continue
                
            is_entry = node_data.is_entrypoint
            
            # Candidate if no one imports it and it's not an entrypoint
            if in_degree == 0 and not is_entry:
                if (node_data.change_velocity_30d or 0) < self.dead_code_velocity:
                    self.graph.nodes[node_id]["is_dead_code_candidate"] = True
                    node_data.is_dead_code_candidate = True

        # 2. SCC-Aware: If an SCC has 0 external in-edges, the whole cluster is dead
        sccs = list(nx.strongly_connected_components(self.graph))
        for scc in sccs:
            if len(scc) <= 1:
                continue
            
            external_in_degree = 0
            has_entrypoint = False
            for node_id in scc:
                if self.modules.get(node_id) and self.modules[node_id].is_entrypoint:
                    has_entrypoint = True
                    break
                # Count edges from outside the SCC
                for u, v in self.graph.in_edges(node_id):
                    if u not in scc:
                        external_in_degree += 1
            
            if external_in_degree == 0 and not has_entrypoint:
                for node_id in scc:
                    self.graph.nodes[node_id]["is_dead_code_candidate"] = True
                    if node_id in self.modules:
                        self.modules[node_id].is_dead_code_candidate = True
                
        return self.graph
                
    def run(self) -> List[ModuleNode]:
        """
        Execute the full survey and return the analyzed modules.
        """
        self.build_module_graph()
        return list(self.modules.values())

    def write_module_graph_json(self, output_path: Path) -> None:
        """Serialize the graph to a self-contained JSON format for the Archivist."""
        data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "repo_root": str(self.repo_root),
                "node_count": self.graph.number_of_nodes(),
                "edge_count": self.graph.number_of_edges(),
                "failed_module_count": len(self.failed_modules),
                "failed_modules": self.failed_modules
            },
            "nodes": {},
            "edges_by_type": {}
        }
        
        for node_id, attrs in self.graph.nodes(data=True):
            # Ensure serialization-friendly attributes
            serializable_attrs = {}
            for k, v in attrs.items():
                if isinstance(v, datetime):
                    serializable_attrs[k] = v.isoformat()
                elif isinstance(v, Language):
                    serializable_attrs[k] = v.value
                else:
                    serializable_attrs[k] = v
            
            # Provide defaults for critical analytics fields
            serializable_attrs.setdefault("page_rank", 0.0)
            serializable_attrs.setdefault("scc_id", None)
            serializable_attrs.setdefault("topological_layer", None)
            serializable_attrs.setdefault("is_dead_code_candidate", False)
            serializable_attrs.setdefault("is_entrypoint", False)
            serializable_attrs.setdefault("parsing_error", None)
            
            data["nodes"][node_id] = serializable_attrs
            
        for u, v, attrs in self.graph.edges(data=True):
            edge_type = attrs.get("type", "UNKNOWN")
            if edge_type not in data["edges_by_type"]:
                data["edges_by_type"][edge_type] = []
            
            edge_entry = {"source": u, "target": v}
            edge_entry.update({k: v for k, v in attrs.items() if k != "type"})
            data["edges_by_type"][edge_type].append(edge_entry)
            
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info(f"[Surveyor] Module graph written to {output_path}")

if __name__ == "__main__":
    # Quick debug run
    surveyor = Surveyor(Path("."))
    modules = surveyor.run()
    print(f"Surveyed {len(modules)} modules.")
    surveyor.write_module_graph_json(Path(".cartography/module_graph.json"))
