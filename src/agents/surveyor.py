import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Set

import networkx as nx
from loguru import logger
from tqdm import tqdm

from src.analyzers import LanguageRouter, TreeSitterAnalyzer, extract_git_velocity
from src.models.nodes import ModuleNode


class Surveyor:
    """
    Static structure analysis agent.
    
    Orchestrates codebase walking, AST analysis, dependency resolution,
    and graph-based metrics (PageRank, SCCs).
    """

    def __init__(self, repo_root: Path):
        self.repo_root = Path(repo_root).resolve()
        self.router = LanguageRouter()
        self.analyzer = TreeSitterAnalyzer()
        # Initialize git velocity once
        self.git_velocity = extract_git_velocity(self.repo_root, days=30)
        self.graph = nx.DiGraph()

    def analyze_module(self, file_path: Path) -> Optional[ModuleNode]:
        """
        Extract metadata for a single file.
        Skips files larger than 1MB or with unknown language.
        """
        try:
            rel_path = Path(file_path).relative_to(self.repo_root).as_posix()
            lang = self.router.get_language_for_path(file_path)
            
            if lang == "unknown":
                return None
            
            # Skip large files (> 1MB)
            if file_path.stat().st_size > 1024 * 1024:
                logger.warning(f"Skipping large file: {rel_path} ({file_path.stat().st_size} bytes)")
                return None
                
            node = ModuleNode(
                path=rel_path,
                language=lang,
                change_velocity_30d=self.git_velocity.get(rel_path, 0),
                last_modified=datetime.fromtimestamp(file_path.stat().st_mtime)
            )
            
            # Parse AST and extract metadata
            root_node = self.analyzer.parse_file(file_path)
            if root_node and lang == "python":
                content = file_path.read_text(encoding="utf-8", errors="replace")
                node.imports = self.analyzer.extract_python_imports(root_node, content)
                node.public_functions = self.analyzer.extract_python_functions(root_node, content)
                node.classes, node.bases = self.analyzer.extract_python_classes(root_node, content)
            
            return node
        except Exception as exc:
            logger.error(f"Error analyzing module {file_path}: {exc}")
            return None

    def build_module_graph(self) -> nx.DiGraph:
        """
        Walks the repository, analyzes modules, resolves imports, 
        and computes graph metrics.
        """
        logger.info(f"Building module graph for {self.repo_root}...")
        
        skip_dirs = {".git", ".venv", "dbt_packages", "target", ".cartography", "__pycache__"}
        modules: Dict[str, ModuleNode] = {}
        
        # 1. Walk and analyze
        for root, dirs, files in os.walk(self.repo_root):
            # Prune directories in-place to prevent os.walk from entering them
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            
            for file in files:
                file_path = Path(root) / file
                node = self.analyze_module(file_path)
                if node:
                    modules[node.path] = node
                    # Use model_dump to store all Pydantic fields as attributes
                    self.graph.add_node(node.path, **node.model_dump())

        # 2. Resolve imports and add edges
        # Map of dotted names to paths (best effort for Python)
        # e.g., "src.models.nodes" -> "src/models/nodes.py"
        python_module_map = {}
        for path in modules:
            if path.endswith(".py"):
                dotted = path.removesuffix(".py").replace("/", ".")
                python_module_map[dotted] = path
                # Also handle __init__.py mapping to the parent dotted name
                if path.endswith("__init__.py"):
                    parent_dotted = Path(path).parent.as_posix().replace("/", ".")
                    if parent_dotted == ".":
                        # Root __init__.py is not usually handled this way but for completeness:
                        pass
                    else:
                        python_module_map[parent_dotted] = path

        for source_path, node in modules.items():
            if node.language != "python":
                continue
                
            # Determine current module's dotted name for backdrop
            is_pkg = source_path.endswith("__init__.py")
            current_mod = source_path.removesuffix(".py").replace("/", ".")
            if is_pkg:
                current_mod = Path(source_path).parent.as_posix().replace("/", ".")
            
            mod_parts = current_mod.split(".")
            if current_mod == ".":
                mod_parts = []

            for imp in node.imports:
                resolved_imp = imp
                
                # 1. Resolve relative imports to absolute dotted paths
                if imp.startswith("."):
                    dots = 0
                    for char in imp:
                        if char == ".":
                            dots += 1
                        else:
                            break
                    
                    symbol = imp[dots:]
                    
                    # Resolve base package parts
                    if is_pkg:
                        # pkg/__init__.py: . -> pkg, .. -> pkg's parent
                        base_parts = mod_parts[: len(mod_parts) - (dots - 1)]
                    else:
                        # pkg/mod.py: . -> pkg, .. -> pkg's parent
                        base_parts = mod_parts[: len(mod_parts) - dots]
                    
                    if symbol:
                        resolved_imp = ".".join(base_parts + [symbol])
                    elif base_parts:
                        resolved_imp = ".".join(base_parts)
                    else:
                        resolved_imp = ""

                # 2. Map resolved dotted path to file path
                if not resolved_imp:
                    continue

                target_path = python_module_map.get(resolved_imp)
                
                # 3. If not found exactly, try prefixes (handle 'from module import class')
                if not target_path:
                    parts = resolved_imp.split(".")
                    for i in range(len(parts) - 1, 0, -1):
                        prefix = ".".join(parts[:i])
                        if prefix in python_module_map:
                            target_path = python_module_map[prefix]
                            break

                if target_path and target_path != source_path:
                    if self.graph.has_edge(source_path, target_path):
                        self.graph[source_path][target_path]["weight"] += 1
                    else:
                        self.graph.add_edge(source_path, target_path, type="IMPORTS", weight=1)

        # 3. Graph Metrics
        if len(self.graph) > 0:
            # PageRank
            try:
                pagerank = nx.pagerank(self.graph, weight="weight")
                for node_id, score in pagerank.items():
                    self.graph.nodes[node_id]["page_rank"] = score
            except Exception as exc:
                logger.warning(f"PageRank computation failed: {exc}")
            
            # SCCs
            sccs = list(nx.strongly_connected_components(self.graph))
            for i, scc in enumerate(sccs):
                if len(scc) > 1:
                    for node_id in scc:
                        self.graph.nodes[node_id]["scc_id"] = i

        # 4. Dead Code Detection
        # Entries are usually cli.py, __main__.py, or any file with no in-edges but that's an entrypoint
        entrypoints = {"cli.py", "__main__.py", "app.py", "main.py"}
        for node_id in self.graph.nodes:
            in_degree = self.graph.in_degree(node_id)
            is_entry = any(node_id.endswith(e) for e in entrypoints) or "src/cli.py" in node_id
            
            if in_degree == 0 and not is_entry:
                self.graph.nodes[node_id]["is_dead_code_candidate"] = True
                
        return self.graph

    def write_module_graph_json(self, output_path: Path) -> None:
        """Serialize the graph to the custom JSON format."""
        data = {
            "nodes": {},
            "edges": []
        }
        
        for node_id, attrs in self.graph.nodes(data=True):
            # Ensure last_modified is serialized correctly
            if "last_modified" in attrs and isinstance(attrs["last_modified"], datetime):
                attrs["last_modified"] = attrs["last_modified"].isoformat()
            data["nodes"][node_id] = attrs
            
        for u, v, attrs in self.graph.edges(data=True):
            edge_entry = {"source": u, "target": v}
            edge_entry.update(attrs)
            data["edges"].append(edge_entry)
            
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Module graph written to {output_path}")
