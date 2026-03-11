from __future__ import annotations
from pathlib import Path
from typing import List, Optional
from tree_sitter import Node
from loguru import logger

from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.models.nodes import TransformationNode
from src.analyzers.sql_lineage import SQLLineageAnalyzer

class PythonDataFlowAnalyzer:
    """
    Analyzes Python AST to extract data flow patterns (Pandas, SQLAlchemy, PySpark).
    """

    def __init__(self):
        self.ts_analyzer = TreeSitterAnalyzer()
        self.sql_analyzer = SQLLineageAnalyzer()

    def analyze(self, path: Path) -> List[TransformationNode]:
        """
        Analyze a Python file for data flow patterns.
        Aggregates all read/write calls in the file into a single TransformationNode.
        """
        root = self.ts_analyzer.parse_file(path)
        if not root:
            return []

        with open(path, "r", encoding="utf-8", errors="replace") as f:
            source = f.read()

        all_sources: set[str] = set()
        all_targets: set[str] = set()
        
        self._find_dataflows(root, source, path, all_sources, all_targets)
        
        if not all_sources and not all_targets:
            return []

        return [TransformationNode(
            id=f"python::{path}",
            source_datasets=sorted(list(all_sources)),
            target_datasets=sorted(list(all_targets)),
            transformation_type="python_dataflow",
            source_file=str(path),
        )]

    def _find_dataflows(self, node: Node, source: str, path: Path, all_sources: set[str], all_targets: set[str]):
        """Recursively find data flow calls."""
        source_bytes = source.encode("utf-8")

        def get_text(n: Node) -> str:
            return source_bytes[n.start_byte:n.end_byte].decode("utf-8", errors="replace")

        if node.type == "call":
            func_node = node.child_by_field_name("function")
            if func_node:
                func_text = get_text(func_node)
                
                # Check for specific data flow methods
                is_read = any(p in func_text for p in [".read_csv", ".read_sql", ".read_parquet", ".read_json"])
                is_write = any(p in func_text for p in [".to_csv", ".to_sql", ".to_parquet", ".write"])
                is_execute = ".execute" in func_text

                if is_read or is_write or is_execute:
                    args_node = node.child_by_field_name("arguments")
                    if args_node:
                        # Extract string arguments
                        datasets = []
                        has_dynamic = False
                        for arg in args_node.children:
                            if arg.type == "string":
                                datasets.append(get_text(arg).strip("'\""))
                            elif arg.type in ("f_string", "identifier", "attribute"):
                                has_dynamic = True
                        
                        if not datasets and has_dynamic:
                            datasets = ["dynamic_reference"]

                        if is_read or is_execute:
                            for ds in datasets:
                                # If it looks like SQL, try to parse it
                                if ds != "dynamic_reference" and ("SELECT" in ds.upper() or "FROM" in ds.upper()):
                                    sq_sources, _ = self.sql_analyzer.extract_lineage_from_string(ds)
                                    all_sources.update(sq_sources)
                                else:
                                    all_sources.add(ds)
                        elif is_write:
                            for ds in datasets:
                                all_targets.add(ds)

        for child in node.children:
            self._find_dataflows(child, source, path, all_sources, all_targets)
