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
    Supports basic constant propagation and dynamic reference flagging.
    """

    def __init__(self, repo_root: Path, verbose: bool = False):
        self.repo_root = repo_root
        self.ts_analyzer = TreeSitterAnalyzer()
        self.sql_analyzer = SQLLineageAnalyzer(repo_root=repo_root)
        self.verbose = verbose
        self.constants: dict[str, str] = {}

    def analyze(self, path: Path) -> List[TransformationNode]:
        """
        Analyze a Python file for data flow patterns.
        Aggregates all read/write calls in the file into a single TransformationNode.
        """
        result = self.ts_analyzer.parse_file(path)
        if not result:
            return []

        all_sources: set[str] = set()
        all_targets: set[str] = set()
        
        # Metadata flags
        flags = {
            "dynamic_reference": False,
            "requires_runtime_context": False,
            "via_sql": False,
            "confidence_score": 1.0
        }

        # Basic constant propagation map
        self.constants = self._get_constant_map(result.root_node, result.source_bytes)
        if self.verbose and self.constants:
            logger.info(f"[PythonDataFlow] Resolved constants in {path}: {self.constants}")

        self._find_dataflows(
            result.root_node, 
            result.source_bytes, 
            path, 
            all_sources, 
            all_targets, 
            flags
        )
        
        if not all_sources and not all_targets:
            return []

        if flags["dynamic_reference"]:
            flags["confidence_score"] = 0.5
            flags["requires_runtime_context"] = True

        return [TransformationNode(
            id=f"python::{path}",
            source_datasets=sorted(list(all_sources)),
            target_datasets=sorted(list(all_targets)),
            transformation_type="python_dataflow",
            source_file=str(path),
            confidence_score=flags["confidence_score"],
            dynamic_reference=flags["dynamic_reference"],
            requires_runtime_context=flags["requires_runtime_context"],
            via_sql=flags["via_sql"],
            metadata={"file_path": str(path)}
        )]

    def _get_constant_map(self, root_node: Node, source_bytes: bytes) -> dict[str, str]:
        """Simple pass to find top-level assignments to string literals."""
        constants = {}
        for child in root_node.children:
            assignments = []
            if child.type == "assignment":
                assignments.append(child)
            elif child.type == "expression_statement":
                # Check all children (sometimes comments or other tokens precede)
                for sub in child.children:
                    if sub.type == "assignment":
                        assignments.append(sub)
            
            for assignment in assignments:
                left = assignment.child_by_field_name("left")
                right = assignment.child_by_field_name("right")
                if left and right and left.type == "identifier" and right.type == "string":
                    name = self.ts_analyzer.get_node_text(left, source_bytes)
                    val = self.ts_analyzer.get_node_text(right, source_bytes).strip("'\"")
                    constants[name] = val
        return constants

    def _find_dataflows(
        self, 
        node: Node, 
        source_bytes: bytes, 
        path: Path, 
        all_sources: set[str], 
        all_targets: set[str],
        flags: dict
    ):
        """Recursively find data flow calls."""

        def get_text(n: Node) -> str:
            return self.ts_analyzer.get_node_text(n, source_bytes)

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
                        # Extract arguments
                        datasets = []
                        has_dynamic = False
                        
                        # Heuristic for .to_sql(name, con) or .read_sql(sql, con)
                        # We want the 'name' or 'sql' arg, not 'con'
                        is_sql_method = ".read_sql" in func_text or ".to_sql" in func_text

                        for i, arg in enumerate(args_node.children):
                            if arg.type == ",":
                                continue
                            
                            # Skip connection arguments for SQL methods (usually 2nd positional or keyword 'con')
                            if is_sql_method:
                                arg_text = get_text(arg)
                                if "con=" in arg_text or (i > 1 and "=" not in arg_text):
                                    continue

                            if arg.type == "string":
                                datasets.append(get_text(arg).strip("'\""))
                            elif arg.type == "f_string":
                                raw_f = get_text(arg)
                                # Aggressive cleaning: remove f/F prefix and any surrounding quotes
                                cleaned_f = raw_f.lstrip("fF").strip("'\"")
                                datasets.append(cleaned_f)
                                has_dynamic = True
                            elif arg.type == "identifier":
                                name = get_text(arg)
                                if name in self.constants:
                                    datasets.append(self.constants[name])
                                    if self.verbose:
                                        logger.info(f"[PythonDataFlow] Resolved variable '{name}' -> '{self.constants[name]}'")
                                else:
                                    has_dynamic = True
                            elif arg.type in ("attribute", "subscript"):
                                has_dynamic = True
                        
                        if has_dynamic:
                            flags["dynamic_reference"] = True
                            if not datasets:
                                datasets = ["dynamic_reference"]
                            if self.verbose:
                                logger.info(f"[PythonDataFlow] Dynamic reference detected in call: {func_text}")

                        if is_read or is_execute:
                            for ds in datasets:
                                if ds == "dynamic_reference":
                                    all_sources.add(ds)
                                    continue
                                
                                # Substitution for all sources
                                if "{" in ds and "}" in ds:
                                    for var, val in self.constants.items():
                                        if f"{{{var}}}" in ds:
                                            old_ds = ds
                                            ds = ds.replace(f"{{{var}}}", val)
                                            if self.verbose:
                                                logger.info(f"[PythonDataFlow] Substituted constant: '{old_ds}' -> '{ds}'")

                                # If it looks like SQL, try to parse it
                                if any(kw in ds.upper() for kw in ["SELECT", "FROM", "WITH", "UPDATE", "INSERT", "DELETE", "MERGE"]):
                                    flags["via_sql"] = True
                                    sq_sources, _ = self.sql_analyzer.extract_lineage_from_string(ds)
                                    if sq_sources:
                                        all_sources.update(sq_sources)
                                        if self.verbose:
                                            logger.debug(f"[PythonDataFlow] Extracted SQL sources: {sq_sources}")
                                    else:
                                        if self.verbose:
                                            logger.debug(f"[PythonDataFlow] Failed to extract SQL lineage from: {ds[:100]}...")
                                        all_sources.add(ds)
                                else:
                                    all_sources.add(ds)
                        elif is_write:
                            for ds in datasets:
                                # Also attempt substitution for write targets
                                if "{" in ds and "}" in ds:
                                    for var, val in self.constants.items():
                                        ds = ds.replace(f"{{{var}}}", val)
                                all_targets.add(ds)

        for child in node.children:
            self._find_dataflows(child, source_bytes, path, all_sources, all_targets, flags)
