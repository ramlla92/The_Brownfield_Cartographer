"""
Analyzer 3: Airflow / dbt YAML config parser.
Extracts pipeline topology from DAG definitions and dbt schema.yml files.
"""

from __future__ import annotations

import re
import yaml
from pathlib import Path
from typing import Any, Optional, Dict, List, Set, Union
from loguru import logger

from src.models.nodes import TransformationNode
from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer


class DAGConfigAnalyzer:
    """
    Advanced analyzer for Airflow DAGs and dbt YAML files.
    Supports recursive dependencies, dynamic tasks, and rich metadata.
    """

    def __init__(self, repo_root: Optional[Path] = None, verbose: bool = False):
        self.repo_root = repo_root or Path.cwd()
        self.verbose = verbose
        self.ts = TreeSitterAnalyzer()

    # ─── DBT Parsing ──────────────────────────────────────────────────────────

    def analyze_dbt_schema(self, schema_path: Path) -> list[TransformationNode]:
        """Parse dbt schema.yml or sources.yml with deep metadata extraction."""
        try:
            with schema_path.open() as f:
                data: dict = yaml.safe_load(f) or {}
        except Exception as exc:
            logger.warning(f"[dag_config_parser] Cannot parse {schema_path}: {exc}")
            return []

        nodes: list[TransformationNode] = []

        # Models
        for model in data.get("models", []):
            nodes.append(self._parse_dbt_model(model, schema_path))
            
        # Sources
        for source_entry in data.get("sources", []):
            nodes.extend(self._parse_dbt_source(source_entry, schema_path))

        return nodes

    def _parse_dbt_model(self, model_data: dict, schema_path: Path) -> TransformationNode:
        """Extract lineage and metadata from a single DBT model entry."""
        name = model_data.get("name", "unknown")
        sources: set[str] = set()
        flags = {"dynamic": False, "requires_context": False}
        
        # 1. Config & Meta
        self._extract_from_config_and_meta(model_data, sources)
        
        # 2. Descriptions & Embedded SQL
        raw_text = str(model_data.get("description", ""))
        self._extract_refs_from_text(raw_text, sources)
        
        # 3. Column-level dependencies
        for col in model_data.get("columns", []):
            self._extract_refs_from_text(str(col.get("description", "")), sources)
            for test in col.get("tests", []):
                self._extract_refs_from_yaml_struct(test, sources)

        # Flag dynamic content
        if "{{" in raw_text or "{%" in raw_text:
            flags["dynamic"] = True
            flags["requires_context"] = True

        rel_schema = str(schema_path.relative_to(self.repo_root)).replace("\\", "/") if self.repo_root in schema_path.parents else str(schema_path)
        
        return TransformationNode(
            id=f"dbt::{rel_schema}::{name}",
            source_datasets=sorted(list(sources)),
            target_datasets=[name],
            transformation_type="dbt",
            transformation_name=name,
            source_file=rel_schema,
            dynamic_reference=flags["dynamic"],
            requires_runtime_context=flags["requires_context"],
            metadata=model_data.get("meta", {})
        )

    def _parse_dbt_source(self, source_entry: dict, schema_path: Path) -> list[TransformationNode]:
        """Extract source definitions and any cross-references."""
        db = source_entry.get("name", "")
        nodes = []
        for table in source_entry.get("tables", []):
            tbl = table.get("name", "")
            name = f"{db}.{tbl}" if db else tbl
            
            sources: set[str] = set()
            desc = table.get("description", "")
            self._extract_refs_from_text(desc, sources)
            
            rel_schema = str(schema_path.relative_to(self.repo_root)).replace("\\", "/") if self.repo_root in schema_path.parents else str(schema_path)
            nodes.append(TransformationNode(
                id=f"dbt_source::{rel_schema}::{name}",
                source_datasets=sorted(list(sources)),
                target_datasets=[name],
                transformation_type="dbt_source",
                transformation_name=name,
                source_file=rel_schema,
                dynamic_reference="{{" in desc or "{%" in desc,
                requires_runtime_context="{{" in desc or "{%" in desc,
                metadata=table.get("meta", {})
            ))
        return nodes

    def _extract_from_config_and_meta(self, data: dict, sources: set[str]):
        """Helper to pull dependencies from 'config' and 'meta' blocks."""
        # config.depends_on
        config = data.get("config", {})
        deps = config.get("depends_on", [])
        if isinstance(deps, list): sources.update(str(d) for d in deps)
        elif isinstance(deps, str): sources.add(deps)
        
        # meta.lineage or similar
        meta = data.get("meta", {})
        if "lineage" in meta and isinstance(meta["lineage"], list):
            sources.update(str(l) for l in meta["lineage"])

    def _extract_refs_from_text(self, text: str, sources: set[str]):
        """Regex-based ref() and source() extraction."""
        if not text: return
        # ref('model')
        refs = re.findall(r"ref\(['\"]([\w_]+)['\"]\)", text)
        sources.update(refs)
        # source('db', 'table')
        srcs = re.findall(r"source\(['\"]([\w_]+)['\"]\s*,\s*['\"]([\w_]+)['\"]\)", text)
        for s in srcs: sources.add(f"{s[0]}.{s[1]}")

    def _extract_refs_from_yaml_struct(self, struct: Any, sources: set[str]):
        """Recursive traversal of YAML objects to find refs."""
        if isinstance(struct, str):
            self._extract_refs_from_text(struct, sources)
        elif isinstance(struct, list):
            for item in struct: self._extract_refs_from_yaml_struct(item, sources)
        elif isinstance(struct, dict):
            for val in struct.values(): self._extract_refs_from_yaml_struct(val, sources)

    # ─── Airflow Parsing ──────────────────────────────────────────────────────

    def analyze_airflow_dag(self, dag_path: Path) -> list[TransformationNode]:
        """Advanced Airflow parsing with Tree-Sitter."""
        result = self.ts.parse_file(dag_path)
        if not result: return []

        nodes: list[TransformationNode] = []
        # var_name -> info
        task_info: dict[str, dict] = {}
        flags = {"dynamic": False}

        # 1. State Capture & Constant Resolution
        constants = self._get_python_constants(result)
        
        # 2. Extract Task Definitions
        def walk_for_tasks(node):
            if node.type in ("for_statement", "list_comprehension"):
                flags["dynamic"] = True
            
            info = self._extract_task_info(node, result.source_bytes, constants)
            if info:
                # var_name is usually the direct parent assignment left node text
                # but _extract_task_info needs to be smarter
                pass
            
            # Implementation note: task extraction needs the variable name
            if node.type == "assignment":
                left = node.child_by_field_name("left")
                right = node.child_by_field_name("right")
                if left and right:
                    var_name = self.ts.get_node_text(left, result.source_bytes)
                    info = self._extract_task_info(right, result.source_bytes, constants)
                    if info:
                        task_info[var_name] = info

            for child in node.children: walk_for_tasks(child)

        walk_for_tasks(result.root_node)

        # 3. Resolve Dependencies (Recursive Chains)
        def walk_for_deps(node):
            if node.type == "binary_operator":
                op_idx, op = self._find_operator(node, result.source_bytes)
                if op in (">>", "<<"):
                    # For a >> b:
                    # terminals of a point to entries of b
                    left_child = self._get_payload_child(node, op_idx, -1)
                    right_child = self._get_payload_child(node, op_idx, 1)
                    
                    if not left_child or not right_child:
                        for child in node.children: walk_for_deps(child)
                        return

                    left_nodes = self._get_terminals(left_child, result.source_bytes)
                    right_nodes = self._get_entries(right_child, result.source_bytes)
                    
                    for ln in left_nodes:
                        for rn in right_nodes:
                            up_var = ln if op == ">>" else rn
                            down_var = rn if op == ">>" else ln
                            
                            up_info = task_info.get(up_var, {"task_id": up_var})
                            down_info = task_info.get(down_var, {"task_id": down_var})
                            
                            nodes.append(self._create_airflow_edge(
                                up_info, down_info, dag_path, flags["dynamic"]
                            ))
            
            for child in node.children: walk_for_deps(child)

        walk_for_deps(result.root_node)

        # Fallback: Individual nodes if no edges
        if not nodes and task_info:
            for info in task_info.values():
                nodes.append(self._create_airflow_node(info, dag_path, flags["dynamic"]))

        return nodes

    def _get_python_constants(self, parse_result) -> dict:
        """Simple constant extractor from the AST, handles expression statements."""
        constants = {}
        def walk(node):
            if node.type == "assignment":
                left = node.child_by_field_name("left")
                right = node.child_by_field_name("right")
                if left and right and right.type == "string":
                    constants[self.ts.get_node_text(left, parse_result.source_bytes)] = \
                        self.ts.get_node_text(right, parse_result.source_bytes).strip("'\"")
            for child in node.children: walk(child)
        walk(parse_result.root_node)
        return constants

    def _extract_task_info(self, node, source_bytes, constants: dict) -> Optional[dict]:
        """Deep metadata extraction for an Operator call."""
        if node.type != "call": return None
        
        func_node = node.child_by_field_name("function")
        if not func_node: return None
        
        func_name = self.ts.get_node_text(func_node, source_bytes)
        if "Operator" not in func_name and "Sensor" not in func_name:
            return None
            
        info = {
            "operator": func_name,
            "task_id": None,
            "python_callable": None,
            "bash_command": None,
            "metadata": {}
        }
        
        args = node.child_by_field_name("arguments")
        if args:
            for arg in args.children:
                text = self.ts.get_node_text(arg, source_bytes)
                if "=" in text:
                    key, val = text.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip("'\"")
                    
                    # Constant substitution
                    if val in constants: val = constants[val]
                    
                    if key == "task_id": info["task_id"] = val
                    elif key == "python_callable": info["python_callable"] = val
                    elif key == "bash_command": info["bash_command"] = val
                    elif key in ("pool", "retries", "trigger_rule", "queue"):
                        info["metadata"][key] = val
        
        return info

    def _find_operator(self, node, source_bytes) -> tuple[int, Optional[str]]:
        """Finds the index and text of the >> or << operator, skipping line continuations."""
        for i, child in enumerate(node.children):
            text = self.ts.get_node_text(child, source_bytes)
            if text in (">>", "<<"):
                return i, text
        return -1, None

    def _get_payload_child(self, node, index, direction: int):
        """Finds the first non-backslash child in the given direction."""
        idx = index + direction
        while 0 <= idx < len(node.children):
            child = node.children[idx]
            if child.type != "line_continuation":
                return child
            idx += direction
        return None

    def _get_entries(self, node, source_bytes) -> list[str]:
        """Finds the 'entry' tasks for a node in a dependency chain."""
        if node.type == "identifier":
            return [self.ts.get_node_text(node, source_bytes)]
        if node.type == "list":
            items = []
            for child in node.children:
                if child.type == "identifier":
                    items.append(self.ts.get_node_text(child, source_bytes))
            return items
        if node.type == "binary_operator":
            op_idx, op = self._find_operator(node, source_bytes)
            if op == ">>":
                left = self._get_payload_child(node, op_idx, -1)
                return self._get_entries(left, source_bytes) if left else []
            if op == "<<":
                right = self._get_payload_child(node, op_idx, 1)
                return self._get_entries(right, source_bytes) if right else []
        return [self.ts.get_node_text(node, source_bytes)]

    def _get_terminals(self, node, source_bytes) -> list[str]:
        """Finds the 'terminal' tasks for a node in a dependency chain."""
        if node.type == "identifier":
            return [self.ts.get_node_text(node, source_bytes)]
        if node.type == "list":
            items = []
            for child in node.children:
                if child.type == "identifier":
                    items.append(self.ts.get_node_text(child, source_bytes))
            return items
        if node.type == "binary_operator":
            op_idx, op = self._find_operator(node, source_bytes)
            if op == ">>":
                right = self._get_payload_child(node, op_idx, 1)
                return self._get_terminals(right, source_bytes) if right else []
            if op == "<<":
                left = self._get_payload_child(node, op_idx, -1)
                return self._get_terminals(left, source_bytes) if left else []
        return [self.ts.get_node_text(node, source_bytes)]

    def _create_airflow_edge(self, up, down, path, dynamic) -> TransformationNode:
        up_id = up.get("task_id") or "unknown"
        down_id = down.get("task_id") or "unknown"
        meta = {
            "upstream_operator": up.get("operator"),
            "downstream_operator": down.get("operator")
        }
        meta.update(up.get("metadata", {}))
        meta.update(down.get("metadata", {}))
        
        if up.get("python_callable"): meta["upstream_python_callable"] = up["python_callable"]
        if down.get("python_callable"): meta["downstream_python_callable"] = down["python_callable"]

        return TransformationNode(
            id=f"airflow::{path}::{up_id}__{down_id}",
            source_datasets=[up_id],
            target_datasets=[down_id],
            transformation_type="airflow",
            transformation_name=f"{up_id} >> {down_id}",
            source_file=str(path),
            requires_runtime_context=dynamic,
            dynamic_reference=dynamic,
            metadata=meta
        )

    def _create_airflow_node(self, info, path, dynamic) -> TransformationNode:
        tid = info.get("task_id") or "unknown"
        return TransformationNode(
            id=f"airflow::{path}::{tid}",
            source_datasets=[],
            target_datasets=[tid],
            transformation_type="airflow",
            transformation_name=tid,
            source_file=str(path),
            requires_runtime_context=dynamic,
            dynamic_reference=dynamic,
            metadata=info.get("metadata", {})
        )

    # ─── Public API ───────────────────────────────────────────────────────────

    def analyze(self, path: Path) -> list[TransformationNode]:
        """Refactored routing and standardized output."""
        name = path.name.lower()
        nodes: list[TransformationNode] = []
        
        if name.endswith((".yml", ".yaml")):
            nodes = self.analyze_dbt_schema(path)
        elif name.endswith(".py") and ("dag" in name or "airflow" in name):
            nodes = self.analyze_airflow_dag(path)
        
        # Post-process for global consistency and scoring
        for node in nodes:
            try:
                p = Path(node.source_file)
                if p.is_absolute():
                    node.source_file = str(p.relative_to(self.repo_root)).replace("\\", "/")
            except Exception: pass
            
            if "::" not in node.id:
                node.id = f"{node.transformation_type}::{node.source_file}::{node.id}"
                
            # Assign confidence score
            node.confidence = 0.5 if node.requires_runtime_context else 1.0
                
        return nodes


def parse_config_file(path: Path) -> list[TransformationNode]:
    """Legacy entry point."""
    return DAGConfigAnalyzer().analyze(path)
