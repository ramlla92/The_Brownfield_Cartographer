"""
Analyzer 3: Airflow / dbt YAML config parser.
Extracts pipeline topology from DAG definitions and dbt schema.yml files.

Status: STUB — full implementation in Phase 2.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import yaml
from loguru import logger

from src.models.nodes import TransformationNode


# ─── dbt schema.yml ───────────────────────────────────────────────────────────

class DAGConfigAnalyzer:
    """
    Extracts pipeline topology from Airflow DAGs and dbt YAML files.
    """

    def analyze_dbt_schema(self, schema_path: Path) -> list[TransformationNode]:
        """
        Parse a dbt schema.yml or sources.yml.
        """
        try:
            with schema_path.open() as f:
                data: dict = yaml.safe_load(f) or {}
        except Exception as exc:
            logger.warning(f"[dag_config_parser] Cannot parse {schema_path}: {exc}")
            return []

        nodes: list[TransformationNode] = []

        # Models
        for model in data.get("models", []):
            name = model.get("name", "unknown")
            depends: list[str] = []
            
            # Look for tests that might imply sources if ref() isn't explicit
            # but usually dbt deps are in the SQL.
            # However, sources themselves are defined here.
            nodes.append(TransformationNode(
                id=f"dbt::{schema_path}::{name}",
                source_datasets=depends,
                target_datasets=[name],
                transformation_type="dbt",
                source_file=str(schema_path),
            ))
            
        # Sources
        for source_entry in data.get("sources", []):
            db = source_entry.get("name", "")
            for table in source_entry.get("tables", []):
                tbl = table.get("name", "")
                name = f"{db}.{tbl}" if db else tbl
                # Sources are entry points, usually no upstream in the config itself
                nodes.append(TransformationNode(
                    id=f"dbt_source::{schema_path}::{name}",
                    source_datasets=[],
                    target_datasets=[name],
                    transformation_type="dbt_source",
                    source_file=str(schema_path),
                ))

        return nodes

    def analyze_airflow_dag(self, dag_path: Path) -> list[TransformationNode]:
        """
        Extract task dependencies from Airflow DAG Python files.
        """
        import re
        import itertools
        try:
            source = dag_path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            logger.warning(f"[dag_config_parser] Cannot read {dag_path}: {exc}")
            return []

        nodes: list[TransformationNode] = []
        
        # 1. Broadly find lines or expressions with >> or <<
        # This handles a >> b >> c by finding the whole chain
        lines = source.splitlines()
        for line in lines:
            if ">>" in line or "<<" in line:
                # Split by operators but keep track of them
                # Regex to split while keeping delimiters
                parts = re.split(r"(\s*(?:>>|<<)\s*)", line)
                tasks = []
                operators = []
                for p in parts:
                    clean = p.strip()
                    if clean in (">>", "<<"):
                        operators.append(clean)
                    elif clean and re.match(r"^[\w_]+$", clean):
                        tasks.append(clean)
                
                # Pair tasks with operators
                for i in range(len(tasks) - 1):
                    t1 = tasks[i]
                    t2 = tasks[i+1]
                    op = operators[i] if i < len(operators) else ">>"
                    
                    if op == ">>":
                        up, down = t1, t2
                    else:
                        up, down = t2, t1
                        
                    nodes.append(TransformationNode(
                        id=f"airflow::{dag_path}::{up}__{down}",
                        source_datasets=[up],
                        target_datasets=[down],
                        transformation_type="airflow",
                        source_file=str(dag_path),
                    ))

        return nodes

    def analyze(self, path: Path) -> list[TransformationNode]:
        """Route a config file to the appropriate parser."""
        name = path.name.lower()
        if name in ("schema.yml", "schema.yaml", "sources.yml", "sources.yaml"):
            return self.analyze_dbt_schema(path)
        if name.endswith(".py") and "dag" in name.lower():
            return self.analyze_airflow_dag(path)
        return []


def parse_config_file(path: Path) -> list[TransformationNode]:
    """Legacy helper."""
    return DAGConfigAnalyzer().analyze(path)
