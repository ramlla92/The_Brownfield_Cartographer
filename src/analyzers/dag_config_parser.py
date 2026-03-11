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

def parse_dbt_schema(schema_path: Path) -> list[TransformationNode]:
    """
    Parse a dbt schema.yml or sources.yml.
    Returns TransformationNodes for each model that declares dependencies.

    TODO: Also parse dbt_project.yml to capture model-level config.
    """
    try:
        with schema_path.open() as f:
            data: dict = yaml.safe_load(f) or {}
    except Exception as exc:
        logger.warning(f"[dag_config_parser] Cannot parse {schema_path}: {exc}")
        return []

    nodes: list[TransformationNode] = []

    for model in data.get("models", []):
        name = model.get("name", "unknown")
        depends: list[str] = []

        # dbt ref() deps listed under meta or columns — usually resolved from .sql
        # Config-level source declarations
        for source_entry in data.get("sources", []):
            for table in source_entry.get("tables", []):
                db = source_entry.get("name", "")
                tbl = table.get("name", "")
                depends.append(f"{db}.{tbl}" if db else tbl)

        nodes.append(TransformationNode(
            id=f"dbt::{schema_path}::{name}",
            source_datasets=depends,
            target_datasets=[name],
            transformation_type="dbt",
            source_file=str(schema_path),
        ))

    return nodes


# ─── Airflow DAG Python Files ─────────────────────────────────────────────────

def parse_airflow_dag(dag_path: Path) -> list[TransformationNode]:
    """
    Naively extract task/operator dependency chains from Airflow DAG Python files.

    Uses regex to find >> / << operator chains.
    TODO (Phase 2): Upgrade to tree-sitter AST for operator argument extraction.
    """
    import re

    try:
        source = dag_path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        logger.warning(f"[dag_config_parser] Cannot read {dag_path}: {exc}")
        return []

    # Find task chains: task_a >> task_b >> task_c
    chains = re.findall(r"([\w_]+)\s*>>\s*([\w_]+)", source)
    nodes: list[TransformationNode] = []

    for upstream, downstream in chains:
        nodes.append(TransformationNode(
            id=f"airflow::{dag_path}::{upstream}__{downstream}",
            source_datasets=[upstream],
            target_datasets=[downstream],
            transformation_type="airflow",
            source_file=str(dag_path),
        ))

    return nodes


# ─── Router ───────────────────────────────────────────────────────────────────

def parse_config_file(path: Path) -> list[TransformationNode]:
    """Route a config file to the appropriate parser."""
    name = path.name.lower()

    if name in ("schema.yml", "schema.yaml", "sources.yml", "sources.yaml"):
        return parse_dbt_schema(path)

    if name.endswith(".py") and "dag" in name.lower():
        return parse_airflow_dag(path)

    logger.debug(f"[dag_config_parser] Unrecognised config file: {path}")
    return []
