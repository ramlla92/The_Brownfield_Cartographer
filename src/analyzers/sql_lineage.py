"""
Analyzer 2: SQL Lineage extraction via sqlglot.
Parses .sql / dbt model files and extracts table-level dependencies.

Status: STUB — full implementation in Phase 2.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import sqlglot
import sqlglot.expressions as exp
from loguru import logger

from src.models.nodes import TransformationNode


SUPPORTED_DIALECTS = ["postgres", "bigquery", "snowflake", "duckdb", "spark", ""]


def extract_sql_lineage(
    sql_path: Path,
    dialect: str = "",
    transform_id_prefix: str = "sql",
) -> Optional[TransformationNode]:
    """
    Parse a SQL file and extract table dependencies.

    Returns a TransformationNode where:
        source_datasets = upstream tables (FROM / JOIN)
        target_datasets = target table (CREATE TABLE AS / INSERT INTO)

    Falls back gracefully on parse error.
    """
    try:
        source = sql_path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        logger.warning(f"[sql_lineage] Cannot read {sql_path}: {exc}")
        return None

    try:
        statements = sqlglot.parse(source, dialect=dialect or None, error_level=sqlglot.ErrorLevel.WARN)
    except Exception as exc:
        logger.warning(f"[sql_lineage] Parse error in {sql_path}: {exc}")
        return None

    sources: set[str] = set()
    targets: set[str] = set()

    for stmt in statements:
        if stmt is None:
            continue

        # Upstream: all table references in FROM / JOIN
        for table_ref in stmt.find_all(exp.Table):
            name = _qualified_name(table_ref)
            if name:
                sources.add(name)

        # Downstream: CREATE TABLE AS / INSERT INTO
        if isinstance(stmt, (exp.Create, exp.Insert)):
            target = stmt.find(exp.Table)
            if target:
                t_name = _qualified_name(target)
                if t_name:
                    targets.add(t_name)
                    sources.discard(t_name)  # target ≠ source

    transform_id = f"{transform_id_prefix}::{sql_path}"
    return TransformationNode(
        id=transform_id,
        source_datasets=sorted(sources),
        target_datasets=sorted(targets),
        transformation_type="sql",
        source_file=str(sql_path),
        sql_query_if_applicable=source[:2000],  # store first 2k chars
    )


def _qualified_name(table: exp.Table) -> Optional[str]:
    """Convert a sqlglot Table expression to a dotted string name."""
    parts = [p for p in [table.args.get("db"), table.name] if p]
    return ".".join(str(p) for p in parts) if parts else None
