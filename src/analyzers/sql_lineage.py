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


class SQLLineageAnalyzer:
    """
    Extracts table-level lineage from SQL files using sqlglot.
    """

    def __init__(self, dialect: str = ""):
        self.dialect = dialect

    def analyze(self, sql_path: Path) -> Optional[TransformationNode]:
        """
        Extract lineage from a SQL file.
        """
        try:
            source = sql_path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            logger.warning(f"[sql_lineage] Cannot read {sql_path}: {exc}")
            return None

        sources, targets = self.extract_lineage_from_string(source)
        if not sources and not targets:
            return None

        return TransformationNode(
            id=f"sql::{sql_path}",
            source_datasets=sorted(list(sources)),
            target_datasets=sorted(list(targets)),
            transformation_type="sql",
            source_file=str(sql_path),
            sql_query_if_applicable=source[:2000],
        )

    def extract_lineage_from_string(self, source: str) -> tuple[set[str], set[str]]:
        """Extract sources and targets from a SQL string."""
        # Handle dbt ref() and source() tags
        import re
        processed_source = re.sub(r"\{\{\s*ref\(['\"]([\w_]+)['\"]\)\s*\}\}", r"\1", source)
        processed_source = re.sub(r"\{\{\s*source\(['\"]([\w_]+)['\"]\s*,\s*['\"]([\w_]+)['\"]\)\s*\}\}", r"\1.\2", processed_source)

        try:
            statements = sqlglot.parse(processed_source, dialect=self.dialect or None, error_level=sqlglot.ErrorLevel.WARN)
        except Exception as exc:
            logger.debug(f"[sql_lineage] Parse error: {exc}")
            return set(), set()

        sources: set[str] = set()
        targets: set[str] = set()
        ctes: set[str] = set()

        for stmt in statements:
            if stmt is None:
                continue

            # Identify CTEs to avoid treating them as source tables
            for cte in stmt.find_all(exp.CTE):
                cte_name = cte.alias
                if cte_name:
                    ctes.add(cte_name.lower())

            # Upstream: all table references in FROM / JOIN
            for table_ref in stmt.find_all(exp.Table):
                name = _qualified_name(table_ref)
                if name:
                    # Skip if it's a CTE
                    if name.lower() not in ctes:
                        sources.add(name)

            # Downstream: CREATE TABLE AS / INSERT INTO
            if isinstance(stmt, (exp.Create, exp.Insert)):
                target = stmt.find(exp.Table)
                if target:
                    t_name = _qualified_name(target)
                    if t_name:
                        targets.add(t_name)
                        # A target shouldn't be its own source in a single statement
                        if t_name in sources:
                            sources.remove(t_name)

        return sources, targets

        if not sources and not targets:
            return None

        return TransformationNode(
            id=f"sql::{sql_path}",
            source_datasets=sorted(list(sources)),
            target_datasets=sorted(list(targets)),
            transformation_type="sql",
            source_file=str(sql_path),
            sql_query_if_applicable=source[:2000],
        )


def _qualified_name(table: exp.Table) -> Optional[str]:
    """Convert a sqlglot Table expression to a dotted string name."""
    db = table.args.get("db")
    catalog = table.args.get("catalog")
    name = table.name
    
    parts = []
    if catalog: parts.append(str(catalog))
    if db: parts.append(str(db))
    if name: parts.append(str(name))
    
    return ".".join(parts) if parts else None


def extract_sql_lineage(sql_path: Path, dialect: str = "") -> Optional[TransformationNode]:
    """Legacy helper for single-file extraction."""
    analyzer = SQLLineageAnalyzer(dialect=dialect)
    return analyzer.analyze(sql_path)
