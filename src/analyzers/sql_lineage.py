"""
Analyzer 2: SQL Lineage extraction via sqlglot.
Parses .sql / dbt model files and extracts table-level dependencies.
"""

from __future__ import annotations

from pathlib import Path
import threading
from typing import Optional, Dict, List
import re

import sqlglot
import sqlglot.expressions as exp
from loguru import logger

from src.models.nodes import TransformationNode


SUPPORTED_DIALECTS = ["postgres", "bigquery", "snowflake", "duckdb", "spark", ""]


class SQLLineageAnalyzer:
    """
    Extracts table-level lineage from SQL files using sqlglot.
    """

    # Class-level cache for resolution
    _resolution_cache: Dict[str, Optional[str]] = {}
    _lock = threading.Lock()

    def __init__(self, repo_root: Path, dialect: str = "", verbose: bool = False):
        self.repo_root = repo_root
        # Validate dialect
        if dialect and dialect.lower() not in SUPPORTED_DIALECTS:
            logger.warning(f"[sql_lineage] Unsupported dialect '{dialect}'. Falling back to default.")
            self.dialect = ""
        else:
            self.dialect = dialect.lower()
        self.verbose = verbose

    def analyze(self, sql_path: Path) -> Optional[TransformationNode]:
        """
        Extract lineage from a SQL file and resolve to module paths.
        """
        try:
            source = sql_path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            logger.warning(f"[sql_lineage] Cannot read {sql_path}: {exc}")
            return None

        sources_raw, targets_raw = self.extract_lineage_from_string(source)
        
        # Resolve to module paths using cache
        sources_resolved = []
        for s in sources_raw:
            resolved = self._cached_resolve(s)
            sources_resolved.append(resolved if resolved else s)
            if not resolved and self.verbose:
                logger.info(f"[sql_lineage] Unresolved source table: {s}")
            
        targets_resolved = []
        for t in targets_raw:
            resolved = self._cached_resolve(t)
            targets_resolved.append(resolved if resolved else t)
            if not resolved and self.verbose:
                logger.info(f"[sql_lineage] Unresolved target table: {t}")

        abs_sql_path = sql_path.resolve()
        try:
            rel_source_file = str(abs_sql_path.relative_to(self.repo_root)).replace("\\", "/")
        except ValueError:
            # Fallback if path is outside repo for some reason
            rel_source_file = str(sql_path)

        return TransformationNode(
            id=f"sql::{rel_source_file}",
            source_datasets=sorted(list(set(sources_resolved))),
            target_datasets=sorted(list(set(targets_resolved))),
            transformation_type="sql",
            source_file=rel_source_file,
            sql_query_if_applicable=source[:2000],
        )

    def _cached_resolve(self, table_name: str) -> Optional[str]:
        """Thread-safe cached resolution."""
        cache_key = f"{self.repo_root}:{table_name}"
        with self._lock:
            if cache_key in self._resolution_cache:
                return self._resolution_cache[cache_key]
        
        resolved = resolve_table_to_module(table_name, self.repo_root)
        
        with self._lock:
            self._resolution_cache[cache_key] = resolved
            
        return resolved
            
        return resolved

    def extract_lineage_from_string(self, source: str) -> tuple[set[str], set[str]]:
        """Extract sources and targets from a SQL string with recursive CTE handling."""
        if self.verbose:
            logger.info(f"[sql_lineage] Parsing query: {source}")
        
        # Handle dbt ref() and source() tags
        processed_source = re.sub(r"\{\{\s*ref\(['\"]([\w_]+)['\"]\)\s*\}\}", r"\1", source)
        processed_source = re.sub(
            r"\{\{\s*source\(['\"]([\w_]+)['\"]\s*,\s*['\"]([\w_]+)['\"]\)\s*\}\}", 
            r"\1.\2", 
            processed_source
        )

        try:
            # Try to parse the first statement
            stmt = sqlglot.parse_one(processed_source, read=self.dialect or None)
            statements = [stmt]
            if self.verbose:
                logger.info(f"[sql_lineage] Parsed query successfully. Type: {type(stmt)}")
        except Exception as exc:
            if self.verbose:
                logger.debug(f"[sql_lineage] parse_one failed, trying parse: {exc}")
            try:
                statements = sqlglot.parse(
                    processed_source, 
                    dialect=self.dialect or None, 
                    error_level=sqlglot.ErrorLevel.WARN
                )
            except Exception as e:
                logger.warning(f"[sql_lineage] Parse error: {e}")
                # Regex safety net for simple SELECT * FROM table
                m = re.search(r"FROM\s+([\w\.]+)", processed_source, re.IGNORECASE)
                if m:
                    return {m.group(1).lower()}, set()
                return set(), set()

        sources: set[str] = set()
        targets: set[str] = set()

        for stmt in statements:
            if stmt is None:
                continue

            # Identify all CTEs in this statement
            ctes: set[str] = set()
            for node in stmt.walk():
                # logger.debug(f"[sql_lineage] WALK node: {type(node)} | {str(node)[:30]}")
                if "CTE" in str(type(node)):
                    ctes.add(node.alias.lower())

            # Upstream: all table references (using manual walk for robustness)
            table_refs = []
            for node in stmt.walk():
                t_str = str(type(node))
                if "Table" in t_str and "TableAlias" not in t_str:
                    table_refs.append(node)
                elif isinstance(node, exp.Table):
                    table_refs.append(node)
            
            logger.info(f"[sql_lineage] Found {len(table_refs)} table references through liberal type check")

            for table_ref in table_refs:
                name = _qualified_normalized_name(table_ref)
                if not name:
                    continue
                
                # Skip if it refers to a local CTE
                if name.lower() in ctes:
                    continue

                # Check if this table is being written to
                # MERGE, UPDATE, DELETE, CREATE, INSERT
                is_target = False
                parent = table_ref.parent
                while parent:
                    p_type = parent.__class__.__name__
                    if p_type in ("Create", "Insert", "Update", "Delete", "Merge") or \
                       isinstance(parent, (exp.Create, exp.Insert, exp.Update, exp.Delete, exp.Merge)):
                        
                        if p_type == "Merge" or isinstance(parent, exp.Merge):
                            if parent.this == table_ref:
                                is_target = True
                        elif p_type in ("Update", "Delete") or isinstance(parent, (exp.Update, exp.Delete)):
                            if parent.this == table_ref:
                                is_target = True
                        elif p_type in ("Create", "Insert") or isinstance(parent, (exp.Create, exp.Insert)):
                            if parent.this == table_ref or parent.find(exp.Table) == table_ref:
                                is_target = True
                        break
                    parent = parent.parent
                
                if is_target:
                    targets.add(name)
                else:
                    sources.add(name)

        # Cleanup: removes targets from sources
        for t in targets:
            if t in sources:
                sources.remove(t)

        return sources, targets


def _qualified_normalized_name(table: exp.Table) -> Optional[str]:
    """Convert a sqlglot Table expression to a lowercase, dot-separated name."""
    if not table.__class__.__name__ == "Table" and not isinstance(table, exp.Table):
        return str(table).strip("\"'`").lower()

    db = table.args.get("db")
    catalog = table.args.get("catalog")
    name = table.name
    
    parts = []
    if catalog: parts.append(str(catalog).strip("\"'`").lower())
    if db: parts.append(str(db).strip("\"'`").lower())
    if name: 
        parts.append(str(name).strip("\"'`").lower())
    else:
        # Fallback to the SQL representation if .name is empty
        fallback = table.sql().strip("\"'`").lower()
        if fallback:
            parts.append(fallback)
    
    return ".".join(parts) if parts else None


def resolve_table_to_module(table_name: str, repo_root: Path) -> Optional[str]:
    """
    Resolves a table name to a physical .sql file path within the repo.
    """
    base_name = table_name.split(".")[-1]
    
    # Common search patterns
    search_globs = [
        f"**/{base_name}.sql",
        f"**/{table_name}.sql",
    ]
    
    for pattern in search_globs:
        matches = list(repo_root.glob(pattern))
        if matches:
            return str(matches[0].relative_to(repo_root)).replace("\\", "/")
            
    return None


def extract_sql_lineage(sql_path: Path, repo_root: Path, dialect: str = "") -> Optional[TransformationNode]:
    """Legacy helper for single-file extraction."""
    analyzer = SQLLineageAnalyzer(repo_root=repo_root, dialect=dialect)
    return analyzer.analyze(sql_path)
