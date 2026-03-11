"""
Analyzer 1: Multi-language AST parsing via tree-sitter.
Provides a LanguageRouter and per-file analyze_module() function.

Status: STUB — full implementation in Phase 1.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from loguru import logger

from src.models.nodes import FunctionNode, Language, ModuleNode


# ─── Language Router ──────────────────────────────────────────────────────────

EXTENSION_MAP: dict[str, Language] = {
    ".py": Language.PYTHON,
    ".sql": Language.SQL,
    ".yaml": Language.YAML,
    ".yml": Language.YAML,
    ".js": Language.JAVASCRIPT,
    ".ts": Language.TYPESCRIPT,
    ".ipynb": Language.NOTEBOOK,
}


def detect_language(path: Path) -> Language:
    """Return the Language enum for a given file path."""
    return EXTENSION_MAP.get(path.suffix.lower(), Language.UNKNOWN)


# ─── Core Analysis Entry Point ────────────────────────────────────────────────

def analyze_module(path: Path, repo_root: Path) -> Optional[ModuleNode]:
    """
    Parse a single file and return a ModuleNode.

    Uses tree-sitter for Python/JS/YAML, sqlglot for SQL.
    Falls back to regex on parse failure and logs the event.
    """
    lang = detect_language(path)
    rel_path = str(path.relative_to(repo_root))

    try:
        source = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        logger.warning(f"Cannot read {rel_path}: {exc}")
        return None

    node = ModuleNode(path=rel_path, language=lang)

    if lang == Language.PYTHON:
        node = _analyze_python(source, node)
    elif lang == Language.SQL:
        node = _analyze_sql(source, node)
    elif lang == Language.YAML:
        node = _analyze_yaml(source, node)
    else:
        pass  # other languages: stub

    node.lines_of_code = source.count("\n") + 1
    return node


# ─── Language-specific Analyzers (stubs) ─────────────────────────────────────

def _analyze_python(source: str, node: ModuleNode) -> ModuleNode:
    """
    TODO (Phase 1): Replace regex with tree-sitter AST queries.
    Extract: imports, function/class defs, docstrings, complexity.
    """
    # Basic regex fallback for imports
    imports = re.findall(r"^(?:import|from)\s+([\w.]+)", source, re.MULTILINE)
    node.imports = list(set(imports))

    # Count comment lines
    comment_lines = len(re.findall(r"^\s*#", source, re.MULTILINE))
    node.comment_ratio = comment_lines / max(node.lines_of_code, 1)

    logger.debug(f"[tree_sitter_analyzer] Python stub: {node.path}")
    return node


def _analyze_sql(source: str, node: ModuleNode) -> ModuleNode:
    """
    TODO (Phase 2): Delegate to sql_lineage.SQLLineageAnalyzer.
    """
    logger.debug(f"[tree_sitter_analyzer] SQL stub: {node.path}")
    return node


def _analyze_yaml(source: str, node: ModuleNode) -> ModuleNode:
    """
    TODO (Phase 2): Delegate to dag_config_parser.DAGConfigAnalyzer.
    """
    logger.debug(f"[tree_sitter_analyzer] YAML stub: {node.path}")
    return node
