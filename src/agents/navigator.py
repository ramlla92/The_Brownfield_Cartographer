"""
Agent 5: The Navigator — LangGraph Query Interface.
Provides four tools for exploring the codebase knowledge graph:
    1. find_implementation(concept)    — semantic search
    2. trace_lineage(dataset, direction) — graph traversal
    3. blast_radius(module_path)       — downstream impact
    4. explain_module(path)            — LLM explanation

Status: STUB — full LangGraph wiring in Phase 4.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

from loguru import logger

from src.models.graph import CartographyResult


class Navigator:
    """
    Interactive query agent for the codebase knowledge graph.

    Usage:
        nav = Navigator(result)
        nav.find_implementation("revenue calculation")
        nav.trace_lineage("orders", direction="upstream")
        nav.blast_radius("src/transforms/revenue.py")
        nav.explain_module("src/ingestion/kafka_consumer.py")
    """

    def __init__(self, result: CartographyResult):
        self.result = result

    # ─── Tool 1: Semantic Search ───────────────────────────────────────

    def find_implementation(self, concept: str) -> str:
        """
        Semantic search over module purpose statements.
        Returns ranked list of matching modules with evidence (file + method).

        TODO (Phase 4): Query the ChromaDB vector store built by Semanticist.
        """
        logger.info(f"[Navigator] find_implementation: '{concept}'")
        modules = self.result.module_graph.modules
        matches = [
            f"  - `{path}` — {node.purpose_statement or '(no purpose statement yet)'}"
            for path, node in modules.items()
            if concept.lower() in (node.purpose_statement or "").lower()
            or concept.lower() in path.lower()
        ]
        if not matches:
            return f"No modules found matching '{concept}'. (Semantic vector search not yet active.)"
        return f"**Matches for '{concept}':**\n" + "\n".join(matches[:10])

    # ─── Tool 2: Lineage Tracing ───────────────────────────────────────

    def trace_lineage(
        self,
        dataset: str,
        direction: Literal["upstream", "downstream"] = "upstream",
    ) -> str:
        """
        Graph traversal to find all upstream producers or downstream consumers.
        Returns result with file:line citations.
        """
        logger.info(f"[Navigator] trace_lineage: '{dataset}' ({direction})")
        lg = self.result.lineage_graph

        if dataset not in lg.datasets:
            return f"Dataset '{dataset}' not found in lineage graph."

        if direction == "upstream":
            related = [
                e.transformation_id
                for e in lg.consumes_edges
                if e.dataset_name == dataset
            ]
            label = "Upstream producers"
        else:
            related = [
                e.transformation_id
                for e in lg.produces_edges
                if e.dataset_name == dataset
            ]
            label = "Downstream consumers"

        if not related:
            return f"No {direction} dependencies found for '{dataset}'."

        lines = [f"**{label} of `{dataset}`:**"]
        for tid in related:
            t = lg.transformations.get(tid)
            if t:
                lines.append(f"  - `{t.source_file}` (type: {t.transformation_type})")
            else:
                lines.append(f"  - {tid}")

        return "\n".join(lines)

    # ─── Tool 3: Blast Radius ─────────────────────────────────────────

    def blast_radius(self, module_path: str) -> str:
        """
        Identify everything that would be affected if module_path changes.
        Combines module import graph (who imports this module) +
        lineage graph (what datasets does this module produce).
        """
        logger.info(f"[Navigator] blast_radius: '{module_path}'")
        mg = self.result.module_graph

        importers = [
            e.source
            for e in mg.import_edges
            if e.target == module_path
        ]

        lines = [f"**Blast radius of `{module_path}`:**"]

        if importers:
            lines.append(f"\n_Modules that import this file ({len(importers)}):_")
            lines.extend(f"  - `{m}`" for m in importers[:20])
        else:
            lines.append("  No direct importers found.")

        # Dataset impact
        lg = self.result.lineage_graph
        produced = [
            e.dataset_name
            for e in lg.produces_edges
            if module_path in e.transformation_id
        ]
        if produced:
            lines.append(f"\n_Datasets produced by this module ({len(produced)}):_")
            lines.extend(f"  - `{d}`" for d in produced)

        return "\n".join(lines)

    # ─── Tool 4: Module Explanation ───────────────────────────────────

    def explain_module(self, path: str) -> str:
        """
        LLM-powered explanation of a module's purpose and architecture role.

        TODO (Phase 4): Call Semanticist.generate_purpose_statement() on-demand.
        """
        logger.info(f"[Navigator] explain_module: '{path}'")
        node = self.result.module_graph.modules.get(path)
        if not node:
            return f"Module `{path}` not found in the knowledge graph."

        lines = [
            f"## `{path}`",
            f"**Language:** {node.language}",
            f"**Lines of Code:** {node.lines_of_code}",
            f"**Change Velocity (30d):** {node.change_velocity_30d} commits",
            f"**PageRank Score:** {self.result.module_graph.pagerank_scores.get(path, 0):.4f}",
            f"**Dead Code Candidate:** {'Yes' if node.is_dead_code_candidate else 'No'}",
            f"**Domain Cluster:** {node.domain_cluster or 'Not yet clustered'}",
            f"\n**Purpose Statement:**",
            f"  {node.purpose_statement or '(Phase 3 LLM analysis not yet run)'}",
        ]
        if node.imports:
            lines.append(f"\n**Imports ({len(node.imports)}):** " + ", ".join(f"`{i}`" for i in node.imports[:10]))

        return "\n".join(lines)
