"""
Agent 3: The Semanticist — LLM-Powered Purpose Analyst.
Generates purpose statements, detects documentation drift, clusters modules
into business domains, and answers the Five FDE Day-One Questions.

Status: STUB — full implementation in Phase 3.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from loguru import logger

from src.models.graph import CartographyResult, ModuleGraph, DataLineageGraph
from src.models.nodes import ModuleNode


# ─── Context Window Budget ────────────────────────────────────────────────────

class ContextWindowBudget:
    """
    Tracks cumulative token usage and enforces model selection policy.

    Policy:
        < 100k tokens  → use FAST_MODEL (Gemini Flash / Mistral)
        >= 100k tokens → use SYNTHESIS_MODEL (Gemini Pro / GPT-4)
    """

    FAST_MODEL: str = os.getenv("DEFAULT_FAST_MODEL", "gemini-1.5-flash")
    SYNTHESIS_MODEL: str = os.getenv("DEFAULT_SYNTHESIS_MODEL", "gemini-1.5-pro")
    FAST_THRESHOLD: int = 100_000

    def __init__(self):
        self._total_tokens: int = 0

    def select_model(self, estimated_tokens: int) -> str:
        return (
            self.FAST_MODEL
            if self._total_tokens + estimated_tokens < self.FAST_THRESHOLD
            else self.SYNTHESIS_MODEL
        )

    def record(self, tokens: int) -> None:
        self._total_tokens += tokens
        logger.debug(f"[Budget] Total tokens used: {self._total_tokens:,}")

    @property
    def total(self) -> int:
        return self._total_tokens


# ─── Semanticist Agent ────────────────────────────────────────────────────────

class Semanticist:
    """
    LLM-powered semantic analysis agent.

    Usage:
        sem = Semanticist()
        sem.enrich(result)  # mutates CartographyResult in place
    """

    def __init__(self):
        self.budget = ContextWindowBudget()
        self._llm = None  # lazy-loaded

    def _get_llm(self, model: str):
        """Lazy-load the LLM client."""
        # TODO (Phase 3): initialise actual LangChain LLM here
        # from langchain_google_genai import ChatGoogleGenerativeAI
        # return ChatGoogleGenerativeAI(model=model)
        raise NotImplementedError("LLM backend not yet configured (Phase 3)")

    # ─── Purpose Statement ────────────────────────────────────────────

    def generate_purpose_statement(
        self, module: ModuleNode, source_code: str
    ) -> str:
        """
        Send module code (NOT docstring) to the LLM.
        Returns a 2-3 sentence business-function purpose statement.

        TODO (Phase 3): Implement prompt + call + response parsing.
        """
        logger.debug(f"[Semanticist] Stub: purpose_statement for {module.path}")
        return f"[STUB] Purpose statement for {module.path} — not yet generated."

    # ─── Documentation Drift Detection ───────────────────────────────

    def detect_docstring_drift(self, module: ModuleNode, source_code: str) -> bool:
        """
        Compare the LLM-generated purpose statement against the module's
        actual docstring. Flag if they contradict.

        TODO (Phase 3): Implement similarity check.
        """
        return False

    # ─── Domain Clustering ────────────────────────────────────────────

    def cluster_into_domains(
        self, modules: dict[str, ModuleNode], n_clusters: int = 6
    ) -> dict[str, list[str]]:
        """
        Embed all Purpose Statements, run k-means, label clusters.

        TODO (Phase 3):
            1. Embed purpose statements with sentence-transformers.
            2. Run KMeans(n_clusters=n_clusters).
            3. Ask LLM to name each cluster given its member modules.

        Returns:
            { cluster_label: [module_path, ...] }
        """
        logger.debug(f"[Semanticist] Stub: cluster_into_domains ({len(modules)} modules)")
        return {"unclustered": list(modules.keys())}

    # ─── Five FDE Day-One Questions ───────────────────────────────────

    def answer_day_one_questions(
        self,
        module_graph: ModuleGraph,
        lineage_graph: DataLineageGraph,
    ) -> dict[str, str]:
        """
        Synthesise Surveyor + Hydrologist output and ask the LLM the
        Five FDE Day-One Questions with evidence citations.

        Questions:
            Q1: Primary data ingestion path?
            Q2: 3-5 most critical output datasets/endpoints?
            Q3: Blast radius if most critical module fails?
            Q4: Where is business logic concentrated vs distributed?
            Q5: What has changed most frequently in the last 90 days?

        TODO (Phase 3): Build synthesis prompt + call LLM.
        """
        logger.debug("[Semanticist] Stub: answer_day_one_questions")
        return {
            "Q1_ingestion_path": "STUB — not yet generated",
            "Q2_critical_outputs": "STUB — not yet generated",
            "Q3_blast_radius": "STUB — not yet generated",
            "Q4_business_logic": "STUB — not yet generated",
            "Q5_git_velocity": "STUB — not yet generated",
        }

    # ─── Main Entry Point ─────────────────────────────────────────────

    def enrich(self, result: CartographyResult, repo_root: Path) -> CartographyResult:
        """
        Enrich a CartographyResult in-place with semantic data.
        Called after Surveyor and Hydrologist have run.
        """
        logger.info("[Semanticist] Enriching — Phase 3 not yet implemented, returning stubs.")
        result.day_one_answers = self.answer_day_one_questions(
            result.module_graph, result.lineage_graph
        )
        result.domain_clusters = self.cluster_into_domains(result.module_graph.modules)
        return result
