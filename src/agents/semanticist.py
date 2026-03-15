"""
Agent 3: The Semanticist — LLM-Powered Purpose Analyst.
Generates purpose statements, detects documentation drift, clusters modules
into business domains, and answers the Five FDE Day-One Questions.
"""

from __future__ import annotations

import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
from typing import Optional, Dict, List, Set, Any

import networkx as nx
import tiktoken
from loguru import logger
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser

from src.models.graph import CartographyResult, ModuleGraph, DataLineageGraph
from src.models.nodes import ModuleNode, Language


# ─── Context Window Budget ────────────────────────────────────────────────────

class ContextWindowBudget:
    """
    Tracks cumulative token usage and enforces model selection policy.
    Uses tiktoken for precise estimation.
    """

    FAST_MODEL: str = os.getenv("DEFAULT_FAST_MODEL", "gemini-2.5-flash")
    SYNTHESIS_MODEL: str = os.getenv("DEFAULT_SYNTHESIS_MODEL", "gemini-2.5-flash")
    EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"
    FAST_THRESHOLD: int = 1_000_000 # Gemini Flash is cheap, threshold can be high

    def __init__(self, model_encoding: str = "cl100k_base"):
        self._total_tokens: int = 0
        self._cache_hits: int = 0
        self._modules_analyzed: int = 0
        try:
            self._encoding = tiktoken.get_encoding(model_encoding)
        except Exception:
            self._encoding = tiktoken.encoding_for_model("gpt-4o")

    def estimate_tokens(self, text: str) -> int:
        return len(self._encoding.encode(text, disallowed_special=()))

    def select_model(self, text: str) -> str:
        estimated = self.estimate_tokens(text)
        return (
            self.FAST_MODEL
            if self._total_tokens + estimated < self.FAST_THRESHOLD
            else self.SYNTHESIS_MODEL
        )

    def record(self, tokens: int, from_cache: bool = False):
        if from_cache:
            self._cache_hits += 1
        else:
            self._total_tokens += tokens
            self._modules_analyzed += 1
        
    @property
    def metrics(self) -> Dict[str, Any]:
        return {
            "total_tokens_used": self._total_tokens,
            "cache_hits": self._cache_hits,
            "modules_analyzed": self._modules_analyzed
        }


# ─── Semanticist Agent ────────────────────────────────────────────────────────

class Semanticist:
    """
    Production-ready LLM-powered semantic analysis agent.
    Includes caching, retry logic, and advanced forensic features.
    """
    
    CACHE_PATH = Path(".cartography/purpose_cache.json")

    def __init__(self, repo_root: Path):
        self.repo_root = Path(repo_root).resolve()
        self.budget = ContextWindowBudget()
        self.cache: Dict[str, Dict[str, Any]] = self._load_cache()
        
        # Pre-load embedding model once
        from sentence_transformers import SentenceTransformer
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Models
        self._fast_llm = ChatGoogleGenerativeAI(model=self.budget.FAST_MODEL, temperature=0)
        self._pro_llm = ChatGoogleGenerativeAI(model=self.budget.SYNTHESIS_MODEL, temperature=0)

    def _load_cache(self) -> Dict[str, Any]:
        if self.CACHE_PATH.exists():
            try:
                return json.loads(self.CACHE_PATH.read_text())
            except Exception as exc:
                logger.warning(f"Failed to load purpose cache: {exc}")
        return {}

    def _save_cache(self):
        self.CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.CACHE_PATH.write_text(json.dumps(self.cache, indent=2))
        except Exception as exc:
            logger.error(f"Failed to save purpose cache: {exc}")

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    def _call_llm(self, llm, prompt_input: Dict[str, Any], prompt_template: ChatPromptTemplate, use_json: bool = False):
        parser = JsonOutputParser() if use_json else StrOutputParser()
        chain = prompt_template | llm | parser
        return chain.invoke(prompt_input)

    def analyze_module_semantics(self, module: ModuleNode, source_code: str) -> Dict[str, Any]:
        """
        Extracts purpose, I/O, and pipeline stage from code.
        Uses caching to avoid redundant LLM calls.
        """
        rel_path = module.path
        code_slice = source_code[:15000] # Representative slice
        
        # Cache Check (Keyed by path and a simple length/start hash)
        cache_key = f"{rel_path}:{len(source_code)}:{source_code[:50]}"
        if cache_key in self.cache:
            self.budget.record(0, from_cache=True)
            return self.cache[cache_key]

        estimated_tokens = self.budget.estimate_tokens(code_slice)
        model_selection = self.budget.select_model(code_slice)
        llm = self._pro_llm if model_selection == self.budget.SYNTHESIS_MODEL else self._fast_llm

        prompt = ChatPromptTemplate.from_template("""
        Analyze the following source code from `{path}`.
        Identify its core purpose, data inputs/outputs, and its stage in a typical data pipeline.
        
        Pipeline Stages: Ingestion, Transformation, Cleaning, Aggregation, Validation, Serving, Utility, Config, Orchestration.
        
        Return a JSON object with:
        {{
            "purpose_statement": "2-3 concise sentences on logic and business value",
            "inputs": ["list of primary datasets, tables, or variables used as input"],
            "outputs": ["list of datasets, tables, or variables produced as output"],
            "pipeline_stage": "One of the stages listed above",
            "confidence": 0.0 to 1.0 based on clarity
        }}

        Code:
        {code}
        """)

        try:
            result = self._call_llm(llm, {"path": rel_path, "code": code_slice}, prompt, use_json=True)
            self.budget.record(estimated_tokens)
            self.cache[cache_key] = result
            return result
        except Exception as exc:
            logger.error(f"LLM failure for {rel_path}: {exc}")
            return {
                "purpose_statement": f"Analysis failed: {exc}",
                "inputs": [], "outputs": [], "pipeline_stage": "Unknown", "confidence": 0.0
            }

    def detect_docstring_drift(self, module: ModuleNode, source_code: str) -> Dict[str, Any]:
        """
        Performs semantic comparison between docstring and generated purpose.
        Returns reasoning instead of a simple flag.
        """
        if module.language != Language.PYTHON:
            return {"drift_flag": False, "reasoning": "Not a Python module"}

        doc_match = re.search(r'"""(.*?)"""|\'\'\'(.*?)\'\'\'', source_code, re.DOTALL)
        if not doc_match:
            return {"drift_flag": True, "reasoning": "Missing module docstring"}

        docstring = (doc_match.group(1) or doc_match.group(2)).strip()
        if len(docstring) < 10:
            return {"drift_flag": True, "reasoning": "Docstring is too short/empty"}

        prompt = ChatPromptTemplate.from_template("""
        Compare the generative purpose statement with the existing module docstring.
        Identify if there is high semantic drift (contradictions or significant updates needed).
        
        Generated Purpose: {llm_purpose}
        Existing Docstring: {docstring}
        
        Return JSON:
        {{
            "drift_flag": boolean,
            "reasoning": "Concise explanation of the mismatch or confirmation of alignment"
        }}
        """)

        try:
            result = self._call_llm(self._fast_llm, {"llm_purpose": module.purpose_statement, "docstring": docstring[:2000]}, prompt, use_json=True)
            self.budget.record(1000) # Fixed cost for drift check
            return result
        except Exception as exc:
            logger.error(f"Drift check failed for {module.path}: {exc}")
            return {"drift_flag": False, "reasoning": "Analysis error"}

    def cluster_into_domains(self, modules: Dict[str, ModuleNode]) -> Dict[str, List[str]]:
        """
        Robust domain clustering with dynamic cluster count and LLM consensus naming.
        """
        from sklearn.cluster import KMeans
        import numpy as np

        valid_nodes = [m for m in modules.values() if m.purpose_statement and "failed" not in m.purpose_statement]
        if not valid_nodes: return {"unclustered": list(modules.keys())}

        # Dynamic cluster count: sqrt(N/2) up to 12
        n_clusters = min(max(int(len(valid_nodes)**0.5 / 1.4), 3), 12)
        
        purposes = [m.purpose_statement for m in valid_nodes]
        embeddings = self.embedding_model.encode(purposes)
        
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init="auto")
        labels = kmeans.fit_predict(embeddings)
        
        temp_clusters = defaultdict(list)
        for node, label in zip(valid_nodes, labels):
            temp_clusters[label].append(node.path)
            
        refined_clusters = {}
        for label, paths in temp_clusters.items():
            # Consensus Naming
            examples = [modules[p].purpose_statement for p in paths[:3]]
            prompt = ChatPromptTemplate.from_template("""
            Suggest a 1-2 word business domain name for a group of modules with these purpose statements:
            {purposes}
            
            Return ONLY the name.
            """)
            try:
                name = self._call_llm(self._fast_llm, {"purposes": "\n- ".join(examples)}, prompt).strip().strip('"')
            except:
                name = f"Domain_{label}"
                
            refined_clusters[name] = paths
            for p in paths:
                modules[p].domain_cluster = name

        return refined_clusters

    def blast_radius_analysis(self, result: CartographyResult):
        """
        Integrates dependency context to identify downstream impact.
        Updates ModuleNode with blast scores.
        """
        # Module-level impact
        mg = nx.DiGraph()
        for edge in result.module_graph.import_edges:
            mg.add_edge(edge.source, edge.target)
            
        for path, node in result.module_graph.modules.items():
            if path in mg:
                # Downstream nodes are those that IMPORT this node
                # Note:nx.descendants in an import graph shows what THIS node depends on.
                # To find impact, we need the reverse: who depends on THIS node.
                impacted = list(nx.ancestors(mg, path))
                node.dependent_modules = len(impacted)
                if node.page_rank and node.page_rank > 0.05:
                    node.is_architectural_hub = True

    def answer_day_one_questions(self, result: CartographyResult) -> Dict[str, str]:
        """
        Final synthesis of the 'Five FDE Day-One Questions'.
        """
        mg = result.module_graph
        lg = result.lineage_graph
        
        top_hubs = sorted(mg.modules.values(), key=lambda x: x.page_rank or 0, reverse=True)[:5]
        dead_code_candidates = [m.path for m in mg.modules.values() if m.is_dead_code_candidate]
        
        context = {
            "critical_hubs": [{"path": m.path, "purpose": m.purpose_statement} for m in top_hubs],
            "data_sources": lg.source_datasets[:10],
            "data_sinks": lg.sink_datasets[:10],
            "dead_code_count": len(dead_code_candidates),
            "failed_parses": result.analysis_metadata.get("failed_module_count", 0)
        }
        
        prompt = ChatPromptTemplate.from_template("""
        As a Senior FDE, synthesize this technical map into clear onboarding answers.
        
        Context: {context}
        
        Questions:
        1. Primary ingestion path and source-of-truth datasets?
        2. Top 3 critical business outputs?
        3. Most fragile architectural module (highest blast radius)?
        4. Strategy for technical debt cleanup (dead code and failures)?
        5. Where is domain logic most concentrated?
        
        Return JSON:
        {{ "Q1": "...", "Q2": "...", "Q3": "...", "Q4": "...", "Q5": "..." }}
        """)
        
        try:
            return self._call_llm(self._pro_llm, {"context": json.dumps(context)}, prompt, use_json=True)
        except:
            return {"error": "Synthesis failed"}

    def enrich(self, result: CartographyResult) -> CartographyResult:
        """Main entry point."""
        logger.info(f"[Semanticist] Starting semantic enrichment for {len(result.module_graph.modules)} modules...")
        
        for path, node in tqdm(result.module_graph.modules.items(), desc="Semanticist: Analysis"):
            file_path = self.repo_root / path
            if not file_path.exists(): continue
            
            try:
                content = file_path.read_text(encoding="utf-8", errors="replace")
                
                # 1. Semantics (Purpose, I/O, Stage)
                sem = self.analyze_module_semantics(node, content)
                node.purpose_statement = sem.get("purpose_statement")
                node.module_type = sem.get("pipeline_stage") # Map stage to module_type
                node.confidence = sem.get("confidence", node.confidence)
                
                # 2. Doc Drift
                drift = self.detect_docstring_drift(node, content)
                node.docstring_drift_flag = drift.get("drift_flag", False)
                # Could store 'reasoning' in a private field or metadata
                
            except Exception as exc:
                logger.error(f"Failed semantic analysis for {path}: {exc}")

        # 3. Clustering
        result.domain_clusters = self.cluster_into_domains(result.module_graph.modules)
        
        # 4. Blast Radius
        self.blast_radius_analysis(result)
        
        # 5. Day-One Questions
        result.day_one_answers = self.answer_day_one_questions(result)
        
        # Cleanup
        self._save_cache()
        logger.info(f"[Semanticist] Enrichment complete. Metrics: {self.budget.metrics}")
        return result


if __name__ == "__main__":
    # Internal dev test logic would go here
    pass
