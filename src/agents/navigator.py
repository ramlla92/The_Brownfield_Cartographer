"""
Agent 5: The Navigator — LangGraph Query Interface.
Provides four tools for exploring the codebase knowledge graph:
    1. find_implementation(concept)    — semantic search (Ranked)
    2. trace_lineage(dataset, direction) — graph traversal (Recursive)
    3. blast_radius(module_path)       — downstream impact (Full)
    4. explain_module(path)            — LLM explanation (Defensive)
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
from typing import Literal, Optional, List, Dict, Set, Any

from loguru import logger
from langchain_core.tools import StructuredTool
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import create_react_agent

from src.models.graph import CartographyResult


class Navigator:
    """
    Interactive query agent for the codebase knowledge graph.
    Powered by LangGraph to enable reasoning and tool use.

    Usage:
        nav = Navigator(result)
        response = nav.ask("Where is revenue calculated?")
    """

    def __init__(self, result: CartographyResult):
        self.result = result
        self.tools = self._build_tools()
        
        # Select model based on env, fallback to flash for efficiency
        model_name = os.getenv("DEFAULT_SYNTHESIS_MODEL", "gemini-2.5-flash")
        self.llm = ChatGoogleGenerativeAI(model=model_name, temperature=0)
        
        system_prompt = (
            "You are the Brownfield Cartographer Navigator, an expert codebase FDE. "
            "You have access to tools that query a pre-analyzed knowledge graph. "
            "CRITICAL INSTRUCTION: Every answer YOU provide MUST cite evidence. "
            "For every file or dataset you mention, you MUST include: "
            "1. The source file / file path. "
            "2. The line range (if available from the tools). "
            "3. The analysis method that produced the insight (state either 'Static Analysis' or 'LLM Inference'). "
            "This distinction matters for trust. If you are unsure of the line range, say 'Line range not available'."
        )
        
        # Create ReAct agent graph
        self.agent_executor = create_react_agent(self.llm, self.tools)

    def _build_tools(self) -> list[StructuredTool]:
        return [
            StructuredTool.from_function(
                func=self.find_implementation,
                name="find_implementation",
                description="Ranked semantic search over module purpose, paths, and datasets to find where a concept is implemented. Powered by LLM Inference."
            ),
            StructuredTool.from_function(
                func=self.trace_lineage,
                name="trace_lineage",
                description="Recursive graph traversal to find the full upstream or downstream chain of a dataset. Powered by Static Analysis."
            ),
            StructuredTool.from_function(
                func=self.blast_radius,
                name="blast_radius",
                description="Identify exhaustive downstream impact (importers, produced datasets, and their consumers) if a module changes. Powered by Static Analysis."
            ),
            StructuredTool.from_function(
                func=self.explain_module,
                name="explain_module",
                description="Get detailed metadata and LLM-powered explanation of a module's purpose. Powered by LLM Inference."
            )
        ]

    def ask(self, query: str) -> str:
        """Query the LangGraph agent."""
        logger.info(f"[Navigator] Received query: {query}")
        start_time = time.time()
        result = self.agent_executor.invoke({"messages": [("user", query)]})
        duration = time.time() - start_time
        logger.info(f"[Navigator] Query fulfilled in {duration:.2f}s")
        return result["messages"][-1].content

    # ─── Tool 1: Semantic Search (Ranked) ──────────────────────────────

    def find_implementation(self, concept: str) -> str:
        """Ranked semantic search over module purpose, paths, and datasets."""
        start_time = time.time()
        concept_low = concept.lower()
        modules = self.result.module_graph.modules
        lg = self.result.lineage_graph
        
        scored_results = []
        for path, node in modules.items():
            score = 0
            reasons = []
            
            # Path match (Highest)
            if concept_low in path.lower():
                score += 10
                reasons.append("Path match")
            
            # Purpose match
            purpose = (node.purpose_statement or "").lower()
            if concept_low in purpose:
                score += 5
                reasons.append("Purpose match")
                
            # Produced dataset match
            produced = [e.dataset_name for e in lg.produces_edges if path in e.transformation_id]
            for ds in produced:
                if concept_low in ds.lower():
                    score += 8
                    reasons.append(f"Produced dataset match: {ds}")
                    break
            
            if score > 0:
                scored_results.append({
                    "path": path,
                    "score": score,
                    "reasons": ", ".join(reasons),
                    "node": node
                })
        
        duration = time.time() - start_time
        logger.info(f"[Navigator Tool] find_implementation: '{concept}' returned {len(scored_results)} matches in {duration:.3f}s")

        if not scored_results:
            return f"No implementations found matching '{concept}'."

        scored_results.sort(key=lambda x: x["score"], reverse=True)
        top_results = scored_results[:15]
        
        lines = [f"## Ranked matches for '{concept}':"]
        for r in top_results:
            n = r["node"]
            lines.append(f"### {r['path']} (Score: {r['score']})")
            lines.append(f"- **Reasons**: {r['reasons']}")
            lines.append(f"- **Purpose**: {n.purpose_statement or 'No summary available.'}")
            lines.append(f"- **Method**: LLM Inference")
            lines.append(f"- **Context**: Lines 1-{n.lines_of_code or 'end'}")
            
        return "\n".join(lines)

    # ─── Tool 2: Lineage Tracing (Recursive) ───────────────────────────

    def trace_lineage(
        self,
        dataset: str,
        direction: Literal["upstream", "downstream"] = "upstream",
    ) -> str:
        """Recursive graph traversal to find the full lineage chain."""
        start_time = time.time()
        lg = self.result.lineage_graph
        
        # Fuzzy matching: if 'customers' is requested, but only 'marts.customers' exists, match it.
        target_ds = dataset.lower()
        all_datasets = list(lg.datasets.keys()) + lg.source_datasets + lg.sink_datasets
        
        actual_dataset = None
        if target_ds in all_datasets:
            actual_dataset = target_ds
        else:
            # Try fuzzy match (ends with or contains)
            matches = [d for d in all_datasets if d.lower().endswith(f".{target_ds}") or d.lower() == target_ds]
            if matches:
                actual_dataset = matches[0]
            else:
                # Check edges
                edge_datasets = {e.dataset_name for e in lg.produces_edges + lg.consumes_edges}
                matches = [d for d in edge_datasets if d.lower().endswith(f".{target_ds}") or d.lower() == target_ds]
                if matches:
                    actual_dataset = matches[0]

        if not actual_dataset:
            return f"Dataset '{dataset}' not found in lineage graph."

        visited_ds: Set[str] = set()
        visited_trans: Set[str] = set()
        
        def traverse(current_ds: str):
            if current_ds in visited_ds: return
            visited_ds.add(current_ds)
            
            if direction == "upstream":
                # Find transformations that PRODUCE this dataset
                producers = [e.transformation_id for e in lg.produces_edges if e.dataset_name == current_ds]
                for tid in producers:
                    if tid not in visited_trans:
                        visited_trans.add(tid)
                        # Find datasets CONSUMED by this transformation
                        inputs = [e.dataset_name for e in lg.consumes_edges if e.transformation_id == tid]
                        for ds in inputs:
                            traverse(ds)
            else:
                # Find transformations that CONSUME this dataset
                consumers = [e.transformation_id for e in lg.consumes_edges if e.dataset_name == current_ds]
                for tid in consumers:
                    if tid not in visited_trans:
                        visited_trans.add(tid)
                        # Find datasets PRODUCED by this transformation
                        outputs = [e.dataset_name for e in lg.produces_edges if e.transformation_id == tid]
                        for ds in outputs:
                            traverse(ds)

        traverse(actual_dataset)
        duration = time.time() - start_time
        logger.info(f"[Navigator Tool] trace_lineage: '{dataset}' ({direction}) explored {len(visited_trans)} nodes in {duration:.3f}s")
        
        if len(visited_trans) == 0:
            return f"No {direction} dependencies found for '{dataset}'."

        lines = [f"## Recursive Lineage for '{dataset}' ({direction})"]
        lines.append(f"*Explored {len(visited_ds)} datasets and {len(visited_trans)} transformation steps.*")
        
        for tid in list(visited_trans)[:20]:
            t = lg.transformations.get(tid)
            if t:
                lr = f"{t.line_range[0]}-{t.line_range[1]}" if t.line_range else "unknown lines"
                lines.append(f"- **{t.source_file}** ({lr}) — {t.transformation_type}")
            else:
                lines.append(f"- Transformation Step: `{tid}`")
        
        if len(visited_trans) > 20:
            lines.append(f"\n*... and {len(visited_trans) - 20} more steps.*")
            
        return "\n".join(lines)

    # ─── Tool 3: Blast Radius (Enhanced) ──────────────────────────────

    def blast_radius(self, module_path: str) -> str:
        """Identify exhaustive downstream impact of changing a module."""
        start_time = time.time()
        mg = self.result.module_graph
        lg = self.result.lineage_graph

        # Resolve module_path to canonical ID
        canonical_id = module_path.replace("\\", "/")
        
        # Fuzzy match for canonical_id if it doesn't exist
        if canonical_id not in mg.modules:
            matching_paths = [p for p in mg.modules.keys() if canonical_id in p]
            if matching_paths:
                canonical_id = matching_paths[0]
                logger.info(f"[Navigator] Fuzzy matched module '{module_path}' to '{canonical_id}'")

        # 1. Direct Importers (Module level)
        importers = [e.source for e in mg.import_edges if e.target == canonical_id]
        
        # 2. Datasets Produced (Lineage level)
        # We check if source_file matches OR if transformation ID contains the path
        # Fallback: if 'stg_orders.sql' is in 'models/staging/stg_orders.sql', it's a match.
        import os
        produced_datasets = []
        target_base = os.path.basename(canonical_id)
        
        for tid, t in lg.transformations.items():
            source_file = (t.source_file or "").replace("\\", "/")
            is_match = (
                canonical_id == source_file or 
                canonical_id in tid or
                (target_base == os.path.basename(source_file) and target_base != "")
            )
            if is_match:
                produced_datasets.extend(t.target_datasets)
        
        # 3. Consumers of those datasets
        downstream_consumptions = []
        downstream_modules = set()
        for ds in set(produced_datasets):
            consumers = [e.transformation_id for e in lg.consumes_edges if e.dataset_name == ds]
            for tid in consumers:
                t = lg.transformations.get(tid)
                if t:
                    downstream_consumptions.append(f"{t.source_file} (consuming `{ds}`)")
                    downstream_modules.add(t.source_file)

        duration = time.time() - start_time
        impact_count = len(importers) + len(produced_datasets) + len(downstream_modules)
        logger.info(f"[Navigator Tool] blast_radius: '{module_path}' impact score: {impact_count}")

        lines = [f"# Blast Radius Report: `{module_path}`"]
        lines.append(f"**Total Impact Score**: {impact_count}")
        lines.append("\n## Phase 1: Direct Module Dependencies")
        if importers:
            for m in importers[:15]:
                node = mg.modules.get(m)
                lr = f"1-{node.lines_of_code}" if node and node.lines_of_code else "unknown lines"
                lines.append(f"- `{m}` ({lr})")
            if len(importers) > 15: lines.append(f"- ... and {len(importers)-15} more.")
        else:
            lines.append("No direct importers found.")

        lines.append("\n## Phase 2: Data Lineage Impacts")
        if produced_datasets:
            lines.append(f"**Datasets Produced**: {', '.join([f'`{d}`' for d in produced_datasets])}")
            if downstream_consumptions:
                lines.append("\n**Downstream Consumers of Produced Data**:")
                for c in sorted(list(set(downstream_consumptions)))[:15]:
                    lines.append(f"- {c}")
                if len(downstream_consumptions) > 15: lines.append("- ... and others.")
        else:
            lines.append("No datasets produced by this module.")

        return "\n".join(lines)

    # ─── Tool 4: Module Explanation ───────────────────────────────────

    def explain_module(self, path: str) -> str:
        """LLM-powered explanation of a module's purpose and architecture role."""
        logger.info(f"[Navigator Tool] explain_module: '{path}'")
        node = self.result.module_graph.modules.get(path)
        if not node:
            return f"Module '{path}' not found in the knowledge graph."

        # Defensive formatting
        lines = [
            f"# Module Summary: `{path}`",
            f"- **Method**: Synthesised (Static Analysis + LLM Inference)",
            f"- **Complexity**: {node.lines_of_code or 'Unknown'} lines of code ({node.language})",
            f"- **Velocity**: {node.change_velocity_30d or 0} commits in 30 days",
            f"- **Confidence**: {node.confidence:.2f}",
            f"- **Audit Status**: {'Deep Audit Required' if node.deep_audit_required else 'Routine'}",
            f"- **Arch. Identity**: `{node.module_type or 'General'}` module in `{node.domain_cluster or 'Unsorted'}` domain",
            f"\n## Purpose Statement",
            f"{node.purpose_statement or 'No semantic analysis performed yet.'}"
        ]
        
        if node.dependent_modules > 0:
            lines.append(f"\n## Architectural Significance")
            lines.append(f"This module is a dependency for **{node.dependent_modules}** other modules.")

        return "\n".join(lines)


if __name__ == "__main__":
    # Internal dev test stub
    pass
