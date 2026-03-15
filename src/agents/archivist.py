"""
Agent 4: The Archivist — Living Context Maintainer.
Generates robust, production-ready artifacts for human onboarding and AI context injection.
Includes atomic writes, input validation, and deep forensic reporting.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from loguru import logger
from pydantic import BaseModel

from src.models.graph import CartographyResult, ModuleGraph, DataLineageGraph


class ArchivistConfig(BaseModel):
    """Configuration for artifact generation."""
    enable_codebase_md: bool = True
    enable_onboarding_brief: bool = True
    enable_module_graph_json: bool = True
    enable_lineage_graph_json: bool = True
    enable_trace_log: bool = True


class Archivist:
    """
    Robust artifact generation agent.
    
    Features:
        - Atomic writes (temp file + rename)
        - Input validation
        - Error isolation (one failure doesn't block others)
        - Configurable outputs
        - Detailed trace logging
    """

    def __init__(self, output_dir: Path, config: Optional[ArchivistConfig] = None):
        self.output_dir = Path(output_dir).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.config = config or ArchivistConfig()
        self._trace_path = self.output_dir / "cartography_trace.jsonl"
        self._start_time = time.time()

    def _safe_write(self, filename: str, content: str) -> Path:
        """Atomic write: write to temp file then rename."""
        target_path = (self.output_dir / filename).resolve()
        temp_file = (self.output_dir / f".tmp_{filename}_{os.getpid()}").resolve()
        
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(content)
            # Ensure target directory exists
            self.output_dir.mkdir(parents=True, exist_ok=True)
            # Atomic rename (use Path.replace which handles existing files)
            temp_file.replace(target_path)
            return target_path
        except Exception as e:
            logger.error(f"[Archivist] Failed atomic write for {filename} (Temp: {temp_file}): {e}")
            if temp_file.exists():
                temp_file.unlink()
            raise e

    def _validate_result(self, result: CartographyResult):
        """Pre-flight check to ensure result is populated enough for artifacts."""
        if not result.module_graph.modules:
            logger.warning("[Archivist] Module graph is empty. Artifacts may be incomplete.")
        if not result.lineage_graph.datasets and not result.lineage_graph.transformations:
            logger.info("[Archivist] Lineage graph is empty.")

    # ─── Artifact 1: CODEBASE.md ──────────────────────────────────────────

    def generate_codebase_md(self, result: CartographyResult) -> str:
        """Produce the living context file for AI coding agent injection."""
        mg = result.module_graph
        lg = result.lineage_graph
        
        # 1. Critical Path (Sorted by PageRank)
        pr = mg.pagerank_scores
        top_hubs = sorted(mg.modules.values(), key=lambda x: pr.get(x.path, 0.0), reverse=True)[:10]
        
        # 2. Top Datasets (by centrality/connections)
        dataset_connections = defaultdict(int)
        for e in lg.produces_edges: dataset_connections[e.dataset_name] += 1
        for e in lg.consumes_edges: dataset_connections[e.dataset_name] += 1
        top_datasets = sorted(dataset_connections.items(), key=lambda x: x[1], reverse=True)[:5]

        # 3. Domain Clusters
        domain_sections = []
        for domain, paths in result.domain_clusters.items():
            domain_mods = [mg.modules[p] for p in paths if p in mg.modules]
            # Sort within domain by PageRank
            domain_mods.sort(key=lambda x: pr.get(x.path, 0.0), reverse=True)
            
            mod_list = "\n".join([f"  - **{m.path}**: {m.purpose_statement or '(no summary)'}" for m in domain_mods])
            domain_sections.append(f"### Domain: {domain}\n{mod_list}")

        # 4. Blast Radius Examples
        critical_blast = []
        for mod in top_hubs[:3]:
            if mod.dependent_modules > 0:
                critical_blast.append(f"  - `{mod.path}`: Affects **{mod.dependent_modules}** downstream modules.")

        nl = "\n"
        ts = result.analysis_timestamp.isoformat() if hasattr(result.analysis_timestamp, "isoformat") else result.analysis_timestamp
        md = f"""# CODEBASE.md — {result.repo_name}
> Generated: {ts}
> Use this as system context for AI agents to provide architectural grounding.

---

## 1. Executive Summary
- **Inventory**: {len(mg.modules)} modules, {len(lg.transformations)} data transformations.
- **Topology**: {len(result.domain_clusters)} domains detected.
- **Data Lineage**: {len(lg.datasets)} datasets ({len(lg.source_datasets)} sources, {len(lg.sink_datasets)} sinks).

## 2. Critical Architectural Hubs (High PageRank)
{"".join([f"1. `{m.path}` — {m.purpose_statement}{nl}" for m in top_hubs[:5]]) or "No hubs identified."}

## 3. High-Impact Blast Radius
{nl.join(critical_blast) or "No significant blast radius detected."}

## 4. Primary Data Entities
{"".join([f"- `{name}` ({count} connections){nl}" for name, count in top_datasets]) or "No datasets detected."}

## 5. Domain Architecture Map
{nl.join(domain_sections) or "Domain clustering not yet performed."}

## 6. Technical Debt & Drift
- **Circular Dependencies**: {len(mg.strongly_connected_components)} SCCs detected.
- **Documentation Drift**: {len([m for m in mg.modules.values() if m.docstring_drift_flag])} modules out of sync.
"""
        return md.strip()

    # ─── Artifact 2: onboarding_brief.md ──────────────────────────────────

    def generate_onboarding_brief(self, result: CartographyResult) -> str:
        """Generate the Senior FDE Day-One Onboarding Brief."""
        ans = result.day_one_answers
        lg = result.lineage_graph
        mg = result.module_graph

        # Ingestion Summary
        ingestion_files = set()
        for s in lg.source_datasets[:5]:
            for e in lg.consumes_edges:
                if e.dataset_name == s:
                    t = lg.transformations.get(e.transformation_id)
                    if t: ingestion_files.add(t.source_file)

        # Critical Paths Example
        path_list = "\n".join([f"  - `{' → '.join(p[:3])}...`" for p in lg.pipeline_paths[:3]])

        nl = "\n"
        ts = result.analysis_timestamp.isoformat() if hasattr(result.analysis_timestamp, "isoformat") else result.analysis_timestamp
        md = f"""# FDE Onboarding Brief: {result.repo_name}
> Generated: {ts}

## Q1: Ingestion & Source-of-Truth
**Sources**: {', '.join([f'`{s}`' for s in lg.source_datasets[:5]])}
**Primary Ingestion Entrypoints**: {', '.join([f'`{f}`' for f in ingestion_files]) or 'Unknown'}

## Q2: Critical Business Outputs
{ans.get('Q2', '_Analysis pending_')}

## Q3: Architectural Fragility (Top Hubs)
The following modules have the highest blast radius and centrality:
{"".join([f"- `{m.path}` (Dependent modules: {m.dependent_modules}){nl}" for m in sorted(mg.modules.values(), key=lambda x: x.dependent_modules, reverse=True)[:5]])}

## Q4: Data Flow Topology
Example dependency paths identified:
{path_list or "No pipeline paths extracted."}

## Q5: Domain Logic Distribution
{ans.get('Q5', '_Analysis pending_')}
"""
        return md.strip()

    # ─── Write All ────────────────────────────────────────────────────

    def write_all(self, result: CartographyResult) -> Dict[str, Any]:
        """Write all configured artifacts with atomic persistence and error isolation."""
        self._validate_result(result)
        stats = {
            "files_written": [],
            "errors": [],
            "module_count": len(result.module_graph.modules),
            "dataset_count": len(result.lineage_graph.datasets)
        }

        # 1. Module Graph JSON
        if self.config.enable_module_graph_json:
            try:
                path = self._safe_write("module_graph.json", result.module_graph.model_dump_json(indent=2))
                stats["files_written"].append("module_graph.json")
            except Exception as e:
                stats["errors"].append(f"module_graph.json: {e}")

        # 2. Lineage Graph JSON
        if self.config.enable_lineage_graph_json:
            try:
                path = self._safe_write("lineage_graph.json", result.lineage_graph.model_dump_json(indent=2))
                stats["files_written"].append("lineage_graph.json")
            except Exception as e:
                stats["errors"].append(f"lineage_graph.json: {e}")

        # 3. CODEBASE.md
        if self.config.enable_codebase_md:
            try:
                content = self.generate_codebase_md(result)
                path = self._safe_write("CODEBASE.md", content)
                stats["files_written"].append("CODEBASE.md")
            except Exception as e:
                stats["errors"].append(f"CODEBASE.md: {e}")

        # 4. onboarding_brief.md
        if self.config.enable_onboarding_brief:
            try:
                content = self.generate_onboarding_brief(result)
                path = self._safe_write("onboarding_brief.md", content)
                stats["files_written"].append("onboarding_brief.md")
            except Exception as e:
                stats["errors"].append(f"onboarding_brief.md: {e}")

        # 5. Trace Log
        duration = time.time() - self._start_time
        if self.config.enable_trace_log:
            try:
                # Size calculation
                total_bytes = 0
                for f in stats["files_written"]:
                    total_bytes += (self.output_dir / f).stat().st_size
                
                self._log_trace(
                    action="write_all",
                    evidence_source="Archivist Enrichment",
                    confidence_level="HIGH",
                    metadata={
                        "duration_sec": round(duration, 3),
                        "total_bytes": total_bytes,
                        "module_count": stats["module_count"],
                        "dataset_count": stats["dataset_count"],
                        "files_written": stats["files_written"],
                        "error_count": len(stats["errors"])
                    }
                )
            except Exception as e:
                logger.error(f"Failed to log trace: {e}")

        logger.info(f"[Archivist] Completed in {duration:.2f}s. Files: {stats['files_written']}")
        if stats["errors"]:
            for err in stats["errors"]: logger.error(f"[Archivist] {err}")
            
        return stats

    def _log_trace(
        self, 
        action: str, 
        evidence_source: str, 
        confidence_level: str, 
        metadata: dict[str, Any] = None
    ) -> None:
        """Append a JSONL audit event to cartography_trace.jsonl."""
        entry = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "action": action,
            "evidence_source": evidence_source,
            "confidence_level": confidence_level,
            **(metadata or {}),
        }
        with self._trace_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")


if __name__ == "__main__":
    # Internal dev test
    pass
