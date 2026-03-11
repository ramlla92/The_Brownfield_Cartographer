"""
Orchestrator: wires Surveyor → Hydrologist → Semanticist → Archivist.
Handles incremental update mode (re-analyse only git-changed files).
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

from src.agents.archivist import Archivist
from src.agents.hydrologist import Hydrologist
from src.agents.semanticist import Semanticist
from src.agents.surveyor import Surveyor
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.graph import CartographyResult


def run_analyze(repo_root: Path) -> None:
    """
    Run Phase 1 (Surveyor) on repo_root and write .cartography/module_graph.json.
    """
    repo_root = repo_root.resolve()
    cartography_dir = repo_root / ".cartography"
    cartography_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"[Orchestrator] Running Surveyor analysis on: {repo_root}")
    
    surveyor = Surveyor(repo_root=repo_root)
    surveyor.build_module_graph()
    
    output_path = cartography_dir / "module_graph.json"
    surveyor.write_module_graph_json(output_path)
    logger.info(f"[Orchestrator] Saved module graph to {output_path}")


def run_lineage(repo_root: Path) -> None:
    """
    Run Phase 2 (Hydrologist) on repo_root and write .cartography/lineage_graph.json.
    """
    repo_root = repo_root.resolve()
    cartography_dir = repo_root / ".cartography"
    cartography_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"[Orchestrator] Running Hydrologist analysis on: {repo_root}")

    hydrologist = Hydrologist(repo_root=repo_root)
    lineage_graph = hydrologist.run()

    output_path = cartography_dir / "lineage_graph.json"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(lineage_graph.model_dump_json(indent=2))
    
    logger.info(f"[Orchestrator] Saved data lineage to {output_path}")


def run_full_pipeline(
    repo_path: Path,
    output_dir: Path,
    skip_semantics: bool = False,
) -> CartographyResult:
    """
    Execute the full Cartographer pipeline.

    Args:
        repo_path:       Absolute path to the target repository root.
        output_dir:      Where to write .cartography/ artifacts.
        skip_semantics:  Skip the Semanticist (saves LLM cost for interim runs).

    Returns:
        CartographyResult with all fields populated.
    """
    repo_path = repo_path.resolve()
    repo_name = repo_path.name
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    logger.info(f"[Orchestrator] Starting full pipeline on: {repo_path}")

    # 1. Surveyor
    logger.info("[Orchestrator] Phase 1: Surveyor")
    surveyor = Surveyor(repo_root=repo_path)
    # The current orchestrator expects surveyor.run() but Surveyor has build_module_graph()
    # Let's fix it to use build_module_graph() and then convert to our model if needed.
    # However, for now, let's keep it consistent with what run_full_pipeline expects 
    # if there was a separate 'run' method, but since there isn't, we adapt.
    nx_graph = surveyor.build_module_graph()
    
    # We need to map nx.DiGraph to our ModuleGraph model. 
    # Since I don't see a helper for that yet, and run_full_pipeline 
    # likely expected a return value of type ModuleGraph from surveyor.run().
    from src.models.graph import ModuleGraph
    # We can reconstruct it from the graph nodes/edges
    modules = {}
    for node_id, attrs in nx_graph.nodes(data=True):
        modules[node_id] = ModuleNode(**attrs)
    module_graph = ModuleGraph(modules=modules)

    # 2. Hydrologist
    logger.info("[Orchestrator] Phase 2: Hydrologist")
    hydrologist = Hydrologist(repo_root=repo_path)
    lineage_graph = hydrologist.run()

    # 3. Build result skeleton
    result = CartographyResult(
        repo_path=str(repo_path),
        repo_name=repo_name,
        analysis_timestamp=timestamp,
        module_graph=module_graph,
        lineage_graph=lineage_graph,
        high_velocity_files=sorted(
            module_graph.modules,
            key=lambda k: module_graph.modules[k].change_velocity_30d,
            reverse=True,
        )[:20],
    )

    # 4. Semanticist (optional)
    if not skip_semantics:
        logger.info("[Orchestrator] Phase 3: Semanticist")
        semanticist = Semanticist()
        result = semanticist.enrich(result, repo_path)
    else:
        logger.info("[Orchestrator] Phase 3: Semanticist SKIPPED (--no-llm flag)")

    # 5. Load into KnowledgeGraph
    kg = KnowledgeGraph()
    kg.load(result)
    result.module_graph.pagerank_scores = kg.pagerank()
    result.module_graph.strongly_connected_components = kg.strongly_connected_components()

    # 6. Archivist
    logger.info("[Orchestrator] Phase 4: Archivist")
    archivist = Archivist(output_dir=output_dir)
    archivist.write_all(result)

    logger.info(f"[Orchestrator] Pipeline complete. Artifacts → {output_dir}")
    return result


def run_incremental_pipeline(
    repo_path: Path,
    output_dir: Path,
    since_commit: str = "HEAD~1",
) -> None:
    """
    Re-analyse only files changed since a given git commit.
    Loads existing graphs, patches them, and re-runs Archivist.

    TODO (Phase 4): Implement full incremental logic.
    """
    logger.info(f"[Orchestrator] Incremental mode since: {since_commit}")

    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "diff", "--name-only", since_commit, "HEAD"],
            capture_output=True, text=True, timeout=15,
        )
        changed_files = [f.strip() for f in result.stdout.splitlines() if f.strip()]
        logger.info(f"[Orchestrator] Changed files: {len(changed_files)}")
    except Exception as exc:
        logger.error(f"[Orchestrator] Cannot determine changed files: {exc}")
        return

    # TODO: load existing graphs, patch only changed modules, re-run Archivist
    logger.warning("[Orchestrator] Incremental patch not yet implemented — running full pipeline.")
    run_full_pipeline(repo_path, output_dir)
