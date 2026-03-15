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
from src.analyzers.git_velocity import adjust_confidence
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.graph import CartographyResult
from src.models.nodes import ModuleNode


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
    load_existing: bool = False,
) -> CartographyResult:
    """
    Execute the full Cartographer pipeline.
    """
    repo_path = repo_path.resolve()
    repo_name = repo_path.name
    # Trust output_dir if it's already absolute (e.g. from CLI), else resolve relative to repo
    cart_dir = output_dir if output_dir.is_absolute() else (repo_path / output_dir).resolve()
    module_graph_path = cart_dir / "module_graph.json"
    lineage_graph_path = cart_dir / "lineage_graph.json"

    logger.info(f"[Orchestrator] Starting full pipeline on: {repo_path}")

    module_graph = None
    lineage_graph = None

    # 1. Attempt Loading Existing Artifacts
    if load_existing:
        if module_graph_path.exists() and lineage_graph_path.exists():
            try:
                logger.info("[Orchestrator] Loading existing artifacts from .cartography/")
                from src.models.graph import ModuleGraph, DataLineageGraph
                module_graph = ModuleGraph.model_validate_json(module_graph_path.read_text())
                lineage_graph = DataLineageGraph.model_validate_json(lineage_graph_path.read_text())
                logger.info("[Orchestrator] Artifacts loaded successfully.")
            except Exception as e:
                logger.warning(f"[Orchestrator] Failed to load artifacts: {e}. Falling back to full analysis.")

    # 2. Phase 1: Surveyor
    if not module_graph:
        logger.info("[Orchestrator] Phase 1: Surveyor")
        surveyor = Surveyor(repo_root=repo_path)
        nx_graph = surveyor.build_module_graph()
        
        from src.models.graph import ModuleGraph
        modules = {}
        for node_id, attrs in nx_graph.nodes(data=True):
            modules[node_id] = ModuleNode(**attrs)
        module_graph = ModuleGraph(modules=modules, import_edges=[
            # Edges are currently in the nx_graph but Surveyor doesn't return them as a list of ImportEdge
            # We need to extract them from nx_graph
        ])
        from src.models.edges import ImportEdge
        import_edges = []
        for u, v, attrs in nx_graph.edges(data=True):
            if attrs.get("type") == "IMPORTS":
                import_edges.append(ImportEdge(
                    source=u, target=v, import_count=attrs.get("weight", 1)
                ))
        module_graph.import_edges = import_edges

    # 3. Phase 2: Hydrologist
    if not lineage_graph:
        logger.info("[Orchestrator] Phase 2: Hydrologist")
        hydrologist = Hydrologist(repo_root=repo_path)
        lineage_graph = hydrologist.run()

    # 4. Build result skeleton
    result = CartographyResult(
        repo_path=str(repo_path),
        repo_name=repo_name,
        analysis_timestamp=datetime.now(tz=timezone.utc).isoformat(),
        module_graph=module_graph,
        lineage_graph=lineage_graph,
        high_velocity_files=sorted(
            module_graph.modules,
            key=lambda k: module_graph.modules[k].change_velocity_30d or 0,
            reverse=True,
        )[:20],
    )

        # 4. Semanticist (optional)
    if not skip_semantics:
        logger.info("[Orchestrator] Phase 3: Semanticist")
        semanticist = Semanticist(repo_root=repo_path)
        result = semanticist.enrich(result)
    else:
        logger.info("[Orchestrator] Phase 3: Semanticist SKIPPED (--no-llm flag)")

    # 5. Load into KnowledgeGraph
    kg = KnowledgeGraph()
    kg.load(result)
    result.module_graph.pagerank_scores = kg.pagerank()
    result.module_graph.strongly_connected_components = kg.strongly_connected_components()

    # Apply confidence adjustment
    for path, node in result.module_graph.modules.items():
        pr = result.module_graph.pagerank_scores.get(path, 0.0)
        node.page_rank = pr
        node.confidence = adjust_confidence(
            base_confidence=node.confidence,
            change_velocity=node.change_velocity_30d or 0,
            page_rank=pr
        )

    # 6. Archivist
    logger.info("[Orchestrator] Phase 4: Archivist")
    archivist = Archivist(output_dir=cart_dir)
    archivist.write_all(result)

    logger.info(f"[Orchestrator] Pipeline complete. Artifacts → {output_dir}")
    return result


def run_incremental_pipeline(
    repo_path: Path,
    output_dir: Path,
    since_commit: str = "HEAD~1",
    skip_semantics: bool = False,
) -> None:
    """
    Re-analyse only files changed since a given git commit.
    Optimized to only run deep analysis on changed files.
    """
    logger.info(f"[Orchestrator] Incremental mode since: {since_commit}")

    cart_dir = output_dir if output_dir.is_absolute() else repo_path / output_dir
    module_graph_path = cart_dir / "module_graph.json"
    lineage_graph_path = cart_dir / "lineage_graph.json"

    if not module_graph_path.exists() or not lineage_graph_path.exists():
        logger.warning("[Orchestrator] Previous Cartography artifacts not found. Running full pipeline.")
        run_full_pipeline(repo_path, output_dir)
        return

    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "diff", "--name-only", since_commit, "HEAD"],
            capture_output=True, text=True, timeout=15,
        )
        changed_files = {f.strip() for f in result.stdout.splitlines() if f.strip()}
        logger.info(f"[Orchestrator] Changed files identified: {len(changed_files)}")
    except Exception as exc:
        logger.error(f"[Orchestrator] Cannot determine changed files: {exc}. Running full.")
        run_full_pipeline(repo_path, output_dir)
        return

    if not changed_files:
        logger.info("[Orchestrator] No files changed. Analysis up to date.")
        return

    # 1. Load previous state
    from src.models.graph import ModuleGraph, DataLineageGraph
    prev_module_graph = ModuleGraph.model_validate_json(module_graph_path.read_text())
    prev_lineage_graph = DataLineageGraph.model_validate_json(lineage_graph_path.read_text())

    # 2. Optimized Surveyor
    logger.info("[Orchestrator] Phase 1: Surveyor (Incremental)")
    surveyor = Surveyor(repo_root=repo_path, include_files=changed_files)
    nx_graph = surveyor.build_module_graph()
    
    # Merge Module Nodes
    for node_id, attrs in nx_graph.nodes(data=True):
        new_node = ModuleNode(**attrs)
        # Restore semantic state for unchanged files (Skeletons)
        if node_id not in changed_files and node_id in prev_module_graph.modules:
            prev_node = prev_module_graph.modules[node_id]
            new_node.purpose_statement = prev_node.purpose_statement
            new_node.domain_cluster = prev_node.domain_cluster
            new_node.docstring_drift_flag = prev_node.docstring_drift_flag
            new_node.confidence = prev_node.confidence
            new_node.module_type = prev_node.module_type
        prev_module_graph.modules[node_id] = new_node

    # Update Edges (Surveyor already built a new nx_graph with updated skeletons)
    from src.models.edges import ImportEdge
    import_edges = []
    for u, v, attrs in nx_graph.edges(data=True):
        if attrs.get("type") == "IMPORTS":
            import_edges.append(ImportEdge(
                source=u, target=v, import_count=attrs.get("weight", 1)
            ))
    prev_module_graph.import_edges = import_edges

    # 3. Optimized Hydrologist
    logger.info("[Orchestrator] Phase 2: Hydrologist (Incremental)")
    hydrologist = Hydrologist(repo_root=repo_path, include_files=changed_files)
    new_lineage_graph = hydrologist.run()

    # Merge Lineage: Evict all transformations from changed files and replace
    t_ids_to_remove = [tid for tid, t in prev_lineage_graph.transformations.items() if t.source_file in changed_files]
    for tid in t_ids_to_remove:
        prev_lineage_graph.transformations.pop(tid, None)
    
    prev_lineage_graph.transformations.update(new_lineage_graph.transformations)
    
    # Rebuild edges for lineage (much safer than patching)
    prev_lineage_graph.produces_edges = [e for e in prev_lineage_graph.produces_edges if e.transformation_id not in t_ids_to_remove]
    prev_lineage_graph.produces_edges.extend(new_lineage_graph.produces_edges)
    
    prev_lineage_graph.consumes_edges = [e for e in prev_lineage_graph.consumes_edges if e.transformation_id not in t_ids_to_remove]
    prev_lineage_graph.consumes_edges.extend(new_lineage_graph.consumes_edges)
    
    # Update datasets (Add new ones, preserve old)
    prev_lineage_graph.datasets.update(new_lineage_graph.datasets)

    # 4. Build result skeleton
    result = CartographyResult(
        repo_path=str(repo_path),
        repo_name=repo_path.name,
        analysis_timestamp=datetime.now(tz=timezone.utc).isoformat(),
        module_graph=prev_module_graph,
        lineage_graph=prev_lineage_graph,
        high_velocity_files=sorted(
            prev_module_graph.modules,
            key=lambda k: prev_module_graph.modules[k].change_velocity_30d or 0,
            reverse=True,
        )[:20],
    )

    # 5. Semanticist (Implicitly incremental via include_files logic or cache)
    if not skip_semantics:
        logger.info("[Orchestrator] Phase 3: Semanticist (Incremental Enrichment)")
        semanticist = Semanticist(repo_root=repo_path)
        # Note: enrich will still iterate but will use cache for unchanged
        result = semanticist.enrich(result)
    else:
        logger.info("[Orchestrator] Phase 3: Semanticist SKIPPED (--no-llm flag)")

    # 6. Global Analytics (Recalculate on full merged graph)
    kg = KnowledgeGraph()
    kg.load(result)
    result.module_graph.pagerank_scores = kg.pagerank()
    result.module_graph.strongly_connected_components = kg.strongly_connected_components()

    for path, node in result.module_graph.modules.items():
        pr = result.module_graph.pagerank_scores.get(path, 0.0)
        node.page_rank = pr
        node.confidence = adjust_confidence(
            base_confidence=node.confidence,
            change_velocity=node.change_velocity_30d or 0,
            page_rank=pr
        )

    # 7. Archivist
    logger.info("[Orchestrator] Phase 4: Archivist (Incremental Write)")
    archivist = Archivist(output_dir=cart_dir)
    archivist.write_all(result)

    logger.info(f"[Orchestrator] Incremental Pipeline complete. Artifacts → {cart_dir}")

