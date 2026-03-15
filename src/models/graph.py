"""Pydantic schema models — Top-level graph containers."""

from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field

from .nodes import DatasetNode, FunctionNode, ModuleNode, TransformationNode
from .edges import CallsEdge, ConfiguresEdge, ConsumesEdge, ImportEdge, ProducesEdge, ImplementsEdge


class ModuleGraph(BaseModel):
    """Full module-level dependency graph (Surveyor output)."""

    modules: dict[str, ModuleNode] = Field(default_factory=dict)
    import_edges: list[ImportEdge] = Field(default_factory=list)
    calls_edges: list[CallsEdge] = Field(default_factory=list)
    functions: dict[str, FunctionNode] = Field(default_factory=dict)
    pagerank_scores: dict[str, float] = Field(default_factory=dict)
    strongly_connected_components: list[list[str]] = Field(default_factory=list)

    # Phase 4 Enhancements
    modules_by_domain: dict[str, list[str]] = Field(default_factory=dict)
    entrypoints: list[str] = Field(default_factory=list)


class DataLineageGraph(BaseModel):
    """Data lineage DAG (Hydrologist output)."""

    datasets: dict[str, DatasetNode] = Field(default_factory=dict)
    transformations: dict[str, TransformationNode] = Field(default_factory=dict)
    produces_edges: list[ProducesEdge] = Field(default_factory=list)
    consumes_edges: list[ConsumesEdge] = Field(default_factory=list)
    configures_edges: list[ConfiguresEdge] = Field(default_factory=list)
    source_datasets: list[str] = Field(default_factory=list)   # in-degree == 0
    sink_datasets: list[str] = Field(default_factory=list)     # out-degree == 0
    implements_edges: list[ImplementsEdge] = Field(default_factory=list)

    # Phase 4 Enhancements
    pipeline_paths: list[list[str]] = Field(default_factory=list)


class CartographyResult(BaseModel):
    """Combined output of the full Cartographer pipeline."""

    repo_path: str
    repo_name: str
    analysis_timestamp: str
    module_graph: ModuleGraph = Field(default_factory=ModuleGraph)
    lineage_graph: DataLineageGraph = Field(default_factory=DataLineageGraph)
    domain_clusters: dict[str, list[str]] = Field(default_factory=dict)
    day_one_answers: dict[str, str] = Field(default_factory=dict)
    high_velocity_files: list[str] = Field(default_factory=list)

    # Phase 4 Enhancements
    cartographer_version: str = "0.1"
    analysis_metadata: dict = Field(default_factory=dict)
