"""Pydantic schema models — Node types for the knowledge graph."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, Any

from pydantic import BaseModel, Field


class Language(str, Enum):
    PYTHON = "python"
    SQL = "sql"
    YAML = "yaml"
    JAVASCRIPT = "javascript"
    TYPESCRIPT = "typescript"
    NOTEBOOK = "notebook"
    UNKNOWN = "unknown"


class StorageType(str, Enum):
    TABLE = "table"
    FILE = "file"
    STREAM = "stream"
    API = "api"


class ModuleNode(BaseModel):
    """Represents a source file / module in the codebase."""

    path: str = Field(..., description="Relative path from repo root")
    language: Language = Language.UNKNOWN
    analysis_method: Optional[str] = None  # tree_sitter | heuristic | llm
    module_type: Optional[str] = None  # pipeline | ingestion | transformation | api | config | test | utility
    is_entrypoint: bool = False
    imports: list[str] = Field(default_factory=list)
    public_functions: list[str] = Field(default_factory=list)
    classes: list[str] = Field(default_factory=list)
    bases: dict[str, list[str]] = Field(default_factory=dict)
    change_velocity_30d: Optional[int] = None
    page_rank: Optional[float] = None
    scc_id: Optional[int] = None
    is_dead_code_candidate: bool = False
    is_high_velocity_core: bool = False
    is_architectural_hub: bool = False
    last_modified: Optional[datetime] = None
    
    # Extra fields from brief/original scaffold if needed for later phases
    purpose_statement: Optional[str] = None
    domain_cluster: Optional[str] = None
    complexity_score: float = 0.0
    lines_of_code: int = 0
    comment_ratio: float = 0.0
    docstring_drift_flag: bool = False

    # Phase 4 & 7 Enhancements
    import_count: int = 0
    dependent_modules: int = 0
    confidence: float = 1.0
    deep_audit_required: bool = False
    max_complexity: float = 0.0
    topological_layer: Optional[int] = None
    parsing_error: Optional[str] = None


class DatasetNode(BaseModel):
    """Represents a data source, sink, or intermediate dataset."""

    name: str = Field(..., description="Table name, file path, or stream topic")
    namespace: str = Field("default", description="Environment namespace, e.g. snowflake.prod.raw")
    storage_type: StorageType = StorageType.TABLE
    schema_snapshot: Optional[dict] = None
    freshness_sla: Optional[str] = None
    owner: Optional[str] = None
    is_source_of_truth: bool = False

    # Phase 4 Enhancements
    location: Optional[str] = None
    dataset_role: Optional[str] = None  # source | staging | intermediate | mart | feature | output
    upstream_count: int = 0
    downstream_count: int = 0
    analysis_method: Optional[str] = None
    confidence: float = 1.0


class FunctionNode(BaseModel):
    """Represents a function or method in the codebase."""

    qualified_name: str = Field(..., description="e.g. src.transforms.revenue.calc_revenue")
    parent_module: str
    signature: str
    purpose_statement: Optional[str] = None
    call_count_within_repo: int = 0
    is_public_api: bool = False
    start_line: int = 0
    end_line: int = 0
    docstring_drift_flag: bool = False

    # Phase 4 Enhancements
    decorators: list[str] = Field(default_factory=list)
    cyclomatic_complexity: Optional[int] = None
    analysis_method: Optional[str] = None
    confidence: float = 1.0


class TransformationNode(BaseModel):
    """Represents a data transformation step."""
    name: Optional[str] = None
    id: str = Field(..., description="Unique identifier")
    framework: Optional[str] = None  # pandas | spark | dbt | airflow | sql | duckdb
    source_datasets: list[str] = Field(default_factory=list)
    target_datasets: list[str] = Field(default_factory=list)
    transformation_type: str = "unknown"  # e.g. pandas, spark, dbt, sql, airflow
    source_file: str = ""
    line_range: tuple[int, int] = (0, 0)
    sql_query_if_applicable: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    confidence_score: float = 1.0

    # Lineage Flags
    dynamic_reference: bool = False
    requires_runtime_context: bool = False
    via_sql: bool = False

    # Phase 4 Enhancements
    confidence: float = 1.0
    analysis_method: Optional[str] = None
