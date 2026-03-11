"""Pydantic schema models — Edge types for the knowledge graph."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ImportEdge(BaseModel):
    """source_module → target_module (IMPORTS)."""

    source: str
    target: str
    import_count: int = 1


class ProducesEdge(BaseModel):
    """transformation → dataset (PRODUCES) — data lineage."""

    transformation_id: str
    dataset_name: str


class ConsumesEdge(BaseModel):
    """transformation → dataset (CONSUMES) — upstream dependency."""

    transformation_id: str
    dataset_name: str


class CallsEdge(BaseModel):
    """function → function (CALLS) — call graph."""

    caller: str
    callee: str
    call_count: int = 1


class ConfiguresEdge(BaseModel):
    """config_file → module/pipeline (CONFIGURES)."""

    config_path: str
    target: str
    config_type: str = "yaml"  # yaml | env | toml
