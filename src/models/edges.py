from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class EdgeType(str, Enum):
    IMPORTS = "IMPORTS"
    PRODUCES = "PRODUCES"
    CONSUMES = "CONSUMES"
    CALLS = "CALLS"
    CONFIGURES = "CONFIGURES"


class ImportEdge(BaseModel):
    """source_module → target_module (IMPORTS)."""

    source: str
    target: str
    import_count: int = 1
    
    # Traceability
    source_file: Optional[str] = None
    line_range: Optional[tuple[int, int]] = None
    confidence: float = 1.0


class ProducesEdge(BaseModel):
    """transformation → dataset (PRODUCES) — data lineage."""

    transformation_id: str
    dataset_name: str
    
    # Traceability
    source_file: Optional[str] = None
    line_range: Optional[tuple[int, int]] = None
    confidence: float = 1.0


class ConsumesEdge(BaseModel):
    """transformation → dataset (CONSUMES) — upstream dependency."""

    transformation_id: str
    dataset_name: str
    
    # Traceability
    source_file: Optional[str] = None
    line_range: Optional[tuple[int, int]] = None
    confidence: float = 1.0


class CallsEdge(BaseModel):
    """function → function (CALLS) — call graph."""

    caller: str
    callee: str
    call_count: int = 1
    
    # Traceability
    source_file: Optional[str] = None
    line_range: Optional[tuple[int, int]] = None
    confidence: float = 1.0


class ConfiguresEdge(BaseModel):
    """config_file → module/pipeline (CONFIGURES)."""

    config_path: str
    target: str
    config_type: str = "yaml"  # yaml | env | toml
    
    # Traceability
    source_file: Optional[str] = None
    line_range: Optional[tuple[int, int]] = None
    confidence: float = 1.0


class ImplementsEdge(BaseModel):
    """code_module/function → transformation (IMPLEMENTS)."""

    source_path: str
    transformation_id: str
    
    # Traceability
    source_file: Optional[str] = None
    line_range: Optional[tuple[int, int]] = None
    confidence: float = 1.0
