"""
Model utilities for DSIS API.

Provides schema validation, model discovery, and serialization utilities.
"""

from .schema_helper import (
    HAS_DSIS_SCHEMAS,
    get_model_by_name,
    get_model_fields,
    is_valid_model,
)
from .serialization import (
    cast_results,
    deserialize_response,
)

__all__ = [
    "HAS_DSIS_SCHEMAS",
    "is_valid_model",
    "get_model_by_name",
    "get_model_fields",
    "cast_results",
    "deserialize_response",
]
