"""Model serialization and casting utilities.

Provides utilities for casting API responses to model instances.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Type

from .schema_helper import HAS_DSIS_SCHEMAS, get_model_by_name

logger = logging.getLogger(__name__)

# Try to import dsis_schemas utilities
if HAS_DSIS_SCHEMAS:
    try:
        from dsis_model_sdk import deserialize_from_json
    except ImportError:
        deserialize_from_json = None
        logger.debug("deserialize_from_json not available")
else:
    deserialize_from_json = None


def cast_results(results: List[Dict[str, Any]], schema_class: Type) -> List[Any]:
    """Cast API response items to model instances.

    Args:
        results: List of dictionaries from API response (typically response["value"])
        schema_class: Pydantic model class to cast to (e.g., Fault, Well)

    Returns:
        List of model instances

    Raises:
        ValidationError: If any result doesn't match the schema

    Example:
        >>> from dsis_model_sdk.models.common import Fault
        >>> items = [{"id": "1", "type": "NORMAL"}, {"id": "2", "type": "REVERSE"}]
        >>> faults = cast_results(items, Fault)
    """
    casted = []
    for i, result in enumerate(results):
        try:
            instance = schema_class(**result)
            casted.append(instance)
        except Exception as e:
            logger.error(f"Failed to cast result {i} to {schema_class.__name__}: {e}")
            raise

    logger.debug(f"Cast {len(casted)} results to {schema_class.__name__}")
    return casted


def deserialize_response(
    response: Dict[str, Any], model_name: str, domain: str = "common"
) -> Optional[Any]:
    """Deserialize API response to a dsis_schemas model instance.

    Requires dsis_schemas package to be installed.

    Args:
        response: API response dictionary
        model_name: Name of the model to deserialize to (e.g., "Well", "Basin")
        domain: Domain to search in - "common" or "native" (default: "common")

    Returns:
        Deserialized model instance if successful, None otherwise

    Raises:
        ImportError: If dsis_schemas package is not installed
        ValueError: If deserialization fails

    Example:
        >>> response = {"well_name": "Test", "native_uid": "123"}
        >>> well = deserialize_response(response, "Well")
        >>> print(well.well_name)
    """
    if not HAS_DSIS_SCHEMAS:
        raise ImportError(
            "dsis_schemas package is required. Install it with: pip install dsis-schemas"
        )

    try:
        logger.debug(f"Deserializing response to {model_name} from {domain} domain")
        model_class = get_model_by_name(model_name, domain)
        if model_class is None:
            raise ValueError(f"Model '{model_name}' not found in dsis_schemas")

        # Convert response to JSON string for deserialization
        response_json = json.dumps(response)
        return deserialize_from_json(response_json, model_class)
    except Exception as e:
        logger.error(f"Failed to deserialize response to {model_name}: {e}")
        raise ValueError(f"Deserialization failed: {e}")
