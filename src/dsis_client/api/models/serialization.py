"""Model serialization and casting utilities.

Provides utilities for casting API responses to model instances.
"""

import logging
from typing import Any, Dict, List, Type

logger = logging.getLogger(__name__)


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
