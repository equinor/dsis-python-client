"""Schema helper utilities for DSIS models.

Provides model validation and schema discovery using dsis_model_sdk.
"""

import logging
import types
from typing import Optional, Type

from dsis_model_sdk import models

logger = logging.getLogger(__name__)

# Keep this for backward compatibility (always True now)
HAS_DSIS_SCHEMAS = True


def is_valid_schema(schema_name: str, domain: str = "common") -> bool:
    """Check if a schema name is valid in dsis_schemas.

    Args:
        schema_name: Name of the schema to check (e.g., "Well", "Basin", "Fault")
        domain: Domain to search in - "common" or "native" (default: "common")

    Returns:
        True if the schema exists, False otherwise
    """
    try:
        schema = get_schema_by_name(schema_name, domain)
        return schema is not None
    except Exception as e:
        logger.info(f"Error validating schema {schema_name}: {e}")
        return False


def get_schema_by_name(schema_name: str, domain: str = "common") -> Optional[Type]:
    """Get a dsis_schemas schema class by name.

    Args:
        schema_name: Name of the schema (e.g., "Well", "Basin", "Wellbore")
        domain: Domain to search in - "common" or "native" (default: "common")

    Returns:
        The schema class if found, None otherwise

    Example:
        >>> Well = get_schema_by_name("Well")
        >>> Basin = get_schema_by_name("Basin", domain="common")
    """
    logger.info(f"Getting schema: {schema_name} from {domain} domain")
    try:
        schema_module: types.ModuleType
        if domain == "common":
            schema_module = models.common
        elif domain == "native":
            schema_module = models.native
        else:
            raise ValueError(f"Unknown domain: {domain}")

        return getattr(schema_module, schema_name, None)
    except Exception as e:
        logger.error(f"Failed to get schema {schema_name}: {e}")
        return None
