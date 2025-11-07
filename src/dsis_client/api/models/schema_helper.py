"""Schema helper utilities for DSIS models.

Provides model validation and schema discovery using dsis_model_sdk.
"""

import logging
from typing import Optional, Type

logger = logging.getLogger(__name__)

# Try to import dsis_schemas utilities
try:
    from dsis_model_sdk import models

    HAS_DSIS_SCHEMAS = True
except ImportError:
    HAS_DSIS_SCHEMAS = False
    logger.debug("dsis_schemas package not available")


def is_valid_schema(schema_name: str, domain: str = "common") -> bool:
    """Check if a schema name is valid in dsis_schemas.

    Args:
        schema_name: Name of the schema to check (e.g., "Well", "Basin", "Fault")
        domain: Domain to search in - "common" or "native" (default: "common")

    Returns:
        True if the schema exists, False otherwise
    """
    if not HAS_DSIS_SCHEMAS:
        logger.debug("dsis_schemas not available, skipping schema validation")
        return True

    try:
        schema = get_schema_by_name(schema_name, domain)
        return schema is not None
    except Exception as e:
        logger.debug(f"Error validating schema {schema_name}: {e}")
        return False


def get_schema_by_name(schema_name: str, domain: str = "common") -> Optional[Type]:
    """Get a dsis_schemas schema class by name.

    Requires dsis_schemas package to be installed.

    Args:
        schema_name: Name of the schema (e.g., "Well", "Basin", "Wellbore")
        domain: Domain to search in - "common" or "native" (default: "common")

    Returns:
        The schema class if found, None otherwise

    Raises:
        ImportError: If dsis_schemas package is not installed

    Example:
        >>> Well = get_schema_by_name("Well")
        >>> Basin = get_schema_by_name("Basin", domain="common")
    """
    if not HAS_DSIS_SCHEMAS:
        raise ImportError(
            "dsis_schemas package is required. Install it with: "
            "pip install dsis-schemas"
        )

    logger.debug(f"Getting schema: {schema_name} from {domain} domain")
    try:
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
