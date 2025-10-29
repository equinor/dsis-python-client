"""Schema helper utilities for DSIS models.

Provides model validation and schema discovery using dsis_model_sdk.
"""

import logging
from typing import Any, Dict, Optional, Type

logger = logging.getLogger(__name__)

# Try to import dsis_schemas utilities
try:
    from dsis_model_sdk import models

    HAS_DSIS_SCHEMAS = True
except ImportError:
    HAS_DSIS_SCHEMAS = False
    logger.debug("dsis_schemas package not available")


def is_valid_model(model_name: str, domain: str = "common") -> bool:
    """Check if a model name is valid in dsis_schemas.

    Args:
        model_name: Name of the model to check
        domain: Domain to search in - "common" or "native" (default: "common")

    Returns:
        True if the model exists, False otherwise
    """
    if not HAS_DSIS_SCHEMAS:
        logger.debug("dsis_schemas not available, skipping model validation")
        return True

    try:
        model = get_model_by_name(model_name, domain)
        return model is not None
    except Exception as e:
        logger.debug(f"Error validating model {model_name}: {e}")
        return False


def get_model_by_name(model_name: str, domain: str = "common") -> Optional[Type]:
    """Get a dsis_schemas model class by name.

    Requires dsis_schemas package to be installed.

    Args:
        model_name: Name of the model (e.g., "Well", "Basin", "Wellbore")
        domain: Domain to search in - "common" or "native" (default: "common")

    Returns:
        The model class if found, None otherwise

    Raises:
        ImportError: If dsis_schemas package is not installed

    Example:
        >>> Well = get_model_by_name("Well")
        >>> Basin = get_model_by_name("Basin", domain="common")
    """
    if not HAS_DSIS_SCHEMAS:
        raise ImportError(
            "dsis_schemas package is required. Install it with: pip install dsis-schemas"
        )

    logger.debug(f"Getting model: {model_name} from {domain} domain")
    try:
        if domain == "common":
            model_module = models.common
        elif domain == "native":
            model_module = models.native
        else:
            raise ValueError(f"Unknown domain: {domain}")

        return getattr(model_module, model_name, None)
    except Exception as e:
        logger.error(f"Failed to get model {model_name}: {e}")
        return None


def get_model_fields(
    model_name: str, domain: str = "common"
) -> Optional[Dict[str, Any]]:
    """Get field information for a dsis_schemas model.

    Requires dsis_schemas package to be installed.

    Args:
        model_name: Name of the model (e.g., "Well", "Basin", "Wellbore")
        domain: Domain to search in - "common" or "native" (default: "common")

    Returns:
        Dictionary of field names and their information

    Raises:
        ImportError: If dsis_schemas package is not installed

    Example:
        >>> fields = get_model_fields("Well")
        >>> print(fields.keys())
    """
    if not HAS_DSIS_SCHEMAS:
        raise ImportError(
            "dsis_schemas package is required. Install it with: pip install dsis-schemas"
        )

    logger.debug(f"Getting fields for model: {model_name} from {domain} domain")
    try:
        model_class = get_model_by_name(model_name, domain)
        if model_class is None:
            return None
        return dict(model_class.model_fields)
    except Exception as e:
        logger.error(f"Failed to get fields for {model_name}: {e}")
        return None
