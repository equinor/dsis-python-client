"""Model operations mixin for DSIS client.

Provides model validation, discovery, and serialization operations.
"""

import logging
from typing import Any, Dict, List, Optional, Type

from ..models import (
    cast_results,
    deserialize_response,
    get_model_by_name,
    get_model_fields,
    is_valid_model,
)

logger = logging.getLogger(__name__)


class ModelOperationsMixin:
    """Mixin providing model utility operations.

    This mixin adds model validation, discovery, and serialization
    capabilities to the DSIS client.
    """

    def _is_valid_model(self, model_name: str, domain: str = "common") -> bool:
        """Check if a model name is valid in dsis_schemas.

        Args:
            model_name: Name of the model to check
            domain: Domain to search in - "common" or "native" (default: "common")

        Returns:
            True if the model exists, False otherwise
        """
        return is_valid_model(model_name, domain)

    def get_model_by_name(
        self, model_name: str, domain: str = "common"
    ) -> Optional[Type]:
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
            >>> Well = client.get_model_by_name("Well")
            >>> Basin = client.get_model_by_name("Basin", domain="common")
        """
        return get_model_by_name(model_name, domain)

    def get_model_fields(
        self, model_name: str, domain: str = "common"
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
            >>> fields = client.get_model_fields("Well")
            >>> print(fields.keys())
        """
        return get_model_fields(model_name, domain)

    def deserialize_response(
        self, response: Dict[str, Any], model_name: str, domain: str = "common"
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
            >>> response = client.get_odata("123", "wells", data_table="Well")
            >>> well = client.deserialize_response(response, "Well")
            >>> print(well.well_name)
        """
        return deserialize_response(response, model_name, domain)

    def cast_results(
        self, results: List[Dict[str, Any]], schema_class: Type
    ) -> List[Any]:
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
            >>> query = QueryBuilder(district_id="123", field="SNORRE").schema(Fault)
            >>> response = client.executeQuery(query)
            >>> faults = client.cast_results(response["value"], Fault)
        """
        return cast_results(results, schema_class)
