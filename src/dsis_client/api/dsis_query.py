"""DSIS Query class for executing OData queries.

Provides a high-level interface for executing queries built with QueryBuilder
against the DSIS API.
"""

import logging
from typing import Any, Dict, List, Optional, Type, Union
from urllib.parse import parse_qs

logger = logging.getLogger(__name__)


class DsisQuery:
    """Represents a DSIS OData query ready for execution.

    Encapsulates a query string built by QueryBuilder along with path parameters
    (district_id, field) needed to construct the full API endpoint.

    This class provides a clean separation between query building (QueryBuilder)
    and query execution (DSISClient.executeQuery).

    Attributes:
        query_string: The OData query string from QueryBuilder.build()
        district_id: Optional district ID for the query
        field: Optional field name for the query

    Example:
        >>> query_builder = QueryBuilder().data_table("Fault").select("id,type").filter("type eq 'NORMAL'")
        >>> query = DsisQuery(
        ...     query_string=query_builder.build(),
        ...     district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
        ...     field="SNORRE"
        ... )
        >>> response = client.executeQuery(query)
    """

    def __init__(
        self,
        query_string: str,
        district_id: Optional[Union[str, int]] = None,
        field: Optional[str] = None,
        model_class: Optional[Type] = None,
    ) -> None:
        """Initialize a DSIS query.

        Args:
            query_string: Query string from QueryBuilder.build()
                         (e.g., "Fault?$format=json&$select=id,type")
            district_id: Optional district ID for the query
            field: Optional field name for the query
            model_class: Optional dsis_model_sdk model class for casting results

        Raises:
            ValueError: If query_string is invalid
        """
        if not query_string:
            raise ValueError("Query string cannot be empty")

        if "?" not in query_string:
            raise ValueError("Query string must contain '?' separator")

        self.query_string = query_string
        self.district_id = district_id
        self.field = field
        self.model_class = model_class

        # Parse and validate the query string
        self._parse_query_string()

    def _parse_query_string(self) -> None:
        """Parse and validate the query string.

        Raises:
            ValueError: If query string format is invalid
        """
        parts = self.query_string.split("?", 1)
        if len(parts) != 2:
            raise ValueError("Query string must contain exactly one '?' separator")

        self._data_table = parts[0]
        self._query_params = parts[1]

        if not self._data_table:
            raise ValueError("Data table name cannot be empty")

        logger.debug(f"Parsed query: data_table={self._data_table}, params={self._query_params}")

    @property
    def data_table(self) -> str:
        """Get the data table name from the query string.

        Returns:
            The data table name (e.g., "Fault", "Well", "Basin")
        """
        return self._data_table

    @property
    def query_params(self) -> str:
        """Get the query parameters part of the query string.

        Returns:
            The URL-encoded query parameters (e.g., "$format=json&$select=id,type")
        """
        return self._query_params

    def get_parsed_params(self) -> Dict[str, Any]:
        """Get parsed query parameters as a dictionary.

        Returns:
            Dictionary of parsed query parameters

        Example:
            >>> query = DsisQuery("Fault?$format=json&$select=id,type")
            >>> params = query.get_parsed_params()
            >>> print(params)
            {'$format': 'json', '$select': 'id,type'}
        """
        params: Dict[str, Any] = {}
        if self._query_params:
            parsed = parse_qs(self._query_params)
            # parse_qs returns lists, so extract single values
            for key, value in parsed.items():
                params[key] = value[0] if len(value) == 1 else value
        return params

    def set_model(self, model_class: Type) -> "DsisQuery":
        """Set the model class for casting results.

        Args:
            model_class: A dsis_model_sdk model class (e.g., Well, Basin, Fault)

        Returns:
            Self for chaining

        Example:
            >>> from dsis_model_sdk.models.common import Fault
            >>> query = DsisQuery("Fault?$format=json&$select=id,type")
            >>> query.set_model(Fault)
        """
        self.model_class = model_class
        logger.debug(f"Set model class: {model_class.__name__}")
        return self

    def cast_result(self, result: Dict[str, Any]) -> Any:
        """Cast a single result item to the model class.

        Args:
            result: A single item from the API response

        Returns:
            Instance of model_class if set, otherwise returns the dict as-is

        Raises:
            ValueError: If model_class is not set
            ValidationError: If result doesn't match model schema

        Example:
            >>> from dsis_model_sdk.models.common import Fault
            >>> query = DsisQuery("Fault?$format=json&$select=id,type").set_model(Fault)
            >>> item = {"id": "123", "type": "NORMAL"}
            >>> fault = query.cast_result(item)
            >>> print(type(fault))  # <class 'dsis_model_sdk.models.common.fault.Fault'>
        """
        if not self.model_class:
            raise ValueError(
                "model_class is not set. Use set_model() or pass model_class to DsisQuery constructor."
            )

        try:
            instance = self.model_class(**result)
            logger.debug(f"Cast result to {self.model_class.__name__}")
            return instance
        except Exception as e:
            logger.error(f"Failed to cast result to {self.model_class.__name__}: {e}")
            raise

    def cast_results(self, results: List[Dict[str, Any]]) -> List[Any]:
        """Cast multiple result items to the model class.

        Args:
            results: List of items from the API response

        Returns:
            List of model instances if model_class is set, otherwise returns dicts as-is

        Raises:
            ValueError: If model_class is not set
            ValidationError: If any result doesn't match model schema

        Example:
            >>> from dsis_model_sdk.models.common import Fault
            >>> query = DsisQuery("Fault?$format=json&$select=id,type").set_model(Fault)
            >>> items = [{"id": "123", "type": "NORMAL"}, {"id": "456", "type": "NORMAL"}]
            >>> faults = query.cast_results(items)
            >>> print(len(faults))  # 2
            >>> print(type(faults[0]))  # <class 'dsis_model_sdk.models.common.fault.Fault'>
        """
        if not self.model_class:
            raise ValueError(
                "model_class is not set. Use set_model() or pass model_class to DsisQuery constructor."
            )

        casted = []
        for i, result in enumerate(results):
            try:
                instance = self.model_class(**result)
                casted.append(instance)
            except Exception as e:
                logger.error(f"Failed to cast result {i} to {self.model_class.__name__}: {e}")
                raise

        logger.debug(f"Cast {len(casted)} results to {self.model_class.__name__}")
        return casted

    def __repr__(self) -> str:
        """Return string representation of the query.

        Returns:
            String representation showing query details
        """
        return (
            f"DsisQuery(data_table='{self._data_table}', "
            f"district_id={self.district_id}, field={self.field})"
        )

    def __str__(self) -> str:
        """Return the full query string.

        Returns:
            The query string
        """
        return self.query_string

