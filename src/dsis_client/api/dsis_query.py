"""DSIS Query class for executing OData queries.

Provides a high-level interface for executing queries built with QueryBuilder
against the DSIS API.
"""

import logging
from typing import Any, Dict, Optional, Union
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
    ) -> None:
        """Initialize a DSIS query.

        Args:
            query_string: Query string from QueryBuilder.build()
                         (e.g., "Fault?$format=json&$select=id,type")
            district_id: Optional district ID for the query
            field: Optional field name for the query

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

