"""Main DSIS API client.

Provides high-level methods for interacting with DSIS OData API.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List

from ..models import cast_results
from .base_client import BaseClient

if TYPE_CHECKING:
    from ..query import QueryBuilder

logger = logging.getLogger(__name__)


class DSISClient(BaseClient):
    """Main client for DSIS API interactions.

    Provides methods for making authenticated requests to the DSIS API.
    Handles authentication, request construction, and response parsing.

    Attributes:
        config: DSISConfig instance with API configuration
        auth: DSISAuth instance handling authentication
    """

    def execute_query(
        self, query: "QueryBuilder", cast: bool = False, fetch_all: bool = True
    ):
        """Execute a DSIS query.

        Args:
            query: QueryBuilder instance containing the query and path parameters
            cast: If True and query has a schema class, automatically cast results to model instances
            fetch_all: If True (default), yields items from all pages as they are fetched (memory efficient).
                If False, returns a single page as a dictionary.

        Yields:
            If fetch_all=True: yields each item (or model instance if cast=True) as soon as it is available.

        Returns:
            If fetch_all=False: Dictionary containing the parsed API response (single page)

        Raises:
            DSISAPIError: If the API request fails
            ValueError: If query is invalid or cast=True but query has no schema class

        Example:
            >>> # Process items one by one (memory efficient)
            >>> for item in client.execute_query(query, fetch_all=True):
            ...     process(item)
            >>>
            >>> # Or aggregate all items into a list
            >>> all_items = list(client.execute_query(query, fetch_all=True))
        """
        # Import here to avoid circular imports
        from ..query import QueryBuilder

        if not isinstance(query, QueryBuilder):
            raise TypeError(f"Expected QueryBuilder, got {type(query)}")

        logger.debug(f"Executing query: {query}")

        # Build endpoint path segments
        segments = [self.config.model_name, self.config.model_version]
        if query.district_id is not None:
            segments.append(str(query.district_id))
        if query.field is not None:
            segments.append(query.field)

        # Get schema name from query
        query_string = query.get_query_string()
        schema_name = query_string.split("?")[0]
        segments.append(schema_name)

        endpoint = "/".join(segments)

        # Get parsed parameters from the query
        params = query.build_query_params()

        logger.debug(f"Making request to endpoint: {endpoint} with params: {params}")
        response = self._request(endpoint, params)

        if fetch_all:
            if cast:
                if not query._schema_class:
                    raise ValueError(
                        "Cannot cast results: query has no schema class. "
                        "Use .schema(ModelClass) when building the query."
                    )
                for item in self._yield_nextlink_pages(response, endpoint):
                    yield query._schema_class(**item)
            else:
                for item in self._yield_nextlink_pages(response, endpoint):
                    yield item
        else:
            # Single page response
            if cast:
                if not query._schema_class:
                    raise ValueError(
                        "Cannot cast results: query has no schema class. "
                        "Use .schema(ModelClass) when building the query."
                    )
                return cast_results(response.get("value", []), query._schema_class)
            return response

    def cast_results(self, results: List[Dict[str, Any]], schema_class) -> List[Any]:
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

    def _yield_nextlink_pages(self, response: Dict[str, Any], endpoint: str):
        """Generator that yields items from all pages following OData nextLinks.

        This yields items as they are fetched, allowing for memory-efficient
        processing of large result sets.

        Args:
            response: Initial API response dict
            endpoint: Full endpoint path from initial request (without query params)

        Yields:
            Individual items from each page's 'value' array
        """
        next_key = "odata.nextLink"

        # Yield items from the initial response
        for item in response.get("value", []):
            yield item

        next_link = response.get(next_key)

        while next_link:
            logger.debug(f"Following nextLink: {next_link}")

            # Replace the last segment of endpoint (schema name) with the full next_link
            endpoint_parts = endpoint.rsplit("/", 1)
            if len(endpoint_parts) == 2:
                temp_endpoint = f"{endpoint_parts[0]}/{next_link}"
            else:
                # Fallback if endpoint has no slash (shouldn't happen in practice)
                temp_endpoint = next_link

            # Make request with the temp endpoint
            next_resp = self._request(temp_endpoint, params=None)

            # Yield items from this page
            for item in next_resp.get("value", []):
                yield item

            # Check for next link in the next response
            next_link = next_resp.get(next_key)
