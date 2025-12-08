"""Query execution for DSIS API.

Provides mixin class for executing QueryBuilder queries and casting results.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List

from ..models import cast_results as _cast_results

if TYPE_CHECKING:
    from ..query import QueryBuilder

logger = logging.getLogger(__name__)


class QueryExecutionMixin:
    """Query execution mixin for DSIS API.

    Provides methods for executing QueryBuilder queries and casting results.
    Requires subclasses to provide: config, _request, _yield_nextlink_pages.
    """

    def execute_query(
        self, query: "QueryBuilder", cast: bool = False, max_pages: int = -1
    ):
        """Execute a DSIS query.

        Args:
            query: QueryBuilder instance containing the query and path parameters
            cast: If True and query has a schema class, automatically cast results
                to model instances
            max_pages: Maximum number of pages to fetch. -1 (default) fetches all pages.
                Use 1 for a single page, 2 for two pages, etc.

        Yields:
            Items from the result pages (or model instances if cast=True)

        Raises:
            DSISAPIError: If the API request fails
            ValueError: If query is invalid or cast=True but query has no schema class

        Example:
            >>> # Fetch all pages (default)
            >>> for item in client.execute_query(query):
            ...     process(item)
            >>>
            >>> # Aggregate all pages into a list
            >>> all_items = list(client.execute_query(query))
            >>>
            >>> # Fetch only one page
            >>> page_items = list(client.execute_query(query, max_pages=1))
            >>>
            >>> # Fetch two pages
            >>> two_pages = list(client.execute_query(query, max_pages=2))
        """
        # Import here to avoid circular imports
        from ..query import QueryBuilder

        if not isinstance(query, QueryBuilder):
            raise TypeError(f"Expected QueryBuilder, got {type(query)}")

        logger.debug(f"Executing query: {query} (max_pages={max_pages})")

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

        # Yield items from all pages (up to max_pages)
        if cast:
            if not query._schema_class:
                raise ValueError(
                    "Cannot cast results: query has no schema class. "
                    "Use .schema(ModelClass) when building the query."
                )
            for item in self._yield_nextlink_pages(response, endpoint, max_pages):
                yield query._schema_class(**item)
        else:
            for item in self._yield_nextlink_pages(response, endpoint, max_pages):
                yield item

    def cast_results(self, results: List[Dict[str, Any]], schema_class) -> List[Any]:
        """Cast API response items to model instances.

        Args:
            results: List of dictionaries from API response
                (typically response["value"])
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
        return _cast_results(results, schema_class)
