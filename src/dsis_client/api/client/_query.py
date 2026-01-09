"""Query execution for DSIS API.

Provides mixin class for executing QueryBuilder queries and casting results.
"""

import json
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ..exceptions import DSISJSONParseError
from ..models import cast_results as _cast_results
from ._base import _PaginationBase

if TYPE_CHECKING:
    from ..query import QueryBuilder

logger = logging.getLogger(__name__)


class QueryExecutionMixin(_PaginationBase):
    """Query execution mixin for DSIS API.

    Provides methods for executing QueryBuilder queries and casting results.
    Requires subclasses to provide: config, _request, _yield_nextlink_pages.
    """

    def _extract_objects_from_value_array(
        self, response_text: str
    ) -> list[Dict[str, Any]]:
        """Extract individual JSON objects from the value array in response text.

        Args:
            response_text: The raw response text.

        Returns:
            List of parsed objects from the value array.
        """
        items: list[Dict[str, Any]] = []

        # Find the "value": [ ... ] section
        value_start = response_text.find('"value"')
        if value_start == -1:
            logger.debug("Could not find 'value' array in response text")
            return items

        bracket_start = response_text.find("[", value_start)
        if bracket_start == -1:
            return items

        # Parse objects between { and } within the array
        current_pos = bracket_start + 1
        brace_count = 0
        obj_start = None

        for i in range(current_pos, len(response_text)):
            char = response_text[i]

            if char == "{":
                if brace_count == 0:
                    obj_start = i
                brace_count += 1
            elif char == "}":
                brace_count -= 1
                if brace_count == 0 and obj_start is not None:
                    obj_text = response_text[obj_start : i + 1]
                    try:
                        obj = json.loads(obj_text, strict=False)
                        items.append(obj)
                    except Exception as e:
                        logger.debug(f"Failed to parse object at {obj_start}: {e}")
                    obj_start = None
            elif char == "]" and brace_count == 0:
                break

        return items

    def _extract_value_array_from_text(
        self, response_text: str
    ) -> tuple[list[Dict[str, Any]], Optional[str]]:
        """Extract value array and nextLink from raw response text.

        Last-resort fallback for when both standard and strict=False JSON parsing fail.

        Args:
            response_text: The raw response text.

        Returns:
            A tuple of (items, nextLink). Items may be empty if extraction fails.
        """
        try:
            next_link = self._extract_nextlink_from_text(response_text)
            items = self._extract_objects_from_value_array(response_text)

            if items:
                logger.info(
                    f"Extracted {len(items)} items using object-by-object fallback"
                )

            return items, next_link
        except Exception as e:
            logger.warning(f"Failed to extract data from raw text: {e}")
            return [], None

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

        logger.info(f"Executing query: {query} (max_pages={max_pages})")

        # Build endpoint path segments
        segments = [self.config.model_name, self.config.model_version]
        if query.district_id is not None:
            segments.append(str(query.district_id))
        if query.project is not None:
            segments.append(query.project)

        # Get schema name from query
        query_string = query.get_query_string()
        schema_name = query_string.split("?")[0]
        segments.append(schema_name)

        endpoint = "/".join(segments)

        # Get parsed parameters from the query
        params = query.build_query_params()

        logger.info(f"Making request to endpoint: {endpoint} with params: {params}")

        # Try to make the request and handle JSON parsing errors
        try:
            response = self._request(endpoint, params)
        except DSISJSONParseError as e:
            logger.warning(
                "JSON parsing failed. Attempting fallback: extracting data from raw text."
            )
            # Try to extract items and nextLink from the malformed response
            items, next_link = self._extract_value_array_from_text(e.response_text)

            if items or next_link:
                # Create a synthetic response dict
                response = {
                    "value": items,
                    "odata.nextLink": next_link,
                }
                logger.info(f"Fallback extraction succeeded: {len(items)} items")
            else:
                # Fallback failed, re-raise the original error
                logger.error("Fallback extraction failed. Cannot process response.")
                raise

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
            >>> query = QueryBuilder(district_id="123", project="SNORRE").schema(Fault)
            >>> response = client.executeQuery(query)
            >>> faults = client.cast_results(response["value"], Fault)
        """
        return _cast_results(results, schema_class)
