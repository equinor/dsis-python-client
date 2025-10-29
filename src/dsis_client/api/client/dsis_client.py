"""Main DSIS API client.

Provides high-level methods for interacting with DSIS OData API.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ..models import HAS_DSIS_SCHEMAS, cast_results, is_valid_model
from .base_client import BaseClient
from .model_operations import ModelOperationsMixin

if TYPE_CHECKING:
    from ..query import QueryBuilder

logger = logging.getLogger(__name__)


class DSISClient(ModelOperationsMixin, BaseClient):
    """Main client for DSIS API interactions.

    Provides methods for making authenticated requests to the DSIS API.
    Handles authentication, request construction, and response parsing.

    Attributes:
        config: DSISConfig instance with API configuration
        auth: DSISAuth instance handling authentication
    """

    def get(
        self,
        district_id: Optional[Union[str, int]] = None,
        field: Optional[str] = None,
        data_table: Optional[str] = None,
        format_type: str = "json",
        select: Optional[str] = None,
        expand: Optional[str] = None,
        filter: Optional[str] = None,
        validate_model: bool = True,
        **extra_query: Any,
    ) -> Dict[str, Any]:
        """Make a GET request to the DSIS OData API.

        Constructs the OData endpoint URL following the pattern:
        /<model_name>/<version>[/<district_id>][/<field>][/<data_table>]

        All path segments are optional and can be omitted.
        The data_table parameter refers to specific data models from dsis-schemas
        (e.g., "Basin", "Well", "Wellbore", "WellLog", etc.).

        Args:
            district_id: Optional district ID for the query
            field: Optional field name for the query
            data_table: Optional data table/model name (e.g., "Basin", "Well", "Wellbore").
                       If None, uses configured model_name
            format_type: Response format (default: "json")
            select: OData $select parameter for field selection (comma-separated)
            expand: OData $expand parameter for related data (comma-separated)
            filter: OData $filter parameter for filtering (OData filter expression)
            validate_model: If True, validates that data_table is a known model (default: True)
            **extra_query: Additional OData query parameters

        Returns:
            Dictionary containing the parsed API response

        Raises:
            DSISAPIError: If the API request fails
            ValueError: If validate_model=True and data_table is not a known model

        Example:
            >>> client.get()  # Just model and version
            >>> client.get("123", "wells", data_table="Basin")
            >>> client.get("123", "wells", data_table="Well", select="name,depth")
            >>> client.get("123", "wells", data_table="Well", filter="depth gt 1000")
        """
        # Determine the data_table to use
        if data_table is not None:
            table_to_use = data_table
        elif district_id is not None or field is not None:
            table_to_use = self.config.model_name
            logger.debug(
                f"Using configured model as data_table: {self.config.model_name}"
            )
        else:
            table_to_use = None

        # Validate data_table if provided and validation is enabled
        if validate_model and table_to_use is not None and HAS_DSIS_SCHEMAS:
            if not is_valid_model(table_to_use):
                raise ValueError(
                    f"Unknown model: '{table_to_use}'. Use get_model_by_name() to discover available models."
                )

        # Build endpoint path segments
        segments = [self.config.model_name, self.config.model_version]
        if district_id is not None:
            segments.append(str(district_id))
        if field is not None:
            segments.append(field)
        if table_to_use is not None:
            segments.append(table_to_use)

        endpoint = "/".join(segments)

        # Build query parameters
        query: Dict[str, Any] = {"$format": format_type}
        if select:
            query["$select"] = select
        if expand:
            query["$expand"] = expand
        if filter:
            query["$filter"] = filter
        if extra_query:
            query.update(extra_query)

        return self._request(endpoint, query)

    def get_odata(
        self,
        district_id: Optional[Union[str, int]] = None,
        field: Optional[str] = None,
        data_table: Optional[str] = None,
        format_type: str = "json",
        select: Optional[str] = None,
        expand: Optional[str] = None,
        filter: Optional[str] = None,
        validate_model: bool = True,
        **extra_query: Any,
    ) -> Dict[str, Any]:
        """Get OData from the configured model.

        Convenience method for retrieving OData. Delegates to get() method.
        Always uses the configured model_name and model_version.
        The data_table parameter refers to specific data models from dsis-schemas
        (e.g., "Basin", "Well", "Wellbore", "WellLog", etc.).

        Args:
            district_id: Optional district ID for the query
            field: Optional field name for the query
            data_table: Optional data table/model name (e.g., "Basin", "Well", "Wellbore").
                       If None, uses configured model_name
            format_type: Response format (default: "json")
            select: OData $select parameter for field selection (comma-separated)
            expand: OData $expand parameter for related data (comma-separated)
            filter: OData $filter parameter for filtering (OData filter expression)
            validate_model: If True, validates that data_table is a known model (default: True)
            **extra_query: Additional OData query parameters

        Returns:
            Dictionary containing the parsed OData response

        Raises:
            DSISAPIError: If the API request fails
            ValueError: If validate_model=True and data_table is not a known model

        Example:
            >>> client.get_odata()  # Just model and version
            >>> client.get_odata("123", "wells", data_table="Basin")
            >>> client.get_odata("123", "wells", data_table="Well", select="name,depth")
            >>> client.get_odata("123", "wells", data_table="Well", filter="depth gt 1000")
        """
        return self.get(
            district_id=district_id,
            field=field,
            data_table=data_table,
            format_type=format_type,
            select=select,
            expand=expand,
            filter=filter,
            validate_model=validate_model,
            **extra_query,
        )

    def executeQuery(
        self, query: "QueryBuilder", cast: bool = False
    ) -> Union[Dict[str, Any], List[Any]]:
        """Execute a DSIS query.

        Executes a query that was built using QueryBuilder.
        This provides a clean, user-friendly interface for query execution.

        Args:
            query: QueryBuilder instance containing the query and path parameters
            cast: If True and query has a schema class, automatically cast results to model instances

        Returns:
            If cast=False: Dictionary containing the parsed API response
            If cast=True: List of model instances (from response["value"])

        Raises:
            DSISAPIError: If the API request fails
            ValueError: If query is invalid or cast=True but query has no schema class

        Example:
            >>> from dsis_model_sdk.models.common import Fault
            >>> query = QueryBuilder(
            ...     district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
            ...     field="SNORRE"
            ... ).schema(Fault).select("id,type").filter("type eq 'NORMAL'")
            >>>
            >>> # Option 1: Get raw response
            >>> response = client.executeQuery(query)
            >>>
            >>> # Option 2: Auto-cast to model instances
            >>> faults = client.executeQuery(query, cast=True)
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

        # Auto-cast if requested
        if cast:
            if not query._schema_class:
                raise ValueError(
                    "Cannot cast results: query has no schema class. "
                    "Use .schema(ModelClass) when building the query."
                )
            return cast_results(response.get("value", []), query._schema_class)

        return response
