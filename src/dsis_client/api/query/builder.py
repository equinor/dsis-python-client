"""Query builder for DSIS OData API.

Provides a fluent interface for building DSIS OData queries.
"""

import logging
from typing import List, Optional, Type, Union

from . import odata

logger = logging.getLogger(__name__)


class QueryBuilder:
    """Fluent query builder for DSIS OData API queries.

    Provides a chainable interface for building OData queries with validation
    against dsis_model_sdk schemas. This class IS the query object - no need
    to call build().

    district_id and field are required parameters that specify the data location.

    Example:
        >>> from dsis_model_sdk.models.common import Fault
        >>> query = QueryBuilder(
        ...     district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
        ...     field="SNORRE"
        ... ).schema(Fault).select("id,type").filter("type eq 'NORMAL'")
    >>> response = client.execute_query(query)
    >>> faults = query.cast_results(response["value"])
    """

    def __init__(self, district_id: Union[str, int], field: str):
        """Initialize the query builder.

        Args:
            district_id: District ID for the query (required)
            field: Field name for the query (required)
        """
        self.district_id = district_id
        self.field = field
        self._schema_name: Optional[str] = None
        self._schema_class: Optional[Type] = None
        self._select: List[str] = []
        self._expand: List[str] = []
        self._filter: Optional[str] = None
        self._format: Optional[str] = "json"

    def schema(self, schema: Union[str, Type]) -> "QueryBuilder":
        """Set the schema (data table) using a name or model class.

        Args:
            schema: Schema name (e.g., "Well", "Fault") or dsis_model_sdk model class

        Returns:
            Self for chaining

        Example:
            >>> # Using schema name
            >>> query = QueryBuilder(district_id="123", field="SNORRE").schema("Fault")

            >>> # Using model class
            >>> from dsis_model_sdk.models.common import Fault
            >>> query = QueryBuilder(district_id="123", field="SNORRE").schema(Fault)
        """
        # If schema is a class, extract the name and store the class
        if isinstance(schema, type):
            self._schema_class = schema
            schema_name = schema.__name__
        else:
            schema_name = schema
            self._schema_class = None

        self._schema_name = schema_name
        logger.debug(f"Set schema: {schema_name}")
        return self

    def select(self, *fields: str) -> "QueryBuilder":
        """Add fields to $select parameter.

        Args:
            *fields: Field names to select (can be comma-separated or individual)

        Returns:
            Self for chaining

        Example:
            >>> builder.select("name", "depth", "status")
            >>> builder.select("name,depth,status")
        """
        for field_spec in fields:
            # Handle comma-separated fields
            self._select.extend([f.strip() for f in field_spec.split(",")])
        logger.debug(f"Added select fields: {fields}")
        return self

    def expand(self, *relations: str) -> "QueryBuilder":
        """Add relations to $expand parameter.

        Args:
            *relations: Relation names to expand (can be comma-separated or individual)

        Returns:
            Self for chaining

        Example:
            >>> builder.expand("wells", "horizons")
            >>> builder.expand("wells,horizons")
        """
        for rel_spec in relations:
            # Handle comma-separated relations
            self._expand.extend([r.strip() for r in rel_spec.split(",")])
        logger.debug(f"Added expand relations: {relations}")
        return self

    def filter(self, filter_expr: str) -> "QueryBuilder":
        """Set the $filter parameter.

        Args:
            filter_expr: OData filter expression (e.g., "depth gt 1000")

        Returns:
            Self for chaining

        Example:
            >>> builder.filter("depth gt 1000")
            >>> builder.filter("name eq 'Well-1'")
        """
        self._filter = filter_expr
        logger.debug(f"Set filter: {filter_expr}")
        return self

    def format(self, format_type: Optional[str] = "json") -> "QueryBuilder":
        """Set the response format.

        Args:
            format_type: Format type ("json", empty string to omit, or None).
                Defaults to "json".

        Returns:
            Self for chaining

        Example:
            >>> builder.format()  # Use default "json"
            >>> builder.format("json")  # Explicitly set to json
            >>> builder.format("")  # Omit format parameter from query
            >>> builder.format(None)  # Omit format parameter from query
        """
        self._format = format_type
        logger.debug(f"Set format: {format_type}")
        return self

    def build_query_params(self) -> dict:
        """Build the OData query parameters.

        Returns:
            Dictionary of query parameters
        """
        return odata.build_query_params(
            select=self._select,
            expand=self._expand,
            filter_expr=self._filter,
            format_type=self._format,
        )

    def get_query_string(self) -> str:
        """Get the full OData query string for this query.

        Returns:
            Full query string (e.g., "Fault?$format=json&$select=id,type")

        Raises:
            ValueError: If schema is not set

        Example:
            >>> query = (
            ...     QueryBuilder(district_id="123", field="SNORRE")
            ...     .schema("Fault")
            ...     .select("id,type")
            ... )
            >>> print(query.get_query_string())
            Fault?$format=json&$select=id,type
        """
        params = self.build_query_params()
        return odata.build_query_string(self._schema_name, params)

    def get_query_params_string(self) -> str:
        """Build just the query parameters part (without schema name).

        Returns:
            Query parameters string (e.g., "$format=json&$select=name,depth")
        """
        params = self.build_query_params()
        return odata.build_query_params_string(params)

    def reset(self) -> "QueryBuilder":
        """Reset the builder to initial state.

        Note: Does not reset district_id or field set in constructor.

        Returns:
            Self for chaining
        """
        self._schema_name = None
        self._schema_class = None
        self._select = []
        self._expand = []
        self._filter = None
        self._format: Optional[str] = "json"
        logger.debug("Reset builder")
        return self

    def __repr__(self) -> str:
        """String representation of the builder."""
        return (
            f"QueryBuilder(district_id={self.district_id}, "
            f"field={self.field}, schema={self._schema_name}, "
            f"select={self._select}, expand={self._expand}, filter={self._filter})"
        )

    def __str__(self) -> str:
        """Return the full query string.

        Returns:
            The query string if schema is set, otherwise a description
        """
        try:
            return self.get_query_string()
        except ValueError:
            return (
                f"QueryBuilder(district_id={self.district_id}, "
                f"field={self.field}, schema=None)"
            )
