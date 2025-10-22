"""Query builder for DSIS OData API.

Provides a fluent interface for building DSIS OData queries using dsis_schemas models.
"""

import logging
from typing import Any, Dict, List, Optional, Type
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

# Try to import dsis_schemas
try:
    from dsis_model_sdk import models
    HAS_DSIS_SCHEMAS = True
except ImportError:
    HAS_DSIS_SCHEMAS = False
    logger.debug("dsis_schemas package not available")


class QueryBuilder:
    """Fluent query builder for DSIS OData API data_table queries.

    Provides a chainable interface for building OData queries with validation
    against dsis_schemas models. Focuses on building the data_table and query
    parameters part of the URL (after model_name/version/district_id/field).

    Example:
        >>> builder = QueryBuilder()
        >>> query = builder.Basin.select("name,id").filter("depth gt 1000").build()
        >>> # Returns: "Basin?$format=json&$select=name,id&$filter=depth gt 1000"

        >>> query = builder.Well.expand("wellbores").select("name").build()
        >>> # Returns: "Well?$format=json&$expand=wellbores&$select=name"
    """

    def __init__(self, domain: str = "common"):
        """Initialize the query builder.

        Args:
            domain: Domain for models - "common" or "native" (default: "common")

        Raises:
            ValueError: If domain is not "common" or "native"
        """
        if domain not in ("common", "native"):
            raise ValueError(f"Domain must be 'common' or 'native', got '{domain}'")

        self._domain = domain
        self._data_table: Optional[str] = None
        self._select: List[str] = []
        self._expand: List[str] = []
        self._filter: Optional[str] = None
        self._format = "json"

    def domain(self, domain: str) -> "QueryBuilder":
        """Set the domain for model validation.

        Args:
            domain: Domain - "common" or "native"

        Returns:
            Self for chaining

        Raises:
            ValueError: If domain is not "common" or "native"
        """
        if domain not in ("common", "native"):
            raise ValueError(f"Domain must be 'common' or 'native', got '{domain}'")
        self._domain = domain
        logger.debug(f"Set domain: {domain}")
        return self

    def data_table(self, table_name: str, validate: bool = True) -> "QueryBuilder":
        """Set the data table (model).

        Args:
            table_name: Data table name (e.g., "Well", "Basin", "Wellbore")
            validate: If True, validates that the model exists (default: True)

        Returns:
            Self for chaining

        Raises:
            ValueError: If validate=True and model is not found
        """
        if validate and HAS_DSIS_SCHEMAS:
            if not self._is_valid_model(table_name):
                raise ValueError(
                    f"Unknown model: '{table_name}' in {self._domain} domain. "
                    f"Use list_available_models() to see available models."
                )
        self._data_table = table_name
        logger.debug(f"Set data_table: {table_name}")
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

    def format(self, format_type: str) -> "QueryBuilder":
        """Set the response format.

        Args:
            format_type: Format type (default: "json")

        Returns:
            Self for chaining
        """
        self._format = format_type
        logger.debug(f"Set format: {format_type}")
        return self

    def build_query_params(self) -> Dict[str, str]:
        """Build the OData query parameters.

        Returns:
            Dictionary of query parameters
        """
        params: Dict[str, str] = {"$format": self._format}

        if self._select:
            params["$select"] = ",".join(self._select)
        if self._expand:
            params["$expand"] = ",".join(self._expand)
        if self._filter:
            params["$filter"] = self._filter

        logger.debug(f"Built query params: {params}")
        return params

    def build(self) -> str:
        """Build the OData query string for the data_table.

        Returns:
            Query string (e.g., "Well?$format=json&$select=name,depth&$filter=depth gt 1000")
        """
        if not self._data_table:
            raise ValueError("data_table must be set before building")

        params = self.build_query_params()

        query_string = ""
        if params:
            query_string = urlencode(params)
            query_string = f"?{query_string}"

        result = f"{self._data_table}{query_string}"
        logger.debug(f"Built query: {result}")
        return result

    def build_query_string(self) -> str:
        """Build just the query parameters part (without data_table).

        Returns:
            Query string (e.g., "$format=json&$select=name,depth")
        """
        params = self.build_query_params()
        if params:
            return urlencode(params)
        return ""

    def _is_valid_model(self, model_name: str) -> bool:
        """Check if a model exists in dsis_schemas.

        Args:
            model_name: Model name to check

        Returns:
            True if model exists, False otherwise
        """
        try:
            if self._domain == "common":
                model_module = models.common
            elif self._domain == "native":
                model_module = models.native
            else:
                return False
            
            return hasattr(model_module, model_name)
        except Exception as e:
            logger.debug(f"Error validating model {model_name}: {e}")
            return False

    @staticmethod
    def list_available_models(domain: str = "common") -> List[str]:
        """List all available models in a domain.

        Args:
            domain: Domain - "common" or "native" (default: "common")

        Returns:
            List of available model names

        Raises:
            ImportError: If dsis_schemas is not installed
            ValueError: If domain is invalid
        """
        if not HAS_DSIS_SCHEMAS:
            raise ImportError(
                "dsis_schemas package is required. Install it with: pip install dsis-schemas"
            )
        
        if domain not in ("common", "native"):
            raise ValueError(f"Domain must be 'common' or 'native', got '{domain}'")
        
        if domain == "common":
            model_module = models.common
        else:
            model_module = models.native
        
        # Get all public attributes that are classes
        models_list = [
            name for name in dir(model_module)
            if not name.startswith('_') and isinstance(getattr(model_module, name), type)
        ]
        
        logger.debug(f"Found {len(models_list)} models in {domain} domain")
        return sorted(models_list)

    def reset(self) -> "QueryBuilder":
        """Reset the builder to initial state.

        Returns:
            Self for chaining
        """
        self._data_table = None
        self._select = []
        self._expand = []
        self._filter = None
        self._format = "json"
        logger.debug("Reset builder")
        return self

    def __repr__(self) -> str:
        """String representation of the builder."""
        return (
            f"QueryBuilder(domain={self._domain}, data_table={self._data_table}, "
            f"select={self._select}, expand={self._expand}, filter={self._filter})"
        )

