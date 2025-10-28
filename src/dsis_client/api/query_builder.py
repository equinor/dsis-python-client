"""Query builder for DSIS OData API.

Provides a fluent interface for building DSIS OData queries using dsis_schemas models.
"""

import logging
from typing import Any, Dict, List, Optional, Type, Union
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

    The build() method returns a DsisQuery object ready for execution with client.executeQuery().

    Example:
        >>> query = QueryBuilder().data_table("Fault").select("id,type").filter("type eq 'NORMAL'").build(
        ...     district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
        ...     field="SNORRE"
        ... )
        >>> response = client.executeQuery(query)

        >>> # Or without path parameters:
        >>> query = QueryBuilder().data_table("Well").expand("wellbores").select("name").build()
        >>> response = client.executeQuery(query, district_id="123", field="wells")
    """

    def __init__(self, domain: str = "common", district_id: Optional[str] = None, field: Optional[str] = None):
        """Initialize the query builder.

        Args:
            domain: Domain for models - "common" or "native" (default: "common")
            district_id: Optional district ID for the query
            field: Optional field name for the query

        Raises:
            ValueError: If domain is not "common" or "native"
        """
        if domain not in ("common", "native"):
            raise ValueError(f"Domain must be 'common' or 'native', got '{domain}'")

        self._domain = domain
        self._district_id = district_id
        self._field = field
        self._data_table: Optional[str] = None
        self._model_class: Optional[Type] = None
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

    def model(self, model_class: Type) -> "QueryBuilder":
        """Set the data table using a dsis_model_sdk model class.

        This is a convenience method that extracts the model name from a Pydantic model class.
        Also stores the model class for automatic result casting.

        Args:
            model_class: A Pydantic model class from dsis_model_sdk (e.g., Well, Basin, Fault)

        Returns:
            Self for chaining

        Raises:
            ValueError: If model_class is not a valid Pydantic model

        Example:
            >>> from dsis_model_sdk.models.common import Well, Basin
            >>> builder.model(Well).select("name,depth").build()
            >>> builder.model(Basin).filter("depth gt 1000").build()
        """
        try:
            # Get the model name from the class
            model_name = model_class.__name__
            logger.debug(f"Using model class: {model_name}")
            self._model_class = model_class
            return self.data_table(model_name, validate=False)
        except AttributeError as e:
            raise ValueError(f"Invalid model class: {model_class}. Must be a Pydantic model class.") from e

    def select_from_model(self, model_class: Type, *field_names: str) -> "QueryBuilder":
        """Select fields from a dsis_model_sdk model class.

        This is a convenience method that validates field names against the model schema.

        Args:
            model_class: A Pydantic model class from dsis_model_sdk
            *field_names: Field names to select (can be comma-separated or individual)

        Returns:
            Self for chaining

        Raises:
            ValueError: If any field name is not in the model

        Example:
            >>> from dsis_model_sdk.models.common import Well
            >>> builder.select_from_model(Well, "name", "depth", "status")
            >>> builder.select_from_model(Well, "name,depth,status")
        """
        try:
            # Get available fields from model
            available_fields = set(model_class.model_fields.keys())

            # Collect all requested fields
            requested_fields = []
            for field_spec in field_names:
                requested_fields.extend([f.strip() for f in field_spec.split(",")])

            # Validate all fields exist
            invalid_fields = set(requested_fields) - available_fields
            if invalid_fields:
                raise ValueError(
                    f"Invalid fields for {model_class.__name__}: {invalid_fields}. "
                    f"Available fields: {sorted(available_fields)}"
                )

            logger.debug(f"Selected fields from {model_class.__name__}: {requested_fields}")
            return self.select(*requested_fields)
        except AttributeError as e:
            raise ValueError(f"Invalid model class: {model_class}. Must be a Pydantic model class.") from e

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

    def build(self, district_id=None, field=None):
        """Build a DsisQuery ready for execution.

        Creates a DsisQuery object that can be directly passed to client.executeQuery().
        This is the recommended way to build queries for execution.

        Args:
            district_id: Optional district ID for the query (overrides constructor value)
            field: Optional field name for the query (overrides constructor value)

        Returns:
            DsisQuery instance ready for execution

        Raises:
            ValueError: If data_table is not set

        Example:
            >>> query = QueryBuilder(
            ...     district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
            ...     field="SNORRE"
            ... ).data_table("Fault").select("id,type").filter("type eq 'NORMAL'").build()
            >>> response = client.executeQuery(query)

            >>> # Or override at build time:
            >>> query = QueryBuilder().data_table("Fault").select("id,type").build(
            ...     district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
            ...     field="SNORRE"
            ... )
            >>> response = client.executeQuery(query)
        """
        # Import here to avoid circular imports
        from .dsis_query import DsisQuery

        if not self._data_table:
            raise ValueError("data_table must be set before building")

        # Use provided values or fall back to constructor values
        final_district_id = district_id if district_id is not None else self._district_id
        final_field = field if field is not None else self._field

        # Build the query string
        params = self.build_query_params()
        query_string = ""
        if params:
            query_string = urlencode(params)
            query_string = f"?{query_string}"

        query_str = f"{self._data_table}{query_string}"
        logger.debug(f"Built query: {query_str}")

        logger.debug(f"Building DsisQuery with district_id={final_district_id}, field={final_field}")
        return DsisQuery(
            query_string=query_str,
            district_id=final_district_id,
            field=final_field,
            schema_class=self._model_class
        )

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

    @staticmethod
    def get_model(model_name: str, domain: str = "common") -> Type:
        """Get a model class by name from dsis_model_sdk.

        Args:
            model_name: Name of the model (e.g., "Well", "Basin", "Fault")
            domain: Domain - "common" or "native" (default: "common")

        Returns:
            The model class if found

        Raises:
            ImportError: If dsis_schemas is not installed
            ValueError: If domain is invalid or model not found

        Example:
            >>> Well = QueryBuilder.get_model("Well")
            >>> Basin = QueryBuilder.get_model("Basin", domain="native")
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

        if not hasattr(model_module, model_name):
            available = QueryBuilder.list_available_models(domain)
            raise ValueError(
                f"Model '{model_name}' not found in {domain} domain. "
                f"Available models: {available}"
            )

        model_class = getattr(model_module, model_name)
        logger.debug(f"Retrieved model class: {model_name} from {domain} domain")
        return model_class

    @staticmethod
    def get_model_fields(model_name: str, domain: str = "common") -> Dict[str, Any]:
        """Get field information for a model.

        Args:
            model_name: Name of the model (e.g., "Well", "Basin")
            domain: Domain - "common" or "native" (default: "common")

        Returns:
            Dictionary of field names and their information

        Raises:
            ImportError: If dsis_schemas is not installed
            ValueError: If domain is invalid or model not found

        Example:
            >>> fields = QueryBuilder.get_model_fields("Well")
            >>> print(fields.keys())
        """
        model_class = QueryBuilder.get_model(model_name, domain)
        return model_class.model_fields

    def reset(self) -> "QueryBuilder":
        """Reset the builder to initial state.

        Note: Does not reset domain, district_id, or field set in constructor.

        Returns:
            Self for chaining
        """
        self._data_table = None
        self._model_class = None
        self._select = []
        self._expand = []
        self._filter = None
        self._format = "json"
        logger.debug("Reset builder")
        return self

    def __repr__(self) -> str:
        """String representation of the builder."""
        return (
            f"QueryBuilder(domain={self._domain}, district_id={self._district_id}, "
            f"field={self._field}, data_table={self._data_table}, "
            f"select={self._select}, expand={self._expand}, filter={self._filter})"
        )

