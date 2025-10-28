"""Main client module for DSIS API.

Provides the DSISClient class for making authenticated requests to the DSIS API.
Handles request construction, authentication header management, and response parsing.
"""

import json
import logging
from typing import Any, Dict, Optional, Type, TypeVar, Union
from urllib.parse import urljoin

import requests

from .auth import DSISAuth
from .config import DSISConfig
from .exceptions import DSISAPIError
from .dsis_query import DsisQuery

logger = logging.getLogger(__name__)

# Type variable for model classes
T = TypeVar('T')

# Try to import dsis_schemas utilities
try:
    from dsis_model_sdk import models
    from dsis_model_sdk import deserialize_from_json
    HAS_DSIS_SCHEMAS = True
except ImportError:
    HAS_DSIS_SCHEMAS = False
    logger.debug("dsis_schemas package not available")


class DSISClient:
    """Main client for DSIS API interactions.

    Provides methods for making authenticated requests to the DSIS API.
    Handles authentication, request construction, and response parsing.

    Attributes:
        config: DSISConfig instance with API configuration
        auth: DSISAuth instance handling authentication
    """

    def __init__(self, config: DSISConfig) -> None:
        """Initialize the DSIS client.

        Args:
            config: DSISConfig instance with required credentials and settings

        Raises:
            DSISConfigurationError: If configuration is invalid
        """
        self.config = config
        self.auth = DSISAuth(config)
        self._session = requests.Session()
        logger.debug(f"DSIS client initialized for {config.environment.value} environment")

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
            logger.debug(f"Using configured model as data_table: {self.config.model_name}")
        else:
            table_to_use = None

        # Validate data_table if provided and validation is enabled
        if validate_model and table_to_use is not None and HAS_DSIS_SCHEMAS:
            if not self._is_valid_model(table_to_use):
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

    def executeQuery(self, query: DsisQuery) -> Dict[str, Any]:
        """Execute a DSIS query.

        Executes a query that was built using QueryBuilder and wrapped in DsisQuery.
        This provides a clean, user-friendly interface for query execution.

        Args:
            query: DsisQuery instance containing the query string and path parameters

        Returns:
            Dictionary containing the parsed API response

        Raises:
            DSISAPIError: If the API request fails
            ValueError: If query is invalid

        Example:
            >>> # Build query with QueryBuilder
            >>> query_builder = QueryBuilder().data_table("Fault").select("id,type").filter("type eq 'NORMAL'")
            >>>
            >>> # Wrap in DsisQuery with path parameters
            >>> query = DsisQuery(
            ...     query_string=query_builder.build(),
            ...     district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
            ...     field="SNORRE"
            ... )
            >>>
            >>> # Execute the query
            >>> response = client.executeQuery(query)
        """
        if not isinstance(query, DsisQuery):
            raise TypeError(f"Expected DsisQuery, got {type(query)}")

        logger.debug(f"Executing query: {query}")

        # Build endpoint path segments
        segments = [self.config.model_name, self.config.model_version]
        if query.district_id is not None:
            segments.append(str(query.district_id))
        if query.field is not None:
            segments.append(query.field)
        segments.append(query.schema)

        endpoint = "/".join(segments)

        # Get parsed parameters from the query
        params = query.get_parsed_params()

        logger.debug(f"Making request to endpoint: {endpoint} with params: {params}")
        return self._request(endpoint, params)

    def _is_valid_model(self, model_name: str, domain: str = "common") -> bool:
        """Check if a model name is valid in dsis_schemas.

        Args:
            model_name: Name of the model to check
            domain: Domain to search in - "common" or "native" (default: "common")

        Returns:
            True if the model exists, False otherwise
        """
        if not HAS_DSIS_SCHEMAS:
            logger.debug("dsis_schemas not available, skipping model validation")
            return True

        try:
            model = self.get_model_by_name(model_name, domain)
            return model is not None
        except Exception as e:
            logger.debug(f"Error validating model {model_name}: {e}")
            return False

    def get_model_by_name(self, model_name: str, domain: str = "common") -> Optional[Type]:
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
        if not HAS_DSIS_SCHEMAS:
            raise ImportError(
                "dsis_schemas package is required. Install it with: pip install dsis-schemas"
            )

        logger.debug(f"Getting model: {model_name} from {domain} domain")
        try:
            if domain == "common":
                model_module = models.common
            elif domain == "native":
                model_module = models.native
            else:
                raise ValueError(f"Unknown domain: {domain}")

            return getattr(model_module, model_name, None)
        except Exception as e:
            logger.error(f"Failed to get model {model_name}: {e}")
            return None

    def get_model_fields(self, model_name: str, domain: str = "common") -> Optional[Dict[str, Any]]:
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
        if not HAS_DSIS_SCHEMAS:
            raise ImportError(
                "dsis_schemas package is required. Install it with: pip install dsis-schemas"
            )

        logger.debug(f"Getting fields for model: {model_name} from {domain} domain")
        try:
            model_class = self.get_model_by_name(model_name, domain)
            if model_class is None:
                return None
            return dict(model_class.model_fields)
        except Exception as e:
            logger.error(f"Failed to get fields for {model_name}: {e}")
            return None

    def deserialize_response(self, response: Dict[str, Any], model_name: str, domain: str = "common") -> Optional[Any]:
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
        if not HAS_DSIS_SCHEMAS:
            raise ImportError(
                "dsis_schemas package is required. Install it with: pip install dsis-schemas"
            )

        try:
            logger.debug(f"Deserializing response to {model_name} from {domain} domain")
            model_class = self.get_model_by_name(model_name, domain)
            if model_class is None:
                raise ValueError(f"Model '{model_name}' not found in dsis_schemas")

            # Convert response to JSON string for deserialization
            response_json = json.dumps(response)
            return deserialize_from_json(response_json, model_class)
        except Exception as e:
            logger.error(f"Failed to deserialize response to {model_name}: {e}")
            raise ValueError(f"Deserialization failed: {e}")

    def refresh_authentication(self) -> None:
        """Refresh authentication tokens.

        Clears cached tokens and acquires new ones. Useful when tokens
        have expired or when you need to ensure fresh authentication.

        Raises:
            DSISAuthenticationError: If token acquisition fails
        """
        logger.debug("Refreshing authentication")
        self.auth.refresh_tokens()

    def test_connection(self) -> bool:
        """Test the connection to the DSIS API.

        Attempts to connect to the DSIS API data endpoint to verify
        that authentication and connectivity are working.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            logger.debug("Testing DSIS API connection")
            headers = self.auth.get_auth_headers()
            response = self._session.get(
                self.config.data_endpoint, headers=headers, timeout=10
            )
            success = response.status_code in [200, 404]
            if success:
                logger.debug("Connection test successful")
            else:
                logger.warning(f"Connection test failed with status {response.status_code}")
            return success
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def _request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make an authenticated GET request to the DSIS API.

        Internal method that constructs the full URL, adds authentication
        headers, and makes the request.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            Parsed JSON response as dictionary

        Raises:
            DSISAPIError: If the request fails or returns non-200 status
        """
        url = urljoin(f"{self.config.data_endpoint}/", endpoint)
        headers = self.auth.get_auth_headers()

        logger.debug(f"Making request to {url}")
        response = self._session.get(url, headers=headers, params=params)

        if response.status_code != 200:
            error_msg = (
                f"API request failed: {response.status_code} - "
                f"{response.reason} - {response.text}"
            )
            logger.error(error_msg)
            raise DSISAPIError(error_msg)

        try:
            return response.json()
        except ValueError as e:
            logger.warning(f"Failed to parse JSON response: {e}")
            return {"data": response.text}
