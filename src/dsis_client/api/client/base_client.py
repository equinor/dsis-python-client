"""Base HTTP client for DSIS API.

Handles HTTP requests, session management, and connection testing.
"""

import logging
from typing import Any, Dict, Optional, Union
from urllib.parse import urljoin

import requests

from ..auth import DSISAuth
from ..config import DSISConfig
from ..exceptions import DSISAPIError

logger = logging.getLogger(__name__)


class BaseClient:
    """Base client for HTTP operations.

    Handles authentication, session management, and HTTP requests.
    """

    def __init__(self, config: DSISConfig) -> None:
        """Initialize the base client.

        Args:
            config: DSISConfig instance with required credentials and settings

        Raises:
            DSISConfigurationError: If configuration is invalid
        """
        self.config = config
        self.auth = DSISAuth(config)
        self._session = requests.Session()
        logger.debug(
            f"Base client initialized for {config.environment.value} environment"
        )

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
                logger.warning(
                    f"Connection test failed with status {response.status_code}"
                )
            return success
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get(
        self,
        district_id: Optional[Union[str, int]] = None,
        field: Optional[str] = None,
        schema: Optional[str] = None,
        format_type: str = "json",
        select: Optional[str] = None,
        expand: Optional[str] = None,
        filter: Optional[str] = None,
        validate_schema: bool = True,
        **extra_query: Any,
    ) -> Dict[str, Any]:
        """Make a GET request to the DSIS OData API.

        Constructs the OData endpoint URL following the pattern:
        /<model_name>/<version>[/<district_id>][/<field>][/<schema>]

        All path segments are optional and can be omitted.
        The schema parameter refers to specific data schemas from dsis-schemas
        (e.g., "Basin", "Well", "Wellbore", "WellLog", etc.).

        Args:
            district_id: Optional district ID for the query
            field: Optional field name for the query
            schema: Optional schema name (e.g., "Basin", "Well", "Wellbore").
                    If None, uses configured model_name
            format_type: Response format (default: "json")
            select: OData $select parameter for field selection (comma-separated)
            expand: OData $expand parameter for related data (comma-separated)
            filter: OData $filter parameter for filtering (OData filter expression)
            validate_schema: If True, validates that schema is a known model
                (default: True)
            **extra_query: Additional OData query parameters

        Returns:
            Dictionary containing the parsed API response

        Raises:
            DSISAPIError: If the API request fails
            ValueError: If validate_schema=True and schema is not a known model

        Example:
            >>> client.get()  # Just model and version
            >>> client.get("123", "wells", schema="Basin")
            >>> client.get("123", "wells", schema="Well", select="name,depth")
            >>> client.get("123", "wells", schema="Well", filter="depth gt 1000")
        """
        # Import here to avoid circular imports
        from ..models import HAS_DSIS_SCHEMAS, is_valid_schema

        # Determine the schema to use
        if schema is not None:
            schema_to_use = schema
        elif district_id is not None or field is not None:
            schema_to_use = self.config.model_name
            logger.debug(f"Using configured model as schema: {self.config.model_name}")
        else:
            schema_to_use = None

        # Validate schema if provided and validation is enabled
        if validate_schema and schema_to_use is not None and HAS_DSIS_SCHEMAS:
            if not is_valid_schema(schema_to_use):
                raise ValueError(
                    f"Unknown schema: '{schema_to_use}'. Use "
                    "get_schema_by_name() to discover available schemas."
                )

        # Build endpoint path segments
        segments = [self.config.model_name, self.config.model_version]
        if district_id is not None:
            segments.append(str(district_id))
        if field is not None:
            segments.append(field)
        if schema_to_use is not None:
            segments.append(schema_to_use)

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
