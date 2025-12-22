"""Base HTTP client for DSIS API.

Handles HTTP requests, session management, and connection testing.
"""

import logging
from typing import Any, Dict, Optional, Union

import requests

from ..auth import DSISAuth
from ..config import DSISConfig
from ._http import HTTPTransportMixin

logger = logging.getLogger(__name__)


class BaseClient(HTTPTransportMixin):
    """Base client for HTTP operations.

    Handles authentication, session management, and HTTP requests.
    Inherits HTTP transport methods from HTTPTransportMixin.
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
        logger.info(
            f"Base client initialized for {config.environment.value} environment"
        )

    def refresh_authentication(self) -> None:
        """Refresh authentication tokens.

        Clears cached tokens and acquires new ones. Useful when tokens
        have expired or when you need to ensure fresh authentication.

        Raises:
            DSISAuthenticationError: If token acquisition fails
        """
        logger.info("Refreshing authentication")
        self.auth.refresh_tokens()

    def test_connection(self) -> bool:
        """Test the connection to the DSIS API.

        Attempts to connect to the DSIS API data endpoint to verify
        that authentication and connectivity are working.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            logger.info("Testing DSIS API connection")
            headers = self.auth.get_auth_headers()
            response = self._session.get(
                self.config.data_endpoint, headers=headers, timeout=10
            )
            success = response.status_code in [200, 404]
            if success:
                logger.info("Connection test successful")
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
        project: Optional[str] = None,
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
        /<model_name>/<version>[/<district_id>][/<project>][/<schema>]

        All path segments are optional and can be omitted.
        The schema parameter refers to specific data schemas from dsis-schemas
        (e.g., "Basin", "Well", "Wellbore", "WellLog", etc.).

        Args:
            district_id: Optional district ID for the query
            project: Optional project name for the query
            schema: Optional schema name (e.g., "Basin", "Well", "Wellbore").
                    If None, uses configured model_name
            format_type: Response format (default: "json")
            select: OData $select parameter for column selection (comma-separated)
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
        from ..models import is_valid_schema

        # Determine the schema to use
        if schema is not None:
            schema_to_use = schema
        elif district_id is not None or project is not None:
            schema_to_use = self.config.model_name
            logger.info(f"Using configured model as schema: {self.config.model_name}")
        else:
            schema_to_use = None

        # Validate schema if provided and validation is enabled
        if validate_schema and schema_to_use is not None:
            if not is_valid_schema(schema_to_use):
                raise ValueError(
                    f"Unknown schema: '{schema_to_use}'. Use "
                    "get_schema_by_name() to discover available schemas."
                )

        # Build endpoint path segments
        segments = [self.config.model_name, self.config.model_version]
        if district_id is not None:
            segments.append(str(district_id))
        if project is not None:
            segments.append(project)
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
