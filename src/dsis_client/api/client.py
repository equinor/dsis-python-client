"""Main client module for DSIS API.

Provides the DSISClient class for making authenticated requests to the DSIS API.
Handles request construction, authentication header management, and response parsing.
"""

import logging
from typing import Any, Dict, Optional, Union
from urllib.parse import urljoin

import requests

from .auth import DSISAuth
from .config import DSISConfig
from .exceptions import DSISAPIError

logger = logging.getLogger(__name__)


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
        *path_segments: Union[str, int],
        format_type: str = "json",
        select: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        **extra_query: Any,
    ) -> Dict[str, Any]:
        """Make a GET request to the DSIS API.

        Constructs an endpoint URL from path segments and makes an authenticated
        GET request to the DSIS API with optional query parameters.

        Args:
            *path_segments: Variable length path segments to construct the endpoint
            format_type: Response format (default: "json")
            select: OData $select parameter for field selection
            params: Dictionary of additional query parameters
            **extra_query: Additional query parameters as keyword arguments

        Returns:
            Dictionary containing the parsed API response

        Raises:
            DSISAPIError: If the API request fails

        Example:
            >>> client.get("OW5000", "5000107", format_type="json")
            >>> client.get("OW5000", select="field1,field2")
        """
        endpoint = "/".join(str(s).strip("/") for s in path_segments if s)
        query: Dict[str, Any] = {"$format": format_type}
        if select:
            query["$select"] = select
        if params:
            query.update(params)
        if extra_query:
            query.update(extra_query)
        return self._request(endpoint, query)

    def get_odata(
        self,
        table: str,
        record_id: Optional[Union[str, int]] = None,
        format_type: str = "json",
        **query: Any,
    ) -> Dict[str, Any]:
        """Get OData from a specific table.

        Convenience method for retrieving OData from a table, optionally
        filtered by record ID.

        Args:
            table: OData table name (e.g., "OW5000")
            record_id: Optional record ID to retrieve a specific record
            format_type: Response format (default: "json")
            **query: Additional OData query parameters

        Returns:
            Dictionary containing the parsed OData response

        Raises:
            DSISAPIError: If the API request fails

        Example:
            >>> client.get_odata("OW5000", "5000107")
            >>> client.get_odata("OW5000")
        """
        segments = (table,) + ((record_id,) if record_id is not None else tuple())
        return self.get(*segments, format_type=format_type, **query)

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
