"""Base HTTP client for DSIS API.

Handles HTTP requests, session management, and connection testing.
"""

import logging
from typing import Any, Dict, Optional
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
