"""HTTP transport layer for DSIS API.

Provides mixin class for making authenticated HTTP requests.
"""

import json
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional
from urllib.parse import urljoin

from ..exceptions import DSISAPIError, DSISJSONParseError

if TYPE_CHECKING:
    import requests

    from ..auth import DSISAuth
    from ..config import DSISConfig

logger = logging.getLogger(__name__)


class HTTPTransportMixin:
    """HTTP transport mixin for DSIS API requests.

    Provides methods for making authenticated HTTP requests to the DSIS API.
    Requires subclasses to set: config, auth, _session.
    """

    config: "DSISConfig"
    auth: "DSISAuth"
    _session: "requests.Session"

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

        logger.info(f"Making request to {url}")
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
            # Try parsing with strict=False to allow control characters
            try:
                logger.info(
                    "Standard JSON parsing failed, trying with strict=False to allow control characters"
                )
                return json.loads(response.text, strict=False)
            except ValueError:
                # Both methods failed, raise the custom exception
                logger.warning(
                    f"Failed to parse JSON response even with strict=False: {e}"
                )
                raise DSISJSONParseError(
                    f"Failed to parse JSON response: {e}",
                    response_text=response.text,
                    original_error=e,
                )

    def _request_binary(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Optional[bytes]:
        """Make an authenticated GET request for binary data.

        Internal method for fetching binary protobuf data from the DSIS API.

        Note: The DSIS API returns binary protobuf data with Accept: application/json,
        not application/octet-stream. This is the actual behavior observed in the API.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            Binary response content, or None if the entity has no bulk data (404)

        Raises:
            DSISAPIError: If the request fails with an error other than 404
        """
        url = urljoin(f"{self.config.data_endpoint}/", endpoint)
        headers = self.auth.get_auth_headers()
        # Use application/json - the API returns binary data with this Accept header
        headers["Accept"] = "application/json"

        logger.info(f"Making binary request to {url}")
        response = self._session.get(url, headers=headers, params=params)

        if response.status_code == 404:
            # Entity exists but has no bulk data field
            logger.info(f"No bulk data available for endpoint: {endpoint}")
            return None
        elif response.status_code != 200:
            error_msg = (
                f"Binary API request failed: {response.status_code} - "
                f"{response.reason} - {response.text}"
            )
            logger.error(error_msg)
            raise DSISAPIError(error_msg)

        return response.content

    def _request_binary_stream(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 10 * 1024 * 1024,
    ):
        """Stream binary data in chunks to avoid loading large datasets into memory.

        Internal method for streaming binary protobuf data from the DSIS API.

        Note: The DSIS API returns binary protobuf data with Accept: application/json,
        not application/octet-stream. This is the actual behavior observed in the API.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            chunk_size: Size of chunks to yield (default: 10MB, recommended by DSIS)

        Yields:
            Binary data chunks as bytes

        Raises:
            DSISAPIError: If the request fails with an error other than 404
            StopIteration: If the entity has no bulk data (404)
        """
        url = urljoin(f"{self.config.data_endpoint}/", endpoint)
        headers = self.auth.get_auth_headers()
        # Use application/json - the API returns binary data with this Accept header
        headers["Accept"] = "application/json"

        logger.info(f"Making streaming binary request to {url}")
        response = self._session.get(url, headers=headers, params=params, stream=True)

        if response.status_code == 404:
            # Entity exists but has no bulk data field
            logger.info(f"No bulk data available for endpoint: {endpoint}")
            return
        elif response.status_code != 200:
            error_msg = (
                f"Binary API request failed: {response.status_code} - "
                f"{response.reason} - {response.text}"
            )
            logger.error(error_msg)
            raise DSISAPIError(error_msg)

        # Stream the content in chunks
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:  # filter out keep-alive new chunks
                yield chunk
