"""HTTP transport layer for DSIS API.

Provides mixin class for making authenticated HTTP requests.
"""

import json
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, Generator, Optional, Union
from urllib.parse import urljoin

import requests

from ..exceptions import DSISAPIError, DSISJSONParseError

if TYPE_CHECKING:
    from ..auth import DSISAuth
    from ..config import DSISConfig

logger = logging.getLogger(__name__)

# Status codes that trigger automatic token refresh and retry
_RETRY_STATUS_CODES = {401, 500}
_STREAM_RETRY_EXCEPTIONS = (requests.exceptions.RequestException, OSError)


class HTTPTransportMixin:
    """HTTP transport mixin for DSIS API requests.

    Provides methods for making authenticated HTTP requests to the DSIS API.
    Requires subclasses to set: config, auth, _session.
    """

    config: "DSISConfig"
    auth: "DSISAuth"
    _session: "requests.Session"

    def _make_request_with_retry(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        stream: bool = False,
        request_type: str = "standard",
        timeout: Optional[Union[float, tuple[float, float]]] = None,
    ) -> "requests.Response":
        """Make an HTTP GET request with automatic token refresh retry.

        Handles the common pattern of making a request, checking for auth-related
        errors (401 or 500), refreshing tokens, and retrying once.

        Args:
            url: Full URL to request
            params: Query parameters
            extra_headers: Additional headers to merge with auth headers
            stream: Whether to stream the response
            request_type: Description for logging (e.g., "binary", "streaming")
            timeout: Request timeout in seconds. Can be a single float for both
                connect and read timeouts, or a (connect, read) tuple.
                None means no timeout (default).

        Returns:
            The HTTP response object (after potential retry)
        """
        headers = self.auth.get_auth_headers()
        if extra_headers:
            headers.update(extra_headers)

        response = self._session.get(
            url, headers=headers, params=params, stream=stream, timeout=timeout
        )

        if response.status_code in _RETRY_STATUS_CODES:
            logger.warning(
                f"{request_type.capitalize()} request failed with {response.status_code}, "
                "refreshing tokens and retrying"
            )
            if stream:
                response.close()
            self.auth.refresh_tokens()
            headers = self.auth.get_auth_headers()
            if extra_headers:
                headers.update(extra_headers)
            response = self._session.get(
                url, headers=headers, params=params, stream=stream, timeout=timeout
            )

        return response

    def _request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, tuple[float, float]]] = None,
    ) -> Dict[str, Any]:
        """Make an authenticated GET request to the DSIS API.

        Internal method that constructs the full URL, adds authentication
        headers, and makes the request. Automatically retries once with
        refreshed tokens on 401 or 500 errors.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            timeout: Request timeout in seconds. Can be a single float for both
                connect and read timeouts, or a (connect, read) tuple.
                None means no timeout (default).

        Returns:
            Parsed JSON response as dictionary

        Raises:
            DSISAPIError: If the request fails or returns non-200 status
        """
        url = urljoin(f"{self.config.data_endpoint}/", endpoint)
        logger.info(f"Making request to {url}")
        response = self._make_request_with_retry(url, params, timeout=timeout)

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
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        accept: str = "application/json",
        timeout: Optional[Union[float, tuple[float, float]]] = None,
    ) -> Optional[bytes]:
        """Make an authenticated GET request for binary data.

        Internal method for fetching binary protobuf data from the DSIS API.
        Automatically retries once with refreshed tokens on 401 or 500 errors.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            accept: Accept header value (default: "application/json")
            timeout: Request timeout in seconds. Can be a single float for both
                connect and read timeouts, or a (connect, read) tuple.
                None means no timeout (default).

        Returns:
            Binary response content, or None if the entity has no bulk data (404)

        Raises:
            DSISAPIError: If the request fails with an error other than 404
        """
        url = urljoin(f"{self.config.data_endpoint}/", endpoint)
        logger.info(f"Making binary request to {url}")
        response = self._make_request_with_retry(
            url,
            params,
            extra_headers={"Accept": accept},
            request_type="binary",
            timeout=timeout,
        )

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
        accept: str = "application/json",
        timeout: Optional[Union[float, tuple[float, float]]] = None,
        stream_retries: int = 0,
        total_timeout: Optional[float] = None,
    ) -> Generator[bytes, None, None]:
        """Stream binary data in chunks to avoid loading large datasets into memory.

        Internal method for streaming binary protobuf data from the DSIS API.
        Automatically retries once with refreshed tokens on 401 or 500 errors.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            chunk_size: Size of chunks to yield (default: 10MB, recommended by DSIS)
            accept: Accept header value (default: "application/json")
            timeout: Request timeout in seconds. Can be a single float for both
                connect and read timeouts, or a (connect, read) tuple.
                None means no timeout (default).
            stream_retries: Number of retry attempts for stream-read failures.
                Retries reopen the stream and resume from the last yielded byte.
                This assumes the endpoint returns deterministic content across
                reconnects. Default is 0 (no stream retries).
            total_timeout: Maximum wall-clock seconds for the entire stream
                (including retries). None means no total timeout (default).
                Unlike ``timeout`` which only guards gaps between bytes, this
                catches slow-trickle streams that never fully stall.

        Yields:
            Binary data chunks as bytes

        Raises:
            DSISAPIError: If the request fails with an error other than 404
            StopIteration: If the entity has no bulk data (404)
        """
        url = urljoin(f"{self.config.data_endpoint}/", endpoint)
        bytes_yielded = 0
        retry_attempt = 0
        deadline = (time.monotonic() + total_timeout) if total_timeout else None

        while True:
            logger.info(f"Making streaming binary request to {url}")
            response = self._make_request_with_retry(
                url,
                params,
                extra_headers={"Accept": accept},
                stream=True,
                request_type="streaming",
                timeout=timeout,
            )

            if response.status_code == 404:
                logger.info(f"No bulk data available for endpoint: {endpoint}")
                response.close()
                return
            if response.status_code != 200:
                error_msg = (
                    f"Binary API request failed: {response.status_code} - "
                    f"{response.reason} - {response.text}"
                )
                logger.error(error_msg)
                response.close()
                raise DSISAPIError(error_msg)

            try:
                remaining = bytes_yielded
                while remaining > 0:
                    discarded = response.raw.read(min(chunk_size, remaining))
                    if not discarded:
                        raise requests.exceptions.ChunkedEncodingError(
                            "Stream ended while resuming previously yielded content"
                        )
                    remaining -= len(discarded)

                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        if deadline and time.monotonic() > deadline:
                            raise DSISAPIError(
                                f"Total timeout ({total_timeout}s) exceeded after "
                                f"{bytes_yielded / (1024 * 1024):.1f} MB"
                            )
                        bytes_yielded += len(chunk)
                        yield chunk
                return
            except _STREAM_RETRY_EXCEPTIONS as exc:
                if retry_attempt >= stream_retries:
                    error_msg = (
                        "Streaming binary request failed after "
                        f"{retry_attempt} retries: {exc}"
                    )
                    logger.error(error_msg)
                    raise DSISAPIError(error_msg) from exc

                retry_attempt += 1
                delay = 1 if retry_attempt == 1 else 5 * (retry_attempt - 1)
                logger.warning(
                    "Streaming binary request failed after %s bytes on attempt %s/%s: %s. "
                    "Retrying in %s second(s)",
                    bytes_yielded,
                    retry_attempt,
                    stream_retries,
                    exc,
                    delay,
                )
                time.sleep(delay)
            finally:
                response.close()
