"""OData pagination support for DSIS API.

Provides mixin class for handling OData nextLink pagination.
"""

import logging
import re
from typing import Any, Dict, Optional

from ..exceptions import DSISJSONParseError
from ._base import _RequestBase

logger = logging.getLogger(__name__)


class PaginationMixin(_RequestBase):
    """OData pagination mixin.

    Provides methods for following OData nextLink pagination.
    Requires subclasses to provide: _request method.
    """

    def _extract_nextlink_from_text(self, response_text: str) -> Optional[str]:
        """Extract nextLink from raw response text when JSON parsing fails.

        This is a fallback mechanism for responses that contain invalid control
        characters but are otherwise valid.

        Args:
            response_text: The raw response text containing the nextLink.

        Returns:
            The extracted nextLink URL, or None if extraction fails.
        """
        try:
            # Try multiple patterns for finding nextLink
            patterns = [
                '"odata.nextLink"',
                "'odata.nextLink'",
                '"nextLink"',
                "'nextLink'",
                "odata.nextLink",
                "nextLink",
            ]

            found_pattern = None
            index = -1

            for pattern in patterns:
                index = response_text.find(pattern)
                if index != -1:
                    found_pattern = pattern
                    logger.debug(
                        f"Found nextLink using pattern: {pattern} at index {index}"
                    )
                    break

            if not found_pattern:
                logger.debug(
                    "No nextLink found in response text with any known pattern"
                )
                # Log a snippet of the end of the response to help debugging
                snippet = (
                    response_text[-500:] if len(response_text) > 500 else response_text
                )
                logger.debug(f"Response text end (last 500 chars): ...{snippet}")
                return None

            # Extract text after the pattern
            raw_nextlink = response_text[index + len(found_pattern) :]

            # Remove leading colons, whitespace, quotes, and equals signs
            raw_nextlink = raw_nextlink.lstrip(":\"=' \t\n\r")

            # Try to extract the URL - it should end with a quote or comma or brace
            # Try multiple regex patterns
            url_patterns = [
                r'^([^"\',}\]]+)',  # URL without quotes until delimiter
                r'^"([^"]+)"',  # URL in double quotes
                r"^'([^']+)'",  # URL in single quotes
            ]

            for pattern in url_patterns:
                match = re.match(pattern, raw_nextlink)
                if match:
                    extracted_url = match.group(1)
                    logger.info(f"Successfully extracted nextLink: {extracted_url}")
                    return extracted_url

            logger.warning(
                f"Could not extract nextLink value from response text. Raw text after pattern: {raw_nextlink[:100]}"
            )
            return None
        except Exception as e:
            logger.warning(f"Failed to extract nextLink from raw text: {e}")
            return None

    def _build_nextlink_endpoint(self, endpoint: str, next_link: str) -> str:
        """Build the full endpoint URL for a nextLink.

        Replaces the last segment of the endpoint (schema name) with the nextLink.

        Args:
            endpoint: The original endpoint path.
            next_link: The nextLink value from the response.

        Returns:
            The full endpoint path for the next page request.
        """
        endpoint_parts = endpoint.rsplit("/", 1)
        if len(endpoint_parts) == 2:
            return f"{endpoint_parts[0]}/{next_link}"
        # Fallback if endpoint has no slash (shouldn't happen in practice)
        return next_link

    def _fetch_next_page(
        self, endpoint: str, next_key: str
    ) -> tuple[list[Any], Optional[str]]:
        """Fetch the next page of results with fallback handling.

        Makes a request to the endpoint and returns the items and next link.
        If JSON parsing fails, attempts to extract the nextLink from raw text.

        Args:
            endpoint: The full endpoint path for the request.
            next_key: The key to look for the next link in the response.

        Returns:
            A tuple of (items, next_link). Items may be empty if JSON parsing
            failed but nextLink extraction succeeded.

        Raises:
            DSISJSONParseError: If JSON parsing fails and fallback extraction
                also fails.
        """
        try:
            response = self._request(endpoint, params=None)
            items = response.get("value", [])
            next_link = response.get(next_key)
            return items, next_link

        except DSISJSONParseError as e:
            logger.warning(
                "JSON parsing failed for pagination. "
                "Attempting to extract nextLink from raw text."
            )

            fallback_next_link = self._extract_nextlink_from_text(e.response_text)

            if fallback_next_link:
                logger.info(
                    f"Fallback nextLink extraction succeeded: {fallback_next_link}"
                )
                # Return empty items since JSON parsing failed,
                # but we can continue to the next page
                return [], fallback_next_link

            # Fallback also failed, re-raise the original error
            logger.error(
                "Fallback nextLink extraction failed. Cannot continue pagination."
            )
            raise

    def _yield_nextlink_pages(
        self, response: Dict[str, Any], endpoint: str, max_pages: int = -1
    ):
        """Generator that yields items from pages following OData nextLinks.

        Yields items up to max_pages. If max_pages=-1, yields all pages.

        Args:
            response: Initial API response dict
            endpoint: Full endpoint path from initial request (without query params)
            max_pages: Maximum number of pages to yield. -1 means unlimited (all pages).

        Yields:
            Individual items from each page's 'value' array
        """
        next_key = "odata.nextLink"
        page_count = 0

        # Yield items from the initial response
        for item in response.get("value", []):
            yield item
        page_count += 1

        if max_pages != -1 and page_count >= max_pages:
            return

        next_link = response.get(next_key)

        while next_link:
            if max_pages != -1 and page_count >= max_pages:
                break

            logger.info(f"Following nextLink: {next_link}")

            temp_endpoint = self._build_nextlink_endpoint(endpoint, next_link)
            items, next_link = self._fetch_next_page(temp_endpoint, next_key)

            for item in items:
                yield item

            page_count += 1
