"""OData pagination support for DSIS API.

Provides mixin class for handling OData nextLink pagination.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class PaginationMixin:
    """OData pagination mixin.

    Provides methods for following OData nextLink pagination.
    Requires subclasses to provide: _request method.
    """

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

        if page_count >= max_pages and max_pages != -1:
            return

        next_link = response.get(next_key)

        while next_link:
            if max_pages != -1 and page_count >= max_pages:
                break

            logger.info(f"Following nextLink: {next_link}")

            # Replace the last segment of endpoint (schema name) with the full next_link
            endpoint_parts = endpoint.rsplit("/", 1)
            if len(endpoint_parts) == 2:
                temp_endpoint = f"{endpoint_parts[0]}/{next_link}"
            else:
                # Fallback if endpoint has no slash (shouldn't happen in practice)
                temp_endpoint = next_link

            # Make request with the temp endpoint
            next_resp = self._request(temp_endpoint, params=None)

            # Yield items from this page
            for item in next_resp.get("value", []):
                yield item

            page_count += 1

            # Check for next link in the next response
            next_link = next_resp.get(next_key)
