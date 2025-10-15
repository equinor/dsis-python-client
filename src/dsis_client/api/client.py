"""Main client module for DSIS API.

Unified `get` method replaces prior separate path builders; `get_odata` remains
as a convenience wrapper. Further complexity (custom exceptions, retries,
context manager) intentionally omitted for early development simplicity.
"""

from typing import Any, Dict, Optional, Union
from urllib.parse import urljoin

import requests

from .auth import DSISAuth
from .config import DSISConfig


class DSISClient:
    """Main client for DSIS API interactions."""

    def __init__(self, config: DSISConfig):
        self.config = config
        self.auth = DSISAuth(config)
        self._session = requests.Session()

    def get(
        self,
        *path_segments: Union[str, int],
        format_type: str = "json",
        select: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        **extra_query: Any,
    ) -> Dict[str, Any]:
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
        segments = (table,) + ((record_id,) if record_id is not None else tuple())
        return self.get(*segments, format_type=format_type, **query)

    def refresh_authentication(self) -> None:
        self.auth.refresh_tokens()

    def test_connection(self) -> bool:
        try:
            headers = self.auth.get_auth_headers()
            response = self._session.get(
                self.config.data_endpoint, headers=headers, timeout=10
            )
            return response.status_code in [200, 404]
        except Exception:
            return False

    def _request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        url = urljoin(f"{self.config.data_endpoint}/", endpoint)
        headers = self.auth.get_auth_headers()
        response = self._session.get(url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(
                f"API request failed: {response.status_code} - {response.text}"
            )
        try:
            return response.json()
        except ValueError:
            return {"data": response.text}
