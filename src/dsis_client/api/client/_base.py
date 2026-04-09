"""Base classes for DSIS client mixins.

Provides TYPE_CHECKING stubs for attributes/methods that mixins require from
the host class. Mixins inherit from these bases so mypy can verify attribute
access without runtime overhead.
"""

from typing import TYPE_CHECKING, Any, Dict, Generator, Optional, Union

if TYPE_CHECKING:
    from ..config import DSISConfig


class _RequestBase:
    """Base providing _request stub for mixins that make JSON requests."""

    if TYPE_CHECKING:
        config: "DSISConfig"

        def _request(
            self,
            endpoint: str,
            params: Optional[Dict[str, Any]] = None,
            timeout: Optional[Union[float, tuple[float, float]]] = None,
        ) -> Dict[str, Any]: ...


class _PaginationBase(_RequestBase):
    """Base providing pagination stub for mixins that iterate pages."""

    if TYPE_CHECKING:

        def _yield_nextlink_pages(
            self,
            response: Dict[str, Any],
            endpoint: str,
            max_pages: int = -1,
            timeout: Optional[Union[float, tuple[float, float]]] = None,
        ) -> Generator[Dict[str, Any], None, None]: ...

        def _extract_nextlink_from_text(self, response_text: str) -> Optional[str]: ...


class _BinaryRequestBase:
    """Base providing binary request stubs for mixins that fetch protobuf data."""

    if TYPE_CHECKING:
        config: "DSISConfig"

        def _request_binary(
            self,
            endpoint: str,
            params: Optional[Dict[str, Any]] = None,
            accept: str = "application/json",
            timeout: Optional[Union[float, tuple[float, float]]] = None,
        ) -> Optional[bytes]: ...

        def _request_binary_stream(
            self,
            endpoint: str,
            params: Optional[Dict[str, Any]] = None,
            chunk_size: int = 10 * 1024 * 1024,
            accept: str = "application/json",
            timeout: Optional[Union[float, tuple[float, float]]] = None,
            stream_retries: int = 0,
            total_timeout: Optional[float] = None,
        ) -> Generator[bytes, None, None]: ...
