"""Base classes for DSIS client mixins.

Provides TYPE_CHECKING stubs for attributes/methods that mixins require from
the host class. Mixins inherit from these bases so mypy can verify attribute
access without runtime overhead.
"""

from collections.abc import Generator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..config import DSISConfig


class _RequestBase:
    """Base providing _request stub for mixins that make JSON requests."""

    if TYPE_CHECKING:
        config: "DSISConfig"

        def _request(
            self,
            endpoint: str,
            params: dict[str, Any] | None = None,
            timeout: float | tuple[float, float] | None = None,
        ) -> dict[str, Any]: ...


class _PaginationBase(_RequestBase):
    """Base providing pagination stub for mixins that iterate pages."""

    if TYPE_CHECKING:

        def _yield_nextlink_pages(
            self,
            response: dict[str, Any],
            endpoint: str,
            max_pages: int = -1,
            timeout: float | tuple[float, float] | None = None,
        ) -> Generator[dict[str, Any], None, None]: ...

        def _extract_nextlink_from_text(self, response_text: str) -> str | None: ...


class _BinaryRequestBase:
    """Base providing binary request stubs for mixins that fetch protobuf data."""

    if TYPE_CHECKING:
        config: "DSISConfig"

        def _request_binary(
            self,
            endpoint: str,
            params: dict[str, Any] | None = None,
            accept: str = "application/json",
            timeout: float | tuple[float, float] | None = None,
        ) -> bytes | None: ...

        def _request_binary_stream(
            self,
            endpoint: str,
            params: dict[str, Any] | None = None,
            chunk_size: int = 10 * 1024 * 1024,
            accept: str = "application/json",
            timeout: float | tuple[float, float] | None = None,
            stream_retries: int = 0,
            total_timeout: float | None = None,
        ) -> Generator[bytes, None, None]: ...
