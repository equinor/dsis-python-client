"""Bulk data operations for DSIS API.

Provides mixin class for fetching binary protobuf data.
"""

import logging
from typing import TYPE_CHECKING, Generator, Optional, Union
from urllib.parse import urlparse

from ._base import _BinaryRequestBase

if TYPE_CHECKING:
    from ..query import QueryBuilder

logger = logging.getLogger(__name__)


class BulkDataMixin(_BinaryRequestBase):
    """Bulk data mixin for binary protobuf operations.

    Provides methods for fetching and streaming binary bulk data.
    Requires subclasses to provide: config, _request_binary, _request_binary_stream.
    """

    @staticmethod
    def _build_bulk_query_root(query: "QueryBuilder") -> str:
        """Build the DSIS path prefix before a collection/media-link segment."""
        if not query._schema_name:
            raise ValueError(
                "Query must define a schema. "
                "Call query.schema(...) before passing it to bulk data methods."
            )

        segments = [query.model_name, query.model_version]
        if query.district_id is not None:
            segments.append(str(query.district_id))
        if query.project is not None:
            segments.append(query.project)
        return "/".join(segments)

    def _resolve_bulk_endpoint(
        self,
        query: "QueryBuilder",
        media_link: Optional[str] = None,
    ) -> str:
        """Resolve a bulk-data endpoint from entity targeting or an OData media link."""
        if media_link is None:
            if query._native_uid is None:
                raise ValueError(
                    "Query must target an entity. "
                    "Call query.entity(native_uid) before passing to get_bulk_data()."
                )
            return query.build_endpoint()

        media_link = media_link.strip()
        if not media_link:
            raise ValueError("media_link must be a non-empty string")

        data_endpoint = self.config.data_endpoint.rstrip("/")
        if media_link.startswith(f"{data_endpoint}/"):
            return media_link[len(data_endpoint) + 1 :]

        parsed_link = urlparse(media_link)
        if parsed_link.scheme or parsed_link.netloc:
            raise ValueError(
                "media_link must be relative to the configured DSIS data endpoint "
                "or use that exact endpoint as its base URL"
            )

        normalized_link = media_link.lstrip("/")
        configured_path = urlparse(data_endpoint).path.strip("/")
        if configured_path and normalized_link.startswith(f"{configured_path}/"):
            return normalized_link[len(configured_path) + 1 :]

        query_root = self._build_bulk_query_root(query)
        if normalized_link.startswith(f"{query_root}/"):
            return normalized_link

        return f"{query_root}/{normalized_link}"

    def get_bulk_data(
        self,
        query: "QueryBuilder",
        *,
        accept: str = "application/json",
        timeout: Optional[Union[float, tuple[float, float]]] = None,
        media_link: Optional[str] = None,
    ) -> Optional[bytes]:
        """Fetch binary bulk data (protobuf) for a specific entity.

        By default, the query must have been configured with
        ``.entity(native_uid)`` to target a specific entity's binary data field.
        When ``media_link`` is provided, the query only needs ``.schema()`` and
        the exact OData media link path returned by DSIS can be used directly.

        The DSIS API serves large binary data fields (horizon z-values, log curves,
        seismic amplitudes) as Protocol Buffers via a special OData endpoint:
        ``/{schema}('{native_uid}')/{data_field}``

        Args:
            query: A QueryBuilder instance configured with ``.schema()``.
                Call ``.entity()`` for the existing endpoint-building flow, or
                keep the collection query and pass ``media_link`` with the exact
                OData link returned by DSIS. Holds model, district, project,
                schema, native_uid, and data_field.
            accept: Accept header value for the HTTP request
                (default: ``"application/json"``). Use ``"application/octet-stream"``
                for endpoints that serve raw binary data (e.g., SurfaceGrid/$value).
            timeout: Request timeout in seconds. Can be a single float for both
                connect and read timeouts, or a (connect, read) tuple.
                None means no timeout (default). For streamed downloads this
                applies to connection setup and waiting for the next bytes to
                arrive, not to the total download duration.
            media_link: Optional OData media link path returned by DSIS
                (for example ``LogCurve(...)/data``). Relative media links are
                resolved against the query's model/version/district/project
                context. Service-root-relative paths and full URLs pointing to
                the configured data endpoint are also accepted.

        Returns:
            Binary protobuf data as bytes, or None if the entity has no bulk data

        Raises:
            ValueError: If query has no entity set and no media_link is provided
            DSISAPIError: If the API request fails (other than 404 for missing data)

        Example:
            >>> from dsis_model_sdk.models.common import LogCurve
            >>> # Build query for metadata
            >>> query = (
            ...     QueryBuilder(
            ...         model_name="OW5000",
            ...         district_id="123",
            ...         project="SNORRE",
            ...     )
            ...     .schema(LogCurve)
            ... )
            >>> curves = list(client.execute_query(query))
            >>>
            >>> # Fetch binary data using the exact media link returned by DSIS
            >>> binary_data = client.get_bulk_data(
            ...     query,
            ...     media_link=curves[0]["data@odata.mediaReadLink"],
            ... )
            >>>
            >>> if binary_data:
            ...     from dsis_model_sdk.protobuf import decode_log_curves
            ...     decoded = decode_log_curves(binary_data)
        """
        endpoint = self._resolve_bulk_endpoint(query, media_link=media_link)
        logger.info(f"Fetching bulk data from: {endpoint}")
        return self._request_binary(endpoint, accept=accept, timeout=timeout)

    def get_bulk_data_stream(
        self,
        query: "QueryBuilder",
        *,
        chunk_size: int = 10 * 1024 * 1024,
        accept: str = "application/json",
        timeout: Optional[Union[float, tuple[float, float]]] = None,
        stream_retries: int = 0,
        total_timeout: Optional[float] = None,
        media_link: Optional[str] = None,
    ) -> Generator[bytes, None, None]:
        """Stream binary bulk data (protobuf) in chunks for memory-efficient processing.

        By default, the query must have been configured with
        ``.entity(native_uid)`` to target a specific entity's binary data field.
        When ``media_link`` is provided, the query only needs ``.schema()`` and
        the exact OData media link path returned by DSIS can be used directly.

        This streaming version yields data in chunks rather than loading everything
        into memory at once. Useful for very large datasets (e.g., seismic volumes).

        Args:
            query: A QueryBuilder instance configured with ``.schema()``.
                Call ``.entity()`` for the existing endpoint-building flow, or
                keep the collection query and pass ``media_link`` with the exact
                OData link returned by DSIS. Holds model, district, project,
                schema, native_uid, and data_field.
            chunk_size: Size of chunks to yield in bytes
                (default: 10MB, recommended by DSIS)
            accept: Accept header value for the HTTP request
                (default: ``"application/json"``). Use ``"application/octet-stream"``
                for endpoints that serve raw binary data (e.g., SurfaceGrid/$value).
            timeout: Request timeout in seconds. Can be a single float for both
                connect and read timeouts, or a (connect, read) tuple.
                None means no timeout (default).
            stream_retries: Number of retry attempts for failures while reading
                streamed chunks. Retries assume the endpoint returns the same
                bytes when reopened. Default is 0 (no retries).
            total_timeout: Maximum wall-clock seconds for the entire stream
                (including retries). None means no total timeout (default).
                Unlike ``timeout`` which only guards gaps between bytes, this
                catches slow-trickle streams that never fully stall.
            media_link: Optional OData media link path returned by DSIS
                (for example ``LogCurve(...)/data``). Relative media links are
                resolved against the query's model/version/district/project
                context. Service-root-relative paths and full URLs pointing to
                the configured data endpoint are also accepted.

        Yields:
            Binary data chunks as bytes. Returns immediately if no bulk data (404).

        Raises:
            ValueError: If query has no entity set and no media_link is provided
            DSISAPIError: If the API request fails (other than 404 for missing data)

        Example:
            >>> from dsis_model_sdk.models.common import SeismicDataSet3D
            >>> query = (
            ...     QueryBuilder(
            ...         model_name="OW5000",
            ...         district_id="123",
            ...         project="SNORRE",
            ...     )
            ...     .schema(SeismicDataSet3D)
            ... )
            >>> datasets = list(client.execute_query(query))
            >>>
            >>> chunks = list(
            ...     client.get_bulk_data_stream(
            ...         query,
            ...         media_link=datasets[0]["data@odata.mediaReadLink"],
            ...     )
            ... )
            >>> if chunks:
            ...     binary_data = b"".join(chunks)
        """
        if media_link is None and query._native_uid is None:
            raise ValueError(
                "Query must target an entity. "
                "Call query.entity(native_uid) before passing to get_bulk_data_stream()."
            )

        endpoint = self._resolve_bulk_endpoint(query, media_link=media_link)
        logger.info(f"Streaming bulk data from: {endpoint} (chunk_size={chunk_size})")
        yield from self._request_binary_stream(
            endpoint,
            chunk_size=chunk_size,
            accept=accept,
            timeout=timeout,
            stream_retries=stream_retries,
            total_timeout=total_timeout,
        )
