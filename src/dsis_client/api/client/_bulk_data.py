"""Bulk data operations for DSIS API.

Provides mixin class for fetching binary protobuf data.
"""

import logging
from typing import TYPE_CHECKING, Generator, Optional

from ._base import _BinaryRequestBase

if TYPE_CHECKING:
    from ..query import QueryBuilder

logger = logging.getLogger(__name__)


class BulkDataMixin(_BinaryRequestBase):
    """Bulk data mixin for binary protobuf operations.

    Provides methods for fetching and streaming binary bulk data.
    Requires subclasses to provide: config, _request_binary, _request_binary_stream.
    """

    def get_bulk_data(
        self,
        query: "QueryBuilder",
        *,
        accept: str = "application/json",
    ) -> Optional[bytes]:
        """Fetch binary bulk data (protobuf) for a specific entity.

        The query must have been configured with ``.entity(native_uid)`` to
        target a specific entity's binary data field.

        The DSIS API serves large binary data fields (horizon z-values, log curves,
        seismic amplitudes) as Protocol Buffers via a special OData endpoint:
        ``/{schema}('{native_uid}')/{data_field}``

        Args:
            query: A QueryBuilder instance configured with ``.schema()`` and
                ``.entity()`` calls. Holds model, district, project, schema,
                native_uid, and data_field.
            accept: Accept header value for the HTTP request
                (default: ``"application/json"``). Use ``"application/octet-stream"``
                for endpoints that serve raw binary data (e.g., SurfaceGrid/$value).

        Returns:
            Binary protobuf data as bytes, or None if the entity has no bulk data

        Raises:
            ValueError: If query has no entity set
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
            >>> # Fetch binary data by targeting a specific entity
            >>> bulk_query = query.entity(curves[0]["native_uid"])
            >>> binary_data = client.get_bulk_data(bulk_query)
            >>>
            >>> if binary_data:
            ...     from dsis_model_sdk.protobuf import decode_log_curves
            ...     decoded = decode_log_curves(binary_data)
        """
        if query._native_uid is None:
            raise ValueError(
                "Query must target an entity. "
                "Call query.entity(native_uid) before passing to get_bulk_data()."
            )

        endpoint = query.build_endpoint()
        logger.info(f"Fetching bulk data from: {endpoint}")
        return self._request_binary(endpoint, accept=accept)

    def get_bulk_data_stream(
        self,
        query: "QueryBuilder",
        *,
        chunk_size: int = 10 * 1024 * 1024,
        accept: str = "application/json",
    ) -> Generator[bytes, None, None]:
        """Stream binary bulk data (protobuf) in chunks for memory-efficient processing.

        The query must have been configured with ``.entity(native_uid)`` to
        target a specific entity's binary data field.

        This streaming version yields data in chunks rather than loading everything
        into memory at once. Useful for very large datasets (e.g., seismic volumes).

        Args:
            query: A QueryBuilder instance configured with ``.schema()`` and
                ``.entity()`` calls. Holds model, district, project, schema,
                native_uid, and data_field.
            chunk_size: Size of chunks to yield in bytes
                (default: 10MB, recommended by DSIS)
            accept: Accept header value for the HTTP request
                (default: ``"application/json"``). Use ``"application/octet-stream"``
                for endpoints that serve raw binary data (e.g., SurfaceGrid/$value).

        Yields:
            Binary data chunks as bytes. Returns immediately if no bulk data (404).

        Raises:
            ValueError: If query has no entity set
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
            >>> bulk_query = query.entity(datasets[0]["native_uid"])
            >>> chunks = list(client.get_bulk_data_stream(bulk_query))
            >>> if chunks:
            ...     binary_data = b"".join(chunks)
        """
        if query._native_uid is None:
            raise ValueError(
                "Query must target an entity. "
                "Call query.entity(native_uid) before passing to get_bulk_data_stream()."
            )

        endpoint = query.build_endpoint()
        logger.info(f"Streaming bulk data from: {endpoint} (chunk_size={chunk_size})")
        yield from self._request_binary_stream(
            endpoint, chunk_size=chunk_size, accept=accept
        )
