"""Bulk data operations for DSIS API.

Provides mixin class for fetching binary protobuf data.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Generator, Optional, Type, Union

from ._base import _BinaryRequestBase

if TYPE_CHECKING:
    from ..query import QueryBuilder

logger = logging.getLogger(__name__)


class BulkDataMixin(_BinaryRequestBase):
    """Bulk data mixin for binary protobuf operations.

    Provides methods for fetching and streaming binary bulk data.
    Requires subclasses to provide: config, _request_binary, _request_binary_stream.
    """

    def _extract_native_uid(self, native_uid: Union[str, Dict[str, Any], Any]) -> str:
        """Extract native_uid from string, dict, or model instance.

        Args:
            native_uid: Either:
                - A native_uid string (e.g., "46075")
                - An entity dict with 'native_uid' key
                - An entity model instance with 'native_uid' attribute

        Returns:
            The extracted native_uid string

        Raises:
            ValueError: If native_uid cannot be extracted
        """
        if isinstance(native_uid, str):
            return native_uid
        elif isinstance(native_uid, dict):
            uid = native_uid.get("native_uid")
            if not uid:
                raise ValueError(
                    f"Entity dict must have a 'native_uid' key to fetch binary data. "
                    f"Got keys: {list(native_uid.keys())}"
                )
            return uid
        else:
            uid = getattr(native_uid, "native_uid", None)
            if not uid:
                raise ValueError(
                    f"Entity must have a 'native_uid' attribute to fetch binary data. "
                    f"Got type: {type(native_uid).__name__}"
                )
            return uid

    def _build_bulk_data_endpoint(
        self,
        schema: Union[str, Type],
        native_uid: str,
        district_id: Optional[str],
        project: Optional[str],
        data_field: str,
        query: Optional["QueryBuilder"],
    ) -> str:
        """Build endpoint path for bulk data requests.

        Args:
            schema: Schema name string or model class
            native_uid: The native_uid string
            district_id: Optional district ID (ignored if query provided)
            project: Optional project name (ignored if query provided)
            data_field: Name of the binary data field
            query: Optional QueryBuilder to extract district_id and project from

        Returns:
            The constructed endpoint path
        """
        # Extract district_id and project from query if provided
        if query is not None:
            district_id = (
                str(query.district_id) if query.district_id is not None else None
            )
            project = query.project

        # Extract schema name if class is provided
        schema_name = schema.__name__ if isinstance(schema, type) else schema

        # Build endpoint path segments
        segments = [self.config.model_name, self.config.model_version]
        if district_id is not None:
            segments.append(str(district_id))
        if project is not None:
            segments.append(project)

        # Add the OData entity key and data field path
        segments.append(f"{schema_name}('{native_uid}')/{data_field}")

        return "/".join(segments)

    def get_bulk_data(
        self,
        schema: Union[str, Type],
        native_uid: Union[str, Dict[str, Any], Any],
        district_id: Optional[str] = None,
        project: Optional[str] = None,
        data_field: str = "data",
        query: Optional["QueryBuilder"] = None,
    ) -> Optional[bytes]:
        """Fetch binary bulk data (protobuf) for a specific entity.

        The DSIS API serves large binary data fields (horizon z-values, log curves,
        seismic amplitudes) as Protocol Buffers via a special OData endpoint:
        /{schema}('{native_uid}')/{data_field}

        Note: The endpoint does NOT include /$value suffix, and the API returns
        binary data with Accept: application/json header.

        Args:
            schema: Schema name string (e.g., "HorizonData3D") or model class
                (e.g., HorizonData3D from dsis_model_sdk.models.common)
            native_uid: Either:
                - A native_uid string (e.g., "46075")
                - An entity dict with 'native_uid' key
                - An entity model instance with 'native_uid' attribute
            district_id: Optional district ID (if required by API).
                Ignored if query is provided.
            project: Optional project name (if required by API).
                Ignored if query is provided.
            data_field: Name of the binary data field (default: "data")
            query: Optional QueryBuilder instance to extract district_id and project from.
                   If provided, district_id and project parameters are ignored.

        Returns:
            Binary protobuf data as bytes, or None if the entity has no bulk data

        Raises:
            ValueError: If native_uid is an entity object without 'native_uid' attribute
            DSISAPIError: If the API request fails (other than 404 for missing data)

        Example:
            >>> from dsis_model_sdk.models.common import LogCurve
            >>> # Option 1: Pass native_uid string directly
            >>> binary_data = client.get_bulk_data(
            ...     schema=LogCurve,
            ...     native_uid="46075",
            ...     district_id="123",
            ...     project="SNORRE"
            ... )
            >>>
            >>> # Option 2: Pass entity object directly (extracts native_uid automatically)
            >>> query = QueryBuilder(district_id="123", project="SNORRE").schema(LogCurve)
            >>> curves = list(client.execute_query(query, cast=True, max_pages=1))
            >>> binary_data = client.get_bulk_data(
            ...     schema=LogCurve,
            ...     native_uid=curves[0],  # Pass entity directly!
            ...     query=query  # Extracts district_id and project
            ... )
            >>>
            >>> # Step 3: Check if data exists and decode
            >>> if binary_data:
            ...     from dsis_model_sdk.protobuf import decode_log_curves
            ...     decoded = decode_log_curves(binary_data)
            ... else:
            ...     print("No bulk data available for this entity")
        """
        # Extract native_uid using helper method
        uid = self._extract_native_uid(native_uid)

        # Build endpoint using helper method
        endpoint = self._build_bulk_data_endpoint(
            schema=schema,
            native_uid=uid,
            district_id=district_id,
            project=project,
            data_field=data_field,
            query=query,
        )

        logger.info(f"Fetching bulk data from: {endpoint}")
        return self._request_binary(endpoint)

    def get_bulk_data_stream(
        self,
        schema: Union[str, Type],
        native_uid: Union[str, Dict[str, Any], Any],
        district_id: Optional[str] = None,
        project: Optional[str] = None,
        data_field: str = "data",
        chunk_size: int = 10 * 1024 * 1024,
        query: Optional["QueryBuilder"] = None,
    ) -> Generator[bytes, None, None]:
        """Stream binary bulk data (protobuf) in chunks for memory-efficient processing.

        The DSIS API serves large binary data fields (horizon z-values, log curves,
        seismic amplitudes) as Protocol Buffers via a special OData endpoint:
        /{schema}('{native_uid}')/{data_field}

        This streaming version yields data in chunks rather than loading everything
        into memory at once. Useful for very large datasets (e.g., seismic volumes).

        Note: The endpoint does NOT include /$value suffix, and the API returns
        binary data with Accept: application/json header.

        Args:
            schema: Schema name string (e.g., "HorizonData3D") or model class
                (e.g., HorizonData3D from dsis_model_sdk.models.common)
            native_uid: Either:
                - A native_uid string (e.g., "46075")
                - An entity dict with 'native_uid' key
                - An entity model instance with 'native_uid' attribute
            district_id: Optional district ID (if required by API).
                Ignored if query is provided.
            project: Optional project name (if required by API).
                Ignored if query is provided.
            data_field: Name of the binary data field (default: "data")
            chunk_size: Size of chunks to yield in bytes
                (default: 10MB, recommended by DSIS)
            query: Optional QueryBuilder instance to extract district_id and project from.
                   If provided, district_id and project parameters are ignored.

        Yields:
            Binary data chunks as bytes. Returns immediately if no bulk data (404).

        Raises:
            ValueError: If native_uid is an entity object without 'native_uid' attribute
            DSISAPIError: If the API request fails (other than 404 for missing data)

        Example:
            >>> from dsis_model_sdk.models.common import SeismicDataSet3D
            >>> # Option 1: Pass native_uid string directly
            >>> for chunk in client.get_bulk_data_stream(
            ...     schema=SeismicDataSet3D,
            ...     native_uid="12345",
            ...     district_id="123",
            ...     project="SNORRE",
            ...     chunk_size=10*1024*1024
            ... ):
            ...     print(f"Received {len(chunk)} bytes")
            >>>
            >>> # Option 2: Pass entity object directly (extracts native_uid automatically)
            >>> query = QueryBuilder(district_id="123", project="SNORRE").schema(SeismicDataSet3D)
            >>> datasets = list(client.execute_query(query, cast=True, max_pages=1))
            >>> chunks = []
            >>> for chunk in client.get_bulk_data_stream(
            ...     schema=SeismicDataSet3D,
            ...     native_uid=datasets[0],  # Pass entity directly!
            ...     query=query  # Extracts district_id and project
            ... ):
            ...     chunks.append(chunk)
            >>>
            >>> # Combine chunks and decode
            >>> if chunks:
            ...     binary_data = b''.join(chunks)
            ...     from dsis_model_sdk.protobuf import decode_seismic_float_data
            ...     decoded = decode_seismic_float_data(binary_data)
        """
        # Extract native_uid using helper method
        uid = self._extract_native_uid(native_uid)

        # Build endpoint using helper method
        endpoint = self._build_bulk_data_endpoint(
            schema=schema,
            native_uid=uid,
            district_id=district_id,
            project=project,
            data_field=data_field,
            query=query,
        )

        logger.info(f"Streaming bulk data from: {endpoint} (chunk_size={chunk_size})")
        yield from self._request_binary_stream(endpoint, chunk_size=chunk_size)
