"""Main DSIS API client.

Provides high-level methods for interacting with DSIS OData API.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Union, Optional, Type

from ..models import cast_results
from .base_client import BaseClient

if TYPE_CHECKING:
    from ..query import QueryBuilder

logger = logging.getLogger(__name__)


class DSISClient(BaseClient):
    """Main client for DSIS API interactions.

    Provides methods for making authenticated requests to the DSIS API.
    Handles authentication, request construction, and response parsing.

    Attributes:
        config: DSISConfig instance with API configuration
        auth: DSISAuth instance handling authentication
    """

    def execute_query(
        self, query: "QueryBuilder", cast: bool = False, max_pages: int = -1
    ):
        """Execute a DSIS query.

        Args:
            query: QueryBuilder instance containing the query and path parameters
            cast: If True and query has a schema class, automatically cast results
                to model instances
            max_pages: Maximum number of pages to fetch. -1 (default) fetches all pages.
                Use 1 for a single page, 2 for two pages, etc.

        Yields:
            Items from the result pages (or model instances if cast=True)

        Raises:
            DSISAPIError: If the API request fails
            ValueError: If query is invalid or cast=True but query has no schema class

        Example:
            >>> # Fetch all pages (default)
            >>> for item in client.execute_query(query):
            ...     process(item)
            >>>
            >>> # Aggregate all pages into a list
            >>> all_items = list(client.execute_query(query))
            >>>
            >>> # Fetch only one page
            >>> page_items = list(client.execute_query(query, max_pages=1))
            >>>
            >>> # Fetch two pages
            >>> two_pages = list(client.execute_query(query, max_pages=2))
        """
        # Import here to avoid circular imports
        from ..query import QueryBuilder

        if not isinstance(query, QueryBuilder):
            raise TypeError(f"Expected QueryBuilder, got {type(query)}")

        logger.debug(f"Executing query: {query} (max_pages={max_pages})")

        # Build endpoint path segments
        segments = [self.config.model_name, self.config.model_version]
        if query.district_id is not None:
            segments.append(str(query.district_id))
        if query.field is not None:
            segments.append(query.field)

        # Get schema name from query
        query_string = query.get_query_string()
        schema_name = query_string.split("?")[0]
        segments.append(schema_name)

        endpoint = "/".join(segments)

        # Get parsed parameters from the query
        params = query.build_query_params()

        logger.debug(f"Making request to endpoint: {endpoint} with params: {params}")
        response = self._request(endpoint, params)

        # Yield items from all pages (up to max_pages)
        if cast:
            if not query._schema_class:
                raise ValueError(
                    "Cannot cast results: query has no schema class. "
                    "Use .schema(ModelClass) when building the query."
                )
            for item in self._yield_nextlink_pages(response, endpoint, max_pages):
                yield query._schema_class(**item)
        else:
            for item in self._yield_nextlink_pages(response, endpoint, max_pages):
                yield item

    def cast_results(self, results: List[Dict[str, Any]], schema_class) -> List[Any]:
        """Cast API response items to model instances.

        Args:
            results: List of dictionaries from API response
                (typically response["value"])
            schema_class: Pydantic model class to cast to (e.g., Fault, Well)

        Returns:
            List of model instances

        Raises:
            ValidationError: If any result doesn't match the schema

        Example:
            >>> from dsis_model_sdk.models.common import Fault
            >>> query = QueryBuilder(district_id="123", field="SNORRE").schema(Fault)
            >>> response = client.executeQuery(query)
            >>> faults = client.cast_results(response["value"], Fault)
        """
        return cast_results(results, schema_class)

    def get_bulk_data(
        self,
        schema: Union[str, Type],
        native_uid: Union[str, Dict[str, Any], Any],
        district_id: str = None,
        field: str = None,
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
            district_id: Optional district ID (if required by API). Ignored if query is provided.
            field: Optional field name (if required by API). Ignored if query is provided.
            data_field: Name of the binary data field (default: "data")
            query: Optional QueryBuilder instance to extract district_id and field from.
                   If provided, district_id and field parameters are ignored.

        Returns:
            Binary protobuf data as bytes, or None if the entity has no bulk data

        Raises:
            ValueError: If native_uid is an entity object without a 'native_uid' attribute
            DSISAPIError: If the API request fails (other than 404 for missing data)

        Example:
            >>> from dsis_model_sdk.models.common import LogCurve
            >>> # Option 1: Pass native_uid string directly
            >>> binary_data = client.get_bulk_data(
            ...     schema=LogCurve,
            ...     native_uid="46075",
            ...     district_id="123",
            ...     field="SNORRE"
            ... )
            >>>
            >>> # Option 2: Pass entity object directly (extracts native_uid automatically)
            >>> query = QueryBuilder(district_id="123", field="SNORRE").schema(LogCurve)
            >>> curves = list(client.execute_query(query, cast=True, max_pages=1))
            >>> binary_data = client.get_bulk_data(
            ...     schema=LogCurve,
            ...     native_uid=curves[0],  # Pass entity directly!
            ...     query=query  # Extracts district_id and field
            ... )
            >>>
            >>> # Step 3: Check if data exists and decode
            >>> if binary_data:
            ...     from dsis_model_sdk.protobuf import decode_log_curves
            ...     decoded = decode_log_curves(binary_data)
            ... else:
            ...     print("No bulk data available for this entity")
        """
        # Extract native_uid from entity if needed
        if isinstance(native_uid, str):
            uid = native_uid
        elif isinstance(native_uid, dict):
            uid = native_uid.get("native_uid")
            if not uid:
                raise ValueError(
                    "Entity dict must have a 'native_uid' key to fetch binary data"
                )
        else:
            uid = getattr(native_uid, "native_uid", None)
            if not uid:
                raise ValueError(
                    "Entity must have a 'native_uid' attribute to fetch binary data"
                )

        # Extract district_id and field from query if provided
        if query is not None:
            district_id = query.district_id
            field = query.field

        # Extract schema name if class is provided
        schema_name = schema.__name__ if isinstance(schema, type) else schema

        # Build endpoint path segments
        segments = [self.config.model_name, self.config.model_version]
        if district_id is not None:
            segments.append(str(district_id))
        if field is not None:
            segments.append(field)

        # Add the OData entity key and data field path
        # Note: No /$value suffix - the API endpoint is just /{schema}('{native_uid}')/data
        segments.append(f"{schema_name}('{uid}')/{data_field}")

        endpoint = "/".join(segments)

        logger.debug(f"Fetching bulk data from: {endpoint}")
        return self._request_binary(endpoint)

    def get_bulk_data_stream(
        self,
        schema: Union[str, Type],
        native_uid: Union[str, Dict[str, Any], Any],
        district_id: str = None,
        field: str = None,
        data_field: str = "data",
        chunk_size: int = 10 * 1024 * 1024,
        query: Optional["QueryBuilder"] = None,
    ):
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
            district_id: Optional district ID (if required by API). Ignored if query is provided.
            field: Optional field name (if required by API). Ignored if query is provided.
            data_field: Name of the binary data field (default: "data")
            chunk_size: Size of chunks to yield in bytes (default: 10MB, recommended by DSIS)
            query: Optional QueryBuilder instance to extract district_id and field from.
                   If provided, district_id and field parameters are ignored.

        Yields:
            Binary data chunks as bytes. Returns immediately if no bulk data available (404).

        Raises:
            ValueError: If native_uid is an entity object without a 'native_uid' attribute
            DSISAPIError: If the API request fails (other than 404 for missing data)

        Example:
            >>> from dsis_model_sdk.models.common import SeismicDataSet3D
            >>> # Option 1: Pass native_uid string directly
            >>> for chunk in client.get_bulk_data_stream(
            ...     schema=SeismicDataSet3D,
            ...     native_uid="12345",
            ...     district_id="123",
            ...     field="SNORRE",
            ...     chunk_size=10*1024*1024
            ... ):
            ...     print(f"Received {len(chunk)} bytes")
            >>>
            >>> # Option 2: Pass entity object directly (extracts native_uid automatically)
            >>> query = QueryBuilder(district_id="123", field="SNORRE").schema(SeismicDataSet3D)
            >>> datasets = list(client.execute_query(query, cast=True, max_pages=1))
            >>> chunks = []
            >>> for chunk in client.get_bulk_data_stream(
            ...     schema=SeismicDataSet3D,
            ...     native_uid=datasets[0],  # Pass entity directly!
            ...     query=query  # Extracts district_id and field
            ... ):
            ...     chunks.append(chunk)
            >>>
            >>> # Combine chunks and decode
            >>> if chunks:
            ...     binary_data = b''.join(chunks)
            ...     from dsis_model_sdk.protobuf import decode_seismic_float_data
            ...     decoded = decode_seismic_float_data(binary_data)
        """
        # Extract native_uid from entity if needed
        if isinstance(native_uid, str):
            uid = native_uid
        elif isinstance(native_uid, dict):
            uid = native_uid.get("native_uid")
            if not uid:
                raise ValueError(
                    "Entity dict must have a 'native_uid' key to fetch binary data"
                )
        else:
            uid = getattr(native_uid, "native_uid", None)
            if not uid:
                raise ValueError(
                    "Entity must have a 'native_uid' attribute to fetch binary data"
                )

        # Extract district_id and field from query if provided
        if query is not None:
            district_id = query.district_id
            field = query.field

        # Extract schema name if class is provided
        schema_name = schema.__name__ if isinstance(schema, type) else schema

        # Build endpoint path segments
        segments = [self.config.model_name, self.config.model_version]
        if district_id is not None:
            segments.append(str(district_id))
        if field is not None:
            segments.append(field)

        # Add the OData entity key and data field path
        segments.append(f"{schema_name}('{uid}')/{data_field}")

        endpoint = "/".join(segments)

        logger.debug(f"Streaming bulk data from: {endpoint} (chunk_size={chunk_size})")
        yield from self._request_binary_stream(endpoint, chunk_size=chunk_size)

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

            logger.debug(f"Following nextLink: {next_link}")

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

    def get_entity_data(
        self,
        entity: Union[Dict[str, Any], Any],
        schema: Union[str, Type],
        query: Optional["QueryBuilder"] = None,
        district_id: Optional[str] = None,
        field: Optional[str] = None,
        data_field: str = "data",
    ) -> Optional[bytes]:
        """Fetch binary bulk data for an entity result.

        .. deprecated:: 0.5.0
            Use :meth:`get_bulk_data` instead, which now accepts entity objects directly.
            This method will be removed in version 1.0.0.

        This is a convenience method that extracts the native_uid from an entity
        (either a dict or model instance) and fetches its binary data.

        Args:
            entity: Entity dict or model instance (must have 'native_uid' attribute/key)
            schema: Schema name string (e.g., "HorizonData3D") or model class
                (e.g., HorizonData3D from dsis_model_sdk.models.common)
            query: Optional QueryBuilder instance to extract district_id and field from.
                   If provided, district_id and field parameters are ignored.
            district_id: Optional district ID (if required by API). Ignored if query is provided.
            field: Optional field name (if required by API). Ignored if query is provided.
            data_field: Name of the binary data field (default: "data")

        Returns:
            Binary protobuf data as bytes, or None if the entity has no bulk data

        Raises:
            ValueError: If entity doesn't have a native_uid
            DSISAPIError: If the API request fails (other than 404 for missing data)

        Example:
            >>> from dsis_model_sdk.models.common import LogCurve
            >>> # Option 1: Pass the query object (recommended - no need to repeat district_id/field)
            >>> query = QueryBuilder(district_id="123", field="SNORRE").schema(LogCurve).select("native_uid,log_curve_name")
            >>> log_curves = list(client.execute_query(query, max_pages=1))
            >>> log_curve = log_curves[0]
            >>> binary_data = client.get_entity_data(log_curve, schema=LogCurve, query=query)  # Type-safe!
            >>>
            >>> # Option 2: Pass district_id and field explicitly
            >>> binary_data = client.get_entity_data(log_curve, schema=LogCurve, district_id="123", field="SNORRE")
            >>>
            >>> # Check if data exists and decode
            >>> if binary_data:
            ...     from dsis_model_sdk.protobuf import decode_log_curves
            ...     decoded = decode_log_curves(binary_data)
            ... else:
            ...     print("No bulk data available for this entity")
        """
        import warnings
        warnings.warn(
            "get_entity_data() is deprecated and will be removed in version 1.0.0. "
            "Use get_bulk_data() instead, which now accepts entity objects directly.",
            DeprecationWarning,
            stacklevel=2
        )

        # Simply delegate to get_bulk_data which now handles entities
        return self.get_bulk_data(
            schema=schema,
            native_uid=entity,
            district_id=district_id,
            field=field,
            data_field=data_field,
            query=query,
        )

    def get_entity_data_stream(
        self,
        entity: Union[Dict[str, Any], Any],
        schema: Union[str, Type],
        query: Optional["QueryBuilder"] = None,
        district_id: Optional[str] = None,
        field: Optional[str] = None,
        data_field: str = "data",
        chunk_size: int = 10 * 1024 * 1024,
    ):
        """Stream binary bulk data for an entity in chunks for memory-efficient processing.

        .. deprecated:: 0.5.0
            Use :meth:`get_bulk_data_stream` instead, which now accepts entity objects directly.
            This method will be removed in version 1.0.0.

        This is a convenience method that extracts the native_uid from an entity
        (either a dict or model instance) and streams its binary data in chunks.

        Args:
            entity: Entity dict or model instance (must have 'native_uid' attribute/key)
            schema: Schema name string (e.g., "HorizonData3D") or model class
                (e.g., HorizonData3D from dsis_model_sdk.models.common)
            query: Optional QueryBuilder instance to extract district_id and field from.
                   If provided, district_id and field parameters are ignored.
            district_id: Optional district ID (if required by API). Ignored if query is provided.
            field: Optional field name (if required by API). Ignored if query is provided.
            data_field: Name of the binary data field (default: "data")
            chunk_size: Size of chunks to yield in bytes (default: 10MB, recommended by DSIS)

        Yields:
            Binary data chunks as bytes. Returns immediately if no bulk data available (404).

        Raises:
            ValueError: If entity doesn't have a native_uid
            DSISAPIError: If the API request fails (other than 404 for missing data)

        Example:
            >>> from dsis_model_sdk.models.common import SeismicDataSet3D
            >>> # Simple usage with query context and schema class
            >>> query = QueryBuilder(district_id="123", field="SNORRE").schema(SeismicDataSet3D)
            >>> seismic_datasets = list(client.execute_query(query, cast=True, max_pages=1))
            >>> seismic = seismic_datasets[0]
            >>>
            >>> # Stream large dataset in chunks to avoid memory issues
            >>> chunks = []
            >>> for chunk in client.get_entity_data_stream(
            ...     entity=seismic,
            ...     schema=SeismicDataSet3D,  # Type-safe!
            ...     query=query,
            ...     chunk_size=10*1024*1024  # 10MB chunks (DSIS recommended)
            ... ):
            ...     chunks.append(chunk)
            ...     print(f"Downloaded {len(chunk):,} bytes")
            >>>
            >>> # Decode when complete
            >>> if chunks:
            ...     binary_data = b''.join(chunks)
            ...     from dsis_model_sdk.protobuf import decode_seismic_float_data
            ...     decoded = decode_seismic_float_data(binary_data)
        """
        import warnings
        warnings.warn(
            "get_entity_data_stream() is deprecated and will be removed in version 1.0.0. "
            "Use get_bulk_data_stream() instead, which now accepts entity objects directly.",
            DeprecationWarning,
            stacklevel=2
        )

        # Simply delegate to get_bulk_data_stream which now handles entities
        yield from self.get_bulk_data_stream(
            schema=schema,
            native_uid=entity,
            district_id=district_id,
            field=field,
            data_field=data_field,
            chunk_size=chunk_size,
            query=query,
        )
