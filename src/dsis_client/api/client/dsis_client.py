"""Main DSIS API client.

Provides high-level methods for interacting with DSIS OData API.
"""

from ._bulk_data import BulkDataMixin
from ._pagination import PaginationMixin
from ._query import QueryExecutionMixin
from .base_client import BaseClient


class DSISClient(
    QueryExecutionMixin,
    BulkDataMixin,
    PaginationMixin,
    BaseClient,
):
    """Main client for DSIS API interactions.

    Provides methods for making authenticated requests to the DSIS API.
    Handles authentication, request construction, and response parsing.

    This client composes functionality from multiple mixins:
    - QueryExecutionMixin: execute_query(), cast_results()
    - BulkDataMixin: get_bulk_data(), get_bulk_data_stream()
    - PaginationMixin: _yield_nextlink_pages()
    - BaseClient: __init__(), test_connection(), refresh_authentication(), get()

    Attributes:
        config: DSISConfig instance with API configuration
        auth: DSISAuth instance handling authentication

    Example:
        >>> from dsis_client import DSISClient, DSISConfig, Environment
        >>> config = DSISConfig(
        ...     environment=Environment.DEV,
        ...     tenant_id="...",
        ...     client_id="...",
        ...     client_secret="...",
        ...     access_app_id="...",
        ...     dsis_username="...",
        ...     dsis_password="...",
        ...     subscription_key_dsauth="...",
        ...     subscription_key_dsdata="...",
        ...     model_name="OW5000",
        ... )
        >>> client = DSISClient(config)
        >>> client.test_connection()
        True
    """

    pass  # All functionality comes from mixins + BaseClient
