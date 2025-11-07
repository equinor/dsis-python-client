"""
DSIS Python Client

A Python SDK for the DSIS (Drilling & Well Services Information System) API
Management system. Provides easy access to DSIS data through Equinor's Azure
API Management gateway.

This package includes:
- DSISClient: Main client for API interactions
- DSISConfig: Configuration management
- Environment: Environment enumeration (DEV, QA, PROD)
- Custom exceptions for error handling

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
    ...     subscription_key_dsdata="..."
    ... )
    >>> client = DSISClient(config)
        >>> data = client.get("OW5000", "5000107")
"""

from .api import (
    DSISAPIError,
    DSISAuth,
    DSISAuthenticationError,
    DSISClient,
    DSISConfig,
    DSISConfigurationError,
    DSISException,
    Environment,
    QueryBuilder,
)

__all__ = [
    "DSISClient",
    "DSISAuth",
    "DSISConfig",
    "Environment",
    "DSISException",
    "DSISAuthenticationError",
    "DSISAPIError",
    "DSISConfigurationError",
    "QueryBuilder",
]
