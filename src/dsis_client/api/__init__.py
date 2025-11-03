"""
DSIS API Client Module

This module provides a Python SDK for the DSIS APIM (API Management) system.
It handles the dual-token authentication flow required for accessing DSIS data
through the Azure API Management gateway.

The module includes:
- Authentication handling for both Azure AD and DSIS tokens
- API client for making authenticated requests
- Environment configuration for dev, qa, and prod
- Subscription key management
- Custom exceptions for error handling

Usage:
    from dsis_client import DSISClient, DSISConfig, Environment

    config = DSISConfig(
        environment=Environment.DEV,
        tenant_id="your-tenant-id",
        client_id="your-client-id",
        client_secret="your-client-secret",
        access_app_id="your-access-app-id",
        dsis_username="your-dsis-username",
        dsis_password="your-dsis-password",
        subscription_key_dsauth="your-dsauth-key",
        subscription_key_dsdata="your-dsdata-key"
    )

    client = DSISClient(config)
    data = client.get_odata("OW5000", "5000107")
"""

from .auth import DSISAuth
from .client import DSISClient
from .config import DSISConfig, Environment
from .exceptions import (
    DSISAPIError,
    DSISAuthenticationError,
    DSISConfigurationError,
    DSISException,
)
from .models import (
    cast_results,
    get_schema_by_name,
    is_valid_schema,
)
from .query import QueryBuilder

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
    # Model utilities - for advanced users
    "cast_results",
    "get_schema_by_name",
    "is_valid_schema",
]
