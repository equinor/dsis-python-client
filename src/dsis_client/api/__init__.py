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

Usage:
    from dsis_client import DSISClient, DSISConfig, Environment

    config = DSISConfig(
        environment=Environment.PROD,
        tenant_id="your-tenant-id",
        client_id="your-client-id",
        client_secret="your-client-secret",
        access_app_id="your-access-app-id",
        dsis_username="your-dsis-username",
        dsis_password="your-dsis-password",
        subscription_key="your-subscription-key"
    )

    client = DSISClient(config)
    data = client.get_odata("OW5000", "5000107")
"""

from .client import DSISClient
from .auth import DSISAuth
from .config import DSISConfig, Environment

__all__ = ["DSISClient", "DSISAuth", "DSISConfig", "Environment"]
