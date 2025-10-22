"""
Configuration module for DSIS API client.

Handles environment-specific settings and endpoints for the DSIS APIM system.
Provides configuration validation and endpoint management for different environments.
"""

from enum import Enum
from dataclasses import dataclass
from typing import List
from .exceptions import DSISConfigurationError


class Environment(Enum):
    """DSIS API environments."""

    DEV = "dev"
    QA = "qa"
    PROD = "prod"


@dataclass
class DSISConfig:
    """Configuration for DSIS API client.

    Attributes:
        environment: Target environment (DEV, QA, or PROD)
        tenant_id: Azure AD tenant ID
        client_id: Azure AD client/application ID
        client_secret: Azure AD client secret
        access_app_id: Azure AD access application ID for token resource
        dsis_username: DSIS username for authentication
        dsis_password: DSIS password for authentication
        subscription_key_dsauth: APIM subscription key for dsauth endpoint
        subscription_key_dsdata: APIM subscription key for dsdata endpoint
        model_name: DSIS model name (e.g., "OW5000" or "OpenWorksCommonModel")
        model_version: Model version (default: "5000107")
        dsis_site: DSIS site header (default: "qa")
    """

    # Environment settings
    environment: Environment

    # Azure AD settings
    tenant_id: str
    client_id: str
    client_secret: str
    access_app_id: str

    # DSIS credentials
    dsis_username: str
    dsis_password: str

    # Subscription keys (APIM products)
    subscription_key_dsauth: str
    subscription_key_dsdata: str

    # DSIS model configuration
    model_name: str

    # Optional model configuration (with defaults)
    model_version: str = "5000107"

    # DSIS site header (typically "qa" for DEV endpoint)
    dsis_site: str = "qa"

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate that all required configuration values are present and valid."""
        required_fields = {
            "tenant_id": self.tenant_id,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "access_app_id": self.access_app_id,
            "dsis_username": self.dsis_username,
            "dsis_password": self.dsis_password,
            "subscription_key_dsauth": self.subscription_key_dsauth,
            "subscription_key_dsdata": self.subscription_key_dsdata,
            "model_name": self.model_name,
            "model_version": self.model_version,
        }

        for field_name, field_value in required_fields.items():
            if not field_value or not isinstance(field_value, str):
                raise DSISConfigurationError(
                    f"Configuration error: '{field_name}' must be a non-empty string"
                )

        if not isinstance(self.environment, Environment):
            raise DSISConfigurationError(
                "Configuration error: 'environment' must be an Environment enum value"
            )

    # Base URLs for each environment
    _base_urls = {
        Environment.DEV: "https://api-dev.gateway.equinor.com",
        Environment.QA: "https://api-test.gateway.equinor.com",
        Environment.PROD: "https://api.gateway.equinor.com",
    }

    @property
    def base_url(self) -> str:
        """Get the base URL for the current environment."""
        return self._base_urls[self.environment]

    @property
    def token_endpoint(self) -> str:
        """Get the token endpoint URL."""
        return f"{self.base_url}/dsauth/v1/token"

    @property
    def data_endpoint(self) -> str:
        """Get the data endpoint base URL."""
        return f"{self.base_url}/dsdata/v1"

    @property
    def authority(self) -> str:
        """Get the Azure AD authority URL."""
        return f"https://login.microsoftonline.com/{self.tenant_id}"

    @property
    def scope(self) -> List[str]:
        """Get the OAuth2 scope for the access application.

        Returns:
            List containing the OAuth2 scope for token acquisition
        """
        return [f"{self.access_app_id}/.default"]

    @classmethod
    def for_native_model(
        cls,
        environment: Environment,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        access_app_id: str,
        dsis_username: str,
        dsis_password: str,
        subscription_key_dsauth: str,
        subscription_key_dsdata: str,
        model_name: str = "OW5000",
        model_version: str = "5000107",
        dsis_site: str = "qa",
    ) -> "DSISConfig":
        """Create a configuration for accessing native model data.

        Convenience factory method for creating a config with native model settings.

        Args:
            environment: Target environment (DEV, QA, or PROD)
            tenant_id: Azure AD tenant ID
            client_id: Azure AD client/application ID
            client_secret: Azure AD client secret
            access_app_id: Azure AD access application ID for token resource
            dsis_username: DSIS username for authentication
            dsis_password: DSIS password for authentication
            subscription_key_dsauth: APIM subscription key for dsauth endpoint
            subscription_key_dsdata: APIM subscription key for dsdata endpoint
            model_name: Native model name (default: "OW5000")
            model_version: Model version (default: "5000107")
            dsis_site: DSIS site header (default: "qa")

        Returns:
            DSISConfig instance configured for native model access

        Example:
            >>> config = DSISConfig.for_native_model(
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
        """
        return cls(
            environment=environment,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            access_app_id=access_app_id,
            dsis_username=dsis_username,
            dsis_password=dsis_password,
            subscription_key_dsauth=subscription_key_dsauth,
            subscription_key_dsdata=subscription_key_dsdata,
            model_name=model_name,
            model_version=model_version,
            dsis_site=dsis_site,
        )

    @classmethod
    def for_common_model(
        cls,
        environment: Environment,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        access_app_id: str,
        dsis_username: str,
        dsis_password: str,
        subscription_key_dsauth: str,
        subscription_key_dsdata: str,
        model_name: str = "OpenWorksCommonModel",
        model_version: str = "5000107",
        dsis_site: str = "qa",
    ) -> "DSISConfig":
        """Create a configuration for accessing common model data.

        Convenience factory method for creating a config with common model settings.

        Args:
            environment: Target environment (DEV, QA, or PROD)
            tenant_id: Azure AD tenant ID
            client_id: Azure AD client/application ID
            client_secret: Azure AD client secret
            access_app_id: Azure AD access application ID for token resource
            dsis_username: DSIS username for authentication
            dsis_password: DSIS password for authentication
            subscription_key_dsauth: APIM subscription key for dsauth endpoint
            subscription_key_dsdata: APIM subscription key for dsdata endpoint
            model_name: Common model name (default: "OpenWorksCommonModel")
            model_version: Model version (default: "5000107")
            dsis_site: DSIS site header (default: "qa")

        Returns:
            DSISConfig instance configured for common model access

        Example:
            >>> config = DSISConfig.for_common_model(
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
        """
        return cls(
            environment=environment,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            access_app_id=access_app_id,
            dsis_username=dsis_username,
            dsis_password=dsis_password,
            subscription_key_dsauth=subscription_key_dsauth,
            subscription_key_dsdata=subscription_key_dsdata,
            model_name=model_name,
            model_version=model_version,
            dsis_site=dsis_site,
        )
