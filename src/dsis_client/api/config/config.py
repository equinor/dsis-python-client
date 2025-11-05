"""
Configuration dataclass for DSIS API client.

Handles configuration validation and endpoint management for different environments.
"""

from dataclasses import dataclass
from typing import List

from ..exceptions import DSISConfigurationError
from .environment import BASE_URLS, Environment


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

    @property
    def base_url(self) -> str:
        """Get the base URL for the current environment."""
        return BASE_URLS[self.environment]

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
