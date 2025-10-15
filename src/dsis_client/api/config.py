"""
Configuration module for DSIS API client.

Handles environment-specific settings and endpoints for the DSIS APIM system.
"""

from enum import Enum
from dataclasses import dataclass


class Environment(Enum):
    """DSIS API environments."""
    DEV = "dev"
    QA = "qa"
    PROD = "prod"


@dataclass
class DSISConfig:
    """Configuration for DSIS API client."""
    
    # Environment settings
    environment: Environment
    
    # Azure AD settings
    tenant_id: str
    client_id: str
    client_secret: str
    access_app_id: str  # Access application ID for the target environment
    
    # DSIS credentials
    dsis_username: str
    dsis_password: str
    
    # Subscription keys (APIM products)
    # - subscription_key_dsauth: used when calling the dsauth token endpoint
    # - subscription_key_dsdata: used when calling dsdata endpoints
    subscription_key_dsauth: str
    subscription_key_dsdata: str
    
    # Base URLs for each environment
    _base_urls = {
        Environment.DEV: "https://api-dev.gateway.equinor.com",
        Environment.QA: "https://api-test.gateway.equinor.com", 
        Environment.PROD: "https://api.gateway.equinor.com"
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
    def scope(self) -> list:
        """Get the OAuth2 scope for the access application."""
        return [f"{self.access_app_id}/.default"]