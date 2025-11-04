"""
Authentication module for DSIS API client.

Handles the dual-token authentication flow required by DSIS APIM:
1. Azure AD token for API gateway access
2. DSIS token for backend system access

This module manages token acquisition, caching, and refresh for both
Azure AD and DSIS authentication mechanisms.
"""

import logging
from typing import Dict, Optional

import msal
import requests

from ..config import DSISConfig
from ..exceptions import DSISAuthenticationError

logger = logging.getLogger(__name__)


class DSISAuth:
    """Handles authentication for DSIS API."""

    def __init__(self, config: DSISConfig):
        """Initialize the authentication handler.

        Args:
            config: DSIS configuration object
        """
        self.config = config
        self._aad_token: Optional[str] = None
        self._dsis_token: Optional[str] = None
        self._session = requests.Session()

    def get_aad_token(self) -> str:
        """Get Azure AD token using client credentials flow.

        Acquires an Azure AD token using the configured client credentials.
        The token is cached for subsequent requests.

        Returns:
            Azure AD access token string

        Raises:
            DSISAuthenticationError: If token acquisition fails
        """
        logger.debug("Acquiring Azure AD token")

        app = msal.ConfidentialClientApplication(
            self.config.client_id,
            authority=self.config.authority,
            client_credential=self.config.client_secret,
        )

        result = app.acquire_token_for_client(scopes=self.config.scope)

        if "access_token" not in result:
            error_desc = result.get("error_description", "Unknown error")
            logger.error(f"Azure AD token acquisition failed: {error_desc}")
            raise DSISAuthenticationError(
                f"Failed to acquire Azure AD token: {error_desc}"
            )

        self._aad_token = result["access_token"]
        logger.debug("Azure AD token acquired successfully")
        return self._aad_token

    def get_dsis_token(self, aad_token: Optional[str] = None) -> str:
        """Get DSIS token using the acquired Azure AD token.

        Acquires a DSIS token using the password grant flow with the provided
        Azure AD token. The token is cached for subsequent requests.

        Args:
            aad_token: Azure AD token (if None, will acquire a new one)

        Returns:
            DSIS access token string

        Raises:
            DSISAuthenticationError: If token acquisition fails
        """
        logger.debug("Acquiring DSIS token")

        if aad_token is None:
            aad_token = self.get_aad_token()

        body = {
            "grant_type": "password",
            "client_id": "dsis-data",
            "username": self.config.dsis_username,
            "password": self.config.dsis_password,
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Bearer {aad_token}",
            "dsis-site": self.config.dsis_site,
            "Ocp-Apim-Subscription-Key": self.config.subscription_key_dsauth,
        }

        response = self._session.post(
            self.config.token_endpoint, headers=headers, data=body
        )

        if response.status_code != 200:
            error_msg = (
                f"Failed to acquire DSIS token: {response.status_code} - "
                f"{response.reason} - {response.text}"
            )
            logger.error(error_msg)
            raise DSISAuthenticationError(error_msg)

        token_data = response.json()
        if "access_token" not in token_data:
            logger.error("DSIS token not found in response")
            raise DSISAuthenticationError("DSIS token not found in response")

        self._dsis_token = token_data["access_token"]
        logger.debug("DSIS token acquired successfully")
        return self._dsis_token

    def get_auth_headers(self) -> Dict[str, str]:
        """Get authenticated headers for API requests.

        Ensures both Azure AD and DSIS tokens are available, acquiring them
        if necessary. Returns a dictionary with all required headers for
        authenticated DSIS API requests.

        Returns:
            Dictionary containing all required headers for DSIS API requests:
            - Authorization: Bearer token with Azure AD token
            - Ocp-Apim-Subscription-Key: APIM subscription key
            - dsis-site: DSIS site identifier
            - dsis-token: DSIS access token
        """
        if not self._aad_token:
            self.get_aad_token()

        if not self._dsis_token:
            self.get_dsis_token(self._aad_token)

        return {
            "Authorization": f"Bearer {self._aad_token}",
            "Ocp-Apim-Subscription-Key": self.config.subscription_key_dsdata,
            "dsis-site": self.config.dsis_site,
            "dsis-token": self._dsis_token,
        }

    def refresh_tokens(self) -> None:
        """Refresh both Azure AD and DSIS tokens.

        Clears cached tokens and acquires new ones. Useful when tokens
        have expired or when you need to ensure fresh authentication.

        Raises:
            DSISAuthenticationError: If token acquisition fails
        """
        logger.debug("Refreshing authentication tokens")
        self._aad_token = None
        self._dsis_token = None
        self.get_aad_token()
        self.get_dsis_token(self._aad_token)
        logger.debug("Authentication tokens refreshed successfully")
