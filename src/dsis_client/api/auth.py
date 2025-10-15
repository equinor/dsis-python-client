"""
Authentication module for DSIS API client.

Handles the dual-token authentication flow required by DSIS APIM:
1. Azure AD token for API gateway access
2. DSIS token for backend system access
"""

import msal
import requests
from typing import Optional, Dict
from .config import DSISConfig


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
        
        Returns:
            Azure AD access token
            
        Raises:
            Exception: If token acquisition fails
        """
        app = msal.ConfidentialClientApplication(
            self.config.client_id,
            authority=self.config.authority,
            client_credential=self.config.client_secret
        )
        
        result = app.acquire_token_for_client(scopes=self.config.scope)
        
        if 'access_token' not in result:
            error_desc = result.get('error_description', 'Unknown error')
            raise Exception(f"Failed to acquire Azure AD token: {error_desc}")
        
        self._aad_token = result['access_token']
        return self._aad_token
    
    def get_dsis_token(self, aad_token: Optional[str] = None) -> str:
        """Get DSIS token using the acquired Azure AD token.
        
        Args:
            aad_token: Azure AD token (if None, will get a new one)
            
        Returns:
            DSIS access token
            
        Raises:
            Exception: If token acquisition fails
        """
        if aad_token is None:
            aad_token = self.get_aad_token()
        
        body = {
            'grant_type': 'password',
            'client_id': 'dsis-data',
            'username': self.config.dsis_username,
            'password': self.config.dsis_password
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Bearer {aad_token}',
            'dsis-site': self.config.environment.value,
            'Ocp-Apim-Subscription-Key': self.config.subscription_key
        }
        
        response = self._session.post(
            self.config.token_endpoint,
            headers=headers,
            data=body
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to acquire DSIS token: {response.status_code} - {response.text}")
        
        token_data = response.json()
        if 'access_token' not in token_data:
            raise Exception("DSIS token not found in response")
        
        self._dsis_token = token_data['access_token']
        return self._dsis_token
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authenticated headers for API requests.
        
        Returns:
            Dictionary containing all required headers for DSIS API requests
        """
        if not self._aad_token:
            self.get_aad_token()
        
        if not self._dsis_token:
            self.get_dsis_token(self._aad_token)
        
        return {
            'Authorization': f'Bearer {self._aad_token}',
            'Ocp-Apim-Subscription-Key': self.config.subscription_key,
            'dsis-site': self.config.environment.value,
            'dsis-token': self._dsis_token
        }
    
    def refresh_tokens(self) -> None:
        """Refresh both Azure AD and DSIS tokens."""
        self._aad_token = None
        self._dsis_token = None
        self.get_aad_token()
        self.get_dsis_token(self._aad_token)