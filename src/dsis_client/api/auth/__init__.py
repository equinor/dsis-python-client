"""
Authentication module for DSIS API client.

Handles dual-token authentication flow for DSIS APIM.
"""

from .auth import DSISAuth

__all__ = ["DSISAuth"]
