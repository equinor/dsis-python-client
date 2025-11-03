"""
Custom exceptions for DSIS API client.

Provides specific exception types for different error scenarios
in the DSIS authentication and API interaction flow.
"""


class DSISException(Exception):
    """Base exception for all DSIS client errors."""

    pass


class DSISAuthenticationError(DSISException):
    """Raised when authentication fails (Azure AD or DSIS token acquisition)."""

    pass


class DSISAPIError(DSISException):
    """Raised when an API request fails."""

    pass


class DSISConfigurationError(DSISException):
    """Raised when configuration is invalid or incomplete."""

    pass
