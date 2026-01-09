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


class DSISJSONParseError(DSISException):
    """Raised when JSON parsing fails on an otherwise successful response.

    Attributes:
        response_text: The raw response text that failed to parse.
        original_error: The original ValueError from JSON parsing.
    """

    def __init__(self, message: str, response_text: str, original_error: Exception):
        super().__init__(message)
        self.response_text = response_text
        self.original_error = original_error


class DSISConfigurationError(DSISException):
    """Raised when configuration is invalid or incomplete."""

    pass
