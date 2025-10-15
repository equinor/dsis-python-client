"""
DSIS Python Client

A Python SDK for the DSIS (Drilling & Well Services Information System) API Management system.
Provides easy access to DSIS data through Equinor's Azure API Management gateway.
"""

from .api import DSISAuth, DSISClient, DSISConfig, Environment

__all__ = ["DSISClient", "DSISAuth", "DSISConfig", "Environment"]
