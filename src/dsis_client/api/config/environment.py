"""
Environment configuration for DSIS API client.

Defines available DSIS API environments and their base URLs.
"""

from enum import Enum


class Environment(Enum):
    """DSIS API environments."""

    DEV = "dev"
    QA = "qa"
    PROD = "prod"


# Base URLs for each environment
BASE_URLS = {
    Environment.DEV: "https://api-dev.gateway.equinor.com",
    Environment.QA: "https://api-test.gateway.equinor.com",
    Environment.PROD: "https://api.gateway.equinor.com",
}
