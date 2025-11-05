"""
Query module for DSIS OData API.

Provides query builder and OData utilities.
"""

from . import odata
from .builder import QueryBuilder

__all__ = ["QueryBuilder", "odata"]
