"""OData query utilities for DSIS API.

Provides utilities for building and formatting OData query strings.
"""

import logging
from typing import Dict, List, Optional
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


def build_query_params(
    select: List[str],
    expand: List[str],
    filter_expr: Optional[str],
    format_type: Optional[str] = "json",
) -> Dict[str, str]:
    """Build OData query parameters dictionary.

    Args:
        select: List of fields to select
        expand: List of relations to expand
        filter_expr: OData filter expression
        format_type: Response format (default: "json"). Use None or empty
            string to omit the $format parameter.

    Returns:
        Dictionary of query parameters
    """
    params: Dict[str, str] = {}

    # Only add format if it's not None or empty
    if format_type:
        params["$format"] = format_type

    if select:
        params["$select"] = ",".join(select)
    if expand:
        params["$expand"] = ",".join(expand)
    if filter_expr:
        params["$filter"] = filter_expr

    logger.info(f"Built query params: {params}")
    return params


def build_query_string(schema_name: Optional[str], params: Dict[str, str]) -> str:
    """Build complete OData query string.

    Args:
        schema_name: Name of the schema/table
        params: Query parameters dictionary

    Returns:
        Full query string (e.g., "Fault?$format=json&$select=id,type")

    Raises:
        ValueError: If schema_name is not provided
    """
    if not schema_name:
        raise ValueError("schema must be set before getting query string")

    query_string = ""
    if params:
        query_string = urlencode(params)
        query_string = f"?{query_string}"

    query_str = f"{schema_name}{query_string}"
    logger.info(f"Built query string: {query_str}")
    return query_str


def build_query_params_string(params: Dict[str, str]) -> str:
    """Build just the query parameters part (without schema name).

    Args:
        params: Query parameters dictionary

    Returns:
        Query parameters string (e.g., "$format=json&$select=name,depth")
    """
    if params:
        return urlencode(params)
    return ""
