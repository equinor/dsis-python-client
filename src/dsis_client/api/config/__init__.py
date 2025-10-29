"""
Configuration module for DSIS API client.

Provides configuration management including environment settings,
validation, and factory methods.
"""

from .config import DSISConfig
from .environment import Environment

# Attach the classmethod factories to DSISConfig
from .factory import for_common_model, for_native_model

DSISConfig.for_native_model = classmethod(for_native_model)
DSISConfig.for_common_model = classmethod(for_common_model)

__all__ = [
    "Environment",
    "DSISConfig",
    "for_native_model",
    "for_common_model",
]
