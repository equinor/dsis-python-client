"""
Factory methods for creating DSISConfig instances.

Provides convenient factory methods for common configuration scenarios.
"""

from .config import DSISConfig
from .environment import Environment


def for_native_model(
    cls,
    environment: Environment,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    access_app_id: str,
    dsis_username: str,
    dsis_password: str,
    subscription_key_dsauth: str,
    subscription_key_dsdata: str,
    model_name: str = "OW5000",
    model_version: str = "5000107",
    dsis_site: str = "qa",
) -> "DSISConfig":
    """Create a configuration for accessing native model data as a classmethod."""
    return cls(
        environment=environment,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        access_app_id=access_app_id,
        dsis_username=dsis_username,
        dsis_password=dsis_password,
        subscription_key_dsauth=subscription_key_dsauth,
        subscription_key_dsdata=subscription_key_dsdata,
        model_name=model_name,
        model_version=model_version,
        dsis_site=dsis_site,
    )


def for_common_model(
    cls,
    environment: Environment,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    access_app_id: str,
    dsis_username: str,
    dsis_password: str,
    subscription_key_dsauth: str,
    subscription_key_dsdata: str,
    model_name: str = "OpenWorksCommonModel",
    model_version: str = "5000107",
    dsis_site: str = "qa",
) -> "DSISConfig":
    """Create a configuration for accessing common model data as a classmethod."""
    return cls(
        environment=environment,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        access_app_id=access_app_id,
        dsis_username=dsis_username,
        dsis_password=dsis_password,
        subscription_key_dsauth=subscription_key_dsauth,
        subscription_key_dsdata=subscription_key_dsdata,
        model_name=model_name,
        model_version=model_version,
        dsis_site=dsis_site,
    )
