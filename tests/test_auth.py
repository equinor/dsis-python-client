"""Tests for DSIS authentication timeout configuration."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
import types


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "dsis_client" / "api"


def load_auth_types():
    """Load auth/config modules directly from source without package side effects."""
    for name in list(sys.modules):
        if name == "dsis_client" or name.startswith("dsis_client."):
            sys.modules.pop(name)

    dsis_pkg = types.ModuleType("dsis_client")
    dsis_pkg.__path__ = [str(ROOT / "src" / "dsis_client")]
    sys.modules["dsis_client"] = dsis_pkg

    api_pkg = types.ModuleType("dsis_client.api")
    api_pkg.__path__ = [str(SRC)]
    sys.modules["dsis_client.api"] = api_pkg
    dsis_pkg.api = api_pkg

    config_pkg = types.ModuleType("dsis_client.api.config")
    config_pkg.__path__ = [str(SRC / "config")]
    sys.modules["dsis_client.api.config"] = config_pkg
    api_pkg.config = config_pkg

    auth_pkg = types.ModuleType("dsis_client.api.auth")
    auth_pkg.__path__ = [str(SRC / "auth")]
    sys.modules["dsis_client.api.auth"] = auth_pkg
    api_pkg.auth = auth_pkg

    def load_module(name: str, path: Path):
        spec = spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load module {name} from {path}")
        module = module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module

    load_module("dsis_client.api.exceptions", SRC / "exceptions.py")
    environment_module = load_module(
        "dsis_client.api.config.environment", SRC / "config" / "environment.py"
    )
    config_module = load_module(
        "dsis_client.api.config.config", SRC / "config" / "config.py"
    )
    config_pkg.environment = environment_module
    config_pkg.config = config_module
    config_pkg.DSISConfig = config_module.DSISConfig
    config_pkg.Environment = environment_module.Environment

    auth_module = load_module("dsis_client.api.auth.auth", SRC / "auth" / "auth.py")
    auth_pkg.auth = auth_module
    auth_pkg.DSISAuth = auth_module.DSISAuth
    return auth_module.DSISAuth, config_module.DSISConfig, environment_module.Environment


def make_config(
    DSISConfig,
    Environment,
    *,
    auth_timeout: float | tuple[float, float] | None = None,
) -> object:
    """Create a valid DSISConfig for auth-focused tests."""
    return DSISConfig(
        environment=Environment.DEV,
        tenant_id="tenant-id",
        client_id="client-id",
        client_secret="client-secret",
        access_app_id="access-app-id",
        dsis_username="username",
        dsis_password="password",
        subscription_key_dsauth="dsauth-key",
        subscription_key_dsdata="dsdata-key",
        dsis_site="qa",
        auth_timeout=auth_timeout,
    )


def test_get_aad_token_uses_configured_auth_timeout(monkeypatch):
    """MSAL client creation forwards auth_timeout from DSISConfig."""
    DSISAuth, DSISConfig, Environment = load_auth_types()
    captured: dict[str, object] = {}

    class FakeConfidentialClientApplication:
        def __init__(self, client_id, **kwargs):
            captured["client_id"] = client_id
            captured["kwargs"] = kwargs

        def acquire_token_for_client(self, scopes):
            captured["scopes"] = scopes
            return {"access_token": "aad-token"}

    monkeypatch.setattr(
        "dsis_client.api.auth.auth.msal.ConfidentialClientApplication",
        FakeConfidentialClientApplication,
    )

    auth = DSISAuth(make_config(DSISConfig, Environment, auth_timeout=(5, 30)))

    assert auth.get_aad_token() == "aad-token"
    assert captured["client_id"] == "client-id"
    assert captured["scopes"] == ["access-app-id/.default"]
    assert captured["kwargs"] == {
        "authority": "https://login.microsoftonline.com/tenant-id",
        "client_credential": "client-secret",
        "timeout": (5, 30),
    }


def test_get_dsis_token_uses_configured_auth_timeout(monkeypatch):
    """DSIS token POST forwards auth_timeout from DSISConfig."""
    DSISAuth, DSISConfig, Environment = load_auth_types()
    captured: dict[str, object] = {}

    class FakeResponse:
        status_code = 200
        reason = "OK"
        text = ""

        @staticmethod
        def json():
            return {"access_token": "dsis-token"}

    auth = DSISAuth(make_config(DSISConfig, Environment, auth_timeout=(2, 60)))

    def fake_post(url, *, headers, data, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["data"] = data
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(auth._session, "post", fake_post)

    assert auth.get_dsis_token(aad_token="aad-token") == "dsis-token"
    assert captured["url"] == "https://api-dev.gateway.equinor.com/dsauth/v1/token"
    assert captured["headers"] == {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer aad-token",
        "dsis-site": "qa",
        "Ocp-Apim-Subscription-Key": "dsauth-key",
    }
    assert captured["data"] == {
        "grant_type": "password",
        "client_id": "dsis-data",
        "username": "username",
        "password": "password",
    }
    assert captured["timeout"] == (2, 60)
