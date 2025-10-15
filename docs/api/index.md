# API Summary

Concise overview of public surface.

## Classes

### DSISClient

Primary entry point.

Methods (essentials only):

- `get(*path_segments, format_type='json', select=None, params=None, **extra_query)` → dict  # unified flexible path
- `get_odata(table, record_id=None, format_type='json', **query)` → dict
- `test_connection()` → bool
- `refresh_authentication()` → None

### DSISConfig

Configuration fields (all required):

| Field | Purpose |
|-------|---------|
| environment | Target deployment (DEV / QA / PROD) |
| tenant_id | Azure AD tenant identifier |
| client_id | App registration client id |
| client_secret | Secret for the client (secure) |
| access_app_id | Access application id (internal) |
| dsis_username | DSIS account user name |
| dsis_password | DSIS account password |
| subscription_key_dsauth | APIM subscription key for dsauth (token exchange) |
| subscription_key_dsdata | APIM subscription key for dsdata (data calls) |

Derived properties:

| Property | Description |
|----------|-------------|
| base_url | Environment base API gateway URL |
| token_endpoint | dsauth endpoint URL |
| data_endpoint | dsdata base endpoint |
| authority | Azure AD authority URL |
| scope | OAuth scope list for access app |

Validation pattern example:

```python
required = [
    'tenant_id','client_id','client_secret','access_app_id',
    'dsis_username','dsis_password','subscription_key'
]
missing = [f for f in required if not getattr(cfg, f)]
if missing:
    raise ValueError(f"Missing config fields: {missing}")
```

Flexible path usage example:

```python
data = client.get(
    "OW5000", "5000107",
    "OpenWorks_OW_BG4FROST_SingleSource-OW_BG4FROST",
    "FD_GRANE", "Rgrid",
    format_type="json",
    select="attribute,grid_id,no_cols,no_rows"
)
```

Produces:

```text
/OW5000/5000107/OpenWorks_OW_BG4FROST_SingleSource-OW_BG4FROST/FD_GRANE/Rgrid?$format=json&$select=attribute,grid_id,no_cols,no_rows
```

### DSISAuth (internal)

Obtains Azure AD token then DSIS token; exposed only via `DSISClient`.

### Environment

Enum: `DEV`, `QA`, `PROD`. This is important as 'dsis-site' variable in the header of the token endpoint is dependent on it. It also defines what base_url we are using.

## Minimal Usage

```python
from dsis_client import DSISClient, DSISConfig, Environment
import os

cfg = DSISConfig(
    environment=Environment.DEV,
    tenant_id=os.getenv("DSIS_TENANT_ID"),
    client_id=os.getenv("DSIS_CLIENT_ID"),
    client_secret=os.getenv("DSIS_CLIENT_SECRET"),
    access_app_id=os.getenv("DSIS_ACCESS_APP_ID"),
    dsis_username=os.getenv("DSIS_USERNAME"),
    dsis_password=os.getenv("DSIS_PASSWORD"),
    subscription_key_dsauth=os.getenv("DSIS_SUBSCRIPTION_KEY_DSAUTH"),
    subscription_key_dsdata=os.getenv("DSIS_SUBSCRIPTION_KEY_DSDATA")
)
client = DSISClient(cfg)
data = client.get_odata("OW5000", "<record-id>")
```

## Error Handling Hint

Treat non-200 responses as exceptions; inspect message for status cues (401/403/404). Refresh tokens on auth failures.

## Request Essentials

Headers assembled internally include both tokens + subscription key; pass only endpoint/table info.

## Notes

- No secrets or IDs should appear in committed code or documentation.
- Extend functionality by wrapping `DSISClient` rather than modifying internals.

For extended patterns refer to guides.

```python
from typing import Dict, Any, Optional, Union
from dsis_client import DSISClient, DSISConfig, Environment

def process_data(client: DSISClient, table: str, record_id: Optional[str] = None) -> Dict[str, Any]:
    """Process data with proper type hints."""
    return client.get_odata(table, record_id, format_type="json")

# Usage with type checking
config: DSISConfig = DSISConfig(...)
client: DSISClient = DSISClient(config)
data: Dict[str, Any] = client.get_odata("OW5000", "5000107")
```
