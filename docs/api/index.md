# API Summary

Concise overview of public surface.

## Classes

### DSISClient
Methods (essentials only):
### `get(*path_segments, format_type="json", select=None, expand=None, filter=None, **extra_query)`

Unified flexible path GET helper. Use positional path segments to build the endpoint.

Examples:

```python
# Get using just model and version
data = client.get()

# Get Basin data for a district and field
data = client.get("123", "wells", "Basin")

# Get with field selection
data = client.get("123", "wells", "Well", select="name,depth,status")

# Get with filtering
data = client.get("123", "wells", "Well", filter="depth gt 1000")

# Get with expand
data = client.get("123", "wells", "Well", expand="logs,completions")
```

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
data = client.get("OW5000", "<record-id>")
```

## Error Handling Hint

Treat non-200 responses as exceptions; inspect message for status cues (401/403/404). Refresh tokens on auth failures.

## Request Essentials

Headers assembled internally include both tokens + subscription key; pass only endpoint/table info.

## Binary Data Methods

### `get_bulk_data(query, *, accept="application/json")`

Fetch binary bulk data (protobuf) for an entity. Loads entire response into memory.

**Parameters:**
- `query`: QueryBuilder instance configured with `.schema()` and `.entity()` calls
- `accept`: Accept header value (default: `"application/json"`). Use `"application/octet-stream"` for raw binary endpoints (e.g., SurfaceGrid/$value)

**Returns:** `Optional[bytes]` - Binary protobuf data or None if no data

**Use for:** Small to medium datasets (< 100MB)

```python
from dsis_model_sdk.models.common import HorizonData3D

# Option 1: String native_uid
query = QueryBuilder(district_id="123", project="SNORRE").schema(HorizonData3D)
horizons = list(client.execute_query(query, cast=True))
bulk_query = query.entity(horizons[0].native_uid)
binary_data = client.get_bulk_data(bulk_query)

# Option 2: SurfaceGrid with $value endpoint and custom accept header
query = QueryBuilder(district_id="123", project="SNORRE").schema("SurfaceGrid")
grids = list(client.execute_query(query))
bulk_query = query.entity(grids[0]["native_uid"], data_field="$value")
binary_data = client.get_bulk_data(bulk_query, accept="application/octet-stream")
```

### `get_bulk_data_stream(query, *, chunk_size=10*1024*1024, accept="application/json")` 

Stream binary bulk data in chunks for memory-efficient processing.

**Parameters:**
- `query`: QueryBuilder instance configured with `.schema()` and `.entity()` calls
- `chunk_size`: Size of chunks to yield (default: 10MB, DSIS recommended)
- `accept`: Accept header value (default: `"application/json"`)

**Yields:** Binary data chunks as bytes

**Use for:** Large datasets (> 100MB), memory-constrained environments

```python
from dsis_model_sdk.models.common import SeismicDataSet3D

query = QueryBuilder(district_id="123", project="SNORRE").schema(SeismicDataSet3D)
datasets = list(client.execute_query(query, cast=True))

bulk_query = query.entity(datasets[0].native_uid)
chunks = []
for chunk in client.get_bulk_data_stream(
    bulk_query,
    chunk_size=10*1024*1024
):
    chunks.append(chunk)

binary_data = b''.join(chunks)
```

## Notes

- No secrets or IDs should appear in committed code or documentation.
- Extend functionality by wrapping `DSISClient` rather than modifying internals.
- For binary data usage, see [Working with Binary Data](../guides/working-with-binary-data.md) guide.

For extended patterns refer to guides.

```python
from typing import Dict, Any, Optional, Union
from dsis_client import DSISClient, DSISConfig, Environment

def process_data(client: DSISClient, table: str, record_id: Optional[str] = None) -> Dict[str, Any]:
    """Process data with proper type hints."""
    return client.get(table, record_id, format_type="json")

# Usage with type checking
config: DSISConfig = DSISConfig(...)
client: DSISClient = DSISClient(config)
data: Dict[str, Any] = client.get("OW5000", "5000107")
```
