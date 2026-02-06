# API Summary

Concise overview of the public surface. For most read workflows, prefer `QueryBuilder` + `execute_query()`.

## Core types

### `DSISConfig`

`DSISConfig` is a dataclass holding auth + environment + model settings. Notably, `model_name` is required.

### `DSISClient`

Commonly used methods:

- `execute_query(query, cast=False, ...)` - executes a `QueryBuilder` and yields items across pages.
- `get(*path_segments, ...)` - low-level escape hatch for custom paths.
- `get_bulk_data(...)` / `get_bulk_data_stream(...)` - fetch/stream binary payloads for entities.

### `Environment`

Enum: `DEV`, `QA`, `PROD`.

## Minimal usage (recommended)

```python
from dsis_client import DSISClient, DSISConfig, Environment, QueryBuilder
import os

cfg = DSISConfig(
    environment=Environment.DEV,
    tenant_id=os.environ["DSIS_TENANT_ID"],
    client_id=os.environ["DSIS_CLIENT_ID"],
    client_secret=os.environ["DSIS_CLIENT_SECRET"],
    access_app_id=os.environ["DSIS_ACCESS_APP_ID"],
    dsis_username=os.environ["DSIS_USERNAME"],
    dsis_password=os.environ["DSIS_PASSWORD"],
    subscription_key_dsauth=os.environ["DSIS_SUBSCRIPTION_KEY_DSAUTH"],
    subscription_key_dsdata=os.environ["DSIS_SUBSCRIPTION_KEY_DSDATA"],
    model_name="OpenWorksCommonModel",
    dsis_site="dev"
)

client = DSISClient(cfg)

district_id = "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA"
project = "SNORRE"

query = QueryBuilder(district_id=district_id, project=project).schema("Well")
for item in client.execute_query(query):
    print(item.get("native_uid"))
```

## Low-level `get()` (escape hatch)

`get()` builds a DSIS path from positional segments and adds OData query options.

```python
data = client.get(
    "OpenWorksCommonModel",
    "5000107",
    "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA",
    "SNORRE",
    "Well",
    format_type="json",
    select="native_uid,name",
)
```

## Bulk / binary data

### `get_bulk_data(schema, native_uid, district_id=None, project=None, data_field="data", query=None)`

Fetches a binary field for an entity using the “bulk field” endpoint:

`/{Schema}('{native_uid}')/{data_field}`

Important nuances:

- The field name can vary (`data`, `data_values`, ...). Use `data_field=...` when needed.
- Some entities expose *media* endpoints via `odata.mediaReadLink` / `...@odata.mediaReadLink` and these may end in `/$value`, `/data`, `/data_values`, etc.
- The required `Accept` header may vary between `application/json` and `application/octet-stream` depending on the entity and endpoint.

```python
from dsis_model_sdk.models.common import HorizonData3D

district_id = "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA"
project = "SNORRE"

binary_data = client.get_bulk_data(
    schema=HorizonData3D,
    native_uid="46075",
    district_id=district_id,
    project=project,
    data_field="data",
)
```

For the media read-link (`...@odata.mediaReadLink`) workaround, streaming downloads, and protobuf
decoding examples (including SurfaceGrid/LGCStructure), see
[Working with Binary Data](../guides/working-with-binary-data.md).

## Notes

- Treat non-2xx responses as exceptions; refresh tokens on `401`.
- Avoid committing secrets or internal IDs.
- For more details, see [Working with Binary Data](../guides/working-with-binary-data.md).
