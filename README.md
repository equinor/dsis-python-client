# DSIS Python Client

Python SDK for the DSIS (DecisionSpace Integration Server) API. Handles dual-token authentication (Azure AD + DSIS) and provides a fluent query builder for OData access.

## Installation

```bash
pip install dsis-client
```

For protobuf bulk data decoding (grids, horizons, seismic):

```bash
pip install dsis-model-sdk[protobuf]
```

## Quick Start

```python
import os
from dsis_client import DSISClient, DSISConfig, Environment, QueryBuilder

config = DSISConfig(
    environment=Environment.DEV,
    tenant_id=os.getenv("DSIS_TENANT_ID"),
    client_id=os.getenv("DSIS_CLIENT_ID"),
    client_secret=os.getenv("DSIS_CLIENT_SECRET"),
    access_app_id=os.getenv("DSIS_ACCESS_APP_ID"),
    dsis_username=os.getenv("DSIS_USERNAME"),
    dsis_password=os.getenv("DSIS_PASSWORD"),
    subscription_key_dsauth=os.getenv("DSIS_SUBSCRIPTION_KEY_DSAUTH"),
    subscription_key_dsdata=os.getenv("DSIS_SUBSCRIPTION_KEY_DSDATA"),
    model_name="OpenWorksCommonModel", #or the native model OW5000
    dsis_site="dev",
)

client = DSISClient(config)

query = (
    QueryBuilder(
        model_name="OpenWorksCommonModel",
        district_id="OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA",
        project="SNORRE",
    )
    .schema("Well")
    .select("name", "depth", "status")
    .filter("depth gt 1000")
    .expand("wellbores")
)

for well in client.execute_query(query):
    print(well)
```

## QueryBuilder

`QueryBuilder` is the primary way to query data. It uses method chaining and IS the query object (no `.build()` needed).

```python
query = (
    QueryBuilder(
        model_name="OW5000",
        district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
        project="SNORRE",
    )
    .schema("Fault")
    .select("fault_id,fault_type,fault_name")
    .filter("fault_type eq 'NORMAL'")
    .expand("interpretations")
)

for item in client.execute_query(query):
    print(item)
```

### Type-safe casting with model classes

Pass a model class to `.schema()` and use `cast=True` to get typed results:

```python
from dsis_model_sdk.models.common import Basin

query = (
    QueryBuilder(model_name="OpenWorksCommonModel", district_id=dist, project=prj)
    .schema(Basin)
    .select("basin_name,basin_id,native_uid")
)

for basin in client.execute_query(query, cast=True):
    print(basin.basin_name)  # IDE autocomplete works
```

### Pagination

`execute_query()` automatically follows `odata.nextLink` across all pages. Control with `max_pages`:

```python
# All pages (default)
all_items = list(client.execute_query(query))

# First page only (max 1000 items)
first_page = list(client.execute_query(query, max_pages=1))
```

## Bulk Data (Protobuf)

Fetch binary data (horizons, log curves, seismic) with `get_bulk_data()` or stream large datasets with `get_bulk_data_stream()`:

```python
from dsis_model_sdk.models.common import HorizonData3D
from dsis_model_sdk.protobuf import decode_horizon_data

binary_data = client.get_bulk_data(
    schema=HorizonData3D,
    native_uid="46075",
    district_id=district_id,
    project=project,
)
decoded = decode_horizon_data(binary_data)
```

For large datasets, stream in chunks:

```python
for chunk in client.get_bulk_data_stream(
    schema=SeismicDataSet3D,
    native_uid=seismic,
    query=query,
    chunk_size=10 * 1024 * 1024,  # 10 MB
):
    process(chunk)
```

## Error Handling

```python
from dsis_client import DSISAuthenticationError, DSISAPIError, DSISConfigurationError

try:
    client = DSISClient(config)
    for item in client.execute_query(query):
        print(item)
except DSISConfigurationError as e:
    print(f"Bad config: {e}")
except DSISAuthenticationError as e:
    print(f"Auth failed: {e}")
except DSISAPIError as e:
    print(f"Request failed: {e}")
```

## Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("dsis_client").setLevel(logging.DEBUG)
```

## Documentation

- [Getting Started](https://equinor.github.io/dsis-python-client/guides/getting-started/)
- [QueryBuilder Guide](https://equinor.github.io/dsis-python-client/guides/query-builder/)
- [Common vs Native Model](https://equinor.github.io/dsis-python-client/guides/common-vs-native-model/)
- [Advanced Serialization](https://equinor.github.io/dsis-python-client/guides/advanced-serialization/)
- [Working with Binary Data](https://equinor.github.io/dsis-python-client/guides/working-with-binary-data/)

## Contributing

See [CONTRIBUTING.md](https://github.com/equinor/dsis-python-client/blob/main/CONTRIBUTING.md).

## License

[MIT](https://github.com/equinor/dsis-python-client/blob/main/LICENSE)
