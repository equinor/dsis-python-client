# DSIS Python Client

A Python SDK for the DSIS (DecisionSpace Integration Server) API Management system. Provides easy access to DSIS data through Equinor's Azure API Management gateway with built-in authentication and error handling.

## Features

- **Dual-Token Authentication**: Handles both Azure AD and DSIS token acquisition automatically
- **Easy Configuration**: Simple dataclass-based configuration management
- **Error Handling**: Custom exceptions for different error scenarios
- **Type Hints**: Full type annotations for better IDE support
- **OData Support**: Convenient methods for OData queries with full parameter support
- **QueryBuilder**: Fluent API for building complex queries

## Installation

```bash
pip install dsis-client
```

## Quick Start (QueryBuilder + execute_query)

We typically fetch data using `QueryBuilder` + `execute_query()`.
For required environment variables and prerequisites, see the Getting Started guide.

```python
import os

from dsis_client import DSISClient, DSISConfig, Environment, QueryBuilder

config = DSISConfig(
    environment=Environment.PROD,
    tenant_id=os.getenv("DSIS_TENANT_ID"),
    client_id=os.getenv("DSIS_CLIENT_ID"),
    client_secret=os.getenv("DSIS_CLIENT_SECRET"),
    access_app_id=os.getenv("DSIS_ACCESS_APP_ID"),
    dsis_username=os.getenv("DSIS_USERNAME"),
    dsis_password=os.getenv("DSIS_PASSWORD"),
    subscription_key_dsauth=os.getenv("DSIS_SUBSCRIPTION_KEY_DSAUTH"),
    subscription_key_dsdata=os.getenv("DSIS_SUBSCRIPTION_KEY_DSDATA"),
    model_name="OpenWorksCommonModel",
    dsis_site="prod",
)

client = DSISClient(config)

# Example district_id for Common Model + SV4TSTA database.
# District naming differs by model; see the QueryBuilder guide for an example helper.
district_id = "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA"
project = "SNORRE"

query = (
    QueryBuilder(district_id=district_id, project=project)
    .schema("Well")
    .select("well_name,well_uwi,spud_date")
    .filter("well_type eq 'Producer'")
)

for item in client.execute_query(query):
    print(item)
```

## Bulk data (workaround for `$value` endpoints)

The built-in bulk data helpers may not work for all schemas/variants yet. For some entity types
(commonly `SurfaceGrid`), the API uses a `/$value` endpoint. The snippet below shows a small,
explicit workaround.

This uses the client's internal session (`client._session`) and is intentionally documented as a
workaround (not a stable public API).

```python
from urllib.parse import urljoin

def build_bulk_url(
    native_uid: str,
    *,
    data_endpoint: str,
    model_name: str,
    model_version: str,
    district_id: str,
    project: str,
    entity_name: str = "SurfaceGrid",
) -> str:
    full_path = (
        f"{model_name}/{model_version}/{district_id}/{project}/"
        f"{entity_name}('{native_uid}')/$value"
    )
    return urljoin(data_endpoint.rstrip("/") + "/", full_path)

district_id = "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA"
project = "SNORRE"
native_uid = "16621"

url = build_bulk_url(
    native_uid,
    data_endpoint=client.config.data_endpoint,
    model_name=client.config.model_name,
    model_version=client.config.model_version,
    district_id=district_id,
    project=project,
)

for attempt in range(2):
    headers = client.auth.get_auth_headers()
    headers["Accept"] = "application/octet-stream"

    resp = client._session.get(url, headers=headers)
    if resp.status_code == 401 and attempt == 0:
        client.refresh_authentication()
        continue
    resp.raise_for_status()
    bulk_bytes = resp.content
    break
```

## Documentation

For detailed documentation, see the docs:

- [Getting Started Guide](docs/guides/getting-started.md)
- [QueryBuilder Guide](docs/guides/query-builder.md)
- [Common vs Native Model](docs/guides/common-vs-native-model.md)
- [Advanced Serialization](docs/guides/advanced-serialization.md)
- [Working with Binary Data](docs/guides/working-with-binary-data.md)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## License

This project is licensed under the terms of the [MIT license](LICENSE).
