# Working with Binary Data

This guide explains how to fetch and decode binary bulk data (Protocol Buffers) from the DSIS API.

## Overview

The DSIS API serves data in two formats:

- **Metadata** (JSON): Via OData - entity properties, relationships, statistics
- **Bulk Data** (Protocol Buffers): Large binary arrays like horizon z-values, log curves, seismic amplitudes, surface grids

## Installation

To decode protobuf bulk payloads, install `dsis-model-sdk` with protobuf support:

```bash
pip install dsis-model-sdk[protobuf]
```

**Note:** Requires Python 3.11+ and protobuf 6.33.0+

## Supported Binary Data Types

| Type | Schema | Description | Decoder |
|------|--------|-------------|---------|
| Horizon 3D | `HorizonData3D` | Interpreted surface z-values | `decode_horizon_data()` |
| Log Curves | `LogCurve` | Well log measurements | `decode_log_curves()` |
| Seismic 3D | `SeismicDataSet3D` | 3D seismic amplitude volume | `decode_seismic_float_data()` |
| Seismic 2D | `SeismicDataSet2D` | 2D seismic trace data | `decode_seismic_float_data()` |
| Surface Grid | `SurfaceGrid` | Gridded surface data | `decode_lgc_structure()` |

## Two Methods for Fetching Binary Data

### Method 1: `get_bulk_data()` - Load All at Once

Use for small to medium datasets (< 100MB):

```python
from dsis_client import DSISClient, QueryBuilder
from dsis_model_sdk.models.common import HorizonData3D
from dsis_model_sdk.protobuf import decode_horizon_data

# Example district_id for Common Model + SV4TSTA database
district_id = "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA"
project = "SNORRE"

# Query for entity
query = QueryBuilder(district_id=district_id, project=project).schema(HorizonData3D)
horizons = list(client.execute_query(query, cast=True, max_pages=1))

# Fetch binary data - pass entity object directly!
horizon = horizons[0]
binary_data = client.get_bulk_data(
    schema=HorizonData3D,
    native_uid=horizon,  # Can pass entity object OR string
    query=query  # Auto-extracts district_id and project
)

# Decode
if binary_data:
    decoded = decode_horizon_data(binary_data)
```

### Method 2: `get_bulk_data_stream()` - Stream in Chunks

Use for large datasets (> 100MB) to avoid memory issues:

```python
from dsis_model_sdk.models.common import SeismicDataSet3D
from dsis_model_sdk.protobuf import decode_seismic_float_data

# Example district_id for Common Model + SV4TSTA database
district_id = "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA"
project = "SNORRE"

# Query for entity
query = QueryBuilder(district_id=district_id, project=project).schema(SeismicDataSet3D)
datasets = list(client.execute_query(query, cast=True, max_pages=1))

# Stream large dataset in chunks
seismic = datasets[0]
chunks = []
for chunk in client.get_bulk_data_stream(
    schema=SeismicDataSet3D,
    native_uid=seismic,  # Pass entity object
    query=query,
    chunk_size=10*1024*1024  # 10MB chunks (DSIS recommended)
):
    chunks.append(chunk)
    print(f"Downloaded {len(chunk):,} bytes")

# Combine and decode
binary_data = b''.join(chunks)
decoded = decode_seismic_float_data(binary_data)
```

## Flexible native_uid Parameter

Both `get_bulk_data()` and `get_bulk_data_stream()` accept three formats:

```python
# Option 1: String native_uid
district_id = "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA"
project = "SNORRE"

binary_data = client.get_bulk_data(
    schema=HorizonData3D,
    native_uid="46075",  # String
    district_id=district_id,
    project=project
)

# Option 2: Entity object (automatically extracts native_uid)
binary_data = client.get_bulk_data(
    schema=HorizonData3D,
    native_uid=horizon,  # Entity object
    query=query
)

# Option 3: Entity dict
binary_data = client.get_bulk_data(
    schema=HorizonData3D,
    native_uid={"native_uid": "46075", "name": "..."},  # Dict
    district_id=district_id,
    project=project
)
```

## Media read-links (`/$value`, `/data`, `/data_values`, ...)

Some entities expose bulk content via *media* endpoints. In metadata responses you may see:

- `odata.mediaReadLink`
- `data@odata.mediaReadLink`
- `data_values@odata.mediaReadLink`

The suffix is not always the same (it can be `/$value`, `/data`, `/data_values`, etc.), and the
required `Accept` header may vary between `application/octet-stream` and `application/json`.

When the built-in bulk helpers do not match the entity’s media endpoint, check the read-link value
and to be sure you use the correct data_field. For large payloads, use `stream=True` and iterate in chunks.

```python
from urllib.parse import urljoin

from dsis_client import QueryBuilder

district_id = "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA"
project = "SNORRE"

query = QueryBuilder(district_id=district_id, project=project).schema("SurfaceGrid")
entity = next(client.execute_query(query))

read_link = (
    entity.get("odata.mediaReadLink")
    or entity.get("data@odata.mediaReadLink")
    or entity.get("data_values@odata.mediaReadLink")
)
if not read_link:
    raise ValueError("No @odata mediaReadLink found on entity")

base = f"{client.config.model_name}/{client.config.model_version}/{district_id}/{project}/"
url = urljoin(client.config.data_endpoint.rstrip("/") + "/", base + read_link.lstrip("/"))

chunk_size = 10 * 1024 * 1024  # 10MB
for attempt in range(2):
    headers = client.auth.get_auth_headers()
    headers["Accept"] = "application/octet-stream, application/json"

    with client._session.get(url, headers=headers, stream=True) as resp:
        if resp.status_code == 401 and attempt == 0:
            client.refresh_authentication()
            continue
        resp.raise_for_status()
        bulk_bytes = b"".join(resp.iter_content(chunk_size=chunk_size))
        break
```

## Decoding SurfaceGrid (LGCStructure)

SurfaceGrid payloads are often encoded as an LGCStructure protobuf. In many cases the payload is
length-prefixed; `decode_lgc_structure(..., skip_length_prefix=True)` handles that.

```python
from typing import Any

from dsis_model_sdk.protobuf import LGCStructure_pb2, decode_lgc_structure


def _element_to_dict(element: Any) -> dict[str, Any]:
    DataType = LGCStructure_pb2.LGCStructure.LGCElement.DataType
    data_type = DataType.Name(element.dataType)

    if element.dataType == DataType.FLOAT:
        values = list(element.data_float)
    elif element.dataType == DataType.DOUBLE:
        values = list(element.data_double)
    elif element.dataType == DataType.INT:
        values = list(element.data_int)
    elif element.dataType == DataType.LONG:
        values = list(element.data_long)
    elif element.dataType == DataType.STRING:
        values = list(element.data_string)
    elif element.dataType == DataType.BOOL:
        values = list(element.data_bool)
    else:
        values = []

    return {
        "name": element.elementName,
        "type": data_type,
        "count": len(values),
        "values": values,
    }


def decode_surfacegrid_to_dict(raw_bytes: bytes) -> dict[str, Any]:
    lgc = decode_lgc_structure(raw_bytes, skip_length_prefix=True)
    return {
        "struct_name": lgc.structName,
        "element_count": len(lgc.elements),
        "elements": [_element_to_dict(el) for el in lgc.elements],
    }
```

## Important Notes

### Memory Management

- **Small data (< 100MB)**: Use `get_bulk_data()` - simpler, loads everything at once
- **Large data (> 100MB)**: Use `get_bulk_data_stream()` - streams in chunks, memory-efficient

### API endpoints and headers

- Bulk endpoints can vary by entity and binary field.
- Use `get_bulk_data(..., data_field=...)` when the payload is exposed as a field (often `data`).
- Use `...@odata.mediaReadLink` when the payload is exposed as a media endpoint (often `/$value`, but not always).
- `Accept` can vary; when in doubt include both: `application/octet-stream, application/json`.

### Null Values

Missing or no-data values in arrays are often represented as:
- `-99999.0` for float/double types
- Check data documentation for specific sentinel values

## Migration from Older Versions

Prior to version 0.5.0, there were separate `get_entity_data()` and `get_entity_data_stream()` methods. These have been removed in favor of the more flexible `get_bulk_data()` and `get_bulk_data_stream()` methods:

```python
# OLD (removed in v0.5.0):
binary_data = client.get_entity_data(horizon, schema=HorizonData3D, query=query)

# NEW:
binary_data = client.get_bulk_data(schema=HorizonData3D, native_uid=horizon, query=query)
```

The new methods automatically detect whether you're passing a string, dict, or entity object for the `native_uid` parameter, eliminating the need for separate methods.

## See Also

- [Query Builder Guide](query-builder.md) - Building OData queries
- [Advanced Serialization](advanced-serialization.md) - Casting JSON responses to model instances
- [dsis-model-sdk](https://github.com/equinor/dsis-model-sdk) - Models and protobuf decoders
