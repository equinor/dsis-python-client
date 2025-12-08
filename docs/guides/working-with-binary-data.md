# Working with Binary Data

This guide explains how to fetch and decode binary bulk data (Protocol Buffers) from the DSIS API.

## Overview

The DSIS API serves data in two formats:

- **Metadata** (JSON): Via OData - entity properties, relationships, statistics
- **Bulk Data** (Protocol Buffers): Large binary arrays like horizon z-values, log curves, seismic amplitudes, surface grids

## Installation

To work with binary data, install with protobuf support:

```bash
pip install dsis-schemas[protobuf]
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

# Query for entity
query = QueryBuilder(district_id="123", field="SNORRE").schema(HorizonData3D)
horizons = list(client.execute_query(query, cast=True, max_pages=1))

# Fetch binary data - pass entity object directly!
horizon = horizons[0]
binary_data = client.get_bulk_data(
    schema=HorizonData3D,
    native_uid=horizon,  # Can pass entity object OR string
    query=query  # Auto-extracts district_id and field
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

# Query for entity
query = QueryBuilder(district_id="123", field="SNORRE").schema(SeismicDataSet3D)
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
binary_data = client.get_bulk_data(
    schema=HorizonData3D,
    native_uid="46075",  # String
    district_id="123",
    field="SNORRE"
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
    district_id="123",
    field="SNORRE"
)
```

## Complete Examples

### Example 1: Horizon Data

```python
import numpy as np
from dsis_client import DSISClient, QueryBuilder
from dsis_model_sdk.models.common import HorizonData3D
from dsis_model_sdk.protobuf import decode_horizon_data
from dsis_model_sdk.utils.protobuf_decoders import horizon_to_numpy

# Query for horizons (exclude binary data field for efficiency)
query = QueryBuilder(district_id="123", field="SNORRE").schema(HorizonData3D).select("horizon_name,native_uid")
horizons = list(client.execute_query(query, cast=True))

# Fetch binary data for specific horizon
horizon = horizons[0]
binary_data = client.get_bulk_data(
    schema=HorizonData3D,
    native_uid=horizon,
    query=query
)

if binary_data:
    # Decode protobuf
    decoded = decode_horizon_data(binary_data)
    
    # Convert to NumPy array
    array, metadata = horizon_to_numpy(decoded)
    
    print(f"Horizon: {horizon.horizon_name}")
    print(f"Grid shape: {array.shape}")
    print(f"Data coverage: {(~np.isnan(array)).sum() / array.size * 100:.1f}%")
    
    # Analyze valid data
    valid_data = array[~np.isnan(array)]
    print(f"Depth range: {np.min(valid_data):.2f} - {np.max(valid_data):.2f}")
```

### Example 2: Log Curves

```python
from dsis_model_sdk.models.common import LogCurve
from dsis_model_sdk.protobuf import decode_log_curves
from dsis_model_sdk.utils.protobuf_decoders import log_curve_to_dict

# Query for log curves
query = QueryBuilder(district_id="123", field="SNORRE").schema(LogCurve).select("log_curve_name,native_uid")
curves = list(client.execute_query(query, max_pages=1))

# Fetch binary data
curve = curves[0]
binary_data = client.get_bulk_data(
    schema=LogCurve,
    native_uid=curve,
    query=query
)

if binary_data:
    # Decode
    decoded = decode_log_curves(binary_data)
    
    print(f"Curve type: {'DEPTH' if decoded.curve_type == decoded.DEPTH else 'TIME'}")
    print(f"Index range: {decoded.index.start_index} to {decoded.index.start_index + decoded.index.number_of_index * decoded.index.increment}")
    
    # Convert to dict for easier access
    data = log_curve_to_dict(decoded)
    
    for curve_name, curve_data in data['curves'].items():
        print(f"Curve: {curve_name}")
        print(f"  Unit: {curve_data['unit']}")
        print(f"  Values: {len(curve_data['values'])} samples")
```

### Example 3: Surface Grid Data

Surface grids use the LGCStructure format (Landmark Graphics Corporation tabular structure):

```python
from io import BytesIO
from dsis_model_sdk.protobuf import decode_lgc_structure, LGCStructure_pb2

# Query for grids
query = QueryBuilder(district_id="123", field="SNORRE").schema("SurfaceGrid").select("native_uid,grid_name")
grids = list(client.execute_query(query, cast=True, max_pages=1))

# Fetch binary data (note: SurfaceGrid uses /$value endpoint, not /data)
grid = grids[0]
endpoint_path = f"OpenWorksCommonModel/5000107/{query.district_id}/{query.field}/SurfaceGrid('{grid.native_uid}')/$value"
full_url = f"{client.config.data_endpoint}/{endpoint_path}"

headers = client.auth.get_auth_headers()
headers["Accept"] = "application/json"
response = client._session.get(full_url, headers=headers)
data = response.content

print(f"Downloaded {len(data):,} bytes")

# LGCStructure uses varint length prefix
def read_varint(stream):
    """Read a varint length prefix from stream."""
    shift = 0
    result = 0
    while True:
        byte_data = stream.read(1)
        if not byte_data:
            return 0
        byte = byte_data[0]
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result
        shift += 7

# Parse length-prefixed message
stream = BytesIO(data)
size = read_varint(stream)
message_data = stream.read(size)

# Decode
lgc = decode_lgc_structure(message_data)

print(f"Structure name: {lgc.structName}")
print(f"Number of elements: {len(lgc.elements)}")

# Process grid elements (columns/rows)
for i, el in enumerate(lgc.elements[:5]):  # Show first 5
    data_type = LGCStructure_pb2.LGCStructure.LGCElement.DataType.Name(el.dataType)
    
    if el.dataType == LGCStructure_pb2.LGCStructure.LGCElement.DataType.FLOAT:
        values = el.data_float
    elif el.dataType == LGCStructure_pb2.LGCStructure.LGCElement.DataType.DOUBLE:
        values = el.data_double
    else:
        values = []
    
    print(f"Element {i}: '{el.elementName}', Type: {data_type}, Values: {len(values):,}")
```

## Important Notes

### Memory Management

- **Small data (< 100MB)**: Use `get_bulk_data()` - simpler, loads everything at once
- **Large data (> 100MB)**: Use `get_bulk_data_stream()` - streams in chunks, memory-efficient

### API Endpoints

- **Standard bulk data**: `/{Schema}('{native_uid}')/data` (no `/$value` suffix)
- **Surface grids**: `/{Schema}('{native_uid}')/$value` (uses `/$value` suffix)

### Accept Header

The DSIS API returns binary protobuf data with `Accept: application/json` header (not `application/octet-stream`).

### Null Values

Missing or no-data values in arrays are often represented as:
- `-99999.0` for float/double types
- Check data documentation for specific sentinel values

## Deprecated Methods

Prior to version 0.5.0, there were separate `get_entity_data()` and `get_entity_data_stream()` methods. These are now deprecated:

```python
# OLD (deprecated):
binary_data = client.get_entity_data(horizon, schema=HorizonData3D, query=query)

# NEW (preferred):
binary_data = client.get_bulk_data(schema=HorizonData3D, native_uid=horizon, query=query)
```

The new methods automatically detect whether you're passing a string or entity object, eliminating the need for separate methods.

## See Also

- [Query Builder Guide](query-builder.md) - Building OData queries
- [dsis-schemas Documentation](https://github.com/equinor/dsis-schemas) - Complete protobuf decoder reference
