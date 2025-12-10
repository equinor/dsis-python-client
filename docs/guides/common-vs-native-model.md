# DSIS Data Models: Common Model vs Native Model

This guide explains the differences between the OpenWorks Common Model and Native Model (OW5000) in DSIS.

## Overview

DSIS provides two data model options for accessing OpenWorks data:

| Aspect | Native Model (OW5000) | Common Model (OpenWorksCommonModel) |
|--------|----------------------|-------------------------------------|
| Model Name | `OW5000` | `OpenWorksCommonModel` |
| Version | `5000107` | `5000107` |
| Entity for Grids | `Rgrid` | `SurfaceGrid` |
| Key Type | Composite (5 fields) | Single (`native_uid`) |
| Large Grid Support | ❌ May fail with 500 | ✅ Works reliably |

## Key Differences

### 1. Entity Keys

**Native Model (OW5000) - Composite Key:**
```
Rgrid(attribute='Z',data_source='ALDAV',geo_name='VIKING GP. Top',geo_type='SURFACE',map_data_set_name='D-15-rev1.1CS pinpj2016')
```

Requires 5 key fields:
- `attribute` - Grid attribute name
- `data_source` - Data source identifier
- `geo_name` - Geological name
- `geo_type` - Geological type (e.g., 'SURFACE')
- `map_data_set_name` - Map data set name

**Common Model (OpenWorksCommonModel) - Single Key:**
```
SurfaceGrid('16621')
```

Uses only `native_uid` - a single unique identifier.

### 2. URL Structure

**Native Model:**
```
/dsdata/v1/OW5000/5000107/{district}/{field}/Rgrid({composite_key})/$value
```

Example:
```
https://api-dev.gateway.equinor.com/dsdata/v1/OW5000/5000107/OpenWorks_OW_SV4FROST_SingleSource-OW_SV4FROST/JS_ALL/Rgrid(attribute=%27Z%27,data_source=%27ALDAV%27,geo_name=%27VIKING%20GP.%20Top%27,geo_type=%27SURFACE%27,map_data_set_name=%27D-15-rev1.1CS%20pinpj2016%27)/$value
```

**Common Model:**
```
/dsdata/v1/OpenWorksCommonModel/5000107/{district}/{field}/SurfaceGrid('{native_uid}')/$value
```

Example:
```
https://api-dev.gateway.equinor.com/dsdata/v1/OpenWorksCommonModel/5000107/OpenWorksCommonModel_OW_SV4FROST-OW_SV4FROST/JS_ALL/SurfaceGrid('16621')/$value
```

### 3. District Naming

Districts have different naming conventions:

| Native Model | Common Model |
|--------------|--------------|
| `OpenWorks_OW_SV4FROST_SingleSource-OW_SV4FROST` | `OpenWorksCommonModel_OW_SV4FROST-OW_SV4FROST` |

### 4. Performance & Reliability

| Scenario | Native Model | Common Model |
|----------|--------------|--------------|
| Small grids (~8K cells) | ✅ Works (~43 KB) | ✅ Works (~28 MB*) |
| Large grids (~27M cells) | ❌ HTTP 500 error | ✅ Works (~136 MB) |

*Note: Data size differs due to different serialization formats.

The Native Model (OW5000) can fail with large grids due to connection resets in the Thrift/Teiid layer when streaming from OpenWorks.

## Code Examples

### Using Native Model (OW5000)

```python
from dsis_client import DSISClient, DSISConfig, Environment
from urllib.parse import quote

config = DSISConfig(
    environment=Environment.DEV,
    model_name="OW5000",
    model_version="5000107",
    # ... other config
)
client = DSISClient(config)

# Composite key (must URL-encode special characters)
key = "attribute='Z',data_source='ALDAV',geo_name='VIKING GP. Top',geo_type='SURFACE',map_data_set_name='D-15-rev1.1CS pinpj2016'"
encoded_key = quote(key, safe="=,")

district = "OpenWorks_OW_SV4FROST_SingleSource-OW_SV4FROST"
field = "JS_ALL"

endpoint = f"OW5000/5000107/{district}/{field}/Rgrid({encoded_key})/$value"
```

### Using Common Model (Recommended)

```python
from dsis_client import DSISClient, DSISConfig, Environment

config = DSISConfig(
    environment=Environment.DEV,
    model_name="OpenWorksCommonModel",
    model_version="5000107",
    # ... other config
)
client = DSISClient(config)

# Simple native_uid key
native_uid = "16621"

district = "OpenWorksCommonModel_OW_SV4FROST-OW_SV4FROST"
field = "JS_ALL"

endpoint = f"OpenWorksCommonModel/5000107/{district}/{field}/SurfaceGrid('{native_uid}')/$value"
```

## When to Use Which Model

### Use Common Model (OpenWorksCommonModel) when:
- ✅ You need to retrieve large grid data
- ✅ You have the `native_uid` (grid_id) 
- ✅ You want simpler, more reliable queries
- ✅ You need consistent performance

### Use Native Model (OW5000) when:
- You need access to entities not available in Common Model
- You need the original OpenWorks data structure
- You're querying by the composite key fields (attribute, data_source, etc.)
- You're working with small datasets only

## Available Districts

To list available districts for each model:

```python
# List models
GET /dsdata/v1/

# List versions for a model
GET /dsdata/v1/OpenWorksCommonModel

# List districts for a model/version
GET /dsdata/v1/OpenWorksCommonModel/5000107
```

## Entity Comparison

| Native Model Entity | Common Model Entity | Key |
|--------------------|---------------------|-----|
| `Rgrid` | `SurfaceGrid` | `native_uid` |
| `Grid` | `SurfaceGrid` | `native_uid` |
| `Fgrid` | `FaultGrid` | `native_uid` |

## Recommendations

1. **Prefer Common Model** for grid data retrieval - it's more reliable and uses simpler keys
2. **Use streaming** with `stream=True` and `iter_content()` for large data
3. **Handle errors gracefully** - Native Model may fail on large grids
4. **Cache `native_uid` values** if you frequently access the same grids
