# DSIS Python Client

A Python SDK for the DSIS (DecisionSpace Integration Server) API Management system. Provides easy access to DSIS data through Equinor's Azure API Management gateway with built-in authentication and error handling.

## Features

- **Dual-Token Authentication**: Handles both Azure AD and DSIS token acquisition automatically
- **Easy Configuration**: Simple dataclass-based configuration management
- **Error Handling**: Custom exceptions for different error scenarios
- **Logging Support**: Built-in logging for debugging and monitoring
- **Type Hints**: Full type annotations for better IDE support
- **OData Support**: Convenient methods for OData queries with full parameter support
- **dsis-schemas Integration**: Built-in support for model discovery, field inspection, and response deserialization
- **Production Ready**: Comprehensive error handling and validation

## Installation

```console
pip install dsis-client
```

## Quick Start

### Basic Usage

```python
from dsis_client import DSISClient, DSISConfig, Environment

# Configure the client for native model (OW5000)
config = DSISConfig(
    environment=Environment.DEV,
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret",
    access_app_id="your-access-app-id",
    dsis_username="your-username",
    dsis_password="your-password",
    subscription_key_dsauth="your-dsauth-key",
    subscription_key_dsdata="your-dsdata-key",
    model_name="OW5000",
    model_version="5000107",
)

# Create client and retrieve data
client = DSISClient(config)

# Get data using just model and version
data = client.get_odata()
print(data)
```

### Advanced Usage

```python
from dsis_client import DSISClient, DSISConfig, Environment

config = DSISConfig(
    environment=Environment.DEV,
    tenant_id="...",
    client_id="...",
    client_secret="...",
    access_app_id="...",
    dsis_username="...",
    dsis_password="...",
    subscription_key_dsauth="...",
    subscription_key_dsdata="...",
    model_name="OpenWorksCommonModel",
    model_version="5000107",
)

client = DSISClient(config)

# Test connection
if client.test_connection():
    print("✓ Connected to DSIS API")

# Get data using just model and version
data = client.get_odata()

# Get Basin data for a specific district and project
data = client.get_odata(
    district_id="123",
    project="wells",
    data_table="Basin"
)

# Get Well data with project selection
data = client.get_odata(
    district_id="123",
    project="wells",
    data_table="Well",
    select="name,depth,status"
)

# Get Wellbore data with filtering
data = client.get_odata(
    district_id="123",
    project="wells",
    data_table="Wellbore",
    filter="depth gt 1000"
)

# Get WellLog data with expand (related data)
data = client.get_odata(
    district_id="123",
    project="wells",
    data_table="WellLog",
    expand="logs,completions"
)

# Refresh tokens if needed
client.refresh_authentication()
```

## Working with dsis-schemas Models

The client provides built-in support for the `dsis-schemas` package, which provides Pydantic models for DSIS data structures.

### Installation

```bash
# Basic installation (metadata/OData support only)
pip install dsis-client

# With protobuf support for bulk data decoding
pip install dsis-client dsis-schemas[protobuf]
```

**Note:** The DSIS API serves data in two formats:
- **Metadata**: Via OData (JSON) - entity properties, relationships, statistics
- **Bulk Data**: Via Protocol Buffers (binary) - large arrays like horizon z-values, log curves, seismic amplitudes

Install `dsis-schemas[protobuf]` to decode binary bulk data fields.

### QueryBuilder: Build OData Queries

The `QueryBuilder` provides a fluent interface for building OData queries. QueryBuilder IS the query object - no need to call `.build()`.

#### Basic Usage

```python
from dsis_client import QueryBuilder, DSISClient, DSISConfig

# Create a query with required path parameters
query = QueryBuilder(
    district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
    project="SNORRE"
).schema("Well").select("name,depth")

# Execute the query with client
client = DSISClient(config)
response = client.execute_query(query)

# Build a complex query with chaining
query = (QueryBuilder(district_id="123", project="wells")
    .schema("Well")
    .select("name", "depth", "status")
    .filter("depth gt 1000")
    .expand("wellbores"))

response = client.execute_query(query)

# Reuse builder for multiple queries
builder = QueryBuilder(district_id="123", project="wells")

# Query 1
query1 = builder.schema("Well").select("name,depth")
response1 = client.execute_query(query1)

# Query 2 (reset builder for new query)
query2 = builder.reset().schema("Basin").select("id,name")
response2 = client.execute_query(query2)
```

#### Using Model Classes with Auto-Casting

For type-safe result casting, use model classes from `dsis_model_sdk`:

```python
from dsis_client import QueryBuilder
from dsis_model_sdk.models.common import Well, Basin, Fault

# Use schema() with model class for type-safe casting
query = (QueryBuilder(district_id="123", project="wells")
    .schema(Basin)
    .select("basin_name", "basin_id", "native_uid"))

# Option 1: Auto-cast results with execute_query
basins = client.execute_query(query, cast=True)
for basin in basins:
    print(f"Basin: {basin.basin_name}")  # Type-safe access with IDE autocomplete

# Option 2: Manual cast with client.cast_results()
response = client.execute_query(query)
basins = client.cast_results(response['value'], Basin)

# Import models directly from dsis_model_sdk
from dsis_model_sdk.models.common import Well, Fault
from dsis_model_sdk.models.native import Well as WellNative
```

### Get Model Information

```python
# Get a model class by name
Well = client.get_model_by_name("Well")
Basin = client.get_model_by_name("Basin")

# Get model from native domain
WellNative = client.get_model_by_name("Well", domain="native")

# Get field information for a model
fields = client.get_model_fields("Well")
print(fields.keys())  # All available fields
```

### Deserialize API Responses

```python
# Get data from API
response = client.get_odata("123", "wells", data_table="Well")

# Deserialize to typed model
well = client.deserialize_response(response, "Well")
print(well.well_name)  # Type-safe access with IDE support
print(well.depth)      # Automatic validation
```

### Available Models

Common models include: `Well`, `Wellbore`, `WellLog`, `Basin`, `Horizon`, `Fault`, `Seismic2D`, `Seismic3D`, and many more.

For a complete list, see the [dsis-schemas documentation](https://github.com/equinor/dsis-schemas).

### Working with Binary Bulk Data (Protobuf)

Some DSIS entities contain large binary data fields (e.g., horizon z-values, log curves, seismic amplitudes) that are served as Protocol Buffers. The `dsis-schemas` package provides decoders to work with this data.

#### Installation for Protobuf Support

```bash
# Install with protobuf support for bulk data decoding
pip install dsis-schemas[protobuf]
```

**Note:** The DSIS API serves data in two formats:
- **Metadata**: Via OData (JSON) - entity properties, relationships, statistics
- **Bulk Data**: Via Protocol Buffers (binary) - large arrays like horizon z-values, log curves, seismic amplitudes

#### Supported Bulk Data Types

- **Horizon 3D** (`HorizonData3D`) - Interpreted surface z-values
- **Log Curves** (`LogCurve`) - Well log measurements vs depth/time
- **Seismic 3D** (`SeismicDataSet3D`) - 3D seismic amplitude volume
- **Seismic 2D** (`SeismicDataSet2D`) - 2D seismic trace data
- **Tabular** - Generic tabular structures

#### Example: Decoding Horizon Data

**Option 1: Query metadata and binary data together (includes data in response)**

```python
import numpy as np
from dsis_model_sdk.models.common import HorizonData3D
from dsis_model_sdk.protobuf import decode_horizon_data
from dsis_model_sdk.utils.protobuf_decoders import horizon_to_numpy

# Step 1: Query for horizon metadata (including binary data field)
query = QueryBuilder(district_id=district_id, project=project).schema(HorizonData3D).select("horizon_name,horizon_mean,horizon_mean_unit,data,native_uid")
response = client.executeQuery(query)

# Step 2: Cast to model and decode binary data field
horizon = HorizonData3D.from_dict(response['value'][0])
print(f"Horizon: {horizon.horizon_name}")
print(f"Mean depth: {horizon.horizon_mean} {horizon.horizon_mean_unit}")

# Step 3: Decode binary bulk data field
if horizon.data:
    decoded = decode_horizon_data(horizon.data)

    # Step 4: Convert to NumPy array for analysis
    array, metadata = horizon_to_numpy(decoded)

    print(f"Grid shape: {array.shape}")
    print(f"Data coverage: {(~np.isnan(array)).sum() / array.size * 100:.1f}%")

    # Use the data
    valid_data = array[~np.isnan(array)]
    print(f"Depth range: {np.min(valid_data):.2f} - {np.max(valid_data):.2f}")
```

**Option 2: Fetch binary data separately (more efficient for large data)**

```python
import numpy as np
from dsis_model_sdk.models.common import HorizonData3D
from dsis_model_sdk.protobuf import decode_horizon_data
from dsis_model_sdk.utils.protobuf_decoders import horizon_to_numpy

# Step 1: Query for horizon metadata only (exclude large binary data field)
query = QueryBuilder(district_id=district_id, project=project).schema(HorizonData3D).select("horizon_name,horizon_mean,horizon_mean_unit,native_uid")
horizons = list(client.execute_query(query, cast=True))

# Step 2: Fetch binary data separately for specific horizon
horizon = horizons[0]
print(f"Horizon: {horizon.horizon_name}")

# Fetch binary data - pass entity object directly!
binary_data = client.get_bulk_data(
    schema=HorizonData3D,
    native_uid=horizon,  # Pass entity object directly
    query=query  # Automatically extracts district_id and project
)

# Step 3: Decode binary bulk data
decoded = decode_horizon_data(binary_data)

# Step 4: Convert to NumPy array for analysis
array, metadata = horizon_to_numpy(decoded)

print(f"Grid shape: {array.shape}")
print(f"Data coverage: {(~np.isnan(array)).sum() / array.size * 100:.1f}%")

# Use the data
valid_data = array[~np.isnan(array)]
print(f"Depth range: {np.min(valid_data):.2f} - {np.max(valid_data):.2f}")
```

#### Example: Decoding Log Curve Data

```python
from dsis_model_sdk.protobuf import decode_log_curves
from dsis_model_sdk.utils.protobuf_decoders import log_curve_to_dict

# Query for log curve metadata (exclude binary data for efficiency)
query = QueryBuilder(district_id=district_id, project=project).schema("LogCurve").select("log_curve_name,native_uid")
log_curves = list(client.execute_query(query, max_pages=1))

# Fetch binary data for specific log curve - pass entity object directly!
log_curve = log_curves[0]
binary_data = client.get_bulk_data(
    schema="LogCurve",
    native_uid=log_curve,  # Pass entity object directly
    query=query  # Automatically extracts district_id and project
)

# Decode log curve binary data
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

#### Example: Decoding Seismic Data

```python
import numpy as np
from dsis_model_sdk.models.common import SeismicDataSet3D
from dsis_model_sdk.protobuf import decode_seismic_float_data
from dsis_model_sdk.utils.protobuf_decoders import seismic_3d_to_numpy

# Query for seismic dataset metadata (exclude binary data - it's very large!)
query = QueryBuilder(district_id=district_id, project=project).schema(SeismicDataSet3D).select("seismic_dataset_name,native_uid")
seismic_datasets = list(client.execute_query(query, cast=True))

# Fetch binary data separately for specific seismic dataset
seismic = seismic_datasets[0]
print(f"Fetching seismic data for: {seismic.seismic_dataset_name}")

# For large datasets, use streaming to avoid loading everything into memory at once
chunks = []
for chunk in client.get_bulk_data_stream(
    schema=SeismicDataSet3D,
    native_uid=seismic,  # Pass entity object directly
    query=query,  # Automatically extracts district_id and project
    chunk_size=10*1024*1024  # 10MB chunks (DSIS recommended)
):
    chunks.append(chunk)
    print(f"Downloaded {len(chunk):,} bytes")

# Combine chunks and decode
binary_data = b''.join(chunks)
decoded = decode_seismic_float_data(binary_data)
array, metadata = seismic_3d_to_numpy(decoded)

print(f"Volume shape: {array.shape}")  # (traces_i, traces_j, samples_k)
print(f"Memory size: {array.nbytes / 1024 / 1024:.2f} MB")
print(f"Amplitude range: {np.min(array):.2f} to {np.max(array):.2f}")

# Extract a single trace
trace = array[100, 100, :]
print(f"Trace samples: {len(trace)}")
```

#### Example: Decoding Surface Grid Data

Surface grids (e.g., `SurfaceGrid` in OpenWorksCommonModel) use the LGCStructure protobuf format, which is a generic tabular data structure from Landmark Graphics Corporation. Each grid is represented as a collection of elements (columns), where each element contains an array of values.

```python
from io import BytesIO
from dsis_model_sdk.protobuf import decode_lgc_structure, LGCStructure_pb2

# Query for surface grid metadata
query = QueryBuilder(district_id=district_id, project=project).schema("SurfaceGrid").select("native_uid,grid_name")
grids = list(client.execute_query(query, cast=True, max_pages=1))

# Fetch binary data for a specific grid
grid = grids[0]
print(f"Downloading grid: {grid.grid_name or grid.native_uid}")

# Build the endpoint URL (SurfaceGrid uses /$value suffix)
endpoint_path = f"{config.model_name}/{config.model_version}/{district_id}/{project}/SurfaceGrid('{grid.native_uid}')/$value"
full_url = f"{config.data_endpoint}/{endpoint_path}"

# Get the binary data
headers = client.auth.get_auth_headers()
headers["Accept"] = "application/json"
response = client._session.get(full_url, headers=headers)
data = response.content

print(f"Downloaded {len(data):,} bytes")

# LGCStructure data is length-prefixed with varint encoding
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

# Parse the length-prefixed message
stream = BytesIO(data)
size = read_varint(stream)
message_data = stream.read(size)

# Decode the LGCStructure
lgc = decode_lgc_structure(message_data)

print(f"Structure name: {lgc.structName}")
print(f"Number of elements: {len(lgc.elements)}")

# Process grid elements (columns)
for i, el in enumerate(lgc.elements[:5]):  # Show first 5 elements
    data_type = LGCStructure_pb2.LGCStructure.LGCElement.DataType.Name(el.dataType)
    
    if el.dataType == LGCStructure_pb2.LGCStructure.LGCElement.DataType.FLOAT:
        values = el.data_float
    elif el.dataType == LGCStructure_pb2.LGCStructure.LGCElement.DataType.DOUBLE:
        values = el.data_double
    elif el.dataType == LGCStructure_pb2.LGCStructure.LGCElement.DataType.INT:
        values = el.data_int
    else:
        values = []
    
    print(f"Element {i}: '{el.elementName}', Type: {data_type}, Values: {len(values):,}")

# For a typical surface grid:
# - Each element represents a row or column in the grid
# - Values are typically FLOAT type representing Z-values (elevation/depth)
# - Missing/null values are often represented as -99999.0 or similar sentinel values
```

**Important Notes:**
- Binary bulk data fields are typically large. Make sure to:
  - Select only the entities you need with appropriate filters
  - Consider memory constraints when working with seismic volumes
  - Use NumPy for efficient array operations
- The `data` field in models like `HorizonData3D`, `LogCurve`, and `SeismicDataSet3D` contains the binary protobuf data
- Always check if the `data` field exists before attempting to decode it
- **API Endpoint Format**: The binary data endpoint is `/{Schema}('{native_uid}')/data` (no `/$value` suffix)
- **Accept Header**: The API returns binary protobuf data with `Accept: application/json` header (not `application/octet-stream`)

## Configuration

### Environment

The client supports three environments:

- `Environment.DEV` - Development environment
- `Environment.QA` - Quality Assurance environment
- `Environment.PROD` - Production environment

### Configuration Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `environment` | Yes | - | Target environment (DEV, QA, or PROD) |
| `tenant_id` | Yes | - | Azure AD tenant ID |
| `client_id` | Yes | - | Azure AD client/application ID |
| `client_secret` | Yes | - | Azure AD client secret |
| `access_app_id` | Yes | - | Azure AD access application ID for token resource |
| `dsis_username` | Yes | - | DSIS username for authentication |
| `dsis_password` | Yes | - | DSIS password for authentication |
| `subscription_key_dsauth` | Yes | - | APIM subscription key for dsauth endpoint |
| `subscription_key_dsdata` | Yes | - | APIM subscription key for dsdata endpoint |
| `model_name` | Yes | - | DSIS model name (e.g., "OW5000" or "OpenWorksCommonModel") |
| `model_version` | No | "5000107" | Model version |
| `dsis_site` | No | "qa" | DSIS site header |

## Error Handling

The client provides specific exception types for different error scenarios:

```python
from dsis_client import (
    DSISClient,
    DSISConfig,
    DSISAuthenticationError,
    DSISAPIError,
    DSISConfigurationError
)

try:
    client = DSISClient(config)
    data = client.get_odata("OW5000")
except DSISConfigurationError as e:
    print(f"Configuration error: {e}")
except DSISAuthenticationError as e:
    print(f"Authentication failed: {e}")
except DSISAPIError as e:
    print(f"API request failed: {e}")
```

### Exception Types

- `DSISException` - Base exception for all DSIS client errors
- `DSISConfigurationError` - Raised when configuration is invalid or incomplete
- `DSISAuthenticationError` - Raised when authentication fails (Azure AD or DSIS token)
- `DSISAPIError` - Raised when an API request fails

## Logging

The client includes built-in logging support. Enable debug logging to see detailed information:

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("dsis_client")

# Now use the client
client = DSISClient(config)
data = client.get_odata("OW5000")
```

## API Methods

### `get(district_id=None, project=None, data_table=None, format_type="json", select=None, expand=None, filter=None, **extra_query)`

Make a GET request to the DSIS OData API.

Constructs the OData endpoint URL following the pattern:
`/<model_name>/<version>[/<district_id>][/<project>][/<data_table>]`

All path segments are optional and can be omitted. The `data_table` parameter refers to specific data models from dsis-schemas (e.g., "Basin", "Well", "Wellbore", "WellLog", etc.).

**Parameters:**
- `district_id`: Optional district ID for the query
- `project`: Optional project name for the query
- `data_table`: Optional data table/model name (e.g., "Basin", "Well", "Wellbore"). If None, uses configured model_name
- `format_type`: Response format (default: "json")
- `select`: OData $select parameter for field selection (comma-separated field names)
- `expand`: OData $expand parameter for related data (comma-separated related entities)
- `filter`: OData $filter parameter for filtering (OData filter expression)
- `**extra_query`: Additional OData query parameters

**Returns:** Dictionary containing the parsed API response

**Example:**
```python
# Get using just model and version
data = client.get()

# Get Basin data for a district and project
data = client.get("123", "SNORRE", data_table="Basin")

# Get with field selection
data = client.get("123", "SNORRE", data_table="Well", select="name,depth,status")

# Get with filtering
data = client.get("123", "SNORRE", data_table="Well", filter="depth gt 1000")

# Get with expand (related data)
data = client.get("123", "SNORRE", data_table="Well", expand="logs,completions")
```

### `get_odata(district_id=None, project=None, data_table=None, format_type="json", select=None, expand=None, filter=None, **extra_query)`

Convenience method for retrieving OData. Delegates to `get()` method.

**Parameters:**
- `district_id`: Optional district ID for the query
- `project`: Optional project name for the query
- `data_table`: Optional data table/model name (e.g., "Basin", "Well", "Wellbore"). If None, uses configured model_name
- `format_type`: Response format (default: "json")
- `select`: OData $select parameter for field selection (comma-separated field names)
- `expand`: OData $expand parameter for related data (comma-separated related entities)
- `filter`: OData $filter parameter for filtering (OData filter expression)
- `**extra_query`: Additional OData query parameters

**Returns:** Dictionary containing the parsed OData response

**Example:**
```python
# Get using just model and version
data = client.get_odata()

# Get Basin data for a district and project
data = client.get_odata("123", "SNORRE", data_table="Basin")

# Get with field selection
data = client.get_odata("123", "SNORRE", data_table="Well", select="name,depth,status")

# Get with filtering
data = client.get_odata("123", "SNORRE", data_table="Well", filter="depth gt 1000")

# Get with expand
data = client.get_odata("123", "SNORRE", data_table="Well", expand="logs,completions")
```

### `execute_query(query, cast=False, max_pages=-1)`

Execute a QueryBuilder query.

**Parameters:**
- `query`: QueryBuilder instance
- `cast`: If True and query has a schema class, automatically cast results to model instances (default: False)
- `max_pages`: Maximum number of pages to fetch. -1 (default) fetches all pages

**Returns:**
- Generator that yields items from the result pages (or model instances if cast=True)

**Raises:**
- TypeError if query is not a QueryBuilder instance
- ValueError if cast=True but query has no schema class

**Example:**
```python
from dsis_model_sdk.models.common import Basin

# Build query with QueryBuilder
query = QueryBuilder(district_id="123", project="SNORRE").schema(Basin).select("basin_name,basin_id")

# Option 1: Iterate over results (memory efficient)
for basin in client.execute_query(query, cast=True):
    print(basin.basin_name)

# Option 2: Collect all results into a list
basins = list(client.execute_query(query, cast=True))
print(f"Total: {len(basins)} basins")

# Option 3: Fetch only first page
first_page = list(client.execute_query(query, cast=True, max_pages=1))
```

### `get_bulk_data(schema, native_uid, district_id=None, project=None, data_field="data")`

Fetch binary bulk data (protobuf) for a specific entity.

The DSIS API serves large binary data fields (horizon z-values, log curves, seismic amplitudes) as Protocol Buffers via a special OData endpoint: `/{schema}('{native_uid}')/{data_field}/$value`

**Parameters:**
- `schema`: Schema name (e.g., "HorizonData3D", "LogCurve", "SeismicDataSet3D")
- `native_uid`: The native_uid of the entity
- `district_id`: Optional district ID (if required by API)
- `project`: Optional project name (if required by API)
- `data_field`: Name of the binary data field (default: "data")

**Returns:**
- Binary protobuf data as bytes

**Raises:**
- DSISAPIError if the API request fails

**Example:**
```python
from dsis_model_sdk.protobuf import decode_horizon_data

# Fetch binary data for a specific horizon
binary_data = client.get_bulk_data(
    schema="HorizonData3D",
    native_uid="horizon_123",
    district_id="123",
    project="SNORRE"
)

# Decode the protobuf data
decoded = decode_horizon_data(binary_data)
```

### `get_model_by_name(model_name, domain="common")`

Get a dsis-schemas model class by name.

**Parameters:**
- `model_name`: Name of the model (e.g., "Well", "Basin", "Wellbore")
- `domain`: Domain to search in - "common" or "native" (default: "common")

**Returns:** The model class if found, None otherwise

**Raises:** ImportError if dsis_schemas package is not installed

**Example:**
```python
Well = client.get_model_by_name("Well")
WellNative = client.get_model_by_name("Well", domain="native")
```

### `get_model_fields(model_name, domain="common")`

Get field information for a dsis-schemas model.

**Parameters:**
- `model_name`: Name of the model (e.g., "Well", "Basin")
- `domain`: Domain to search in - "common" or "native" (default: "common")

**Returns:** Dictionary of field names and their information

**Raises:** ImportError if dsis_schemas package is not installed

**Example:**
```python
fields = client.get_model_fields("Well")
print(fields.keys())  # All available fields
```

### `deserialize_response(response, model_name, domain="common")`

Deserialize API response to a dsis-schemas model instance.

**Parameters:**
- `response`: API response dictionary
- `model_name`: Name of the model to deserialize to (e.g., "Well", "Basin")
- `domain`: Domain to search in - "common" or "native" (default: "common")

**Returns:** Deserialized model instance

**Raises:** ImportError if dsis_schemas package is not installed, ValueError if deserialization fails

**Example:**
```python
response = client.get_odata("123", "wells", data_table="Well")
well = client.deserialize_response(response, "Well")
print(well.well_name)  # Type-safe access
```

## QueryBuilder API

### `QueryBuilder(district_id, project)`

Create a new query builder instance. QueryBuilder IS the query object - no need to call `.build()`.

**Parameters:**
- `district_id`: District ID for the query (required)
- `project`: Project name for the query (required)

**Example:**
```python
# Create a query builder with required parameters
query = QueryBuilder(district_id="123", project="SNORRE")

# Chain methods to build the query
query = QueryBuilder(district_id="123", project="SNORRE").schema("Well").select("name,depth")
```

### `schema(schema)`

Set the schema (data table) using a name or model class.

**Parameters:**
- `schema`: Schema name (e.g., "Well", "Basin") or dsis_model_sdk model class

**Returns:** Self for chaining

**Example:**
```python
# Using schema name
query = QueryBuilder(district_id="123", project="SNORRE").schema("Well")

# Using model class for type-safe casting
from dsis_model_sdk.models.common import Basin
query = QueryBuilder(district_id="123", project="SNORRE").schema(Basin)
```

### `select(*fields)`

Add fields to the $select parameter.

**Parameters:**
- `*fields`: Field names to select (can be comma-separated or individual)

**Returns:** Self for chaining

**Example:**
```python
builder.select("name", "depth", "status")
builder.select("name,depth,status")
```

### `expand(*relations)`

Add relations to the $expand parameter.

**Parameters:**
- `*relations`: Relation names to expand (can be comma-separated or individual)

**Returns:** Self for chaining

**Example:**
```python
builder.expand("wells", "horizons")
builder.expand("wells,horizons")
```

### `filter(filter_expr)`

Set the $filter parameter.

**Parameters:**
- `filter_expr`: OData filter expression (e.g., "depth gt 1000")

**Returns:** Self for chaining

**Example:**
```python
builder.filter("depth gt 1000")
builder.filter("name eq 'Well-1'")
```

### `get_query_string()`

Get the full OData query string for this query.

**Returns:** Full query string (e.g., "Well?$format=json&$select=name,depth")

**Raises:** ValueError if schema is not set

**Example:**
```python
query = QueryBuilder(district_id="123", project="SNORRE").schema("Well").select("name,depth")
print(query.get_query_string())
# Returns: "Well?$format=json&$select=name,depth"
```

### `reset()`

Reset the builder to initial state (clears schema, select, expand, filter, format).

Note: Does not reset district_id or project set in constructor.

**Returns:** Self for chaining

**Example:**
```python
builder = QueryBuilder(district_id="123", project="SNORRE")
builder.schema("Well").select("name")
builder.reset()  # Clears schema and select, keeps district_id and project
builder.schema("Basin").select("id")  # Reuse for new query
```

## DSISClient Casting Methods

### `cast_results(results, schema_class)`

Cast API response items to model instances.

**Parameters:**
- `results`: List of dictionaries from API response (typically response["value"])
- `schema_class`: Pydantic model class to cast to (e.g., Basin, Well)

**Returns:** List of model instances

**Raises:** ValidationError if any result doesn't match schema

**Example:**
```python
from dsis_model_sdk.models.common import Basin

query = QueryBuilder(district_id="123", project="SNORRE").schema(Basin).select("basin_name,basin_id")
response = client.execute_query(query)
basins = client.cast_results(response['value'], Basin)
for basin in basins:
    print(f"Basin: {basin.basin_name}")
```

### Result Casting with QueryBuilder

QueryBuilder supports automatic casting when used with model classes:

```python
from dsis_client import QueryBuilder, DSISClient
from dsis_model_sdk.models.common import Basin

# Set schema with model class
query = QueryBuilder(district_id="123", project="SNORRE").schema(Basin).select("basin_name,basin_id,native_uid")

# Option 1: Auto-cast with executeQuery
basins = client.executeQuery(query, cast=True)
for basin in basins:
    print(f"Basin: {basin.basin_name}")  # Type-safe access with IDE autocomplete

# Option 2: Manual cast with client.cast_results()
response = client.executeQuery(query)
basins = client.cast_results(response['value'], Basin)
```

### `test_connection()`

Test the connection to the DSIS API.

**Returns:** True if connection is successful, False otherwise

**Example:**
```python
if client.test_connection():
    print("✓ Connected to DSIS API")
```

### `refresh_authentication()`

Refresh both Azure AD and DSIS tokens.

**Example:**
```python
client.refresh_authentication()
```

## Contributing

See [contributing guidelines](https://github.com/equinor/dsis-python-client/blob/main/CONTRIBUTING.md).

## License

This project is licensed under the terms of the [MIT license](https://github.com/equinor/dsis-python-client/blob/main/LICENSE).
