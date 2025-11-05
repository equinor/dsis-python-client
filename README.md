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
config = DSISConfig.for_native_model(
    environment=Environment.DEV,
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret",
    access_app_id="your-access-app-id",
    dsis_username="your-username",
    dsis_password="your-password",
    subscription_key_dsauth="your-dsauth-key",
    subscription_key_dsdata="your-dsdata-key"
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

# Use factory method for common model
config = DSISConfig.for_common_model(
    environment=Environment.DEV,
    tenant_id="...",
    client_id="...",
    client_secret="...",
    access_app_id="...",
    dsis_username="...",
    dsis_password="...",
    subscription_key_dsauth="...",
    subscription_key_dsdata="...",
    model_name="OpenWorksCommonModel",  # Optional, defaults to "OpenWorksCommonModel"
    model_version="1000001"  # Optional, defaults to "5000107"
)

client = DSISClient(config)

# Test connection
if client.test_connection():
    print("✓ Connected to DSIS API")

# Get data using just model and version
data = client.get_odata()

# Get Basin data for a specific district and field
data = client.get_odata(
    district_id="123",
    field="wells",
    data_table="Basin"
)

# Get Well data with field selection
data = client.get_odata(
    district_id="123",
    field="wells",
    data_table="Well",
    select="name,depth,status"
)

# Get Wellbore data with filtering
data = client.get_odata(
    district_id="123",
    field="wells",
    data_table="Wellbore",
    filter="depth gt 1000"
)

# Get WellLog data with expand (related data)
data = client.get_odata(
    district_id="123",
    field="wells",
    data_table="WellLog",
    expand="logs,completions"
)

# Refresh tokens if needed
client.refresh_authentication()
```

## Working with dsis-schemas Models

The client provides built-in support for the `dsis-schemas` package, which provides Pydantic models for DSIS data structures.

### QueryBuilder: Build OData Queries

The `QueryBuilder` provides a fluent interface for building OData queries. QueryBuilder IS the query object - no need to call `.build()`.

#### Basic Usage

```python
from dsis_client import QueryBuilder, DSISClient, DSISConfig

# Create a query with required path parameters
query = QueryBuilder(
    district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
    field="SNORRE"
).schema("Well").select("name,depth")

# Execute the query with client
client = DSISClient(config)
response = client.execute_query(query)

# Build a complex query with chaining
query = (QueryBuilder(district_id="123", field="wells")
    .schema("Well")
    .select("name", "depth", "status")
    .filter("depth gt 1000")
    .expand("wellbores"))

response = client.execute_query(query)

# Reuse builder for multiple queries
builder = QueryBuilder(district_id="123", field="wells")

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
query = (QueryBuilder(district_id="123", field="wells")
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

### `get(district_id=None, field=None, data_table=None, format_type="json", select=None, expand=None, filter=None, **extra_query)`

Make a GET request to the DSIS OData API.

Constructs the OData endpoint URL following the pattern:
`/<model_name>/<version>[/<district_id>][/<field>][/<data_table>]`

All path segments are optional and can be omitted. The `data_table` parameter refers to specific data models from dsis-schemas (e.g., "Basin", "Well", "Wellbore", "WellLog", etc.).

**Parameters:**
- `district_id`: Optional district ID for the query
- `field`: Optional field name for the query
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

# Get Basin data for a district and field
data = client.get("123", "wells", data_table="Basin")

# Get with field selection
data = client.get("123", "wells", data_table="Well", select="name,depth,status")

# Get with filtering
data = client.get("123", "wells", data_table="Well", filter="depth gt 1000")

# Get with expand (related data)
data = client.get("123", "wells", data_table="Well", expand="logs,completions")
```

### `get_odata(district_id=None, field=None, data_table=None, format_type="json", select=None, expand=None, filter=None, **extra_query)`

Convenience method for retrieving OData. Delegates to `get()` method.

**Parameters:**
- `district_id`: Optional district ID for the query
- `field`: Optional field name for the query
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

# Get Basin data for a district and field
data = client.get_odata("123", "wells", data_table="Basin")

# Get with field selection
data = client.get_odata("123", "wells", data_table="Well", select="name,depth,status")

# Get with filtering
data = client.get_odata("123", "wells", data_table="Well", filter="depth gt 1000")

# Get with expand
data = client.get_odata("123", "wells", data_table="Well", expand="logs,completions")
```

### `execute_query(query, cast=False)`

Execute a QueryBuilder query.

**Parameters:**
- `query`: QueryBuilder instance
- `cast`: If True and query has a schema class, automatically cast results to model instances (default: False)

**Returns:**
- If `cast=False`: Dictionary containing the parsed API response
- If `cast=True`: List of model instances (from response["value"])

**Raises:**
- TypeError if query is not a QueryBuilder instance
- ValueError if cast=True but query has no schema class

**Example:**
```python
from dsis_model_sdk.models.common import Basin

# Build query with QueryBuilder
query = QueryBuilder(district_id="123", field="wells").schema(Basin).select("basin_name,basin_id")

# Option 1: Get raw response
response = client.execute_query(query)
print(response)

# Option 2: Auto-cast to model instances
basins = client.execute_query(query, cast=True)
for basin in basins:
    print(basin.basin_name)
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

### `QueryBuilder(district_id, field)`

Create a new query builder instance. QueryBuilder IS the query object - no need to call `.build()`.

**Parameters:**
- `district_id`: District ID for the query (required)
- `field`: Field name for the query (required)

**Example:**
```python
# Create a query builder with required parameters
query = QueryBuilder(district_id="123", field="wells")

# Chain methods to build the query
query = QueryBuilder(district_id="123", field="wells").schema("Well").select("name,depth")
```

### `schema(schema)`

Set the schema (data table) using a name or model class.

**Parameters:**
- `schema`: Schema name (e.g., "Well", "Basin") or dsis_model_sdk model class

**Returns:** Self for chaining

**Example:**
```python
# Using schema name
query = QueryBuilder(district_id="123", field="wells").schema("Well")

# Using model class for type-safe casting
from dsis_model_sdk.models.common import Basin
query = QueryBuilder(district_id="123", field="wells").schema(Basin)
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
query = QueryBuilder(district_id="123", field="wells").schema("Well").select("name,depth")
print(query.get_query_string())
# Returns: "Well?$format=json&$select=name,depth"
```

### `reset()`

Reset the builder to initial state (clears schema, select, expand, filter, format).

Note: Does not reset district_id or field set in constructor.

**Returns:** Self for chaining

**Example:**
```python
builder = QueryBuilder(district_id="123", field="wells")
builder.schema("Well").select("name")
builder.reset()  # Clears schema and select, keeps district_id and field
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

query = QueryBuilder(district_id="123", field="wells").schema(Basin).select("basin_name,basin_id")
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
query = QueryBuilder(district_id="123", field="wells").schema(Basin).select("basin_name,basin_id,native_uid")

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
