# DSIS Python Client

A Python SDK for the DSIS (Drilling & Well Services Information System) API Management system. Provides easy access to DSIS data through Equinor's Azure API Management gateway with built-in authentication and error handling.

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

The `QueryBuilder` provides a fluent interface for building OData queries with validation against dsis-schemas models. It returns a `DsisQuery` object that can be directly executed with `client.executeQuery()`.

```python
from dsis_client import QueryBuilder, DSISClient, DSISConfig

# Create a builder with path parameters
builder = QueryBuilder(
    district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
    field="SNORRE"
)

# Build a simple query
query = builder.data_table("Well").select("name,depth").build()
# Returns: DsisQuery object ready for execution

# Build a complex query
query = (QueryBuilder(district_id="123", field="wells")
    .data_table("Well")
    .select("name", "depth", "status")
    .filter("depth gt 1000")
    .expand("wellbores")
    .build())

# Execute the query with client
client = DSISClient(config)
response = client.executeQuery(query)

# Reuse builder for multiple queries
builder = QueryBuilder(district_id="123", field="wells")

# Query 1
query1 = builder.data_table("Well").select("name,depth").build()
response1 = client.executeQuery(query1)

# Query 2 (reset builder for new query)
query2 = builder.reset().data_table("Basin").select("id,name").build()
response2 = client.executeQuery(query2)

# List available models
models = QueryBuilder.list_available_models("common")
print(models)  # ['Well', 'Basin', 'Wellbore', ...]

# Use native domain models
native_builder = QueryBuilder(domain="native", district_id="123", field="wells")
native_query = native_builder.data_table("Well").build()
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

### `executeQuery(query)`

Execute a DsisQuery built with QueryBuilder.

**Parameters:**
- `query`: DsisQuery object (returned from QueryBuilder.build())

**Returns:** Dictionary containing the parsed API response

**Raises:** TypeError if query is not a DsisQuery instance

**Example:**
```python
# Build query with QueryBuilder
query = QueryBuilder(district_id="123", field="wells").data_table("Well").select("name,depth").build()

# Execute the query
response = client.executeQuery(query)
print(response)
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

### `QueryBuilder(domain="common", district_id=None, field=None)`

Create a new query builder instance.

**Parameters:**
- `domain`: Domain for models - "common" or "native" (default: "common")
- `district_id`: Optional district ID for the query (can be overridden in build())
- `field`: Optional field name for the query (can be overridden in build())

**Example:**
```python
# Basic builder
builder = QueryBuilder()

# Builder with path parameters
builder = QueryBuilder(district_id="123", field="wells")

# Native domain builder
native_builder = QueryBuilder(domain="native", district_id="123", field="wells")
```

### `data_table(table_name, validate=True)`

Set the data table (model) for the query.

**Parameters:**
- `table_name`: Data table name (e.g., "Well", "Basin", "Wellbore")
- `validate`: If True, validates that the model exists (default: True)

**Returns:** Self for chaining

**Raises:** ValueError if validate=True and model is not found

**Example:**
```python
builder.data_table("Well")
builder.data_table("Basin", validate=False)
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

### `build(district_id=None, field=None)`

Build a DsisQuery object ready for execution with client.executeQuery().

**Parameters:**
- `district_id`: Optional district ID (overrides constructor value if provided)
- `field`: Optional field name (overrides constructor value if provided)

**Returns:** DsisQuery object with query string and path parameters

**Raises:** ValueError if data_table is not set

**Example:**
```python
# Using path parameters from constructor
builder = QueryBuilder(district_id="123", field="wells")
query = builder.data_table("Well").select("name").build()
# Returns: DsisQuery with district_id="123", field="wells"

# Overriding path parameters at build time
builder = QueryBuilder()
query = builder.data_table("Well").select("name").build(
    district_id="456",
    field="logs"
)
# Returns: DsisQuery with district_id="456", field="logs"

# Execute with client
response = client.executeQuery(query)
```

### `build_query_string()`

Build just the query parameters part (without data_table).

**Returns:** Query string (e.g., "$format=json&$select=name,depth")

**Example:**
```python
query_str = builder.select("name").filter("depth gt 1000").build_query_string()
# Returns: "$format=json&$select=name&$filter=depth gt 1000"
```

### `list_available_models(domain="common")`

List all available models in a domain.

**Parameters:**
- `domain`: Domain - "common" or "native" (default: "common")

**Returns:** List of available model names

**Raises:** ImportError if dsis_schemas is not installed

**Example:**
```python
models = QueryBuilder.list_available_models("common")
print(models)  # ['Well', 'Basin', 'Wellbore', ...]
```

### `reset()`

Reset the builder to initial state (clears data_table, select, expand, filter, format).

Note: Does not reset domain, district_id, or field set in constructor.

**Returns:** Self for chaining

**Example:**
```python
builder.reset()
```

## DsisQuery

The `DsisQuery` class encapsulates a complete OData query with path parameters. It is returned by `QueryBuilder.build()` and passed to `client.executeQuery()`.

### Properties

- `query_string`: The OData query string (e.g., "Well?$format=json&$select=name,depth")
- `district_id`: Optional district ID for the query
- `field`: Optional field name for the query
- `data_table`: The data table name extracted from the query string

### Example

```python
from dsis_client import QueryBuilder, DsisQuery

# Create a DsisQuery using QueryBuilder
query = QueryBuilder(district_id="123", field="wells").data_table("Well").select("name").build()

# Access properties
print(query.query_string)  # "Well?$format=json&$select=name"
print(query.district_id)   # "123"
print(query.field)         # "wells"
print(query.data_table)    # "Well"

# Execute with client
response = client.executeQuery(query)
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
