# DSIS Python Client

A Python SDK for the DSIS (Drilling & Well Services Information System) API Management system. Provides easy access to DSIS data through Equinor's Azure API Management gateway with built-in authentication and error handling.

## Features

- **Dual-Token Authentication**: Handles both Azure AD and DSIS token acquisition automatically
- **Easy Configuration**: Simple dataclass-based configuration management
- **Error Handling**: Custom exceptions for different error scenarios
- **Logging Support**: Built-in logging for debugging and monitoring
- **Type Hints**: Full type annotations for better IDE support
- **OData Support**: Convenient methods for OData queries
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

# Get all records
data = client.get_odata()

# Get specific record
data = client.get_odata("5000107")

# Get with field selection
data = client.get_odata(select="field1,field2,field3")

# Get with custom query parameters
data = client.get_odata(filter="field1 eq 'value'")

# Refresh tokens if needed
client.refresh_authentication()
```

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

### `get(*path_segments, format_type="json", select=None, params=None, **extra_query)`

Make a GET request to the DSIS API using the configured model and version.

**Parameters:**
- `*path_segments`: Additional path segments after model_name/model_version
- `format_type`: Response format (default: "json")
- `select`: OData $select parameter for field selection
- `params`: Dictionary of additional query parameters
- `**extra_query`: Additional query parameters as keyword arguments

**Returns:** Dictionary containing the parsed API response

**Example:**
```python
# Get all records
data = client.get()

# Get specific record
data = client.get("5000107")

# Get with field selection
data = client.get(select="field1,field2")
```

### `get_odata(record_id=None, format_type="json", **query)`

Convenience method for retrieving OData using the configured model and version.

**Parameters:**
- `record_id`: Optional record ID to retrieve a specific record
- `format_type`: Response format (default: "json")
- `**query`: Additional OData query parameters

**Returns:** Dictionary containing the parsed OData response

**Example:**
```python
# Get all records
data = client.get_odata()

# Get specific record
data = client.get_odata("5000107")

# Get with field selection
data = client.get_odata(select="field1,field2")
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
