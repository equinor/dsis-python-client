# Advanced Serialization with dsis-model-sdk

This guide shows how to deserialize DSIS API responses using `dsis-model-sdk` directly.

For most data access patterns, prefer `QueryBuilder` + `DSISClient.execute_query()` and then:

- Instantiate models directly (`Well(**item)`) for best performance, or
- Use `cast_results()` for convenience when you already have a list of dict items.

## Installation

```bash
pip install dsis-model-sdk
```

## Quick Start

The client provides `cast_results()` for convenience, but you can also use `dsis-model-sdk` directly:

```python
from dsis_client import DSISClient, DSISConfig, QueryBuilder
from dsis_model_sdk.models.common import Basin

# Example district_id for Common Model + SV4TSTA database
district_id = "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA"
project = "SNORRE"

# Get data from API
client = DSISClient(config)
query = QueryBuilder(district_id=district_id, project=project).schema(Basin)
items = list(client.execute_query(query))

# Deserialize using dsis-model-sdk directly (recommended - fastest)
basins = [Basin(**item) for item in items]

# Or use client's helper
from dsis_client.api.models import cast_results
basins = cast_results(items, Basin)
```

## Three Ways to Deserialize

### 1. Direct Instantiation (Recommended - Fastest)

```python
from dsis_model_sdk.models.common import Well

query = QueryBuilder(district_id=district_id, project=project).schema(Well)
wells = [Well(**item) for item in client.execute_query(query)]
```

### 2. Using Client's cast_results Helper

```python
from dsis_client.api.models import cast_results, get_schema_by_name

# Option A: Import schema class directly
from dsis_model_sdk.models.common import Well

query = QueryBuilder(district_id=district_id, project=project).schema(Well)
items = list(client.execute_query(query))
wells = cast_results(items, Well)

# Option B: Get schema dynamically
Well = get_schema_by_name("Well")
wells = cast_results(items, Well)
```

### 3. Using dsis-model-sdk's deserialize_from_json

Only use this if you have JSON strings (it's slower):

```python
from dsis_model_sdk import deserialize_from_json
from dsis_model_sdk.models.common import Basin
import json

query = QueryBuilder(district_id=district_id, project=project).schema(Basin)
items = list(client.execute_query(query))

basins = []
for item in items:
    json_str = json.dumps(item)  # Convert to JSON string
    basin = deserialize_from_json(json_str, Basin)
    basins.append(basin)
```

## With Error Handling

```python
from dsis_model_sdk.models.common import Fault
from pydantic import ValidationError

query = QueryBuilder(district_id=district_id, project=project).schema(Fault)
items = list(client.execute_query(query))

faults = []
for idx, item in enumerate(items):
    try:
        faults.append(Fault(**item))
    except ValidationError as e:
        print(f"Skipping invalid fault at index {idx}: {e}")

print(f"Successfully deserialized {len(faults)} faults")
```

## Using QueryBuilder with Auto-Cast

The easiest approach for most cases:

```python
from dsis_client import QueryBuilder
from dsis_model_sdk.models.common import Basin

query = QueryBuilder(district_id=district_id, project="SNORRE").schema(Basin)
for basin in client.execute_query(query, cast=True):
    print(basin)
```

## Bulk/protobuf payloads

Bulk payload decoding (including SurfaceGrid/LGCStructure and media read-links like `/$value`) is
documented in [Working with Binary Data](working-with-binary-data.md).

## Performance Comparison

| Method | Speed | Use When |
|--------|-------|----------|
| `Basin(**item)` | Fastest | Production, large datasets |
| `cast_results(items, Basin)` | Fast | You want convenience |
| `deserialize_from_json(json_str, Basin)` | Slower | You have JSON strings |
| QueryBuilder with `cast=True` | Fast | You want model instances |

**Recommendation**: Use direct instantiation (`Basin(**item)`) for best performance.

## See Also

- [Getting Started Guide](getting-started.md)
- [dsis-model-sdk Documentation](https://github.com/equinor/dsis-model-sdk)
