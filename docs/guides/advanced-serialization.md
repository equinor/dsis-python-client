# Advanced Serialization with dsis-model-sdk

This guide shows how to deserialize DSIS API responses using `dsis-model-sdk` directly.

## Installation

```bash
pip install dsis-model-sdk
```

## Quick Start

The client provides `cast_results()` for convenience, but you can also use `dsis-model-sdk` directly:

```python
from dsis_client import DSISClient, DSISConfig, QueryBuilder
from dsis_model_sdk.models.common import Basin

# Get data from API
client = DSISClient(config)
response = client.get(district_id="123", field="SNORRE", schema="Basin")

# Deserialize using dsis-model-sdk directly (recommended - fastest)
basins = [Basin(**item) for item in response["value"]]

# Or use client's helper
from dsis_client.api.models import cast_results
basins = cast_results(response["value"], Basin)
```

## Three Ways to Deserialize

### 1. Direct Instantiation (Recommended - Fastest)

```python
from dsis_model_sdk.models.common import Well

response = client.get(district_id="123", field="SNORRE", schema="Well")
wells = [Well(**item) for item in response["value"]]
```

### 2. Using Client's cast_results Helper

```python
from dsis_client.api.models import cast_results, get_schema_by_name

# Option A: Import schema class directly
from dsis_model_sdk.models.common import Well
wells = cast_results(response["value"], Well)

# Option B: Get schema dynamically
Well = get_schema_by_name("Well")
wells = cast_results(response["value"], Well)
```

### 3. Using dsis-model-sdk's deserialize_from_json

Only use this if you have JSON strings (it's slower):

```python
from dsis_model_sdk import deserialize_from_json
from dsis_model_sdk.models.common import Basin
import json

response = client.get(district_id="123", field="SNORRE", schema="Basin")
basins = []
for item in response["value"]:
    json_str = json.dumps(item)  # Convert to JSON string
    basin = deserialize_from_json(json_str, Basin)
    basins.append(basin)
```

## With Error Handling

```python
from dsis_model_sdk.models.common import Fault
from pydantic import ValidationError

response = client.get(district_id="123", field="SNORRE", schema="Fault")

faults = []
for idx, item in enumerate(response["value"]):
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

query = QueryBuilder(district_id="123", field="SNORRE").schema(Basin)
basins = client.execute_query(query, cast=True)  # Returns list of Basin objects
```

## Performance Comparison

| Method | Speed | Use When |
|--------|-------|----------|
| `Basin(**item)` | ⚡ Fastest | Production, large datasets |
| `cast_results(items, Basin)` | ⚡ Fast | You want convenience |
| `deserialize_from_json(json_str, Basin)` | 🐌 Slower | You have JSON strings |
| QueryBuilder with `cast=True` | ⚡ Fast | Building queries |

**Recommendation**: Use direct instantiation (`Basin(**item)`) for best performance.

## See Also

- [Getting Started Guide](getting-started.md)
- [dsis-model-sdk Documentation](https://github.com/equinor/dsis-model-sdk)
