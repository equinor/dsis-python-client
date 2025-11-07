# QueryBuilder Guide

Complete guide to using QueryBuilder for flexible DSIS data queries.

## Overview

`QueryBuilder` provides a fluent API for constructing OData queries with type safety and automatic result casting when used with `dsis_model_sdk`.

## Basic Configuration

Use environment variables for configuration (recommended). Set these in your CI/infra or a local `.env` file and load them with `python-dotenv`.

```python
import os
from dotenv import load_dotenv

load_dotenv()

config = DSISConfig.for_native_model(
    environment=Environment[os.getenv("ENVIRONMENT", "DEV")],
    tenant_id=os.getenv("TENANT_ID"),
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    access_app_id=os.getenv("ACCESS_APP_ID"),
    dsis_username=os.getenv("DSIS_USERNAME"),
    dsis_password=os.getenv("DSIS_PASSWORD"),
    subscription_key_dsauth=os.getenv("SUBSCRIPTION_KEY_DSAUTH"),
    subscription_key_dsdata=os.getenv("SUBSCRIPTION_KEY_DSDATA"),
)
```

## QueryBuilder Basics

QueryBuilder requires `district_id` and `field` parameters, then builds the query using method chaining.

### Simple Query with String Schema

```python
# Build query - QueryBuilder IS the query object (no .build() needed)
query = (
    QueryBuilder(district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA", field="SNORRE")
    .schema("Fault")
    .select("fault_id,fault_type,fault_name")
    .filter("fault_type eq 'NORMAL'")
)

# Execute query
response = client.execute_query(query)
items = response.get("value", [])
```

### Type-Safe Query with Model Class

```python
from dsis_model_sdk.models.common import Basin

# Build query with model class for automatic type safety
query = (
    QueryBuilder(district_id="your-district-id", field="your-field")
    .schema(Basin)
    .select("basin_name,basin_id,native_uid")
)

# Auto-cast results to Basin instances
basins = client.execute_query(query, cast=True)

# Access typed properties
for basin in basins:
    print(f"Basin: {basin.basin_name} (ID: {basin.basin_id})")
```

## QueryBuilder Methods

### schema()

Set the data schema (table) to query.

```python
# String schema name
query = QueryBuilder(district_id=dist, field=fld).schema("Well")

# Model class (enables type-safe casting)
from dsis_model_sdk.models.native import Well
query = QueryBuilder(district_id=dist, field=fld).schema(Well)
```

### select()

Choose specific fields to retrieve.

```python
# Single field
query.select("well_name")

# Multiple fields (comma-separated)
query.select("well_name,well_uwi,spud_date")

# Chain multiple selects (they concatenate)
query.select("well_name").select("well_uwi")
```

### filter()

Apply OData filter expressions.

```python
# Simple equality
query.filter("well_type eq 'Producer'")

# Comparison operators
query.filter("depth gt 1000")
query.filter("depth lt 5000")

# Logical operators
query.filter("well_type eq 'Producer' and depth gt 1000")

# String functions
query.filter("contains(well_name, 'A-')")
```

### expand()

Include related entities.

```python
# Expand single relationship
query.expand("wellbores")

# Expand multiple relationships
query.expand("wellbores,interpretations")
```

### top()

Limit number of results.

```python
query.top(100)  # Return max 100 items
```

### skip()

Skip first N results (pagination).

```python
query.skip(50)  # Skip first 50 items
```

### reset()

Clear query parameters for reuse.

```python
query = QueryBuilder(district_id=dist, field=fld)

# First query
query.schema("Well").select("well_name")
response1 = client.execute_query(query)

# Reset and build new query
query.reset().schema("Fault").select("fault_type")
response2 = client.execute_query(query)
```

## Automatic Pagination

The DSIS API returns a maximum of 1000 items per response. When there are more results, the response includes an `odata.nextLink` field pointing to the next page.

By default, `execute_query()` automatically follows all `odata.nextLink` references and **yields items as they are fetched** (memory efficient). You can control this behavior with the `fetch_all` parameter.

```python
# Default: Fetch all pages and yield items one at a time (fetch_all=True)
query = QueryBuilder(district_id=dist, field=fld).schema("Well")

# Option 1: Process items as they arrive 
for well in client.execute_query(query):
    process(well)  # Process each item immediately

# Option 2: Collect all items into a list 
all_wells = list(client.execute_query(query))
print(f"Total wells: {len(all_wells)}")

In my tests I could not find significant memory differences between these two approaches, but it was a small dataset. Option 2 was a bit faster.

# Manual pagination: Get only first page (fetch_all=False)
response = client.execute_query(query, fetch_all=False)
first_page = response.get("value", [])  # Max 1000 items
next_link = response.get("odata.nextLink")  # URL for next page if available
```

**When to use `fetch_all=True` (default):**

- You want to process all data automatically across all pages
- You can process items one at a time (streaming/iteration)
- You want memory-efficient processing of large datasets

**When to use `fetch_all=False`:**

- You only need a sample of data
- You want to implement custom pagination logic
- You're displaying paginated results in a UI

**Memory Considerations:**

When `fetch_all=True`, the library yields items as they are fetched rather than loading everything into memory at once. This means:

- ✅ **Good**: `for item in client.execute_query(query): process(item)` - Memory efficient
- ⚠️ **Use carefully**: `all_items = list(client.execute_query(query))` - Loads everything into memory

If you have a very large dataset (e.g., 100,000+ items), process items as they arrive rather than converting to a list.

## Execution Patterns

### Pattern 1: Basic Execution (Streaming)

```python
query = QueryBuilder(district_id=dist, field=fld).schema("Basin")

# Process items as they arrive (memory efficient)
for item in client.execute_query(query):
    print(item.get("basin_name"))

# Or collect all items into a list (uses more memory)
all_items = list(client.execute_query(query))
print(f"Total items: {len(all_items)}")
```

### Pattern 1b: Single Page Execution

```python
query = QueryBuilder(district_id=dist, field=fld).schema("Basin")
response = client.execute_query(query, fetch_all=False)

# Response structure
items = response.get("value", [])      # List of items (max 1000)
next_link = response.get("odata.nextLink")  # URL for next page
```

### Pattern 2: Auto-Casting with Model Class

```python
from dsis_model_sdk.models.common import Basin

query = QueryBuilder(district_id=dist, field=fld).schema(Basin).select("basin_name,basin_id")

# Option 1: Stream and cast each item as it arrives (memory efficient)
for basin in client.execute_query(query, cast=True):
    print(f"Basin: {basin.basin_name} (ID: {basin.basin_id})")

# Option 2: Collect all cast items into a list
basins = list(client.execute_query(query, cast=True))

# Option 3: Manual cast after single-page execution
response = client.execute_query(query, fetch_all=False)
basins = client.cast_results(response["value"], Basin)
```

### Pattern 3: Error Handling

```python
try:
    query = QueryBuilder(district_id=dist, field=fld).schema("Well")
    
    # Process items as they arrive
    item_count = 0
    for item in client.execute_query(query):
        item_count += 1
        # Process each item
    
    print(f"Retrieved {item_count} wells")
except Exception as e:
    print(f"Query failed: {e}")
```

## Complete Examples

### Example 1: Filtered Query with Streaming

```python
# Use the `DSISConfig.for_native_model(...)` example from the "Basic Configuration"
# section above to create `config` and `client`. The snippet below assumes
# `client` is already created and available.

# Build query with filters
query = (
    QueryBuilder(
        district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
        field="SNORRE",
    )
    .schema("Well")
    .select("well_name,well_uwi,spud_date")
    .filter("well_type eq 'Producer'")
    .top(50)
)

# Process wells as they arrive
for well in client.execute_query(query):
    print(f"Well: {well['well_name']}")

# Or collect all into a list
wells = list(client.execute_query(query))
print(f"Retrieved {len(wells)} producer wells")
```

### Example 2: Type-Safe Query with Error Handling

```python
from dsis_model_sdk.models.common import Basin

query = (
    QueryBuilder(district_id=dist, field=fld)
    .schema(Basin)
    .select("basin_name,basin_id,native_uid")  # Include required fields
)

try:
    # Stream and auto-cast each basin as it arrives
    for basin in client.execute_query(query, cast=True):
        print(f"Basin: {basin.basin_name}")
        print(f"  ID: {basin.basin_id}")
        print(f"  UID: {basin.native_uid}")
        
except ImportError:
    print("dsis_model_sdk not installed - install for type-safe casting")
except Exception as e:
    print(f"Query failed: {e}")
```

### Example 3: Reusable Query Builder

```python
# Create base query builder
base_query = QueryBuilder(district_id=dist, field=fld)

# Query 1: Get all faults
fault_query = base_query.schema("Fault").select("fault_id,fault_type")
faults = list(client.execute_query(fault_query))

# Query 2: Get all wells (reset and rebuild)
well_query = base_query.reset().schema("Well").select("well_name,well_uwi")
wells = list(client.execute_query(well_query))
```

### Example 4: Manual Pagination Control

```python
# Get first page only
query = QueryBuilder(district_id=dist, field=fld).schema("Well")
response = client.execute_query(query, fetch_all=False)

wells = response.get("value", [])
print(f"First page: {len(wells)} wells")

# Check if more pages exist
if "odata.nextLink" in response:
    print("More results available")
    # You can implement custom pagination logic here if needed
```

## Tips and Best Practices

1. **Always specify required fields**: When using model classes, ensure you select all required fields for the model
2. **Use environment variables**: Never hardcode credentials in your code
3. **Handle errors gracefully**: Wrap `execute_query()` in try-except blocks
4. **Reuse QueryBuilder**: Use `.reset()` to clear and rebuild queries instead of creating new instances
5. **Enable auto-casting**: Use `cast=True` with model classes for type-safe results
6. **Test connection first**: Call `client.test_connection()` when setting up to see if credentials are correct

## See Also

- [Getting Started Guide](getting-started.md) - Basic setup and usage
- [Advanced Serialization](advanced-serialization.md) - Working with model instances
