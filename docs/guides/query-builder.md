# QueryBuilder Guide

Complete guide to using QueryBuilder for flexible DSIS data queries.

## Overview

`QueryBuilder` provides a fluent API for constructing OData queries with type safety and automatic result casting when used with `dsis_model_sdk`.

## Prerequisites

This guide assumes you have a configured `DSISClient`. For setup, see [Getting Started](getting-started.md).

For building `district_id` values and choosing between Common Model vs Native Model, see [Common vs Native Model](common-vs-native-model.md).

```python
from dsis_client import DSISClient, QueryBuilder

# Assumes client is already configured (see Getting Started)
# For district_id construction, see Common vs Native Model guide
dist = "OpenWorksCommonModel_OW_SV4TSTA-OW_SV4TSTA"
prj = "SNORRE"
```

## QueryBuilder Basics

QueryBuilder requires `district_id` and `project` parameters, then builds the query using method chaining.

### Simple Query with String Schema

```python
# Build query - QueryBuilder IS the query object (no .build() needed)
query = (
    QueryBuilder(district_id=dist, project=prj)
    .schema("Fault")
    .select("fault_id,fault_type,fault_name")
    .filter("fault_type eq 'NORMAL'")
)

# Execute query - returns a generator that yields items
for item in client.execute_query(query):
    print(item)

# Or collect all items into a list
items = list(client.execute_query(query))
```

### Type-Safe Query with Model Class

```python
from dsis_model_sdk.models.common import Basin

# Build query with model class for automatic type safety
query = (
    QueryBuilder(district_id=dist, project=prj)
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
query = QueryBuilder(district_id=dist, project=prj).schema("Well")

# Model class (enables type-safe casting)
from dsis_model_sdk.models.native import Well
query = QueryBuilder(district_id=dist, project=prj).schema(Well)
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

### format()

Set the response format parameter.

```python
# Default: json format (included by default)
query = QueryBuilder(district_id=dist, project=prj).schema("Well").select("well_name")
# Result: Well?$format=json&$select=well_name

# Explicitly set to json
query.format("json")

# Omit format parameter entirely
query.format("")  # or .format(None)
# Result: Well?$select=well_name
```

### reset()

Clear query parameters for reuse.

```python
query = QueryBuilder(district_id=dist, project=prj)

# First query
query.schema("Well").select("well_name")
response1 = client.execute_query(query)

# Reset and build new query
query.reset().schema("Fault").select("fault_type")
response2 = client.execute_query(query)
```

## Automatic Pagination

The DSIS API returns a maximum of 1000 items per response. When there are more results, the response includes an `odata.nextLink` field pointing to the next page.

By default, `execute_query()` automatically follows all `odata.nextLink` references and **yields items as they are fetched** (memory efficient). You can control pagination with the `max_pages` parameter:

```python
# Default: Fetch all pages (max_pages=-1)
query = QueryBuilder(district_id=dist, project=prj).schema("Well")

# Option 1: Process items as they arrive (streaming, memory efficient)
for well in client.execute_query(query):
    process(well)  # Process each item immediately

# Option 2: Collect all items into a list
all_wells = list(client.execute_query(query))
print(f"Total wells: {len(all_wells)}")

# Option 3: Fetch only first page (max_pages=1)
first_page_items = list(client.execute_query(query, max_pages=1))
print(f"First page: {len(first_page_items)} wells (max 1000)")

# Option 4: Fetch first two pages (max_pages=2)
two_pages_items = list(client.execute_query(query, max_pages=2))
print(f"First two pages: {len(two_pages_items)} wells")
```

**max_pages Parameter:**

- `max_pages=-1` (default): Fetch and yield from all pages
- `max_pages=1`: Yield items from first page only (max 1000 items)
- `max_pages=2`: Yield items from first two pages
- `max_pages=N`: Yield items from first N pages (or fewer if fewer pages available)

**When to use different max_pages values:**

- `-1` (unlimited): You want all data automatically across all pages
- `1`: You only need a sample, or want to implement custom pagination
- `N>1`: You want to process data in page-sized chunks

## Execution Patterns

### ⚠️ Critical: Schema Requirement for `cast=True`

**If you want to use `cast=True` to automatically convert results to model instances, you MUST pass a model class (not a string) to `.schema()`:**

```python
# ✅ Correct: Pass model class for casting
from dsis_model_sdk.models.common import Basin
query = QueryBuilder(district_id=dist, project=prj).schema(Basin)
results = client.execute_query(query, cast=True)  # Works!

# ❌ Wrong: String schema name won't work with cast=True
query = QueryBuilder(district_id=dist, project=prj).schema("Basin")
results = client.execute_query(query, cast=True)  # Has no effect!
```

### Pattern 1: Basic Execution (Streaming)

```python
query = QueryBuilder(district_id=dist, project=prj).schema("Basin")

# Process items as they arrive (memory efficient)
for item in client.execute_query(query):
    print(item.get("basin_name"))

# Or collect all items into a list (uses more memory)
all_items = list(client.execute_query(query))
print(f"Total items: {len(all_items)}")
```

### Pattern 1b: Single Page Execution

```python
query = QueryBuilder(district_id=dist, project=prj).schema("Basin")

# Fetch only first page (max 1000 items)
first_page_items = list(client.execute_query(query, max_pages=1))
print(f"Retrieved {len(first_page_items)} items from first page")
```

### Pattern 2: Auto-Casting with Model Class

```python
from dsis_model_sdk.models.common import Basin

query = QueryBuilder(district_id=dist, project=prj).schema(Basin).select("basin_name,basin_id")

# Option 1: Stream and cast each item as it arrives (memory efficient)
for basin in client.execute_query(query, cast=True):
    print(f"Basin: {basin.basin_name} (ID: {basin.basin_id})")

# Option 2: Collect all cast items into a list
basins = list(client.execute_query(query, cast=True))

# Option 3: Fetch only first page and cast
basins = list(client.execute_query(query, cast=True, max_pages=1))
```

**⚠️ IMPORTANT: Using `cast=True`**

To use `cast=True`, you **MUST** build your query using a model class imported from `dsis_model_sdk`, not a string schema name:

```python
# ✅ CORRECT: Using model class from dsis_model_sdk
from dsis_model_sdk.models.common import Basin  # or native
query = QueryBuilder(district_id=dist, project=prj).schema(Basin)
basins = list(client.execute_query(query, cast=True))

# ❌ INCORRECT: Using string schema name with cast=True
query = QueryBuilder(district_id=dist, project=prj).schema("Basin")
basins = list(client.execute_query(query, cast=True))  # Will not work!
```

The schema model can come from either:

- `from dsis_model_sdk.models.common import Basin`
- `from dsis_model_sdk.models.native import Basin`

If you use a string schema name, `cast=True` will have no effect. Omit `cast=True` or use a model class instead.

### Pattern 3: Error Handling

```python
try:
    query = QueryBuilder(district_id=dist, project=prj).schema("Well")
    
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
# Use the `DSISConfig(...)` example from the "Basic Configuration"
# section above to create `config` and `client`. The snippet below assumes
# `client` is already created and available.

# Build query with filters
query = (
    QueryBuilder(
        district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
        project="SNORRE",
    )
    .schema("Well")
    .select("well_name,well_uwi,spud_date")
    .filter("well_type eq 'Producer'")
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
    QueryBuilder(district_id=dist, project=prj)
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
base_query = QueryBuilder(district_id=dist, project=prj)

# Query 1: Get all faults
fault_query = base_query.schema("Fault").select("fault_id,fault_type")
faults = list(client.execute_query(fault_query))

# Query 2: Get all wells (reset and rebuild)
well_query = base_query.reset().schema("Well").select("well_name,well_uwi")
wells = list(client.execute_query(well_query))
```

### Example 4: Single Page Execution

```python
# Get first page only (max 1000 items)
query = QueryBuilder(district_id=dist, project=prj).schema("Well")
first_page_wells = list(client.execute_query(query, max_pages=1))

print(f"First page: {len(first_page_wells)} wells")

# For limited pagination (e.g., 2-3 pages), use max_pages parameter
two_pages_wells = list(client.execute_query(query, max_pages=2))
print(f"First two pages: {len(two_pages_wells)} wells")
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
