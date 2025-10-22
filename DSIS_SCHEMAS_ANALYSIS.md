# DSIS Schemas Package Analysis

## Overview

The `dsis-schemas` package (installed as `dsis_model_sdk`) provides comprehensive Pydantic models for DSIS data structures. It includes models for both **Common OpenWorks** and **Native** data models.

## Package Structure

```
dsis_model_sdk/
├── models/
│   ├── common/      # OpenWorks Common Model (200+ models)
│   └── native/      # OW5000 Native Model (1000+ models)
├── utils/
│   ├── schema_utils.py
│   ├── serialization.py
│   ├── validation.py
│   └── type_mapping.py
└── __init__.py
```

## Key Features

### 1. **Pydantic Models**
- All models are Pydantic v2 BaseModel subclasses
- Full type hints and validation
- JSON serialization/deserialization support

### 2. **Available Utilities**
- `deserialize_from_json()` - Parse JSON to models
- `serialize_to_json()` - Convert models to JSON
- `validate_data()` - Validate data against models
- `get_model_schema()` - Get model schema information
- `get_field_info()` - Get field metadata

### 3. **Common Models** (200+ models)
Key models for DSIS data:
- **Well** - Well information (UWI, name, location, etc.)
- **Wellbore** - Wellbore details
- **WellLog** - Well log data
- **DataSource** - Data source information
- **Seismic2DList**, **Seismic3DList** - Seismic data
- **Horizon**, **Fault** - Geological features
- **Project**, **Lease**, **Field** - Business entities

### 4. **Native Models** (1000+ models)
OW5000 native database models for detailed data access.

## Integration Opportunities with dsis-client

### 1. **Type-Safe OData URL Building**
```python
from dsis_model_sdk.models.common import Well
from dsis_client import DSISClient

# Use model metadata to build OData URLs
client = DSISClient(config)

# Instead of: client.get_odata("OW5000", "5000107")
# Could use: client.get_odata_by_model(Well, record_id="5000107")
```

### 2. **Response Validation**
```python
from dsis_model_sdk import deserialize_from_json
from dsis_model_sdk.models.common import Well

response = client.get_odata("OW5000", "5000107")
well_data = deserialize_from_json(response, Well)
# Now well_data is a validated Well object with type hints
```

### 3. **Query Building**
```python
from dsis_model_sdk.utils import schema_utils

# Get available fields for a model
well_fields = schema_utils.get_model_fields(Well)
# Use for building $select parameters
select_fields = ",".join(list(well_fields.keys())[:10])
data = client.get_odata("OW5000", select=select_fields)
```

### 4. **Model Discovery**
```python
# Find models by pattern
models = schema_utils.find_models_by_pattern("Well*")

# Get models by domain
seismic_models = schema_utils.get_models_by_domain("seismic")
```

## Recommended Implementation Strategy

### Phase 1: Response Validation (Low Risk)
- Add optional response validation using dsis-schemas
- Create helper methods to deserialize responses
- No breaking changes to existing API

### Phase 2: Type-Safe Queries (Medium Risk)
- Create model-aware query methods
- Build OData URLs from model metadata
- Maintain backward compatibility

### Phase 3: Advanced Features (Future)
- Query builder based on model fields
- Automatic field selection
- Schema-driven documentation

## Example Implementation

```python
from dsis_model_sdk.models.common import Well
from dsis_model_sdk import deserialize_from_json

class DSISClientEnhanced(DSISClient):
    def get_model_data(self, model_class, record_id=None, **kwargs):
        """Get OData and deserialize to model."""
        # Get model name from class
        model_name = model_class.__name__
        
        # Make API call
        response = self.get_odata(model_name, record_id, **kwargs)
        
        # Deserialize and validate
        return deserialize_from_json(response, model_class)

# Usage
client = DSISClientEnhanced(config)
well = client.get_model_data(Well, "5000107")
print(well.well_name)  # Type-safe access
```

## Benefits

1. **Type Safety** - Full IDE support and type checking
2. **Validation** - Automatic data validation
3. **Documentation** - Self-documenting code
4. **Discoverability** - Find available models and fields
5. **Serialization** - Easy JSON conversion
6. **Maintainability** - Single source of truth for schemas

## Next Steps

1. ✅ Analyze dsis-schemas capabilities
2. ⏳ Design integration approach
3. ⏳ Implement response validation helpers
4. ⏳ Add model-aware query methods
5. ⏳ Update documentation with examples
6. ⏳ Add tests for model integration

