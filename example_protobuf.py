"""
Example: Working with Protobuf Binary Data in DSIS Python Client

This example demonstrates how to use the DSIS Python Client to work with
binary bulk data (protobuf) from the DSIS API.

The DSIS API serves data in two formats:
- Metadata: Via OData (JSON) - entity properties, relationships, statistics
- Bulk Data: Via Protocol Buffers (binary) - large arrays like horizon z-values,
  log curves, seismic amplitudes

This example shows:
1. Query for metadata (without binary data for efficiency)
2. Fetch binary data separately using get_bulk_data()
3. Decode protobuf data
4. Convert to NumPy arrays for analysis
"""

import os
import numpy as np
from dotenv import load_dotenv

from dsis_client import DSISClient, DSISConfig, Environment, QueryBuilder

# Load environment variables
load_dotenv()

# Configuration
config = DSISConfig(
    environment=Environment.DEV,
    tenant_id=os.getenv("TENANT_ID"),
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    subscription_key=os.getenv("SUBSCRIPTION_KEY"),
)

# Initialize client
client = DSISClient(config)

# Test parameters
district_id = os.getenv("DISTRICT_ID")
field = os.getenv("FIELD")

print("=" * 80)
print("DSIS Python Client - Protobuf Binary Data Examples")
print("=" * 80)


# Example 1: Working with Horizon Data
print("\n\nExample 1: Horizon Data (3D interpreted surface)")
print("-" * 80)

from dsis_model_sdk.models.common import HorizonData3D
from dsis_model_sdk.protobuf import decode_horizon_data
from dsis_model_sdk.utils.protobuf_decoders import horizon_to_numpy

# Step 1: Query for horizon metadata (exclude binary data field for efficiency)
print("Step 1: Querying for horizon metadata...")
query = QueryBuilder(district_id=district_id, field=field).schema(HorizonData3D).select(
    "horizon_name,horizon_mean,horizon_mean_unit,horizon_min,horizon_max,native_uid"
)

horizons = list(client.execute_query(query, cast=True, max_pages=1))
print(f"✓ Found {len(horizons)} horizons")

if horizons:
    horizon = horizons[0]
    print(f"\nHorizon: {horizon.horizon_name}")
    print(f"Mean depth: {horizon.horizon_mean} {horizon.horizon_mean_unit}")
    print(f"Depth range: {horizon.horizon_min} - {horizon.horizon_max}")

    # Step 2: Fetch binary data separately using get_entity_data()
    print("\nStep 2: Fetching binary bulk data...")
    binary_data = client.get_entity_data(
        entity=horizon,
        schema=HorizonData3D,  # Type-safe!
        query=query
    )

    if binary_data is None:
        print("⚠ No bulk data available for this horizon")
        exit(0)

    print(f"✓ Received {len(binary_data):,} bytes of binary data")

    # Step 3: Decode protobuf data
    print("\nStep 3: Decoding protobuf data...")
    decoded = decode_horizon_data(binary_data)
    print(f"✓ Decoded successfully")
    print(f"  Mode: {'FULL' if decoded.mode == decoded.FULL else 'SAMPLES'}")
    print(f"  Grid dimensions: {decoded.numberOfRows} x {decoded.numberOfColumns}")

    # Step 4: Convert to NumPy array
    print("\nStep 4: Converting to NumPy array...")
    array, metadata = horizon_to_numpy(decoded)
    print(f"✓ Array shape: {array.shape}")
    print(f"  Data coverage: {(~np.isnan(array)).sum() / array.size * 100:.1f}%")

    # Step 5: Analyze data
    print("\nStep 5: Analyzing data...")
    valid_data = array[~np.isnan(array)]
    if len(valid_data) > 0:
        print(f"  Min depth: {np.min(valid_data):.2f}")
        print(f"  Max depth: {np.max(valid_data):.2f}")
        print(f"  Mean depth: {np.mean(valid_data):.2f}")
        print(f"  Std deviation: {np.std(valid_data):.2f}")

    print("\n✓ Example 1 completed successfully!")


# Example 2: Working with Log Curve Data
print("\n\nExample 2: Log Curve Data (well log measurements)")
print("-" * 80)

from dsis_model_sdk.models.common import LogCurve
from dsis_model_sdk.protobuf import decode_log_curves
from dsis_model_sdk.utils.protobuf_decoders import log_curve_to_dict

# Step 1: Query for log curve metadata
print("Step 1: Querying for log curve metadata...")
query = QueryBuilder(district_id=district_id, field=field).schema(LogCurve).select(
    "log_curve_name,native_uid"
)

log_curves = list(client.execute_query(query, cast=True, max_pages=1))
print(f"✓ Found {len(log_curves)} log curves")

if log_curves:
    log_curve = log_curves[0]
    print(f"\nLog Curve: {log_curve.log_curve_name}")

    # Step 2: Fetch binary data using get_entity_data()
    print("\nStep 2: Fetching binary bulk data...")
    binary_data = client.get_entity_data(
        entity=log_curve,
        schema=LogCurve,  # Type-safe!
        query=query
    )

    if binary_data is None:
        print("⚠ No bulk data available for this log curve")
    else:
        print(f"✓ Received {len(binary_data):,} bytes")

        # Step 3: Decode and analyze
        print("\nStep 3: Decoding protobuf data...")
        decoded = decode_log_curves(binary_data)
        print(f"✓ Decoded successfully")
        print(f"  Curve type: {'DEPTH' if decoded.curve_type == decoded.DEPTH else 'TIME'}")
        print(f"  Index start: {decoded.index.start_index}")
        print(f"  Index increment: {decoded.index.increment}")
        print(f"  Number of samples: {decoded.index.number_of_index}")

        # Step 4: Convert to dict for easier access
        print("\nStep 4: Converting to dict...")
        data = log_curve_to_dict(decoded)
        print(f"✓ Found {len(data['curves'])} curves in dataset")

        for curve_name, curve_data in list(data['curves'].items())[:3]:  # Show first 3
            print(f"\n  Curve: {curve_name}")
            print(f"    Unit: {curve_data['unit']}")
            print(f"    Values: {len(curve_data['values'])} samples")
            if len(curve_data['values']) > 0:
                print(f"    Range: {min(curve_data['values']):.2f} - {max(curve_data['values']):.2f}")

        print("\n✓ Example 2 completed successfully!")


# Example 3: Working with Seismic Data
print("\n\nExample 3: Seismic Data (3D seismic amplitude volume)")
print("-" * 80)

from dsis_model_sdk.models.common import SeismicDataSet3D
from dsis_model_sdk.protobuf import decode_seismic_float_data
from dsis_model_sdk.utils.protobuf_decoders import seismic_3d_to_numpy

# Step 1: Query for seismic metadata
print("Step 1: Querying for seismic dataset metadata...")
query = QueryBuilder(district_id=district_id, field=field).schema(SeismicDataSet3D).select(
    "seismic_dataset_name,native_uid"
)

seismic_datasets = list(client.execute_query(query, cast=True, max_pages=1))
print(f"✓ Found {len(seismic_datasets)} seismic datasets")

if seismic_datasets:
    seismic = seismic_datasets[0]
    print(f"\nSeismic Dataset: {seismic.seismic_dataset_name}")

    # Step 2: Fetch binary data using get_entity_data() (WARNING: Can be very large!)
    print("\nStep 2: Fetching binary bulk data (this may take a while)...")
    binary_data = client.get_entity_data(
        entity=seismic,
        schema=SeismicDataSet3D,  # Type-safe!
        query=query
    )

    if binary_data is None:
        print("⚠ No bulk data available for this seismic dataset")
    else:
        print(f"✓ Received {len(binary_data):,} bytes ({len(binary_data) / 1024 / 1024:.2f} MB)")

        # Step 3: Decode protobuf data
        print("\nStep 3: Decoding protobuf data...")
        decoded = decode_seismic_float_data(binary_data)
        print(f"✓ Decoded successfully")
        print(f"  Dimensions: i={decoded.length.i}, j={decoded.length.j}, k={decoded.length.k}")

        # Step 4: Convert to NumPy array
        print("\nStep 4: Converting to NumPy array...")
        array, metadata = seismic_3d_to_numpy(decoded)
        print(f"✓ Array shape: {array.shape}")
        print(f"  Memory size: {array.nbytes / 1024 / 1024:.2f} MB")
        print(f"  Amplitude range: {np.min(array):.2f} to {np.max(array):.2f}")

        # Step 5: Extract a single trace
        print("\nStep 5: Extracting a single trace...")
        trace_i, trace_j = min(100, array.shape[0] - 1), min(100, array.shape[1] - 1)
        trace = array[trace_i, trace_j, :]
        print(f"✓ Trace at ({trace_i}, {trace_j}):")
        print(f"  Samples: {len(trace)}")
        print(f"  Min: {np.min(trace):.2f}, Max: {np.max(trace):.2f}")

        print("\n✓ Example 3 completed successfully!")


print("\n" + "=" * 80)
print("All examples completed!")
print("=" * 80)
print("\nKey Takeaways:")
print("1. Use get_bulk_data() to fetch binary protobuf data separately")
print("2. This is more efficient than including 'data' field in OData queries")
print("3. Decode binary data using dsis_model_sdk.protobuf decoders")
print("4. Convert to NumPy arrays for analysis and visualization")
print("5. Always check data size before fetching (especially seismic!)")
print("=" * 80)

