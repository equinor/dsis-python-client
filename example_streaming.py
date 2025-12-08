"""
Example: Streaming Binary Data for Memory-Efficient Processing

This example demonstrates how to use the streaming API to handle large
binary bulk data without loading everything into memory at once.

Key Benefits:
- Memory efficient: Only stores one chunk at a time
- Progress tracking: Can report download progress
- Early termination: Can stop streaming if needed
- Large dataset friendly: Handles multi-GB datasets
"""

import os
from dotenv import load_dotenv

from dsis_client import DSISClient, DSISConfig, Environment, QueryBuilder
from dsis_model_sdk.models.common import SeismicDataSet3D, HorizonData3D, LogCurve
from dsis_model_sdk.protobuf import (
    decode_seismic_float_data,
    decode_horizon_data,
    decode_log_curves,
)

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
print("DSIS Python Client - Streaming Binary Data")
print("=" * 80)


# Example 1: Stream with Progress Tracking
print("\n\nExample 1: Stream Seismic Data with Progress Tracking")
print("-" * 80)

# Query for seismic metadata
query = QueryBuilder(district_id=district_id, field=field).schema(SeismicDataSet3D).select(
    "seismic_dataset_name,native_uid"
)

seismic_datasets = list(client.execute_query(query, cast=True, max_pages=1))

if seismic_datasets:
    seismic = seismic_datasets[0]
    print(f"Seismic Dataset: {seismic.seismic_dataset_name}")
    print("Streaming binary data in 10MB chunks (DSIS recommended)...")

    chunks = []
    total_bytes = 0
    chunk_count = 0

    # Stream data with progress tracking
    for chunk in client.get_bulk_data_stream(
        schema=SeismicDataSet3D,  # Type-safe!
        native_uid=seismic,  # Pass entity directly
        query=query,
        chunk_size=10 * 1024 * 1024,  # 10MB chunks (DSIS recommended)
    ):
        chunks.append(chunk)
        chunk_count += 1
        total_bytes += len(chunk)
        print(f"  Chunk {chunk_count}: {len(chunk):,} bytes (Total: {total_bytes / 1024 / 1024:.2f} MB)")

    if chunks:
        print(f"\n✓ Downloaded {chunk_count} chunks, total: {total_bytes / 1024 / 1024:.2f} MB")

        # Combine and decode
        print("Decoding protobuf data...")
        binary_data = b"".join(chunks)

        decoded = decode_seismic_float_data(binary_data)
        print(f"✓ Decoded successfully: {decoded.length.i} x {decoded.length.j} x {decoded.length.k}")
    else:
        print("⚠ No bulk data available")


# Example 2: Stream to File (Save Without Loading to Memory)
print("\n\nExample 2: Stream Directly to File")
print("-" * 80)

query = QueryBuilder(district_id=district_id, field=field).schema(HorizonData3D).select(
    "horizon_name,native_uid"
)

horizons = list(client.execute_query(query, cast=True, max_pages=1))

if horizons:
    horizon = horizons[0]
    print(f"Horizon: {horizon.horizon_name}")
    print("Streaming to file...")

    output_file = "/tmp/horizon_data.bin"
    total_bytes = 0

    with open(output_file, "wb") as f:
        for chunk in client.get_bulk_data_stream(
            schema=HorizonData3D,  # Type-safe!
            native_uid=horizon,  # Pass entity directly
            query=query,
            chunk_size=10 * 1024 * 1024,  # 10MB chunks (DSIS recommended)
        ):
            f.write(chunk)
            total_bytes += len(chunk)

    if total_bytes > 0:
        print(f"✓ Saved {total_bytes:,} bytes to {output_file}")

        # Now read and decode from file
        print("Reading from file and decoding...")
        with open(output_file, "rb") as f:
            binary_data = f.read()

        decoded = decode_horizon_data(binary_data)
        print(f"✓ Decoded: {decoded.numberOfRows} x {decoded.numberOfColumns} grid")
    else:
        print("⚠ No bulk data available")


# Example 3: Conditional Streaming (Early Termination)
print("\n\nExample 3: Conditional Streaming with Size Limit")
print("-" * 80)

query = QueryBuilder(district_id=district_id, field=field).schema(LogCurve).select(
    "log_curve_name,native_uid"
)

log_curves = list(client.execute_query(query, cast=True, max_pages=1))

if log_curves:
    log_curve = log_curves[0]
    print(f"Log Curve: {log_curve.log_curve_name}")

    max_size = 100 * 1024 * 1024  # 100MB limit
    chunks = []
    total_bytes = 0

    print(f"Streaming with {max_size / 1024 / 1024:.1f}MB size limit...")

    for chunk in client.get_bulk_data_stream(
        schema=LogCurve,  # Type-safe!
        native_uid=log_curve,  # Pass entity directly
        query=query,
        chunk_size=10 * 1024 * 1024,  # 10MB chunks (DSIS recommended)
    ):
        chunks.append(chunk)
        total_bytes += len(chunk)

        # Stop if we exceed size limit
        if total_bytes > max_size:
            print(f"⚠ Size limit exceeded! Downloaded: {total_bytes / 1024 / 1024:.2f} MB")
            print("  Stopping stream early...")
            break

    if chunks and total_bytes <= max_size:
        print(f"✓ Complete download: {total_bytes:,} bytes")
        binary_data = b"".join(chunks)

        decoded = decode_log_curves(binary_data)
        print(f"✓ Decoded: {decoded.index.number_of_index} samples")
    else:
        print("⚠ No data or size limit exceeded")


print("\n" + "=" * 80)
print("Streaming Examples Complete!")
print("=" * 80)
print("\nKey Advantages of Streaming:")
print("1. Memory efficient - only one chunk in memory at a time")
print("2. Progress tracking - see download progress in real-time")
print("3. Direct to file - save without loading into memory")
print("4. Early termination - stop if data exceeds limits")
print("5. Large datasets - handle multi-GB files without memory issues")
print("=" * 80)
