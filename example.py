"""
Example usage of the DSIS Python Client.

This example demonstrates how to:
1. Configure the DSIS client with your credentials
2. Authenticate with Azure AD and DSIS
3. Retrieve data from the DSIS API
"""

import json
import os
from dotenv import load_dotenv
from src.dsis_client import DSISClient, DSISConfig, Environment, QueryBuilder

# ============================================================================
# CONFIGURATION - Load credentials from .env file
# ============================================================================

# Load environment variables from .env file
load_dotenv()

# Azure AD configuration
TENANT_ID = os.getenv('TENANT_ID')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
ACCESS_APP_ID = os.getenv('ACCESS_APP_ID')

# DSIS credentials
DSIS_USERNAME = os.getenv('DSIS_USERNAME')
DSIS_PASSWORD = os.getenv('DSIS_PASSWORD')

# Subscription key (APIM product)
SUBSCRIPTION_KEY = os.getenv('SUBSCRIPTION_KEY')

# Environment
env_str = os.getenv('ENVIRONMENT', 'DEV')
ENVIRONMENT = Environment[env_str]  # Convert string to Environment enum

# ============================================================================
# MAIN EXAMPLE
# ============================================================================

def main():
    """Main example function."""

    # Validate that all required environment variables are set
    required_vars = {
        'TENANT_ID': TENANT_ID,
        'CLIENT_ID': CLIENT_ID,
        'CLIENT_SECRET': CLIENT_SECRET,
        'ACCESS_APP_ID': ACCESS_APP_ID,
        'DSIS_USERNAME': DSIS_USERNAME,
        'DSIS_PASSWORD': DSIS_PASSWORD,
        'SUBSCRIPTION_KEY': SUBSCRIPTION_KEY
    }

    missing_vars = [name for name, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}. Please check your .env file.")

    # Create configuration using factory method for native model
    config = DSISConfig.for_native_model(
        environment=ENVIRONMENT,
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        access_app_id=ACCESS_APP_ID,
        dsis_username=DSIS_USERNAME,
        dsis_password=DSIS_PASSWORD,
        subscription_key_dsauth=SUBSCRIPTION_KEY,
        subscription_key_dsdata=SUBSCRIPTION_KEY,
    )
    
    # Create client
    client = DSISClient(config)
    
    print("=" * 70)
    print("DSIS Python Client Example")
    print("=" * 70)

    # Print configuration (without sensitive data)
    print(f"\nConfiguration:")
    print(f"  Environment: {ENVIRONMENT.value}")
    print(f"  DSIS Username: {DSIS_USERNAME}")
    print(f"  Model: {config.model_name}")
    print(f"  Model Version: {config.model_version}")
    print(f"  Token Endpoint: {config.token_endpoint}")
    print(f"  Data Endpoint: {config.data_endpoint}")

    # Test connection by acquiring tokens
    print("\n1. Acquiring authentication tokens...")
    try:
        client.auth.get_auth_headers()
        print("   ✓ Azure AD token: OK")
        print("   ✓ DSIS token: OK")
    except Exception as e:
        print(f"   ✗ Token acquisition failed: {e}")
        print(f"\n   Debugging info:")
        print(f"   - Check if subscription key is valid for {ENVIRONMENT.value} environment")
        print(f"   - Check if DSIS username and password are correct")
        print(f"   - Check if Azure AD credentials are correct")
        return
    
    # Retrieve data from DSIS API
    print("\n2. Retrieving data from DSIS API...")
    try:
        # Real data from sample.py
        district_id = "OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA"
        field = "SNORRE"

        # Example 1: Get Fault data using client.get_odata()
        print(f"\n   Example 1: Retrieving Fault data...")
        response = client.get_odata(
            district_id=district_id,
            field=field,
            data_table="Fault"
        )
        print(f"   ✓ Fault data retrieved: OK")
        print(f"   Response (first 500 chars):")
        response_str = json.dumps(response, indent=2)
        print(response_str[:500] + ("..." if len(response_str) > 500 else ""))

        # Example 2: Build query with QueryBuilder using schema
        print(f"\n   Example 2: Building query with QueryBuilder.schema()...")

        # Query with schema - QueryBuilder IS the query object (no .build() needed)
        query = QueryBuilder(district_id=district_id, field=field).schema("Fault").select("id,type").filter("type eq 'NORMAL'")
        print(f"   ✓ Query built successfully: {query}")
        response = client.executeQuery(query)
        print(f"   ✓ Filtered Fault data retrieved: OK")
        print(f"   Response (first 500 chars):")
        response_str = json.dumps(response, indent=2)
        print(response_str[:500] + ("..." if len(response_str) > 500 else ""))

        # Example 3: Auto-cast results with dsis_model_sdk
        print(f"\n   Example 3: Auto-casting results with dsis_model_sdk...")
        try:
            from dsis_model_sdk.models.common import Basin

            # Build query with model class for auto-casting
            # Select required fields explicitly (basin_name and basin_id are required for Basin model)
            query = QueryBuilder(district_id=district_id, field=field).schema(Basin).select("basin_name,basin_id,native_uid")

            # Option 1: Auto-cast with executeQuery
            basins = client.executeQuery(query, cast=True)
            print(f"   ✓ Retrieved and cast {len(basins)} Basin objects")
            if basins:
                print(f"   First basin: {basins[0].basin_name} (id: {basins[0].basin_id})")

            # Option 2: Manual cast with client.cast_results()
            response = client.executeQuery(query)
            basins_manual = client.cast_results(response['value'], Basin)
            print(f"   ✓ Manual cast: {len(basins_manual)} Basin objects")

        except ImportError:
            print(f"   ⚠ dsis_model_sdk not available - skipping casting example")
        except Exception as e:
            print(f"   ⚠ Casting example skipped: {str(e)[:100]}")
            print(f"   Note: Ensure required fields are selected (e.g., basin_name, basin_id for Basin)")

        # Example 4: Demonstrate QueryBuilder features
        print(f"\n   Example 4: QueryBuilder features summary...")
        print(f"   ✓ QueryBuilder IS the query object - no .build() method needed")
        print(f"   ✓ district_id and field are required parameters")
        print(f"   ✓ Use .schema() to set the data schema (e.g., 'Fault', 'Well')")
        print(f"   ✓ Use .schema(ModelClass) with dsis_model_sdk for type-safe result casting")
        print(f"   ✓ Chain methods: .select(), .filter(), .expand()")
        print(f"   ✓ Pass QueryBuilder directly to client.executeQuery()")
        print(f"   ✓ Two casting options: cast=True or client.cast_results()")

    except Exception as e:
        print(f"   ✗ Data retrieval failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 70)
    print("✓ Example completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()

