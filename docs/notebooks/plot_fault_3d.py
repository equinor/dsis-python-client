# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Fault plane exploration
# Fetches FaultPlane data from DSIS, lists available fault planes,
# then fetches and visualises a single fault with its trimesh and segments.

# %% [markdown]
# ## Imports

# %%
%matplotlib widget

# %%
from dotenv import load_dotenv
import os
import inspect
from dsis_client import DSISClient, DSISConfig, QueryBuilder, Environment
from plotting_functions import PlotContext, plot_fault_combined_3d, build_district_id, plot_fault_map

# %% [markdown]
# %% [markdown]
# ## Configuration

# %%
load_dotenv(".env_dsis")

config = DSISConfig(
    environment=Environment.PROD,
    tenant_id=os.getenv("tenant_id"),
    client_id=os.getenv("client_id"),
    client_secret=os.getenv("client_secret"),
    access_app_id=os.getenv("resource_id"),
    dsis_username=os.getenv("dsis_function_key"),
    dsis_password=os.getenv("dsis_password"),
    subscription_key_dsauth=os.getenv("subscription_key_dsauth"),
    subscription_key_dsdata=os.getenv("subscription_key_dsdata"),
    dsis_site=os.getenv("dsis_site"),
)

# %% [markdown]
# ## Model / project parameters
# ### Adjust these to point to the desired model / project / district. The notebook will list available FaultPlanes for the specified district, so you can pick one of those for the later part of the notebook.

# %%
MODEL_NAME = "OW5000"
MODEL_VERSION = "5000107"
DISTRICT = "BG4FROST"
PROJECT = "GULLFAKS"

plot_ctx: PlotContext = {
    "model_name": MODEL_NAME,
    "model_version": MODEL_VERSION,
    "district": DISTRICT,
    "project": PROJECT,
}

# %% [markdown]
# ## Build query kwargs and connect

# %%
qkw: dict = {
    "district_id": build_district_id(DISTRICT, model_name=MODEL_NAME),
    "project": PROJECT,
}
if "model_name" in inspect.signature(QueryBuilder).parameters:
    qkw["model_name"] = MODEL_NAME

dsis_client = DSISClient(config)

# %% [markdown]
# ## List available FaultPlanes

# %%
fault_plane_ids = [
    fault["fault_plane_id"]
    for fault in dsis_client.execute_query(QueryBuilder(**qkw).schema("FaultPlane"))
]
print(f"FaultPlanes available: {fault_plane_ids}")
print(f"Number of FaultPlanes available: {len(fault_plane_ids)}")

# %% [markdown]
# ## Fetch a specific FaultPlane with trimesh and segments

# %%
FAULT_PLANE_ID = 14608

fault = list(
    dsis_client.execute_query(
        QueryBuilder(**qkw)
        .schema("FaultPlane")
        .filter(f"fault_plane_id eq {FAULT_PLANE_ID}")
        .expand("FaultTrimesh,FaultSegment"),
        max_pages=1,
        timeout=250,
    )
)

# %% [markdown]
# ## Plot

# %%
plot_fault_combined_3d(fault, plot_ctx)
