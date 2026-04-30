from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
from collections import defaultdict
from typing import Iterable
import inspect
from dsis_client import DSISClient, DSISConfig, QueryBuilder, Environment

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

MODEL_NAME = "OW5000"
MODEL_VERSION = "5000107"
DISTRICT = "BG4FROST"
PROJECT = "GULLFAKS"


def build_district_id(database: str, *, model_name: str) -> str:
    """Build DSIS district_id from database name.

    DSIS uses different district-id conventions for different models.

    Examples:
    - OpenWorksCommonModel: OpenWorksCommonModel_OW_<DB>-OW_<DB>
    - OpenWorks native models (e.g., OW5000): OpenWorks_OW_<DB>_SingleSource-OW_<DB>
    """
    if model_name == "OpenWorksCommonModel":
        return f"OpenWorksCommonModel_OW_{database}-OW_{database}"
    return f"OpenWorks_OW_{database}_SingleSource-OW_{database}"


def plot_fault_trimesh_map(fault_planes: Iterable[dict]) -> None:
    """Plot FaultTrimesh geometry as a 2-D plan-view scatter map, one colour per FaultPlane.

    Expects a list of FaultPlane dicts each containing an embedded ``FaultTrimesh``
    list (i.e. queried via ``QueryBuilder.schema("FaultPlane").expand("FaultTrimesh")``).
    The ``vertices`` field on each trimesh is a dict with ``x``, ``y``, ``z`` string lists.

    X = projected easting  (unit read from min_wrk_prj_x_dsdsunit on FaultTrimesh)
    Y = projected northing (unit read from min_wrk_prj_y_dsdsunit on FaultTrimesh)
    """
    by_plane: dict = defaultdict(list)
    x_unit = "None"
    y_unit = "None"

    for plane in fault_planes:
        pid = plane.get("native_uid") or plane.get("fault_plane_id")
        for tm in plane.get("FaultTrimesh", []):
            by_plane[pid].append(tm)
            x_unit = tm.get("min_wrk_prj_x_dsdsunit") or x_unit
            y_unit = tm.get("min_wrk_prj_y_dsdsunit") or y_unit

    if not by_plane:
        print("No trimeshes to plot.")
        return

    plane_ids = list(by_plane.keys())
    cmap = plt.colormaps["tab20"]
    colours = [cmap(i % 20) for i in range(len(plane_ids))]

    fig, ax = plt.subplots(figsize=(10, 10))

    for colour, pid in zip(colours, plane_ids):
        for tm in by_plane[pid]:
            verts = tm.get("vertices")
            if not verts:
                continue
            xs = [float(v) for v in verts["x"]]
            ys = [float(v) for v in verts["y"]]
            if any(y < 6600000 for y in ys):  # crude filter to exclude outliers (e.g., Volve faults plotted in UTM32N)
                continue
            ax.scatter(xs, ys, color=colour, s=2.0, linewidths=0)

    n = len(plane_ids)
    ax.set_title(f"{PROJECT} — {n} fault{'s' if n != 1 else ''}")
    ax.set_aspect("equal")
    ax.grid(True, linewidth=0.4)
    ax.set_xlabel(f"Easting ({x_unit})")
    ax.set_ylabel(f"Northing ({y_unit})")
    plt.xticks(rotation=-70)
    plt.tight_layout()
    plt.show()




if __name__ == "__main__":
    qkw: dict = {
        "district_id": build_district_id(DISTRICT, model_name=MODEL_NAME),
        "project": PROJECT,
    }
    if "model_name" in inspect.signature(QueryBuilder).parameters:
        qkw["model_name"] = MODEL_NAME
    dsis_client = DSISClient(config)
    from IPython import embed

    list_faults = list(dsis_client.execute_query(QueryBuilder(**qkw).schema("FaultPlane").expand("FaultTrimesh"), max_pages=1, timeout=250))
    list_faults_with_trimesh = [f for f in list_faults if f.get("FaultTrimesh")]
    embed()    
    plot_fault_trimesh_map(list_faults_with_trimesh)