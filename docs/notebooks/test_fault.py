from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 — registers 3d projection
from mpl_toolkits.mplot3d.art3d import Poly3DCollection
import numpy as np
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


FAULT_SEGMENT_COLOR = (1.0, 0.0, 0.0)      # strong red
TRIMESH_EDGE_COLOR  = (0.0, 0.2, 0.9)      # strong blue
TRIMESH_FACE_COLOR  = (0.7, 0.85, 1.0)     # very light blue



def plot_fault_combined(fault_planes: Iterable[dict]) -> None:
    """Overlay FaultTrimesh (scatter cloud) and FaultSegment (polylines) in one plan-view map.

    Each FaultPlane gets a distinct colour shared between both geometry types so
    the two representations can be compared side-by-side:
    - FaultTrimesh vertices → translucent scatter points
    - FaultSegment polylines → solid lines (one per horizon cross-section)
    """
    by_plane_tm: dict = defaultdict(list)
    by_plane_seg: dict = defaultdict(list)
    x_unit = "m"
    y_unit = "m"

    for plane in fault_planes:
        pid = plane.get("native_uid") or plane.get("fault_plane_id")
        for tm in plane.get("FaultTrimesh", []):
            by_plane_tm[pid].append(tm)
            x_unit = tm.get("min_wrk_prj_x_dsdsunit") or x_unit
            y_unit = tm.get("min_wrk_prj_y_dsdsunit") or y_unit
        for seg in plane.get("FaultSegment", []):
            by_plane_seg[pid].append(seg)
            x_unit = seg.get("bounding_pt1_x_dsdsunit") or x_unit
            y_unit = seg.get("bounding_pt1_y_dsdsunit") or y_unit

    all_pids = sorted(set(list(by_plane_tm) + list(by_plane_seg)))
    if not all_pids:
        print("No geometry to plot.")
        return

    cmap = plt.colormaps["tab10"]
    colours = {pid: cmap(i % 10) for i, pid in enumerate(all_pids)}

    fig, ax = plt.subplots(figsize=(12, 10))
    has_trimesh = False
    has_segment = False

    for pid in all_pids:
        colour = colours[pid]

        # FaultTrimesh — scatter cloud of mesh vertices
        for tm in by_plane_tm.get(pid, []):
            verts = tm.get("vertices")
            if not verts:
                continue
            xs = [float(v) for v in verts["x"]]
            ys = [float(v) for v in verts["y"]]
            if any(y < 6600000 for y in ys):  # crude UTM-zone outlier filter
                continue
            ax.scatter(xs, ys, color=colour, s=2.0, alpha=0.35, linewidths=0)
            has_trimesh = True

        # FaultSegment — one polyline per horizon cross-section
        for seg in by_plane_seg.get(pid, []):
            vals = seg.get("values")
            if not vals:
                continue
            xs = [float(v) for v in vals["x"]]
            ys = [float(v) for v in vals["y"]]
            ax.plot(xs, ys, color=colour, linewidth=1.5)
            has_segment = True

    legend_handles = []
    if has_trimesh:
        legend_handles.append(mpatches.Patch(color="grey", alpha=0.5, label="FaultTrimesh (vertices)"))
    if has_segment:
        legend_handles.append(mlines.Line2D([], [], color="grey", linewidth=1.5, label="FaultSegment (polylines)"))
    if legend_handles:
        ax.legend(handles=legend_handles, loc="best")

    n = len(all_pids)
    ax.set_title(f"{PROJECT} — {n} fault plane{'s' if n != 1 else ''}: trimesh + segments")
    ax.set_aspect("equal")
    ax.grid(True, linewidth=0.4)
    ax.set_xlabel(f"Easting ({x_unit})")
    ax.set_ylabel(f"Northing ({y_unit})")
    plt.xticks(rotation=-70)
    plt.tight_layout()
    plt.show()


def plot_fault_combined_3d_safe(fault_planes):
    """
    SAFETY-GUARANTEED 3D plot for FaultTrimesh + FaultSegment.

    This version defensively handles:
      - NaN / inf vertices
      - missing Z values
      - 1-based triangle indexing
      - invalid triangle bounds
      - flat (edge-on) surfaces
      - Poly3DCollection visibility pitfalls
    """

    from collections import defaultdict
    import numpy as np
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d.art3d import Poly3DCollection
    import matplotlib.lines as mlines
    import matplotlib.patches as mpatches

    by_plane_tm = defaultdict(list)
    by_plane_seg = defaultdict(list)

    x_unit = y_unit = z_unit = "m"

    # ─────────────────────────────────────────────────────────────
    # Collect geometry
    # ─────────────────────────────────────────────────────────────
    for plane in fault_planes:
        pid = plane.get("native_uid") or plane.get("fault_plane_id")
        for tm in plane.get("FaultTrimesh", []):
            by_plane_tm[pid].append(tm)
            x_unit = tm.get("min_wrk_prj_x_dsdsunit") or x_unit
            y_unit = tm.get("min_wrk_prj_y_dsdsunit") or y_unit
            z_unit = tm.get("min_z_dsdsunit") or z_unit

        for seg in plane.get("FaultSegment", []):
            by_plane_seg[pid].append(seg)
            x_unit = seg.get("bounding_pt1_x_dsdsunit") or x_unit
            y_unit = seg.get("bounding_pt1_y_dsdsunit") or y_unit

    all_pids = sorted(set(by_plane_tm) | set(by_plane_seg))
    if not all_pids:
        print("No geometry to plot.")
        return

    # ─────────────────────────────────────────────────────────────
    # Plot setup
    # ─────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(14, 10))
    ax = fig.add_subplot(111, projection="3d")
    ax.view_init(elev=25, azim=-60)

    cmap = plt.colormaps["tab10"]
    colours = {pid: cmap(i % 10) for i, pid in enumerate(all_pids)}

    all_xs, all_ys, all_zs = [], [], []
    has_trimesh = has_segment = False

    # ─────────────────────────────────────────────────────────────
    # Plot per fault plane
    # ─────────────────────────────────────────────────────────────
    for pid in all_pids:
        colour = colours[pid]
        rgba = tuple(colour[:3])

        # ── FaultTrimesh ──────────────────────────────────────────
        for tm in by_plane_tm.get(pid, []):
            verts = tm.get("vertices")
            if not verts or "x" not in verts or "y" not in verts:
                continue

            xs = np.asarray(verts["x"], dtype=float)
            ys = np.asarray(verts["y"], dtype=float)
            zs = np.asarray(
                verts.get("z", np.zeros(len(xs))), dtype=float
            )

            v_array = np.column_stack([xs, ys, zs])

            # Sanitize vertices
            if not np.isfinite(v_array).all():
                v_array[~np.isfinite(v_array)] = np.nanmin(v_array)

            # Detect flat Z → introduce epsilon to make surface visible
            if np.ptp(v_array[:, 2]) == 0.0:
                v_array[:, 2] += np.linspace(0, 1e-3, len(v_array))

            tris = tm.get("triangles")
            tri_indices = None

            if isinstance(tris, dict):
                if all(k in tris for k in ("vertex_1", "vertex_2", "vertex_3")):
                    tri_indices = np.column_stack([
                        np.asarray(tris["vertex_1"], dtype=int),
                        np.asarray(tris["vertex_2"], dtype=int),
                        np.asarray(tris["vertex_3"], dtype=int),
                    ])

            # Convert from 1‑based to 0‑based indexing
            if tri_indices is not None:
                tri_indices -= 1

            elif isinstance(tris, (list, np.ndarray)):
                flat = np.asarray(tris, dtype=int)
                if len(flat) % 3 == 0:
                    tri_indices = flat.reshape(-1, 3)

            if tri_indices is not None and len(tri_indices):
                # Fix 1-based indexing if detected
                if tri_indices.max() >= len(v_array):
                    tri_indices -= 1

                # Final bounds check
                if tri_indices.min() < 0 or tri_indices.max() >= len(v_array):
                    tri_indices = None

            if tri_indices is not None:
                polys = v_array[tri_indices]

                mesh = Poly3DCollection(
                    polys,
                    facecolors=(*TRIMESH_FACE_COLOR, 0.18),
                    edgecolors=(*TRIMESH_EDGE_COLOR, 0.9),
                    linewidths=0.2,
                )
                mesh.set_alpha(0.85)
                mesh.set_zorder(0)

                ax.add_collection3d(mesh)
                has_trimesh = True
            else:
                # Guaranteed fallback (never silent fail)
                ax.scatter(
                    v_array[:, 0],
                    v_array[:, 1],
                    v_array[:, 2],
                    s=2,
                    color=(*FAULT_SEGMENT_COLOR, 0.7),
                    alpha=0.7,
                )

            all_xs.extend(v_array[:, 0])
            all_ys.extend(v_array[:, 1])
            all_zs.extend(v_array[:, 2])

        # ── FaultSegment ──────────────────────────────────────────
        for seg in by_plane_seg.get(pid, []):
            vals = seg.get("values")
            if not vals:
                continue

            xs = np.asarray(vals["x"], dtype=float)
            ys = np.asarray(vals["y"], dtype=float)
            zs = np.asarray(vals.get("z", np.zeros(len(xs))), dtype=float)

            ax.plot(xs, ys, zs, color=(*FAULT_SEGMENT_COLOR, 0.7), linewidth=2.0, zorder=10)
            all_xs.extend(xs)
            all_ys.extend(ys)
            all_zs.extend(zs)
            has_segment = True

    # ─────────────────────────────────────────────────────────────
    # Axis limits (Poly3DCollection safe)
    # ─────────────────────────────────────────────────────────────
    if all_xs:
        ax.set_xlim(min(all_xs), max(all_xs))
        ax.set_ylim(min(all_ys), max(all_ys))
        zmin, zmax = min(all_zs), max(all_zs)
        ax.set_zlim(
            zmin if zmin != zmax else zmin - 1,
            zmax if zmin != zmax else zmax + 1,
        )

    # ─────────────────────────────────────────────────────────────
    # Legend & labels
    # ─────────────────────────────────────────────────────────────
    legend = []
    if has_trimesh:
        legend.append(
            mpatches.Patch(color="grey", alpha=0.8, label="FaultTrimesh")
        )
    if has_segment:
        legend.append(
            mlines.Line2D([], [], color="grey", linewidth=2.0, label="FaultSegment")
        )
    if legend:
        ax.legend(handles=legend)

    ax.set_xlabel(f"Easting ({x_unit})")
    ax.set_ylabel(f"Northing ({y_unit})")
    ax.set_zlabel(f"Depth ({z_unit})")
    ax.set_title("Fault planes — Trimesh & Segments (safe 3D)")

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
    FAULT_PLANE_ID = 6820
    list_fault = list(dsis_client.execute_query(QueryBuilder(**qkw)
                                                 .schema("FaultPlane")
                                                 .filter(f"fault_plane_id eq {FAULT_PLANE_ID}")
                                                 .expand("FaultTrimesh,FaultSegment"), max_pages=1, timeout=250))
    embed()
    plot_fault_combined_3d_safe(list_fault)