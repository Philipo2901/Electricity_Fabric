# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

# NB_LineageSemanticModels
#
# Scans Power BI and Fabric semantic models with sempy and semantic-link-labs and
# writes semantic_models.platin_* into the governance lakehouse: models, columns,
# measures, relationships, lineage, calculation dependencies, shortcuts and model
# sources. Each model's Direct Lake source is resolved back to the gold entity
# behind it.
#
# PL_Lineage runs this before NB_LineageSnapshot, which folds the platin_* tables
# into lineage.snapshot as layer = platin rows so that one table spans bronze
# through platin. The coupling is only ordering: the snapshot skips those tables
# when they are missing, so this notebook also runs fine on its own schedule.
#
# No default lakehouse is attached and the Variable Library is never read, so the
# notebook works under a service principal. Every table is addressed by abfss
# path, built from the GUIDs in the parameters cell.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# semantic-link-labs does not ship with the runtime, so it is installed inline.
# sempy itself is already there. The version is pinned so that a new release
# cannot break a scheduled run; bump it deliberately after testing. Anything
# below 0.13.0 lacks sempy_labs.directlake.get_direct_lake_sources.
#
# Inline installs are disabled in pipeline runs unless the activity passes
# _inlineInstallationEnabled = true, which PL_Lineage does. Without it this cell
# is skipped without a word and the run fails later at import sempy_labs. They
# are also unsupported in reference runs through notebookutils.notebook.run,
# which is why PL_Lineage calls this notebook as its own activity.
#
# If an import fails right after a successful install, restart the kernel with
# notebookutils.session.restartPython() and run again.

%pip install -q semantic-link-labs==0.17.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# All imports for the notebook. This cell has to stay below the %pip cell
# above, which installs sempy_labs.

import json
import re
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import sempy.fabric as fabric
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
from sempy_labs.lakehouse import list_shortcuts
from sempy_labs.directlake import get_direct_lake_sources

try:
    from deltalake import DeltaTable, write_deltalake
except ImportError:
    raise ImportError(
        "The deltalake package is missing. It ships with the Fabric Python "
        "notebook runtime; on a runtime without it, add deltalake to the "
        "%pip install above.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Self-contained metadata-lakehouse access --------------------------------
# The collector resolves the governance lakehouse itself and writes by abfss
# path, with no %run, no Variable Library and no attached default lakehouse.


def pin_metadata_lakehouse(lakehouse_id: str, workspace_id: str) -> dict:
    """The governance lakehouse identity for the abfss writes, from the GUIDs.
    It must be the same lakehouse NB_LineageSnapshot reads from."""
    lakehouse_id, workspace_id = str(lakehouse_id).lower(), str(workspace_id)
    print(f"Resolved workspace {workspace_id}, "
          f"metadata lakehouse {lakehouse_id}, pinned by GUID")
    return {
        "workspace_id":         workspace_id,
        "lineage_lakehouse_id": lakehouse_id,
        "lakehouse_workspace":  {lakehouse_id: workspace_id},
    }


def _storage_options() -> dict:
    """delta-rs storage options for OneLake (the documented Fabric pattern)."""
    return {"bearer_token": notebookutils.credentials.getToken("storage"),
            "use_fabric_endpoint": "true"}


# Files that an overwrite supersedes stay on disk until something reclaims them.
# 168 hours is Delta's own default retention and satisfies its safety check, so
# only files superseded a week ago are ever removed. Set to 0 to skip vacuuming.
VACUUM_RETENTION_HOURS = 168


def _vacuum(uri: str) -> None:
    """Reclaim the files each overwrite supersedes. delta-rs has no full VACUUM,
    which finds orphans the log never recorded; every orphan here is a tombstone
    written by our own overwrite, which is the case its vacuum does cover.
    Maintenance, never correctness: a failure here must not fail the run."""
    if not VACUUM_RETENTION_HOURS:
        return
    try:
        removed = DeltaTable(uri, storage_options=_storage_options()).vacuum(
            retention_hours=VACUUM_RETENTION_HOURS, dry_run=False)
        if removed:
            print(f"  vacuumed {len(removed)} superseded file(s) from "
                  f"{uri.rsplit('/', 1)[-1]}")
    except Exception as e:
        print(f"  WARNING: vacuum skipped for {uri.rsplit('/', 1)[-1]} "
              f"({type(e).__name__}: {e})")


def write_metadata_table(arrow_table, config: dict, qualified_name: str,
                         mode: str = "overwrite") -> None:
    """Write a Delta table into the metadata lakehouse by abfss path, via
    delta-rs. In a schema-enabled lakehouse the Tables/<schema>/<table> write
    creates the schema folder, so no CREATE SCHEMA and no attached lakehouse are
    needed. Written without reader features, so Spark, the SQL endpoint and
    Direct Lake all read the result."""
    lh = config["lineage_lakehouse_id"]
    ws = (config.get("lakehouse_workspace") or {}).get(lh) or config["workspace_id"]
    schema, table = (qualified_name.split(".", 1) if "." in qualified_name
                     else ("dbo", qualified_name))
    uri = (f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/"
           f"{lh}/Tables/{schema}/{table}")
    write_deltalake(
        uri, arrow_table, mode=mode,
        schema_mode="overwrite" if mode == "overwrite" else "merge",
        storage_options=_storage_options())
    _vacuum(uri)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

# Parameters (overridable by a pipeline activity)
# Opt-in switch for the semantic-model (Power BI / "Platin") module. Set False in
# environments without semantic models to skip this stage entirely.


enable_semantic_models = True

# The Power BI workspaces to scan. Takes one GUID, several separated by commas,
# a JSON array or a real list, since a pipeline hands this over as a string
# whichever way it was typed.
#
# In this repo the semantic models live in the Serve workspace. A "<...>"
# placeholder is skipped with a message rather than failing, so the stage stays
# a no-op until the GUID is filled in.
platin_workspaces = "<dev-serve-workspace-guid>"

# The governance lakehouse this collector writes to, by GUID. PL_Lineage injects
# both from the Variable Library (DataWorkspaceID, LH_Governance_ID). It has to
# be the same lakehouse NB_LineageSnapshot and NB_LineageSetup use.
data_workspace_id = "1cfeab7c-4df3-48cb-8407-c31a3bb8a7e5"

governance_lakehouse_id = "1c93cf41-5b66-4694-9ef0-8ea6d3d89ed4"

# Scan control.
#   scan_mode = "full"     -> scan every model in the workspaces
#   scan_mode = "targeted" -> scan only the models named in scan_models
# exclude_models is always skipped (drop utils/template/test models even in a
# full scan). Names are matched case-insensitively, exact (not substring).
scan_mode      = "full"          # "full" | "targeted"
scan_models    = [
    # "Model A", "Model B",
]
exclude_models = [
    "Template", "Template_leer", "UDF Test", "xx",
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


def as_list(value) -> list:
    """A pipeline parameter as a list. Accepts a real list, a JSON array, or a
    comma-separated string, because pipeline parameters arrive as strings."""
    if not isinstance(value, str):
        return list(value or [])
    value = value.strip()
    if not value:
        return []
    return (json.loads(value) if value.startswith("[")
            else [v.strip() for v in value.split(",") if v.strip()])


def as_bool(value) -> bool:
    """Pipeline booleans arrive as the strings 'True'/'False'."""
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return bool(value)


def pinned(value) -> str | None:
    """A GUID parameter, or None when blank or still a "<...>" placeholder."""
    value = str(value or "").strip()
    return None if not value or value.startswith("<") else value.lower()


enable_semantic_models = as_bool(enable_semantic_models)

if not enable_semantic_models:
    print("Semantic-model (Platin) collection disabled; skipping.")
    notebookutils.notebook.exit(json.dumps({"status": "skipped"}))

# A pipeline hands each of these over as a string, in whatever form it was typed
# into the activity. as_list takes a bare GUID, a comma-separated list, a JSON
# array or a real list.
platin_workspaces = as_list(platin_workspaces)
exclude_models    = as_list(exclude_models)
scan_models       = as_list(scan_models)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

config = {
    "platin_workspaces": platin_workspaces,

    # Which models to scan (see the parameters cell).
    "scan_mode":      scan_mode,        # "full" | "targeted"
    "scan_models":    scan_models,      # include list (targeted mode)
    "exclude_models": exclude_models,   # always skipped

    # Output table names, schema-qualified for the schema-enabled
    # metadata lakehouse (semantic_models.<table>).
    "tables": {
        "models":        "semantic_models.platin_models",
        "columns":       "semantic_models.platin_columns",
        "measures":      "semantic_models.platin_measures",
        "relationships": "semantic_models.platin_relationships",
        "lineage":       "semantic_models.platin_lineage",
        "calc_deps":     "semantic_models.platin_calc_dependencies",
        "shortcuts":     "semantic_models.platin_shortcuts",
        "model_sources": "semantic_models.platin_model_sources",
    },

    "write_mode": "overwrite",   # overwrite | append
}

# Lakehouse identity for the abfss writes, required. Missing GUIDs stop the run
# rather than being guessed; PL_Lineage injects both from the Variable Library.
_lh_pin, _ws_pin = pinned(governance_lakehouse_id), pinned(data_workspace_id)
if not (_lh_pin and _ws_pin):
    raise ValueError(
        "governance_lakehouse_id and data_workspace_id are required. Fill them "
        "in the parameters cell, or run through PL_Lineage, which injects them "
        "from the Variable Library (DataWorkspaceID, LH_Governance_ID).")
_conn = pin_metadata_lakehouse(_lh_pin, _ws_pin)
config["workspace_id"]         = _conn["workspace_id"]
config["lineage_lakehouse_id"] = _conn["lineage_lakehouse_id"]
config["lakehouse_workspace"]  = _conn["lakehouse_workspace"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Single timestamp for the whole run, stamped on every row.
COLLECTED_AT = datetime.now(timezone.utc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


def _s(v):
    """Safe str for .NET / None values; empty string -> None."""
    if v is None:
        return None
    try:
        s = str(v)
    except Exception:
        return None
    return s if s != "" else None


def _enum(v):
    """Render a .NET enum (or anything) as its short name.
    'PartitionSourceType.M' -> 'M'."""
    s = _s(v)
    return s.rsplit(".", 1)[-1] if s is not None else None


def resolve_workspace_name(ws_id):
    try:
        return fabric.resolve_workspace_name(ws_id)
    except Exception:
        return None


def _pick_col(df, *needles):
    """First column whose lowercased name contains all needles."""
    for c in df.columns:
        cl = c.lower()
        if all(n in cl for n in needles):
            return c
    return None


def list_models(ws_id):
    """[{model_id, model_name, storage_mode}] for a workspace, tolerant of
    column-name differences between sempy_labs and sempy.fabric."""
    df, api_errors = None, []
    for fn in (lambda: labs.list_semantic_models(workspace=ws_id),
               lambda: fabric.list_datasets(workspace=ws_id)):
        try:
            df = fn()
            if df is not None and len(df.columns):
                break
        except Exception as e:
            api_errors.append(f"{type(e).__name__}: {e}")
            df = None
    if df is None and api_errors:
        # Both APIs failed. Returning [] here would read an unreachable
        # workspace as one with no models and end the run green.
        raise RuntimeError("listing semantic models failed via sempy_labs and "
                           "sempy: " + "; ".join(api_errors))
    if df is None or len(df) == 0:
        return []
    name_col = _pick_col(df, "name")
    id_col   = _pick_col(df, "id")
    mode_col = _pick_col(df, "storage", "mode") or _pick_col(df, "mode")
    out = []
    for _, r in df.iterrows():
        out.append({
            "model_name":   _s(r[name_col]) if name_col else None,
            "model_id":     _s(r[id_col])   if id_col   else None,
            "storage_mode": _enum(r[mode_col]) if mode_col else None,
        })
    return [m for m in out if m["model_name"]]


# Best-effort extraction of a "schema.table" reference from an M partition
# expression (import models). Direct Lake partitions don't need this, they
# expose EntityName directly.
_M_ITEM_RE = re.compile(
    r'\[\s*Schema\s*=\s*"(?P<schema>[^"]+)"\s*,\s*Item\s*=\s*"(?P<item>[^"]+)"',
    re.IGNORECASE,
)
_M_NAV_RE = re.compile(r'Item\s*=\s*"(?P<item>[^"]+)"', re.IGNORECASE)


def parse_m_target(expr):
    if not expr:
        return None
    m = _M_ITEM_RE.search(expr)
    if m:
        return f'{m.group("schema")}.{m.group("item")}'
    m = _M_NAV_RE.search(expr)
    return m.group("item") if m else None


def select_models(models, config):
    """Filter list_models() output by scan_mode / scan_models /
    exclude_models (case-insensitive exact name match). Returns
    (selected, dropped) where dropped is [(name, reason), ...]."""
    mode    = (config.get("scan_mode") or "full").lower()
    include = {m.strip().lower() for m in (config.get("scan_models") or [])}
    exclude = {m.strip().lower() for m in (config.get("exclude_models") or [])}
    selected, dropped = [], []
    for m in models:
        nm = (m.get("model_name") or "").strip().lower()
        if nm in exclude:
            dropped.append((m["model_name"], "excluded"))
            continue
        if mode == "targeted" and nm not in include:
            dropped.append((m["model_name"], "not in scan_models"))
            continue
        selected.append(m)
    if mode == "targeted":
        present = {(m.get("model_name") or "").strip().lower() for m in models}
        for want in sorted(include - present):
            dropped.append((want, "requested but not found in workspace"))
    return selected, dropped

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


def collect_model(ws_id, ws_name, model):
    """Returns (model_row, columns, measures, relationships, lineage)."""
    name = model["model_name"]
    base = {
        "workspace_id":   ws_id,
        "workspace_name": ws_name,
        "model_id":       model["model_id"],
        "model_name":     name,
    }
    cols, meas, rels, lin = [], [], [], []

    with connect_semantic_model(dataset=name, workspace=ws_id,
                                readonly=True) as tom:
        table_count = sum(1 for _ in tom.model.Tables)

        for c in tom.all_columns():
            cols.append({**base,
                "table_name":    _s(getattr(getattr(c, "Parent", None), "Name", None)),
                "column_name":   _s(getattr(c, "Name", None)),
                "data_type":     _enum(getattr(c, "DataType", None)),
                "column_type":   _enum(getattr(c, "Type", None)),
                "source_column": _s(getattr(c, "SourceColumn", None)),
                "expression":    _s(getattr(c, "Expression", None)),
                "is_hidden":     bool(getattr(c, "IsHidden", False)),
            })

        for m in tom.all_measures():
            meas.append({**base,
                "table_name":     _s(getattr(getattr(m, "Parent", None), "Name", None)),
                "measure_name":   _s(getattr(m, "Name", None)),
                "expression":     _s(getattr(m, "Expression", None)),
                "display_folder": _s(getattr(m, "DisplayFolder", None)),
                "format_string":  _s(getattr(m, "FormatString", None)),
                "is_hidden":      bool(getattr(m, "IsHidden", False)),
            })

        for r in tom.model.Relationships:
            fc = getattr(r, "FromColumn", None)
            tc = getattr(r, "ToColumn", None)
            rels.append({**base,
                "from_table":       _s(getattr(getattr(fc, "Table", None), "Name", None)),
                "from_column":      _s(getattr(fc, "Name", None)),
                "to_table":         _s(getattr(getattr(tc, "Table", None), "Name", None)),
                "to_column":        _s(getattr(tc, "Name", None)),
                "from_cardinality": _enum(getattr(r, "FromCardinality", None)),
                "to_cardinality":   _enum(getattr(r, "ToCardinality", None)),
                "cross_filtering":  _enum(getattr(r, "CrossFilteringBehavior", None)),
                "is_active":        bool(getattr(r, "IsActive", True)),
            })

        # Lineage to the lakehouse, via each table's partitions.
        #   - Direct Lake / Dual: Entity partitions -> EntityName is the table.
        #   - Import: M partitions -> best-effort parse of the M expression.
        for t in tom.model.Tables:
            tname = _s(getattr(t, "Name", None))
            for p in getattr(t, "Partitions", []):
                src   = getattr(p, "Source", None)
                stype = _enum(getattr(p, "SourceType", None)) or (
                    _enum(getattr(src, "Type", None)) if src is not None else None)
                m_expr   = _s(getattr(src, "Expression", None))
                entity   = _s(getattr(src, "EntityName", None))
                schema   = _s(getattr(src, "SchemaName", None))
                expr_src = getattr(src, "ExpressionSource", None)
                if entity:
                    parsed = f"{schema}.{entity}" if schema else entity
                else:
                    parsed = parse_m_target(m_expr)
                lin.append({**base,
                    "model_table":       tname,
                    "partition_name":    _s(getattr(p, "Name", None)),
                    "source_type":       stype,
                    "entity_name":       entity,
                    "schema_name":       schema,
                    "expression_source": _s(getattr(expr_src, "Name", None)),
                    "m_expression":      m_expr,
                    "parsed_ref":        parsed,
                })

    model_row = {**base,
        "storage_mode":  model["storage_mode"],
        "table_count":   int(table_count),
        "column_count":  len(cols),
        "measure_count": len(meas),
    }
    return model_row, cols, meas, rels, lin


def collect_calc_deps(ws_id, ws_name, model):
    name = model["model_name"]
    base = {
        "workspace_id":   ws_id,
        "workspace_name": ws_name,
        "model_id":       model["model_id"],
        "model_name":     name,
    }
    rows = []
    try:
        df = labs.get_model_calc_dependencies(dataset=name, workspace=ws_id)
    except Exception as e:
        print(f"    calc deps unavailable for {name}: {type(e).__name__}: {e}")
        return rows
    for _, r in df.iterrows():
        rows.append({**base,
            "table_name":             _s(r.get("Table Name")),
            "object_name":            _s(r.get("Object Name")),
            "object_type":            _s(r.get("Object Type")),
            "referenced_table":       _s(r.get("Referenced Table")),
            "referenced_object":      _s(r.get("Referenced Object")),
            "referenced_object_type": _s(r.get("Referenced Object Type")),
            "expression":             _s(r.get("Expression")),
        })
    return rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


def _first(row, *cands):
    """First present, non-null value from a pandas row (Series) by candidate
    column names, tolerates column-name drift across sempy versions."""
    for c in cands:
        if c in row.index:
            v = row[c]
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                return v
    return None


def collect_shortcuts(ws_id, ws_name):
    """Every shortcut across every lakehouse in a workspace -> rows for
    platin_shortcuts, with the resolved source (workspace + item GUID + path)
    so the bridge can tie a Platin Lakehouse table back to its gold source.

    Handles both kinds:
      - table shortcut:  shortcut_path = Tables/<entity>
      - schema shortcut: shortcut_path = Tables/<schema>, exposing every table
        beneath it (one row, not one per table). The bridge resolves
        Tables/<schema>/<entity> -> <source_path>/<entity> by prefix.
    """
    rows = []
    try:
        lhs = labs.list_lakehouses(workspace=ws_id)
    except Exception as e:
        print(f"  list_lakehouses failed for {ws_name or ws_id}: "
              f"{type(e).__name__}: {e}")
        return rows
    for _, lh in lhs.iterrows():
        lh_id   = _s(_first(lh, "Lakehouse ID", "Lakehouse Id"))
        lh_name = _s(_first(lh, "Lakehouse Name"))
        schema_enabled = bool(_first(lh, "Schema Enabled") or False)
        try:
            sc = list_shortcuts(lakehouse=lh_id, workspace=ws_id)
        except Exception as e:
            print(f"    list_shortcuts failed for {lh_name}: "
                  f"{type(e).__name__}: {e}")
            continue
        for _, s in sc.iterrows():
            rows.append({
                "workspace_id":          ws_id,
                "workspace_name":        ws_name,
                "lakehouse_id":          lh_id,
                "lakehouse_name":        lh_name,
                "schema_enabled":        schema_enabled,
                "shortcut_name":         _s(_first(s, "Shortcut Name")),
                "shortcut_path":         _s(_first(s, "Shortcut Path")),
                "source_type":           _s(_first(s, "Source Type")),
                "source_workspace_id":   _s(_first(s, "Source Workspace Id")),
                "source_workspace_name": _s(_first(s, "Source Workspace Name")),
                "source_item_id":        _s(_first(s, "Source Item Id")),
                "source_item_name":      _s(_first(s, "Source Item Name")),
                "source_item_type":      _s(_first(s, "Source Item Type")),
                "source_path":           _s(_first(s, "OneLake Path")),
                "source_location":       _s(_first(s, "Location")),
                "source_subpath":        _s(_first(s, "SubPath")),
            })
    return rows


def build_lakehouse_index(ws_id):
    """Map every id that can identify a lakehouse -> the Lakehouse item GUID.
    Includes the lakehouse id itself and its SQL analytics endpoint id, so a
    Direct Lake source that reports the SQL endpoint still resolves to the
    lakehouse that platin_shortcuts.lakehouse_id is keyed on."""
    idx = {}
    try:
        lhs = labs.list_lakehouses(workspace=ws_id)
    except Exception as e:
        print(f"  build_lakehouse_index failed for {ws_id}: {type(e).__name__}: {e}")
        return idx
    for _, lh in lhs.iterrows():
        lh_id  = _s(_first(lh, "Lakehouse ID", "Lakehouse Id"))
        # semantic-link-labs labels the SQL-endpoint column differently across
        # versions; try the known variants so Direct Lake sources still resolve.
        sql_id = _s(_first(lh, "SQL Endpoint ID", "SQL Endpoint Id",
                           "SQL Analytics Endpoint ID", "SQLEndpointId"))
        if lh_id:
            idx[lh_id] = lh_id          # lakehouse id maps to itself (no-op when already correct)
            if sql_id:
                idx[sql_id] = lh_id     # endpoint id -> lakehouse id
    return idx


def collect_direct_lake_sources(ws_id, ws_name, model, lh_index=None):
    """Semantic model -> the Direct Lake source(s) it binds to (the Platin
    Lakehouse/Warehouse). Empty for import models. Rows for
    platin_model_sources; links a model to the lakehouse whose shortcuts
    collect_shortcuts resolved."""
    name = model["model_name"]
    base = {
        "workspace_id":   ws_id,
        "workspace_name": ws_name,
        "model_id":       model["model_id"],
        "model_name":     name,
    }
    rows = []
    try:
        sources = get_direct_lake_sources(dataset=name, workspace=ws_id)
    except Exception as e:
        # Import models have no Direct Lake source and land here too, but the
        # reason is printed so a permission failure on a real Direct Lake
        # model does not silently read as "no source".
        print(f"    no direct lake sources for {name}: "
              f"{type(e).__name__}: {e}")
        return rows
    lh_index = lh_index or {}
    for src in (sources or []):
        raw_id = _s(src.get("itemId"))
        use = src.get("usesSqlEndpoint")
        rows.append({**base,
            "source_item_id":        lh_index.get(raw_id, raw_id),   # normalized to lakehouse GUID
            "source_item_name":      _s(src.get("itemName")),
            "source_item_type":      _s(src.get("itemType")),
            "source_workspace_id":   _s(src.get("workspaceId")),
            "source_workspace_name": _s(src.get("workspaceName")),
            "uses_sql_endpoint":     bool(use) if use is not None else None,
        })
    return rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


all_models, all_cols, all_meas, all_rels, all_lin, all_deps = [], [], [], [], [], []
all_shortcuts, all_sources = [], []
errors = []
skipped_empty = []

for ws_id in config["platin_workspaces"]:
    if not ws_id or str(ws_id).startswith("<"):
        print(f"skip placeholder workspace {ws_id!r}")
        continue
    ws_name = resolve_workspace_name(ws_id)
    shortcuts = collect_shortcuts(ws_id, ws_name)
    lh_index  = build_lakehouse_index(ws_id)
    all_shortcuts += shortcuts
    try:
        models = list_models(ws_id)
    except Exception as e:
        errors.append((ws_id, "<list models>", f"{type(e).__name__}: {e}"))
        print(f"  ! listing models failed for {ws_name or ws_id}: "
              f"{type(e).__name__}: {e}")
        continue
    models, dropped = select_models(models, config)
    n_not_listed = sum(1 for _, why in dropped if why == "not in scan_models")
    for nm, why in dropped:
        if why != "not in scan_models":
            print(f"  ~ {nm}: {why}")
    print(f"Workspace {ws_name or ws_id}: {len(models)} selected "
          f"({config.get('scan_mode', 'full')}), {n_not_listed} not in scan list, "
          f"{len(shortcuts)} shortcut(s)")
    for model in models:
        mname = model["model_name"]
        try:
            mrow, cols, meas, rels, lin = collect_model(ws_id, ws_name, model)
            if mrow["table_count"] == 0:
                # Empty model: typically a lakehouse/warehouse *default*
                # semantic model, which exposes no TOM objects and rejects
                # DISCOVER_CALC_DEPENDENCY ("database is empty"). Not a real
                # Platin model, skip it (and skip the calc-deps call).
                skipped_empty.append((ws_id, mname))
                print(f"  - {mname}: empty model (0 tables) - skipped")
                continue
            deps = collect_calc_deps(ws_id, ws_name, model)
            sources = collect_direct_lake_sources(ws_id, ws_name, model, lh_index)
            all_models.append(mrow)
            all_cols += cols
            all_meas += meas
            all_rels += rels
            all_lin  += lin
            all_deps += deps
            all_sources += sources
            print(f"  + {mname}: {len(cols)} cols, {len(meas)} measures, "
                  f"{len(rels)} rels, {len(lin)} partitions, {len(deps)} deps, "
                  f"{len(sources)} source(s)")
        except Exception as e:
            errors.append((ws_id, mname, f"{type(e).__name__}: {e}"))
            print(f"  ! {mname} failed: {type(e).__name__}: {e}")

print(f"\nCollected: {len(all_models)} models, {len(all_cols)} columns, "
      f"{len(all_meas)} measures, {len(all_rels)} relationships, "
      f"{len(all_lin)} partitions, {len(all_deps)} calc deps, "
      f"{len(all_shortcuts)} shortcuts, {len(all_sources)} model sources")
if skipped_empty:
    print(f"Skipped {len(skipped_empty)} empty/default model(s): "
          + ", ".join(m for _, m in skipped_empty))
if errors:
    print(f"{len(errors)} model(s) failed:")
    for w, m, e in errors:
        print(f"  {m} ({w}): {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

_ID = [
    ("workspace_id",   pa.string()),
    ("workspace_name", pa.string()),
    ("model_id",       pa.string()),
    ("model_name",     pa.string()),
]
_TS = [("collected_at", pa.timestamp("us", tz="UTC"))]

MODELS_SCHEMA = pa.schema(_ID + [
    ("storage_mode",  pa.string()),
    ("table_count",   pa.int64()),
    ("column_count",  pa.int64()),
    ("measure_count", pa.int64()),
] + _TS)

COLUMNS_SCHEMA = pa.schema(_ID + [
    ("table_name",    pa.string()),
    ("column_name",   pa.string()),
    ("data_type",     pa.string()),
    ("column_type",   pa.string()),
    ("source_column", pa.string()),
    ("expression",    pa.string()),
    ("is_hidden",     pa.bool_()),
] + _TS)

MEASURES_SCHEMA = pa.schema(_ID + [
    ("table_name",     pa.string()),
    ("measure_name",   pa.string()),
    ("expression",     pa.string()),
    ("display_folder", pa.string()),
    ("format_string",  pa.string()),
    ("is_hidden",      pa.bool_()),
] + _TS)

RELATIONSHIPS_SCHEMA = pa.schema(_ID + [
    ("from_table",       pa.string()),
    ("from_column",      pa.string()),
    ("to_table",         pa.string()),
    ("to_column",        pa.string()),
    ("from_cardinality", pa.string()),
    ("to_cardinality",   pa.string()),
    ("cross_filtering",  pa.string()),
    ("is_active",        pa.bool_()),
] + _TS)

LINEAGE_SCHEMA = pa.schema(_ID + [
    ("model_table",       pa.string()),
    ("partition_name",    pa.string()),
    ("source_type",       pa.string()),
    ("entity_name",       pa.string()),
    ("schema_name",       pa.string()),
    ("expression_source", pa.string()),
    ("m_expression",      pa.string()),
    ("parsed_ref",        pa.string()),
] + _TS)

CALC_DEPS_SCHEMA = pa.schema(_ID + [
    ("table_name",             pa.string()),
    ("object_name",            pa.string()),
    ("object_type",            pa.string()),
    ("referenced_table",       pa.string()),
    ("referenced_object",      pa.string()),
    ("referenced_object_type", pa.string()),
    ("expression",             pa.string()),
] + _TS)

SHORTCUTS_SCHEMA = pa.schema([
    ("workspace_id",          pa.string()),
    ("workspace_name",        pa.string()),
    ("lakehouse_id",          pa.string()),
    ("lakehouse_name",        pa.string()),
    ("schema_enabled",        pa.bool_()),
    ("shortcut_name",         pa.string()),
    ("shortcut_path",         pa.string()),
    ("source_type",           pa.string()),
    ("source_workspace_id",   pa.string()),
    ("source_workspace_name", pa.string()),
    ("source_item_id",        pa.string()),
    ("source_item_name",      pa.string()),
    ("source_item_type",      pa.string()),
    ("source_path",           pa.string()),
    ("source_location",       pa.string()),
    ("source_subpath",        pa.string()),
    ("collected_at",          pa.timestamp("us", tz="UTC")),
])

MODEL_SOURCES_SCHEMA = pa.schema(_ID + [
    ("source_item_id",        pa.string()),
    ("source_item_name",      pa.string()),
    ("source_item_type",      pa.string()),
    ("source_workspace_id",   pa.string()),
    ("source_workspace_name", pa.string()),
    ("uses_sql_endpoint",     pa.bool_()),
] + _TS)


def _cell(v):
    """A value from a sempy pandas frame can arrive as a float NaN even in a
    text column. pyarrow would reject that against a string field, so it becomes
    a proper null."""
    return None if isinstance(v, float) and pd.isna(v) else v


def write_table(rows, schema, table_name):
    """Stamp the run timestamp, project the rows onto the declared columns, and
    write. The explicit pyarrow schema keeps a column that is None in every row
    at its declared type, so a collector that found nothing still writes a
    correctly typed empty table."""
    for r in rows:
        r["collected_at"] = COLLECTED_AT
    projected = [{name: _cell(r.get(name)) for name in schema.names}
                 for r in rows]
    # abfss write to the metadata lakehouse (no attached lakehouse / CREATE SCHEMA).
    write_metadata_table(pa.Table.from_pylist(projected, schema=schema),
                         config, table_name, mode=config["write_mode"])
    print(f"  wrote {table_name}: {len(rows)} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


T = config["tables"]
write_table(all_models, MODELS_SCHEMA,        T["models"])
write_table(all_cols,   COLUMNS_SCHEMA,       T["columns"])
write_table(all_meas,   MEASURES_SCHEMA,      T["measures"])
write_table(all_rels,   RELATIONSHIPS_SCHEMA, T["relationships"])
write_table(all_lin,    LINEAGE_SCHEMA,       T["lineage"])
write_table(all_deps,   CALC_DEPS_SCHEMA,     T["calc_deps"])
write_table(all_shortcuts,  SHORTCUTS_SCHEMA,     T["shortcuts"])
write_table(all_sources,    MODEL_SOURCES_SCHEMA, T["model_sources"])
print("done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- pipeline status: short exit value for orchestration / alerting ---
try:
    if errors and not all_models:
        st = "error"
    elif errors:
        st = "partial"
    elif all_models:
        st = "ok"
    else:
        st = "empty"
    status = {
        "status":        st,
        "models":        len(all_models),
        "columns":       len(all_cols),
        "measures":      len(all_meas),
        "relationships": len(all_rels),
        "lineage":       len(all_lin),
        "calc_deps":     len(all_deps),
        "shortcuts":     len(all_shortcuts),
        "model_sources": len(all_sources),
        "skipped_empty": len(skipped_empty),
        "errors":        len(errors),
    }
except Exception as e:
    status = {"status": "error", "error": f"{type(e).__name__}: {e}"}
print(status)
# Fail the activity on a hard error so the run shows red and retries/alerts fire.
# A "partial" run (some models failed, some succeeded) keeps its written data and
# succeeds, but prints a prominent warning for monitoring.
if status.get("status") == "error":
    raise RuntimeError(f"NB_LineageSemanticModels failed: {status}")
if status.get("errors"):
    print(f"WARNING: {status['errors']} model(s) failed to collect (partial run).")
notebookutils.notebook.exit(json.dumps(status))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
