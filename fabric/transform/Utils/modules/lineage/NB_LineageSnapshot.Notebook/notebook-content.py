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

# NB_LineageSnapshot
#
# Parses the OpenLineage events the ETL notebooks emit into four Delta tables in
# the governance lakehouse:
#
#   lineage.events          one row per parsed column-lineage event
#   lineage.snapshot        one row per entity, with its current lineage shape
#   lineage.edges           the snapshot flattened to one row per edge
#   lineage.dataset_schema  column type, comment and nullability, silver and gold
#
# lineage.snapshot is a state table rather than a window aggregate. An entity
# that emitted nothing in this run keeps its last known shape until
# snapshot_stale_days pass without an event, so a weekly pipeline does not
# flicker in and out of the lineage and a retired table drops out on its own.
#
# If NB_LineageSemanticModels has written its semantic_models.platin_* tables,
# those models are folded into the same snapshot as layer = platin. There is
# nothing to switch on, and absent tables are skipped.
#
# Setup
#   1. Put the lakehouse GUIDs in the Variable Library: DataWorkspaceID,
#      LH_Governance_ID, LH_Bronze_ID, LH_Silver_ID, LH_Gold_ID. Give the running
#      identity access to that workspace.
#   2. Run NB_LineageSetup once so the ETL notebooks start emitting events.
#   3. Schedule PL_Lineage. It injects those GUIDs here into the other two
#      notebooks, so all three address the same lakehouses. Daily is usual.
#
# The notebook does not read the Variable Library, use a default lakehouse or run
# child notebooks, so it works under a service principal. PL_Lineage resolves the
# library before the session starts and passes plain strings.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

# The lakehouses this run reads and writes, by GUID. PL_Lineage injects them
# from the Variable Library.
#
# The workspace holding the lakehouses below, from DataWorkspaceID.
data_workspace_id = "1cfeab7c-4df3-48cb-8407-c31a3bb8a7e5"

# Holds Files/lineage_events and the four lineage.* tables (LH_Governance_ID).
governance_lakehouse_id = "1c93cf41-5b66-4694-9ef0-8ea6d3d89ed4"

# Used to label each dataset path bronze, silver or gold, by matching the
# lakehouse id inside it (LH_Bronze_ID, LH_Silver_ID, LH_Gold_ID).
bronze_lakehouse_id = "f5df3ca3-6568-4c3b-80e3-6f4ceb6a2d6f"
silver_lakehouse_id = "abb261c6-e1bd-4a24-bc3d-ff740faa8a87"
gold_lakehouse_id   = "d8a807cb-0836-437f-826c-416c5c8f05d1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Settings ---------------------------------------------------------------
# Everything the run reads, in one dict, each setting defined exactly once. The
# lakehouse identity is added by resolve_lakehouses() in the run cell.

import io as _io
import re
import json
import hashlib
from datetime import datetime, timedelta, timezone
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor

import requests
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from deltalake import DeltaTable, write_deltalake
except ImportError:
    raise ImportError(
        "NB_LineageSnapshot uses delta-rs for "
        "Delta I/O. Install it once per session: %pip install deltalake")


def pinned(value) -> str | None:
    """A GUID parameter, or None when blank or still a "<...>" placeholder.
    A missing GUID stops the run in resolve_lakehouses."""
    value = str(value or "").strip()
    return None if not value or value.startswith("<") else value.lower()


# {lakehouse_guid: layer} from the injected ids. Only the ids actually supplied
# appear, so pinning two of the three still works.
LAYER_IDS = {gid: layer for gid, layer in (
    (pinned(bronze_lakehouse_id), "bronze"),
    (pinned(silver_lakehouse_id), "silver"),
    (pinned(gold_lakehouse_id),   "gold"),
) if gid}

# How a dataset path maps to (data_source, application, entity), the identity a
# snapshot row is keyed on. Taken from the write paths in this repo's ETL:
#
#   bronze   Files/<data_source>/<entity>/{Delta|Full}/Year=/Month=/Day=/...
#            The bronze copy concatenates the source schema and table without a
#            separator, so <entity> is SalesLTCustomer and sits at the second
#            Files segment, not the third.
#   silver   Tables/<data_source>/<entity>, e.g. Tables/AdventureWorks/SalesLTCustomer
#   gold     Tables/<application>/<entity>, e.g. Tables/Sales/DimCustomer
#
# application comes from the gold notebooks' DESTINATION_SCHEMA rather than a
# silver path segment, so it stays empty for bronze and silver rows. Adjust
# these two for a different layout; nothing else assumes a scheme.
DEFAULT_FILES_REGEX = r"/Files/(?P<data_source>[^/]+)/(?P<entity>[^/]+)(?:/|$)"

DEFAULT_ENTITY_RULES = {
    "silver": {"data_source_from": "schema", "entity_from": "table"},
    "gold":   {"application_from": "schema", "entity_from": "table"},
}

CONFIG = {
    # Where the transport writes events, inside the governance lakehouse.
    "lineage_folder": "Files/lineage_events",
    # Path to identity mapping, documented above.
    "files_regex":  DEFAULT_FILES_REGEX,
    "entity_rules": {k: dict(v) for k, v in DEFAULT_ENTITY_RULES.items()},

    # Output tables, schema.table in the governance lakehouse. lineage.snapshot
    # also carries layer = platin rows when NB_LineageSemanticModels has written
    # the platin_tables below; absent tables are simply skipped.
    "parsed_table":   "lineage.events",
    "snapshot_table": "lineage.snapshot",
    "schema_table":   "lineage.dataset_schema",
    "edges_table":    "lineage.edges",
    "platin_tables": {
        "lineage":       "semantic_models.platin_lineage",
        "columns":       "semantic_models.platin_columns",
        "calc_deps":     "semantic_models.platin_calc_dependencies",
        "shortcuts":     "semantic_models.platin_shortcuts",
        "model_sources": "semantic_models.platin_model_sources",
    },

    # De-noising window, in days. The snapshot keeps the most frequent lineage
    # shape an entity emitted inside this window, so a table that reports more
    # than one shape (full vs incremental load, merge vs insert) does not flip
    # between runs. It is a sample, never a delay. One day is enough for a clean
    # daily pipeline; raise it only if a table's shape flaps, or to let a missed
    # run catch up.
    "snapshot_window_days": 1,
    # How often the winning shape must be seen inside the window. One puts a
    # table in the snapshot right after its first run.
    "snapshot_min_occurrences": 1,
    # Carry-forward horizon. An entity keeps its last known shape while its most
    # recent event is younger than this, even when it ran outside the window.
    # Zero makes the snapshot a pure window aggregate.
    "snapshot_stale_days": 30,
    # Raw event partitions older than this are pruned after a successful parse.
    # Must be larger than snapshot_window_days.
    "lineage_retention_days": 14,
    # Which layers get their column schemas collected into dataset_schema.
    # Bronze paths are partitioned files rather than tables, so they are out.
    "schema_collect_layers": ["silver", "gold"],
    # Layers whose entities are listed in the snapshot even when no write with
    # column lineage was captured, so the table stays a complete inventory of
    # what ran. Those rows carry lineage_status = "observed" and null lineage.
    "observed_layers": ["silver", "gold"],
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Engine: lakehouse resolution, table access, path parsing ----------------


def resolve_lakehouses(data_workspace_id, governance_lakehouse_id,
                       layer_ids) -> dict:
    """Where the run reads and writes, resolved once:

        governance_lakehouse_id  holds the events folder and the lineage.* tables
        governance_workspace_id  its workspace, the OneLake authority for writes
        lakehouse_layer          {lakehouse_guid: bronze|silver|gold}
        lakehouse_workspace      {lakehouse_guid: workspace_guid}

    A pure mapping over the GUIDs from the parameters cell: no API call, so the
    running identity needs OneLake access only. Missing GUIDs stop the run
    rather than being guessed; the Variable Library holds the right values per
    environment, and PL_Lineage injects them.
    """
    layer_ids = dict(layer_ids or {})
    missing = [name for name, value in (
        ("data_workspace_id", data_workspace_id),
        ("governance_lakehouse_id", governance_lakehouse_id),
        ("bronze/silver/gold lakehouse ids", layer_ids),
    ) if not value]
    if missing:
        raise ValueError(
            f"Missing lakehouse GUIDs: {', '.join(missing)}. Fill them in the "
            f"parameters cell, or run through PL_Lineage, which injects them "
            f"from the Variable Library (DataWorkspaceID, LH_Governance_ID, "
            f"LH_Bronze_ID, LH_Silver_ID, LH_Gold_ID).")

    lakehouse_workspace = {gid: data_workspace_id for gid in layer_ids}
    lakehouse_workspace[governance_lakehouse_id] = data_workspace_id
    print(f"Resolved workspace {data_workspace_id}, governance lakehouse "
          f"{governance_lakehouse_id}, {len(layer_ids)} layer lakehouse(s) "
          f"{sorted(set(layer_ids.values()))}")
    return {"governance_lakehouse_id": governance_lakehouse_id,
            "governance_workspace_id": data_workspace_id,
            "lakehouse_layer":         layer_ids,
            "lakehouse_workspace":     lakehouse_workspace}


# ---- OneLake + Delta access (no Spark session) -------------------------------
# Two thin layers over the notebook identity's storage token:
#   - ADLS-gen2 REST against onelake.dfs.fabric.microsoft.com for raw files
#     (event JSONs, _delta_log reads, retention deletes)
#   - delta-rs for the module's own Delta tables
# Our tables are written without reader features beyond the base protocol, so
# Spark, the SQL endpoint and Direct Lake all read them.

ONELAKE = "https://onelake.dfs.fabric.microsoft.com"


def _storage_token() -> str:
    return notebookutils.credentials.getToken("storage")


def _storage_options() -> dict:
    """delta-rs storage options for OneLake (the documented Fabric pattern)."""
    return {"bearer_token": _storage_token(), "use_fabric_endpoint": "true"}


def _onelake_request(method: str, url: str, expect=(200, 201, 202), **kw):
    resp = requests.request(
        method, url, timeout=120,
        headers={"Authorization": "Bearer " + _storage_token(),
                 **kw.pop("headers", {})}, **kw)
    if resp.status_code not in expect:
        raise RuntimeError(f"{method} {url.split('?')[0]} -> "
                           f"{resp.status_code}: {resp.text[:300]}")
    return resp


def _is_dir(entry: dict) -> bool:
    # The list API marks directories with the string "true".
    return str(entry.get("isDirectory", "")).lower() == "true"


def onelake_list(workspace_id: str, path: str, recursive: bool = False) -> list:
    """Entries under <workspace>/<path> from the ADLS-gen2 list API, paginated
    via the continuation header. [] when the path does not exist."""
    out, token = [], None
    while True:
        url = (f"{ONELAKE}/{workspace_id}?resource=filesystem"
               f"&directory={path}&recursive={'true' if recursive else 'false'}")
        if token:
            url += "&continuation=" + requests.utils.quote(token, safe="")
        resp = _onelake_request("GET", url, expect=(200, 404))
        if resp.status_code == 404:
            return out
        out += (resp.json().get("paths") or [])
        token = resp.headers.get("x-ms-continuation")
        if not token:
            return out


def onelake_read(workspace_id: str, path: str) -> bytes:
    return _onelake_request("GET", f"{ONELAKE}/{workspace_id}/{path}").content


def onelake_delete(workspace_id: str, path: str) -> None:
    _onelake_request("DELETE", f"{ONELAKE}/{workspace_id}/{path}?recursive=true")


def onelake_exists(workspace_id: str, path: str) -> bool:
    resp = _onelake_request(
        "HEAD", f"{ONELAKE}/{workspace_id}/{path}?action=getStatus",
        expect=(200, 404))
    return resp.status_code == 200


def _lakehouse_workspace(config: dict, lakehouse_id: str) -> str:
    return ((config.get("lakehouse_workspace") or {}).get(lakehouse_id)
            or config["governance_workspace_id"])


def lineage_ingest(config: dict) -> tuple:
    """(workspace_id, '<lakehouse>/<folder>') of the collector folder."""
    lh = config["governance_lakehouse_id"]
    return _lakehouse_workspace(config, lh), f"{lh}/{config['lineage_folder']}"


def _table_location(config: dict, qualified_name: str) -> tuple:
    """(workspace_id, '<lakehouse>/Tables/<schema>/<table>') for a lineage
    table. Accepts 'schema.table' or a bare 'table' (Fabric default 'dbo')."""
    lh = config["governance_lakehouse_id"]
    schema, table = (qualified_name.split(".", 1) if "." in qualified_name
                     else ("dbo", qualified_name))
    return _lakehouse_workspace(config, lh), f"{lh}/Tables/{schema}/{table}"


def _table_abfss(config: dict, qualified_name: str) -> str:
    ws, path = _table_location(config, qualified_name)
    return f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{path}"


def read_metadata_table(config: dict, qualified_name: str) -> list:
    """Rows of a metadata-lakehouse Delta table as dicts, via delta-rs."""
    table = DeltaTable(_table_abfss(config, qualified_name),
                       storage_options=_storage_options())
    return table.to_pyarrow_table().to_pylist()


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
        print(f"  Warning: vacuum skipped for {uri.rsplit('/', 1)[-1]} "
              f"({type(e).__name__}: {e})")


def write_metadata_table(rows: list, schema: "pa.Schema", config: dict,
                         qualified_name: str) -> None:
    """Overwrite one of the module's Delta tables. The explicit pyarrow schema
    keeps a column that is None in every row at its declared type."""
    uri = _table_abfss(config, qualified_name)
    write_deltalake(uri, pa.Table.from_pylist(rows, schema=schema),
                    mode="overwrite", schema_mode="overwrite",
                    storage_options=_storage_options())
    _vacuum(uri)


def metadata_table_exists(config: dict, qualified_name: str) -> bool:
    """True if the table's _delta_log exists (a Delta table's one invariant).
    A 404 is False; any other failure raises, because an unreachable table read
    as an absent one would make the snapshot silently drop its carried state."""
    ws, path = _table_location(config, qualified_name)
    return onelake_exists(ws, f"{path}/_delta_log")


# ---- path parsing ------------------------------------------------------------

LH_ID_RE = re.compile(
    r"/(?P<lh_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/",
    re.IGNORECASE,
)
TABLES_RE = re.compile(r"/Tables/(?P<schema>[^/]+)/(?P<table>[^/]+?)(?:/|$)")


def extract_lakehouse_id(path: str) -> str | None:
    if not path:
        return None
    m = LH_ID_RE.search(path)
    return m.group("lh_id").lower() if m else None


def _apply_entity_rule(base: dict, rule: dict, schema: str, table: str) -> None:
    """Populate data_source/application/entity on `base` from a layer rule.

    A rule may specify a `table_regex` (named groups data_source/application/
    entity) and/or `<field>_from` maps ('schema' | 'table'). Field maps are
    applied after the regex so they can override it.
    """
    src = {"schema": schema, "table": table}
    tr = rule.get("table_regex")
    if tr:
        m = re.search(tr, table or "")
        if m:
            gd = m.groupdict()
            for k in ("data_source", "application", "entity"):
                if gd.get(k) is not None:
                    base[k] = gd[k]
    for field in ("data_source", "application", "entity"):
        frm = rule.get(f"{field}_from")
        if frm in src and src[frm] is not None:
            base[field] = src[frm]


def parse_path(path: str, lakehouse_layer: dict, entity_rules: dict | None = None,
               files_regex=None) -> dict:
    """Identify the layer (via the lakehouse GUID in the path) and extract
    data_source/application/entity using the configured naming conventions."""
    entity_rules = DEFAULT_ENTITY_RULES if entity_rules is None else entity_rules
    files_re = files_regex if files_regex is not None else DEFAULT_FILES_REGEX
    if isinstance(files_re, str):
        files_re = re.compile(files_re)

    base = {
        "path_kind": None, "layer": "unknown",
        "data_source": None, "application": None, "entity": None,
        "schema": None, "table": None, "is_delta_internal": False,
    }
    if not path:
        return base

    lh_id = extract_lakehouse_id(path)
    base["layer"] = lakehouse_layer.get(lh_id, "unknown") if lh_id else "unknown"
    base["is_delta_internal"] = "/_delta_log" in path

    if "/Files/" in path:
        m = files_re.search(path)
        if m:
            gd = m.groupdict()
            base.update({
                "path_kind":   "FILES",
                "data_source": gd.get("data_source"),
                "application": gd.get("application"),
                "entity":      gd.get("entity"),
            })
            return base

    if "/Tables/" in path:
        m = TABLES_RE.search(path)
        if m:
            base["path_kind"] = "TABLES"
            base["schema"]    = m.group("schema")
            base["table"]     = m.group("table")
            rule = (entity_rules or {}).get(base["layer"])
            if rule:
                _apply_entity_rule(base, rule, base["schema"], base["table"])
            return base

    return base

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Step 1: events -> Delta -------------------------------------------------


def _is_run_event(ev) -> bool:
    """Run events carry both run and job. Job and dataset events, which share
    the batch files, carry one or the other."""
    return isinstance(ev, dict) and "run" in ev and "job" in ev


def load_events(config: dict) -> list:
    """Read OpenLineage run events from the collector folder.

    Two file shapes are accepted, so the transport can change without a flag
    day. The single-event transport writes `<epochMs>-<uuid>-<kind>.json` per
    event, of which only `-run.json` has the run shape. The batching transport
    writes `<epochMs>-<uuid>-<n>events.ndjson` with one event of any kind per
    line, so run events are picked by content there. Both land in
    `dt=YYYY-MM-DD` partitions. Files are a few KB and are fetched over the
    OneLake REST API in a small thread pool, so this stays fast without a
    Spark read."""
    ws, folder = lineage_ingest(config)
    print(f"Lineage source: {ws}/{folder}")
    try:
        entries = onelake_list(ws, folder, recursive=True)
    except RuntimeError as e:
        # Only a missing folder counts as "no events yet". Anything else, a
        # permission or network failure, must raise: swallowing it would
        # rebuild lineage.events empty and let the run pass green.
        if "-> 404" not in str(e):
            raise
        print("  events folder missing (created on the first event); no events.")
        return []
    paths = [e["name"] for e in entries if not _is_dir(e)
             and (e.get("name") or "").endswith(("-run.json", ".ndjson"))]

    def fetch(path):
        """Every event in one file, and how many lines did not parse."""
        try:
            raw = onelake_read(ws, path).decode("utf-8")
        except Exception as e:
            # One bad file is reported, not allowed to cost every other event.
            print(f"  unreadable, skipped: {path} ({type(e).__name__}: {e})")
            return [], 0
        lines = raw.splitlines() if path.endswith(".ndjson") else [raw]
        parsed, malformed = [], 0
        for line in lines:
            if not line.strip():
                continue
            try:
                parsed.append(json.loads(line))
            except ValueError:
                malformed += 1
        return parsed, malformed

    events, other_kinds, malformed = [], 0, 0
    with ThreadPoolExecutor(max_workers=16) as pool:
        for parsed, bad in pool.map(fetch, paths):
            malformed += bad
            for ev in parsed:
                if _is_run_event(ev):
                    events.append(ev)
                else:
                    other_kinds += 1
    note = ""
    if other_kinds or malformed:
        note = (f", skipped {other_kinds} job/dataset event(s) and "
                f"{malformed} malformed line(s)")
    print(f"  loaded {len(events)} run events from {len(paths)} file(s){note}")
    return events


def extract_sources_from_column_lineage(column_lineage: dict | None) -> list[str]:
    """Recover source dataset paths nested inside the columnLineage facet.

    Delta MERGE events report inputs:[] as empty at the top level, the real
    sources are inside columnLineage.fields[].inputFields[].name and
    columnLineage.dataset[].name.
    """
    if not column_lineage:
        return []
    sources = set()
    for info in (column_lineage.get("fields") or {}).values():
        for inp in (info.get("inputFields") or []):
            if inp.get("name"):
                sources.add(inp["name"])
    for ds in (column_lineage.get("dataset") or []):
        if ds.get("name"):
            sources.add(ds["name"])
    return sorted(sources)


def flatten_event(ev: dict, lakehouse_layer: dict, entity_rules: dict | None = None,
                  files_regex=None) -> dict:
    """One OpenLineage run event as a flat row of the events table: run/job
    identity, every input and output path parsed to layer and entity, and the
    columnLineage facet kept whole as JSON for the snapshot to fingerprint."""
    run         = ev.get("run", {}) or {}
    job         = ev.get("job", {}) or {}
    facets      = run.get("facets", {}) or {}
    parent      = facets.get("parent", {}) or {}
    spark_props = (facets.get("spark_properties", {}) or {}).get("properties", {}) or {}

    inputs  = ev.get("inputs", []) or []
    outputs = ev.get("outputs", []) or []

    def _pp(name):
        return parse_path(name, lakehouse_layer, entity_rules, files_regex)

    parsed_top_inputs = [{"name": i.get("name"), **_pp(i.get("name"))} for i in inputs]
    parsed_outputs    = [{"name": o.get("name"), **_pp(o.get("name"))} for o in outputs]

    column_lineage = None
    for o in outputs:
        cl = (o.get("facets", {}) or {}).get("columnLineage")
        if cl:
            column_lineage = cl
            break

    lineage_source_paths  = extract_sources_from_column_lineage(column_lineage)
    lineage_source_parsed = [_pp(p) for p in lineage_source_paths]

    top_input_layers     = {p["layer"] for p in parsed_top_inputs     if p["layer"] != "unknown"}
    lineage_input_layers = {p["layer"] for p in lineage_source_parsed if p["layer"] != "unknown"}
    all_input_layers     = sorted(top_input_layers | lineage_input_layers)

    primary_out = next(
        (p for p in parsed_outputs
         if p["path_kind"] == "TABLES" and not p["is_delta_internal"]),
        None,
    ) or (parsed_outputs[0] if parsed_outputs else None)

    return {
        "event_time":            ev.get("eventTime"),
        "event_type":            ev.get("eventType"),
        "run_id":                run.get("runId"),
        "parent_run_id":         (parent.get("run") or {}).get("runId"),
        "parent_job_name":       (parent.get("job") or {}).get("name"),
        "job_namespace":         job.get("namespace"),
        "job_name":              job.get("name"),
        "job_type":              ((job.get("facets") or {}).get("jobType") or {}).get("jobType"),
        "spark_app_id":          spark_props.get("spark.app.id"),
        "notebook_name":         spark_props.get("spark.synapse.context.notebookname"),
        "top_level_input_paths": [p["name"] for p in parsed_top_inputs],
        "lineage_source_paths":  lineage_source_paths,
        "output_paths":          [p["name"] for p in parsed_outputs],
        "input_layers":          all_input_layers,
        "output_layer":          primary_out["layer"]       if primary_out else None,
        "output_data_source":    primary_out["data_source"] if primary_out else None,
        "output_application":    primary_out["application"] if primary_out else None,
        "output_entity":         primary_out["entity"]      if primary_out else None,
        "output_schema":         primary_out["schema"]      if primary_out else None,
        "output_table":          primary_out["table"]       if primary_out else None,
        "output_path_kind":      primary_out["path_kind"]   if primary_out else None,
        "has_column_lineage":    "true" if column_lineage else "false",
        "column_lineage_json":   json.dumps(column_lineage) if column_lineage else None,
    }


def noise_reason(row: dict) -> str | None:
    """Why an event carries no lineage signal, or None when it does.

    The reason is returned rather than a bare flag so run_parser can report it
    per entity: a table whose every event was noise reaches neither
    lineage.events nor the per-entity diagnostics, and would otherwise be
    missing from the run with nothing said about it.
    """
    if row["job_type"] == "APPLICATION":
        return "spark application wrapper"
    if not row["output_paths"]:
        return "no output"
    if all("_delta_log" in p for p in row["output_paths"]):
        return "delta log only"
    if (row["top_level_input_paths"] == row["output_paths"]
            and row["job_type"] == "RDD_JOB"
            and not row["lineage_source_paths"]):
        return "rdd read reported as a write"
    return None


PARSED_SCHEMA = pa.schema([
    ("event_time",            pa.string()),
    ("event_type",            pa.string()),
    ("run_id",                pa.string()),
    ("parent_run_id",         pa.string()),
    ("parent_job_name",       pa.string()),
    ("job_namespace",         pa.string()),
    ("job_name",              pa.string()),
    ("job_type",              pa.string()),
    ("spark_app_id",          pa.string()),
    ("notebook_name",         pa.string()),
    ("top_level_input_paths", pa.list_(pa.string())),
    ("lineage_source_paths",  pa.list_(pa.string())),
    ("output_paths",          pa.list_(pa.string())),
    ("input_layers",          pa.list_(pa.string())),
    ("output_layer",          pa.string()),
    ("output_data_source",    pa.string()),
    ("output_application",    pa.string()),
    ("output_entity",         pa.string()),
    ("output_schema",         pa.string()),
    ("output_table",          pa.string()),
    ("output_path_kind",      pa.string()),
    ("has_column_lineage",    pa.string()),
    ("column_lineage_json",   pa.string()),
])


def cleanup_lineage_files(config: dict) -> None:
    """Delete raw event partitions (dt=YYYY-MM-DD folders) older than
    `lineage_retention_days` from the collector folder.

    Refuses to run when retention does not exceed snapshot_window_days, so
    cleanup can never remove events the snapshot still needs, and deletes only
    whole dt= partitions, so the writer never has an active file in one."""
    retention = config["lineage_retention_days"]
    window    = config["snapshot_window_days"]
    if retention <= window:
        print(f"Warning: lineage cleanup skipped, lineage_retention_days="
              f"{retention} must exceed snapshot_window_days={window}. "
              f"No files deleted.")
        return

    ws, folder = lineage_ingest(config)
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=retention)).date()
    print(f"\n=== Lineage partition cleanup (retention {retention}d, "
          f"cutoff {cutoff_date}) ===")

    deleted = kept = 0
    try:
        for entry in onelake_list(ws, folder):
            name = (entry.get("name") or "").rsplit("/", 1)[-1]
            if not (_is_dir(entry) and name.startswith("dt=")):
                continue
            try:
                part_date = datetime.strptime(name[3:], "%Y-%m-%d").date()
            except ValueError:
                continue
            if part_date < cutoff_date:
                onelake_delete(ws, entry["name"])
                deleted += 1
            else:
                kept += 1
        print(f"Lineage cleanup: deleted {deleted} partition(s), kept {kept}")
    except Exception as e:
        print(f"  cleanup failed: {type(e).__name__}: {e}")


def run_parser(config: dict) -> int:
    """Read OpenLineage events from the collector folder, parse them, and write
    lineage.events. Returns the row count written."""
    lakehouse_layer = config["lakehouse_layer"]
    output_table    = config["parsed_table"]
    entity_rules    = config["entity_rules"]
    files_regex     = config["files_regex"]

    raw_events = load_events(config)
    print(f"\nTotal raw events: {len(raw_events)}")

    flat_rows = [flatten_event(ev, lakehouse_layer, entity_rules, files_regex)
                 for ev in raw_events]
    print(f"Flattened: {len(flat_rows)} rows")

    signal_rows, dropped = [], Counter()
    dropped_by_entity = defaultdict(Counter)
    for r in flat_rows:
        reason = noise_reason(r)
        if not reason:
            signal_rows.append(r)
            continue
        dropped[reason] += 1
        if r["output_entity"]:
            key = (r["output_layer"] or "unknown", r["output_entity"])
            dropped_by_entity[key][reason] += 1
    print(f"After noise filter: {len(signal_rows)} rows")
    if dropped:
        print(f"  dropped {sum(dropped.values())}: "
              + ", ".join(f"{n} {reason}"
                          for reason, n in dropped.most_common()))
    # Entities whose every event was noise ran, but contribute nothing and never
    # reach the per-entity diagnostics. Name them here, with the reason. Keyed on
    # (layer, entity) because bronze and silver share entity names: a bronze file
    # read and a silver table write are different things with the same name.
    with_signal = {(r["output_layer"] or "unknown", r["output_entity"])
                   for r in signal_rows if r["output_entity"]}
    for key in sorted(k for k in dropped_by_entity if k not in with_signal):
        detail = ", ".join(f"{n} {reason}" for reason, n
                           in dropped_by_entity[key].most_common())
        print(f"  no signal event for {key[0]}/{key[1]}: {detail}")

    # Dedup on (run_id, event_time).
    seen, deduped = set(), []
    for r in signal_rows:
        key = (r["run_id"], r["event_time"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    print(f"After dedup: {len(deduped)} rows")

    write_metadata_table(deduped, PARSED_SCHEMA, config, output_table)
    print(f"Wrote {len(deduped)} rows to {output_table}")

    # Every entity the platform touched, with where and when it was last seen.
    # Taken from ALL flattened rows, before the noise filter, because a merge
    # that changed nothing appears only as the input of a read.
    observed_layers = set(config.get("observed_layers") or [])
    observed = {}
    for r in flat_rows if observed_layers else []:
        for path in (r["output_paths"] or []) + (r["top_level_input_paths"] or []):
            ident = dataset_identity(config, path)
            if ident["layer"] not in observed_layers or not ident["entity"]:
                continue
            key = (ident["layer"], ident["data_source"],
                   ident["application"], ident["entity"])
            seen_at = r["event_time"] or ""
            if key not in observed or seen_at > observed[key][0]:
                observed[key] = (seen_at, normalize_path(path))
    if observed_layers:
        print(f"Entities touched in {sorted(observed_layers)}: {len(observed)}")

    # Diagnostics, counted in Python, no extra Spark job.
    by_layer = Counter(r["output_layer"] or "unknown" for r in deduped)
    with_cl  = Counter(r["output_layer"] or "unknown" for r in deduped
                       if r["has_column_lineage"] == "true")
    print("\n=== Events by output layer ===")
    for layer in sorted(by_layer):
        print(f"  {layer:10s} {by_layer[layer]:5d} events, "
              f"{with_cl.get(layer, 0):5d} with column lineage")

    # Retention cleanup, only after the events table is safely written, and in
    # its own try so a cleanup failure never fails the parse. Because the table
    # is rebuilt from the surviving files on every run, it shrinks to the
    # retention horizon by itself.
    try:
        cleanup_lineage_files(config)
    except Exception as e:
        print(f"Lineage file cleanup skipped: {type(e).__name__}: {e}")

    return len(deduped), observed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Step 2: snapshot helpers + schema scan ----------------------------------

# The logical dataset behind a physical path: the first two Files segments, or
# the Tables/schema/table pair. Load type, date partitions, the parquet file and
# _delta_log are stripped so an entity keeps one identity across runs. Keep the
# Files depth in step with DEFAULT_FILES_REGEX above, or the same bronze table
# gets a new identity every run.


FILES_LOGICAL_RE  = re.compile(r"(.*/Files/[^/]+/[^/]+)(/.*)?$")
TABLES_LOGICAL_RE = re.compile(r"(.*/Tables/[^/]+/[^/]+?)(/_delta_log.*)?$")

# Explicit schemas: a column that is None in every row (data_source for gold
# rows, comment for most schema rows) still keeps its declared type.
SNAPSHOT_SCHEMA = pa.schema([
    ("data_source",               pa.string()),
    ("application",               pa.string()),
    ("entity",                    pa.string()),
    ("layer",                     pa.string()),
    # captured: a write with column lineage was seen in this window.
    # carried:  no write this window, shape kept from the previous snapshot.
    # observed: referenced by events, but no write with lineage ever seen.
    ("lineage_status",            pa.string()),
    ("fingerprint",               pa.string()),
    ("occurrence_count",          pa.int32()),
    ("events_considered",         pa.int32()),
    ("distinct_shapes_seen",      pa.int32()),
    ("representative_run_id",     pa.string()),
    ("representative_event_time", pa.string()),
    ("output_path",               pa.string()),
    ("input_paths_normalized",    pa.string()),
    ("column_lineage_json",       pa.string()),
    ("window_days",               pa.int32()),
    ("min_occurrences",           pa.int32()),
    ("snapshot_taken_at",         pa.string()),
])

SCHEMA_TABLE_SCHEMA = pa.schema([
    ("dataset_path", pa.string()),
    ("column_name",  pa.string()),
    ("data_type",    pa.string()),
    ("comment",      pa.string()),
    ("nullable",     pa.bool_()),
    ("collected_at", pa.string()),
])


def normalize_path(path: str) -> str:
    """Strip partition / delta-log suffixes so the same logical dataset has a
    stable identity across runs."""
    if not path:
        return path
    m = FILES_LOGICAL_RE.match(path)
    if m:
        return m.group(1)
    m = TABLES_LOGICAL_RE.match(path)
    if m:
        return m.group(1)
    return path


def fingerprint_lineage(column_lineage_json: str | None,
                        output_path: str | None) -> str | None:
    """Deterministic hash of the lineage shape, ignoring runtime details. Two
    runs of the same transformation produce the same fingerprint; a changed
    transformation produces a different one."""
    if not column_lineage_json:
        return None
    try:
        cl = json.loads(column_lineage_json)
    except Exception:
        return None

    def source_tuples(ref: dict) -> list:
        """One (path, field, type, subtype, masking) tuple per transformation on
        an inputFields / dataset entry. Sorted tuples make the shape order-free."""
        path  = normalize_path(ref.get("name", ""))
        field = ref.get("field", "")
        return [(path, field, t.get("type", ""), t.get("subtype", ""),
                 bool(t.get("masking", False)))
                for t in (ref.get("transformations") or [])]

    fields_part = []
    for target_col in sorted((cl.get("fields") or {}).keys()):
        sources = []
        for inp in ((cl["fields"][target_col] or {}).get("inputFields") or []):
            sources += source_tuples(inp)
        fields_part.append((target_col, tuple(sorted(sources))))

    indirect_part = sorted(t for ds in (cl.get("dataset") or [])
                           for t in source_tuples(ds))

    blob = json.dumps({
        "output_path_normalized": normalize_path(output_path or ""),
        "direct_fields": fields_part,
        "indirect": indirect_part,
    }, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _dataset_paths_in_snapshot(snapshot_rows: list[dict]) -> set[str]:
    """Every dataset path the snapshot touches, outputs plus the sources nested
    inside column_lineage_json."""
    paths: set[str] = set()
    for row in snapshot_rows:
        if row.get("output_path"):
            paths.add(normalize_path(row["output_path"]))
        cl_raw = row.get("column_lineage_json")
        if not cl_raw:
            continue
        try:
            cl = json.loads(cl_raw)
        except Exception:
            continue
        for info in (cl.get("fields") or {}).values():
            for inp in (info.get("inputFields") or []):
                if inp.get("name"):
                    paths.add(normalize_path(inp["name"]))
        for ds in (cl.get("dataset") or []):
            if ds.get("name"):
                paths.add(normalize_path(ds["name"]))
    return paths


SPARK_SIMPLE_TYPES = {"integer": "int", "long": "bigint",
                      "short": "smallint", "byte": "tinyint"}


def _simple_type(t) -> str:
    """A Spark-style simpleString for a schemaString type node, giving
    data_type values like int, bigint or array<string>."""
    if isinstance(t, str):
        return SPARK_SIMPLE_TYPES.get(t, t)
    kind = (t or {}).get("type")
    if kind == "array":
        return f"array<{_simple_type(t.get('elementType'))}>"
    if kind == "map":
        return (f"map<{_simple_type(t.get('keyType'))},"
                f"{_simple_type(t.get('valueType'))}>")
    if kind == "struct":
        inner = ",".join(f"{f.get('name')}:{_simple_type(f.get('type'))}"
                         for f in (t.get("fields") or []))
        return f"struct<{inner}>"
    return json.dumps(t)


def _schema_string_columns(schema_string: str) -> list:
    """The columns encoded in a Delta metaData.schemaString."""
    return [{
        "column_name": f.get("name"),
        "data_type":   _simple_type(f.get("type")),
        "comment":     (f.get("metadata") or {}).get("comment"),
        "nullable":    bool(f.get("nullable", True)),
    } for f in (json.loads(schema_string).get("fields") or [])]


def read_schema_from_delta_log(workspace_id: str, table_path: str) -> list:
    """A table's columns read straight from its _delta_log, a couple of small
    HTTP reads rather than opening the table.

    The most recent metaData action wins. Newest JSON commits are scanned first;
    when none of the retained JSON commits carries one (a long-lived table
    whose schema predates them), the checkpoint named by _last_checkpoint
    provides it."""
    log_dir = f"{table_path}/_delta_log"
    entries = onelake_list(workspace_id, log_dir)
    commits = sorted(
        (e["name"] for e in entries if not _is_dir(e)
         and e["name"].endswith(".json")
         and e["name"].rsplit("/", 1)[-1][:-5].isdigit()),
        reverse=True)
    for path in commits[:30]:            # schema changes are rare; newest first
        for line in onelake_read(workspace_id, path).decode("utf-8").splitlines():
            if '"metaData"' not in line:
                continue
            meta = (json.loads(line).get("metaData") or {})
            if meta.get("schemaString"):
                return _schema_string_columns(meta["schemaString"])

    lc = json.loads(onelake_read(workspace_id, f"{log_dir}/_last_checkpoint"))
    version, parts = int(lc["version"]), int(lc.get("parts") or 1)
    names = ([f"{log_dir}/{version:020d}.checkpoint.parquet"] if parts == 1
             else [f"{log_dir}/{version:020d}.checkpoint.{p:010d}.{parts:010d}"
                   f".parquet" for p in range(1, parts + 1)])
    for name in names:
        table = pq.read_table(_io.BytesIO(onelake_read(workspace_id, name)),
                              columns=["metaData"])
        for meta in table.column("metaData").to_pylist():
            if meta and meta.get("schemaString"):
                return _schema_string_columns(meta["schemaString"])
    raise ValueError("no metaData action found in _delta_log")


def _fs_path(dataset_path: str) -> str:
    """'<lakehouse>/Tables/...' from either '/lh/Tables/...' or a full abfss
    URL, the two shapes a snapshot dataset path arrives in."""
    if dataset_path.startswith("abfss://"):
        return dataset_path.split("onelake.dfs.fabric.microsoft.com/", 1)[1]
    return dataset_path.lstrip("/")


def collect_schemas(snapshot_rows: list, config: dict) -> list:
    """For every unique /Tables/ dataset path in the snapshot whose layer is in
    `schema_collect_layers`, read the columns from the table's _delta_log and
    emit one row per column.

    Bronze paths are usually partitioned file paths rather than tables, so they
    are outside the default layer filter. Failures on individual tables are
    logged and skipped, schema collection never blocks the snapshot."""
    layer_filter    = set(config["schema_collect_layers"])
    lakehouse_layer = config["lakehouse_layer"]
    entity_rules    = config["entity_rules"]
    files_regex     = config["files_regex"]

    if not layer_filter:
        print("Schema collection: no layers configured, skipping.")
        return []

    eligible, skipped_layer, skipped_not_table = [], 0, 0
    for p in sorted(_dataset_paths_in_snapshot(snapshot_rows)):
        if parse_path(p, lakehouse_layer, entity_rules,
                      files_regex)["layer"] not in layer_filter:
            skipped_layer += 1
        elif "/Tables/" not in p:
            skipped_not_table += 1
        else:
            eligible.append(p)

    print(f"Schema collection: {len(eligible)} eligible datasets "
          f"({skipped_layer} skipped by layer filter, {skipped_not_table} not /Tables/)")

    rows, failed = [], []
    now_ts = datetime.now(timezone.utc).isoformat()
    for dataset_path in eligible:
        try:
            # Resolve the source's own workspace so a cross-workspace lakehouse
            # is read from its real OneLake authority.
            ws = _lakehouse_workspace(config, extract_lakehouse_id(dataset_path))
            for column in read_schema_from_delta_log(ws, _fs_path(dataset_path)):
                rows.append({"dataset_path": dataset_path,
                             "collected_at": now_ts, **column})
        except Exception as e:
            failed.append((dataset_path, f"{type(e).__name__}: {e}"))

    if failed:
        print(f"Schema collection: {len(failed)} datasets failed schema read")
        for path, err in failed[:5]:
            print(f"  {path[-60:]:60s}  {err}")
        if len(failed) > 5:
            print(f"  ...and {len(failed) - 5} more")

    print(f"Schema collection: {len(rows)} column rows captured")
    return rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Step 3: the platin (semantic-model) layer -------------------------------
# NB_LineageSemanticModels writes semantic_models.platin_* with sempy. When
# those tables exist, each semantic-model table becomes a `layer = platin`
# snapshot row in exactly the same shape as an event-derived row, so one table
# carries bronze -> silver -> gold -> platin and impact analysis reaches reports.
# When the tables are absent this is skipped and nothing else changes.
#
# Resolution chain (Direct Lake, precise): platin_lineage gives the model table
# and the entity it reads, platin_model_sources gives the model's source
# lakehouse, platin_shortcuts resolves a shortcut there to the real gold path.
# Import models fall back to a uniquely named gold table, and are left
# unresolved rather than linked ambiguously.


PLATIN_DIRECT = {"type": "DIRECT", "subtype": "DIRECT_LAKE", "masking": False}


def _model_key(row: dict) -> str:
    """Stable per-model key, shared across every platin_* table."""
    return (row.get("model_id")
            or f'{row.get("workspace_id")}::{row.get("model_name")}')


def _read_platin(config: dict, name):
    """Rows of a platin table as dicts, or None when the table does not exist."""
    if not name or not metadata_table_exists(config, name):
        return None
    return read_metadata_table(config, name)


def _tables_key(path: str):
    """(lakehouse_id, schema, table) from any OneLake /Tables/ path, or None. The
    join key between a resolved platin source and a gold entity the snapshot
    already knows: it matches on identity, not on the exact path string."""
    if not path:
        return None
    lh = extract_lakehouse_id(path)
    m  = TABLES_RE.search(path)
    return (lh, m.group("schema"), m.group("table")) if (lh and m) else None


def _gold_abfss(config: dict, key) -> str:
    lh, schema, table = key
    ws = _lakehouse_workspace(config, lh)
    return (f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/"
            f"{lh}/Tables/{schema}/{table}")


def _resolve_gold(lin: dict, sources_by_model: dict, shortcuts_by_lh: dict):
    """Resolve one platin_lineage row (a semantic-model table) to a gold
    (lakehouse_id, schema, table), or None."""
    entity = lin.get("entity_name") or lin.get("parsed_ref")
    if not entity:
        return None
    schema = lin.get("schema_name")
    # parsed_ref can arrive as "schema.table" when entity_name is absent.
    if not lin.get("entity_name") and schema is None and "." in entity:
        schema, entity = entity.split(".", 1)
    tbl_path = f"Tables/{schema}/{entity}" if schema else f"Tables/{entity}"

    for lh in sources_by_model.get(_model_key(lin), []):
        # (1) a shortcut in the source lakehouse covering this table?
        for sc in shortcuts_by_lh.get(lh, []):
            sp = (sc.get("shortcut_path") or "").strip("/")
            if sp and (tbl_path == sp or tbl_path.startswith(sp + "/")):
                key = _tables_key((sc.get("source_path") or "") + tbl_path[len(sp):])
                if key:
                    return key
        # (2) no shortcut -> the table lives in the source lakehouse itself
        return (lh, schema or "dbo", entity)
    return None


def _fallback_gold(lin: dict, gold_keys: dict):
    """Import models have no Direct Lake source. Fall back to a uniquely named
    gold table; an ambiguous name stays unresolved rather than linked wrongly."""
    ent = lin.get("entity_name") or lin.get("parsed_ref") or ""
    if "." in ent and not lin.get("entity_name"):
        ent = ent.split(".", 1)[1]
    cand = [k for k in gold_keys if k[2] == ent]
    return cand[0] if len(cand) == 1 else None


def _iso(value):
    """A platin collected_at timestamp as the ISO string representative_event_time
    carries (it is what the staleness check compares)."""
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def build_platin_rows(config: dict, event_rows: list, now_ts: str) -> list:
    """Snapshot rows for the semantic-model layer, or [] when the collector has
    not run. One row per model table, with a synthesized columnLineage facet:
    Direct Lake columns point at their gold column, DAX measures and calculated
    columns point at what they reference inside the model."""
    T = config["platin_tables"]
    lineage = _read_platin(config, T.get("lineage"))
    if lineage is None:
        print("Platin layer: no semantic_models.platin_lineage; skipping.")
        return []

    columns = _read_platin(config, T.get("columns"))       or []
    calc    = _read_platin(config, T.get("calc_deps"))     or []
    shorts  = _read_platin(config, T.get("shortcuts"))     or []
    sources = _read_platin(config, T.get("model_sources")) or []
    print(f"Platin layer: {len(lineage)} lineage, {len(columns)} columns, "
          f"{len(calc)} calc-deps, {len(shorts)} shortcuts, {len(sources)} sources")

    sources_by_model, shortcuts_by_lh = defaultdict(list), defaultdict(list)
    for s in sources:
        lh = (s.get("source_item_id") or "").lower()
        if lh:
            sources_by_model[_model_key(s)].append(lh)
    for sc in shorts:
        lh = (sc.get("lakehouse_id") or "").lower()
        if lh:
            shortcuts_by_lh[lh].append(sc)

    cols_by_mt, calc_by_mt = defaultdict(list), defaultdict(list)
    for c in columns:
        cols_by_mt[(_model_key(c), c.get("table_name") or "")].append(c)
    for d in calc:
        calc_by_mt[(_model_key(d), d.get("table_name") or "")].append(d)

    # The gold entities the event snapshot already knows, so a platin row points
    # at the same path the gold row was written under (and for the name fallback).
    gold_keys = {}
    for r in event_rows:
        if r["layer"] == "gold":
            k = _tables_key(r.get("output_path"))
            if k:
                gold_keys[k] = normalize_path(r["output_path"])

    by_table = defaultdict(list)
    for lin in lineage:
        if lin.get("model_table"):
            by_table[(_model_key(lin), lin["model_table"])].append(lin)

    rows, unresolved = [], 0
    for (mk, mtable), lins in by_table.items():
        head  = lins[0]
        mname = head.get("model_name") or mk

        # A model table with several partitions contributes all of its sources;
        # the first resolved one carries the column-level links.
        gold_paths = []
        for lin in lins:
            key = (_resolve_gold(lin, sources_by_model, shortcuts_by_lh)
                   or _fallback_gold(lin, gold_keys))
            if key:
                path = gold_keys.get(key) or _gold_abfss(config, key)
                if path not in gold_paths:
                    gold_paths.append(path)
        if not gold_paths:
            unresolved += 1
        primary  = gold_paths[0] if gold_paths else None
        out_path = f"platin://{mname}/{mtable}"

        fields = {}
        for c in cols_by_mt.get((mk, mtable), []):
            name, src = c.get("column_name"), c.get("source_column")
            if name and src and primary:
                fields[name] = {"inputFields": [{"name": primary, "field": src,
                                                 "transformations": [PLATIN_DIRECT]}]}
        for d in calc_by_mt.get((mk, mtable), []):
            name  = d.get("object_name")
            ref_t = d.get("referenced_table")
            ref_o = d.get("referenced_object")
            if not (name and ref_t and ref_o):
                continue
            subtype = ("AGGREGATION"
                       if (d.get("object_type") or "").upper() == "MEASURE"
                       else "TRANSFORMATION")
            fields.setdefault(name, {"inputFields": []})["inputFields"].append(
                {"name": f"platin://{mname}/{ref_t}", "field": ref_o,
                 "transformations": [{"type": "DIRECT", "subtype": subtype,
                                      "masking": False}]})
        if not fields:
            continue

        cl_json = json.dumps({"fields": fields})
        rows.append({
            "data_source": head.get("workspace_name"),
            "application": mname,
            "entity":      mtable,
            "layer":       "platin",
            "lineage_status": "captured",
            "fingerprint": fingerprint_lineage(cl_json, out_path),
            "occurrence_count":     1,
            "events_considered":    len(lins),
            "distinct_shapes_seen": 1,
            "representative_run_id":     head.get("model_id"),
            "representative_event_time": _iso(head.get("collected_at")) or now_ts,
            "output_path": out_path,
            "input_paths_normalized": json.dumps(sorted(gold_paths)),
            "column_lineage_json": cl_json,
            "window_days":       config["snapshot_window_days"],
            "min_occurrences":   config["snapshot_min_occurrences"],
            "snapshot_taken_at": now_ts,
        })

    print(f"Platin layer: {len(rows)} model table(s) snapshotted, "
          f"{unresolved} without a resolved gold source")
    return rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Step 4: flat edges for consumers ----------------------------------------
# lineage.snapshot keeps the column detail as an OpenLineage facet in
# column_lineage_json, which is precise but makes every consumer write a JSON
# parser first. This flattens the same content into one row per edge, so a
# report, a graph renderer, a Data Agent or plain SQL can all read it directly:
#
#   feeds        dataset -> dataset   what loads this table
#   derivesFrom  column  -> column    what this column is computed from
#   influences   column  -> dataset   a filter or join key that shaped the
#                                     output without landing in one column
#
# Both endpoints carry their identity (layer, data_source, application, entity)
# so queries join on names, not on abfss paths.


PLATIN_PATH_RE = re.compile(r"^platin://(?P<model>[^/]+)/(?P<table>.+)$")

EDGES_SCHEMA = pa.schema([
    ("edge_type",              pa.string()),
    ("source_path",            pa.string()),
    ("source_layer",           pa.string()),
    ("source_data_source",     pa.string()),
    ("source_application",     pa.string()),
    ("source_entity",          pa.string()),
    ("source_column",          pa.string()),
    ("target_path",            pa.string()),
    ("target_layer",           pa.string()),
    ("target_data_source",     pa.string()),
    ("target_application",     pa.string()),
    ("target_entity",          pa.string()),
    ("target_column",          pa.string()),
    ("transformation_type",    pa.string()),
    ("transformation_subtype", pa.string()),
    ("is_masking",             pa.bool_()),
    ("snapshot_taken_at",      pa.string()),
])

IDENTITY_FIELDS = ("layer", "data_source", "application", "entity")


def _loads(value, default):
    """Parse a JSON column, falling back rather than failing the run."""
    try:
        return json.loads(value) if value else default
    except Exception:
        return default


def dataset_identity(config: dict, path: str) -> dict:
    """Identity of a dataset path that is not itself a snapshot row, typically a
    bronze file path. Handles the synthetic platin://<model>/<table> too."""
    m = PLATIN_PATH_RE.match(path or "")
    if m:
        return {"layer": "platin", "data_source": None,
                "application": m.group("model"), "entity": m.group("table")}
    info = parse_path(path or "", config["lakehouse_layer"],
                      config["entity_rules"], config["files_regex"])
    return {k: info[k] for k in IDENTITY_FIELDS}


def build_edge_rows(config: dict, snapshot_rows: list, now_ts: str) -> list:
    """One row per edge in the snapshot, de-duplicated and sorted.

    An endpoint that is itself a snapshot row takes that row's identity, so the
    two tables always agree on names; anything else (a bronze source, a gold
    table nothing wrote this window) is derived from its path.
    """
    identities = {}
    for row in snapshot_rows:
        path = normalize_path(row.get("output_path") or "")
        if path:
            identities[path] = {k: row.get(k) for k in IDENTITY_FIELDS}

    def identity(path: str) -> dict:
        if path not in identities:
            identities[path] = dataset_identity(config, path)
        return identities[path]

    def edge(edge_type, source_path, source_column, target_path, target_column,
             transformation=None) -> tuple:
        """One EDGES_SCHEMA row. Built here so the column order is defined once."""
        s, t = identity(source_path), identity(target_path)
        tr = transformation or {}
        return (edge_type,
                source_path, s["layer"], s["data_source"], s["application"],
                s["entity"], source_column,
                target_path, t["layer"], t["data_source"], t["application"],
                t["entity"], target_column,
                tr.get("type"), tr.get("subtype"), bool(tr.get("masking", False)),
                now_ts)

    edges = set()
    for row in snapshot_rows:
        target_path = normalize_path(row.get("output_path") or "")
        if not target_path:
            continue

        for source_path in _loads(row.get("input_paths_normalized"), []):
            if source_path:
                edges.add(edge("feeds", source_path, None, target_path, None))

        column_lineage = _loads(row.get("column_lineage_json"), {})
        for target_column, info in (column_lineage.get("fields") or {}).items():
            for inp in (info.get("inputFields") or []):
                source_path = normalize_path(inp.get("name") or "")
                if not source_path:
                    continue
                # `or [None]` keeps the edge when an event reports no
                # transformation at all: the link is real, the detail is not.
                for tr in (inp.get("transformations") or [None]):
                    edges.add(edge("derivesFrom", source_path, inp.get("field"),
                                   target_path, target_column, tr))

        for ds in (column_lineage.get("dataset") or []):
            source_path = normalize_path(ds.get("name") or "")
            if not source_path:
                continue
            for tr in (ds.get("transformations") or [None]):
                edges.add(edge("influences", source_path, ds.get("field"),
                               target_path, None, tr))

    by_type = Counter(e[0] for e in edges)
    print(f"Edges: {len(edges)} "
          f"({', '.join(f'{n} {t}' for t, n in sorted(by_type.items())) or 'none'})")
    return sorted(edges)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Step 5: the snapshot ----------------------------------------------------


def _layer_of(config: dict, output_path) -> str:
    """Layer of a snapshot row, from its output path."""
    if (output_path or "").startswith("platin://"):
        return "platin"
    return parse_path(output_path or "", config["lakehouse_layer"],
                      config.get("entity_rules"), config.get("files_regex"))["layer"]


def carry_forward(config: dict, fresh_rows: list, stale_days: int):
    """Merge the rows just computed over the previous snapshot.

    Without this the snapshot is a pure window aggregate: an entity that did not
    run inside snapshot_window_days disappears from the rebuilt table, so a
    weekly pipeline flickers in and out of the lineage. Here an entity keeps its
    last known shape until its most recent event ages past `snapshot_stale_days`,
    at which point a retired table leaves the snapshot on its own.

    Returns (rows, carried, dropped).
    """
    merged = {(r["layer"], r["data_source"], r["application"], r["entity"]): r
              for r in fresh_rows}
    snapshot_table = config["snapshot_table"]
    if not stale_days or not metadata_table_exists(config, snapshot_table):
        return list(merged.values()), 0, 0

    try:
        previous = read_metadata_table(config, snapshot_table)
    except Exception as e:
        # The table exists (checked above) but cannot be read. Rewriting the
        # snapshot without it would drop every carried-forward entity, so fail
        # the run instead of shrinking a state table on a transient error.
        raise RuntimeError(
            f"Previous snapshot {snapshot_table} exists but could not be "
            f"read; refusing to rewrite the snapshot without its carried "
            f"state.") from e

    cutoff = (datetime.now(timezone.utc) - timedelta(days=stale_days)).isoformat()
    carried = dropped = 0
    for row in previous:
        if not row.get("layer"):          # older snapshot row without a layer
            row["layer"] = _layer_of(config, row.get("output_path"))
        key = (row["layer"], row.get("data_source"), row.get("application"),
               row.get("entity"))
        if key in merged:                 # refreshed by this run
            continue
        if (row.get("representative_event_time") or "") < cutoff:
            dropped += 1
            continue
        # A row that never had lineage stays "observed" when carried, so the
        # status always describes the lineage, not how the row got here.
        row["lineage_status"] = ("carried" if row.get("column_lineage_json")
                                 else "observed")
        merged[key] = row
        carried += 1

    print(f"Carry-forward: kept {carried} entity/entities not seen this run, "
          f"dropped {dropped} with no event in {stale_days}d")
    return list(merged.values()), carried, dropped


def build_observed_rows(config: dict, observed: dict, present: set,
                        now_ts: str) -> list:
    """Rows for entities the platform touched but whose write was never
    captured, so the snapshot lists everything that ran rather than only what
    produced lineage.

    A merge that changes no rows launches no Spark job, so OpenLineage reports
    nothing for it. Without these rows such a table is simply absent, which
    reads as "does not exist" rather than "did not change". The lineage columns
    are null and lineage_status is "observed"; output_path is kept so the
    schema scan still picks the table up.
    """
    window_days = config["snapshot_window_days"]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    rows = []
    for key, (seen_at, path) in sorted(observed.items()):
        if key in present or seen_at < cutoff:
            continue
        layer, data_source, application, entity = key
        rows.append({
            "data_source": data_source,
            "application": application,
            "entity":      entity,
            "layer":       layer,
            "lineage_status": "observed",
            "fingerprint": None,
            "occurrence_count":     0,
            "events_considered":    0,
            "distinct_shapes_seen": 0,
            "representative_run_id":     None,
            "representative_event_time": seen_at,
            "output_path": path,
            "input_paths_normalized": None,
            "column_lineage_json":   None,
            "window_days":       window_days,
            "min_occurrences":   config["snapshot_min_occurrences"],
            "snapshot_taken_at": now_ts,
        })
    return rows


def read_candidate_events(config: dict) -> list:
    """The events a snapshot can be built from: inside the window, carrying a
    column-lineage facet, writing a table, and with an entity resolved from the
    output path."""
    window_days = config["snapshot_window_days"]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    print(f"Window: events with event_time >= {cutoff} ({window_days}d)")

    rows = [r for r in read_metadata_table(config, config["parsed_table"])
            if (r["event_time"] or "") >= cutoff
            and r["has_column_lineage"] == "true"
            and r["output_path_kind"] == "TABLES"
            and r["output_entity"] is not None]
    print(f"Candidate events: {len(rows)}")
    return rows


def build_event_rows(candidates: list, config: dict, now_ts: str):
    """Snapshot rows from the candidate events: group them by entity, keep the
    most frequent lineage shape, and accept it once it clears min_occurrences.

    Returns (rows, diagnostics). The diagnostics carry every entity considered,
    accepted or not, and are what the run prints at the end.
    """
    window_days     = config["snapshot_window_days"]
    min_occurrences = config["snapshot_min_occurrences"]

    per_entity = defaultdict(list)
    without_lineage = Counter()
    entity_layer = {}
    for row in candidates:
        entity_key  = (row["output_data_source"], row["output_application"],
                       row["output_entity"])
        entity_layer.setdefault(entity_key, row["output_layer"] or "unknown")
        output_path = row["output_paths"][0] if row["output_paths"] else None
        fingerprint = fingerprint_lineage(row["column_lineage_json"], output_path)
        if fingerprint:
            per_entity[entity_key].append((fingerprint, row))
        else:
            without_lineage[entity_key] += 1

    # An entity whose events all lack a column-lineage facet produces no shape
    # and cannot enter the snapshot. It gets a diagnostics row below rather than
    # disappearing: a table that ran but emitted nothing is worth investigating.
    silent = {k: n for k, n in without_lineage.items() if k not in per_entity}
    print(f"Distinct entities in window: {len(per_entity)}"
          + (f", plus {len(silent)} with no column lineage" if silent else ""))

    rows, diagnostics = [], []
    for (data_source, application, entity), items in per_entity.items():
        shapes = Counter(fingerprint for fingerprint, _ in items)
        winning_fp, winning_count = shapes.most_common(1)[0]
        accepted = winning_count >= min_occurrences

        diagnostics.append({
            "data_source": data_source, "application": application,
            "entity": entity,
            "layer": entity_layer[(data_source, application, entity)],
            "events_considered": len(items),
            "distinct_lineage_shapes": len(shapes),
            "winning_count": winning_count, "accepted": accepted,
            "reason": ("accepted" if accepted
                       else f"rejected, needs x{min_occurrences}"),
        })
        if not accepted:
            continue

        # The most recent event carrying the winning shape represents the entity.
        rep = max((row for fingerprint, row in items if fingerprint == winning_fp),
                  key=lambda row: row["event_time"])
        rows.append({
            "data_source": data_source,
            "application": application,
            "entity":      entity,
            "layer":       rep["output_layer"] or "unknown",
            "lineage_status": "captured",
            "fingerprint": winning_fp,
            "occurrence_count":     winning_count,
            "events_considered":    len(items),
            "distinct_shapes_seen": len(shapes),
            "representative_run_id":     rep["run_id"],
            "representative_event_time": rep["event_time"],
            "output_path": rep["output_paths"][0] if rep["output_paths"] else None,
            "input_paths_normalized": json.dumps(
                sorted({normalize_path(p)
                        for p in (rep["lineage_source_paths"] or [])})),
            "column_lineage_json": rep["column_lineage_json"],
            "window_days":       window_days,
            "min_occurrences":   min_occurrences,
            "snapshot_taken_at": now_ts,
        })

    for key, seen in silent.items():
        data_source, application, entity = key
        diagnostics.append({
            "data_source": data_source, "application": application,
            "entity": entity, "layer": entity_layer[key],
            "events_considered": seen, "distinct_lineage_shapes": 0,
            "winning_count": 0, "accepted": False,
            "reason": "no column lineage in any event",
        })

    print(f"Accepted: {len(rows)}, rejected: {len(diagnostics) - len(rows)}")
    return rows, diagnostics


# Medallion order for the diagnostics listing; anything else sorts last.
LAYER_ORDER = {"bronze": 0, "silver": 1, "gold": 2, "platin": 3}


def _entity_label(d: dict) -> str:
    """'<owner> / <entity>' for a diagnostics row. A silver entity is owned by
    its data source and a gold entity by its application, so exactly one of the
    two is ever set."""
    owner = d["data_source"] or d["application"] or "-"
    return f"{owner} / {d['entity'] or '-'}"


def print_entity_diagnostics(diagnostics: list) -> None:
    """One line per entity considered, the answer to "why is my table not in
    the snapshot". Entities whose events carried no column lineage are listed
    too, so a table that ran but emitted nothing shows up rather than vanishing.
    """
    print("\n=== Per-entity diagnostics ===")
    if not diagnostics:
        print("  (no entities in the window)")
        return
    width = max(len(_entity_label(d)) for d in diagnostics)
    for d in sorted(diagnostics, key=lambda d: (LAYER_ORDER.get(d["layer"], 9),
                                                d["entity"] or "")):
        print(f"  {(d['layer'] or 'unknown'):8s}{_entity_label(d):{width}s}  "
              f"{d['events_considered']:3d} events, "
              f"{d['distinct_lineage_shapes']:2d} shapes, "
              f"winner x{d['winning_count']}  {d['reason']}")


def run_snapshot(config: dict, observed: dict | None = None) -> dict:
    """Rewrite lineage.snapshot: the winning lineage shape of everything that ran
    inside the window, plus the platin layer when the collector has run, plus
    every entity carried over from the previous snapshot. Then flatten it into
    lineage.edges and scan the schemas of the datasets it references."""
    snapshot_table = config["snapshot_table"]
    schema_table   = config["schema_table"]
    edges_table    = config["edges_table"]
    now_ts = datetime.now(timezone.utc).isoformat()

    candidates = read_candidate_events(config)
    event_rows, diagnostics = build_event_rows(candidates, config, now_ts)

    print("\n=== Platin (semantic-model) layer ===")
    platin_rows = build_platin_rows(config, event_rows, now_ts)

    all_rows, carried, dropped = carry_forward(
        config, event_rows + platin_rows, config["snapshot_stale_days"])

    present = {(r["layer"], r.get("data_source"), r.get("application"),
                r.get("entity")) for r in all_rows}
    observed_rows = build_observed_rows(config, observed or {}, present, now_ts)
    if observed_rows:
        print(f"Observed without lineage: {len(observed_rows)} entity/entities "
              f"listed with null lineage")
    all_rows = all_rows + observed_rows

    if all_rows:
        write_metadata_table(all_rows, SNAPSHOT_SCHEMA, config, snapshot_table)
        print(f"Wrote {len(all_rows)} rows to {snapshot_table} "
              f"({len(event_rows)} event, {len(platin_rows)} platin, "
              f"{carried} carried, {len(observed_rows)} observed)")
    else:
        print("Nothing to snapshot; table not updated.")

    # The flat consumption shape. Its own try: it is derived from rows already
    # written, so a failure here must not lose the snapshot.
    edges = 0
    try:
        edge_rows = build_edge_rows(config, all_rows, now_ts)
        if edge_rows:
            names = [f.name for f in EDGES_SCHEMA]
            write_metadata_table([dict(zip(names, r)) for r in edge_rows],
                                 EDGES_SCHEMA, config, edges_table)
            edges = len(edge_rows)
            print(f"Wrote {edges} rows to {edges_table}")
        else:
            print("No edges derived; edge table not updated.")
    except Exception as e:
        print(f"Edge export failed (snapshot still written): "
              f"{type(e).__name__}: {e}")

    # Scanned over the whole snapshot, so a carried-forward table keeps its
    # schema too. Its own try, so a schema failure does not lose the snapshot.
    print(f"\n=== Collecting schemas for layers: "
          f"{config['schema_collect_layers']} ===")
    try:
        schema_rows = collect_schemas(all_rows, config)
        if schema_rows:
            write_metadata_table(schema_rows, SCHEMA_TABLE_SCHEMA,
                                 config, schema_table)
            print(f"Wrote {len(schema_rows)} rows to {schema_table}")
        else:
            print("No schema rows captured; schema table not updated.")
    except Exception as e:
        print(f"Schema collection failed (snapshot still written): "
              f"{type(e).__name__}: {e}")

    print_entity_diagnostics(diagnostics)
    return {"total": len(all_rows), "event": len(event_rows),
            "platin": len(platin_rows), "carried": carried,
            "observed": len(observed_rows),
            "stale_dropped": dropped, "edges": edges}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Run ---------------------------------------------------------------------


config = {
    **CONFIG,
    **resolve_lakehouses(pinned(data_workspace_id),
                         pinned(governance_lakehouse_id),
                         LAYER_IDS),
}

try:
    event_rows, observed = run_parser(config)      # -> lineage.events
    snapshot = run_snapshot(config, observed)      # -> snapshot + edges + schema
    status = {"status": "ok" if snapshot["total"] else "empty",
              "event_rows": event_rows, **snapshot}
except Exception as e:
    status = {"status": "error", "error": f"{type(e).__name__}: {e}"}

print(status)
# Fail the run on error so a broken schedule shows red, not a green empty run.
if status["status"] == "error":
    raise RuntimeError(f"NB_LineageSnapshot failed: {status}")
notebookutils.notebook.exit(json.dumps(status))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
