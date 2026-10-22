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

# NB_LineageSetup
#
# Prepares the Fabric Environment that makes the ETL notebooks emit OpenLineage
# events, and attaches it to them.
#
# A git sync cannot finish this on its own. The Environment's transport
# properties carry the governance lakehouse's workspace and item GUIDs, which
# differ in every deployment, and staged changes only take effect after a
# publish. When they are wrong nothing complains: the ETL runs green, no events
# are written, and the lineage stays empty.
#
# Run it once per deployment, and again after changing the JAR or the properties:
#
#   1. Ensure the Environment exists in this workspace, creating it if missing.
#   2. Put the transport JAR in its custom libraries, uploading it from this
#      notebook's resource folder or from the governance lakehouse.
#   3. Point the spark.openlineage.* properties at the governance lakehouse.
#   4. Publish the staged changes and wait for the publish to finish.
#   5. Attach the Environment to the ETL notebooks matching
#      attach_notebook_patterns.
#   6. Verify the published properties, and report whether the events folder has
#      received anything yet.
#
# Step 5 is a one-off bootstrap, not something to schedule. Each notebook costs
# about a minute of Fabric API calls regardless of its size, and checking whether
# one is already attached costs a full definition read on its own, so a large
# workspace takes hours even when nothing needs changing. You only pay it once
# though: a notebook's environment lives in its definition and git carries
# definitions, so run the rollout, sync the workspace back to git, and every
# later deployment applies the attachment. Set attach_environment_to_notebooks
# to False once that is done. Sync before the next deployment either way,
# because a notebook whose git definition carries no environment is republished
# without one, which silently undoes the rollout.
#
# The identity needs write access on this workspace and read on the governance
# lakehouse.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

# The Environment to create or repair, in this workspace.
#
# Keep it an Environment that exists in git. One created only at runtime is
# deleted by unpublish_all_orphan_items on the next deployment. The JAR and the
# spark.openlineage.* properties are not in the git definition either, which is
# why this notebook is re-run after every deployment.
environment_name = "ENV_Transform_Lineage_Starter"

# Transport JAR. When it is not already staged in the Environment it is uploaded
# from this notebook's resource folder, or alternatively from jar_source_path in
# the governance lakehouse.
jar_name        = "openlineage-transport-onelake-1.0.0.jar"
jar_source_path = "Files/setup/openlineage-transport-onelake-1.0.0.jar"

# The governance lakehouse, by GUID. PL_Lineage injects both from the Variable
# Library; the literals are the dev values, for an interactive run. They have to
# resolve the same lakehouse NB_LineageSnapshot uses, since this notebook writes
# the transport target that one reads from.
#
# Enter the Workspace ID that holds the data lakehouses.
data_workspace_id = "1cfeab7c-4df3-48cb-8407-c31a3bb8a7e5"

governance_lakehouse_id = "1c93cf41-5b66-4694-9ef0-8ea6d3d89ed4"

# OpenLineage job namespace stamped on every event (any short label).
openlineage_namespace = "fabric"

# Whether this run attaches the Environment to notebooks at all.
#
#   True   this notebook makes the attachment. Use it for the first setup, and
#          for projects with no deployment pipeline, where running it and
#          committing the result is how the change reaches the next environment.
#   False  skip the attach entirely: no notebook listing, no definition reads,
#          nothing rewritten. Use it once the attachments are committed and a
#          deployment carries them. The run then only repairs the Environment
#          itself, which is the part a deployment cannot do.
#
attach_environment_to_notebooks = True

# Which notebooks get the Environment. Case-insensitive regexes against display
# names. Nothing else is touched, so notebooks on other environments stay as
# they are. This should be all ETL notebooks that you want to track lineage in.
attach_notebook_patterns = [
    "Bronze2Silver",
    "MergeEntityInto",
    "Silver2Gold",
    "MergeInto",
]
# A notebook carries only one environment, so attaching replaces what is there.
# A target that already has a different one is skipped and reported rather than
# overwritten. Set this True only when you mean to move those notebooks across,
# having first put the libraries they need on this Environment.
force_environment_attachment = False

# Publish staged changes. Leave True.
publish_environment = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Parameter coercion and metadata-lakehouse discovery ---------------------

import json
import os
import re
import time

import requests


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
    """A GUID parameter, or None when blank or still a "<...>" placeholder,
    which falls back to the name search."""
    value = str(value or "").strip()
    return None if not value or value.startswith("<") else value.lower()


attach_notebook_patterns = as_list(attach_notebook_patterns)
publish_environment      = as_bool(publish_environment)
force_environment_attachment    = as_bool(force_environment_attachment)
attach_environment_to_notebooks = as_bool(attach_environment_to_notebooks)


def resolve_metadata_lakehouse(notebookutils, lakehouse_id, workspace_id) -> dict:
    """{workspace_id, lakehouse_id, current_workspace} of the governance lakehouse.

    Both ids are required, and the lakehouse is checked to exist: a typo would
    otherwise publish transport properties pointing at nothing, and no event
    would ever be written while the ETL still ran green.
    """
    ctx = notebookutils.runtime.context or {}
    current = ctx.get("currentWorkspaceId") or ctx.get("workspaceId")
    if not current:
        raise ValueError("Could not resolve a workspace id, run inside a "
                         "Fabric workspace.")
    if not (lakehouse_id and workspace_id):
        raise ValueError(
            "governance_lakehouse_id and data_workspace_id are required. Fill "
            "them in the parameters cell, or run through PL_Lineage, which "
            "injects them from the Variable Library (DataWorkspaceID, "
            "LH_Governance_ID).")
    try:
        lh = notebookutils.lakehouse.get(lakehouse_id, workspace_id)
    except Exception as e:
        raise ValueError(
            f"Governance lakehouse {lakehouse_id} was not found in workspace "
            f"{workspace_id}: {type(e).__name__}: {e}. Check LH_Governance_ID "
            f"and DataWorkspaceID for this environment (harvest with "
            f"workspace_setup/export_all_fabric_ids.py).") from e
    name = (lh.get("displayName") if isinstance(lh, dict)
            else getattr(lh, "displayName", None)) or "?"
    print(f"Governance lakehouse: {name} ({lakehouse_id}) "
          f"in workspace {workspace_id}")
    return {"workspace_id": str(workspace_id),
            "lakehouse_id": str(lakehouse_id).lower(),
            "current_workspace": str(current)}


meta = resolve_metadata_lakehouse(notebookutils,
                                  pinned(governance_lakehouse_id),
                                  pinned(data_workspace_id))
WORKSPACE_ID = meta["current_workspace"]   # environment + notebooks live here

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Fabric REST helpers -----------------------------------------------------
# Thin wrappers over api.fabric.microsoft.com with the notebook identity's
# token (works for a user and for a service principal). Every call raises on an
# unexpected status, with the response body in the message so a failed run is
# diagnosable from the log alone.

API = "https://api.fabric.microsoft.com/v1"


def _headers(content_type: str = "application/json") -> dict:
    # A fresh token per call: publish polling can outlive a token's lifetime.
    token = notebookutils.credentials.getToken("pbi")
    return {"Authorization": f"Bearer {token}", "Content-Type": content_type}


def fabric_request(method: str, path: str, expect=(200, 201, 202), **kw):
    """One REST call. `path` is relative to the v1 API root."""
    resp = requests.request(method, f"{API}{path}",
                            headers=kw.pop("headers", None) or _headers(),
                            timeout=120, **kw)
    if resp.status_code not in expect:
        raise RuntimeError(f"{method} {path} -> {resp.status_code}: "
                           f"{resp.text[:500]}")
    return resp


def fabric_get_all(path: str, key: str) -> list:
    """A listing endpoint, following continuationToken pagination."""
    items, token = [], None
    while True:
        sep = "&" if "?" in path else "?"
        url = path + (f"{sep}continuationToken={token}" if token else "")
        body = fabric_request("GET", url).json()
        items += body.get(key) or []
        token = body.get("continuationToken")
        if not token:
            return items

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Step 1+2: the Environment exists and carries the transport JAR ----------


def ensure_environment(workspace_id: str, name: str) -> str:
    """Id of the Environment with this display name, created when missing."""
    envs = fabric_get_all(f"/workspaces/{workspace_id}/environments", "value")
    match = [e for e in envs if e.get("displayName") == name]
    if match:
        print(f"Environment {name}: exists ({match[0]['id']})")
        return match[0]["id"]

    print(f"Environment {name}: not found, creating")
    fabric_request("POST", f"/workspaces/{workspace_id}/environments",
                   json={"displayName": name,
                         "description": "OpenLineage collector environment "
                                        "(created by NB_LineageSetup)"})
    # Create may be a long-running operation; the environment appears in the
    # listing when done. Poll the listing rather than the operation endpoint.
    for _ in range(30):
        envs = fabric_get_all(f"/workspaces/{workspace_id}/environments", "value")
        match = [e for e in envs if e.get("displayName") == name]
        if match:
            print(f"Environment {name}: created ({match[0]['id']})")
            return match[0]["id"]
        time.sleep(5)
    raise RuntimeError(f"Environment {name} did not appear after creation.")


def _resource_jar_path():
    """Driver-local path of the JAR in this notebook's built-in resource folder,
    or None. Upload the JAR there once in the Fabric UI, it then travels with
    the notebook item, so the setup is self-contained in every workspace it is
    deployed to. Checked under the relative alias and the mounted path, so a
    change of mount point between runtimes does not break the lookup."""
    candidates = [f"builtin/{jar_name}"]
    try:
        base = notebookutils.nbResPath
        candidates += [os.path.join(base, jar_name),
                       os.path.join(base, "builtin", jar_name)]
    except Exception:
        pass
    for path in candidates:
        if os.path.isfile(path):
            return path
    return None


def _onelake_get(workspace_id: str, path: str) -> bytes:
    """Read one OneLake file over the ADLS-gen2 REST API."""
    resp = requests.get(
        f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{path}",
        timeout=120,
        headers={"Authorization":
                 "Bearer " + notebookutils.credentials.getToken("storage")})
    if resp.status_code != 200:
        raise RuntimeError(f"GET {path} -> {resp.status_code}: "
                           f"{resp.text[:200]}")
    return resp.content


def _upload_jar(workspace_id: str, environment_id: str, data: bytes) -> None:
    """POST the JAR bytes into the environment's staging libraries
    (octet-stream, <=100 MB)."""
    fabric_request(
        "POST", f"/workspaces/{workspace_id}/environments/{environment_id}"
                f"/staging/libraries/{jar_name}",
        headers=_headers("application/octet-stream"), data=data)


def ensure_transport_jar(workspace_id: str, environment_id: str) -> str:
    """The transport JAR is among the environment's staged custom libraries.
    When missing it is uploaded from the first source that has it: this
    notebook's resource folder, then the metadata lakehouse Files."""
    body = fabric_request(
        "GET", f"/workspaces/{workspace_id}/environments/{environment_id}"
               f"/staging/libraries", expect=(200, 404)).json()
    custom = (body.get("customLibraries") or {})
    staged = [j for jars in custom.values() if isinstance(jars, list)
              for j in jars]
    if jar_name in staged:
        print(f"Transport JAR: already staged ({jar_name})")
        return "staged"

    # (1) this notebook's resource folder, travels with the notebook item.
    resource = _resource_jar_path()
    if resource:
        print(f"Transport JAR: uploading from notebook resources ({resource})")
        with open(resource, "rb") as f:
            _upload_jar(workspace_id, environment_id, f.read())
        return "uploaded (resources)"

    # (2) metadata lakehouse Files, fetched over the OneLake REST API.
    lakehouse_file = f"{meta['lakehouse_id']}/{jar_source_path}"
    try:
        data = _onelake_get(meta["workspace_id"], lakehouse_file)
    except Exception as e:
        raise RuntimeError(
            f"Transport JAR {jar_name} not found anywhere: not staged in the "
            f"environment, not in this notebook's resource folder, not at "
            f"{lakehouse_file}. Upload it to the notebook's resource folder "
            f"in the Fabric UI (or to the metadata lakehouse under "
            f"{jar_source_path}), then rerun. ({type(e).__name__}: {e})")
    print(f"Transport JAR: uploading from {jar_source_path}")
    _upload_jar(workspace_id, environment_id, data)
    return "uploaded (lakehouse)"


environment_id = ensure_environment(WORKSPACE_ID, environment_name)
jar_state = ensure_transport_jar(WORKSPACE_ID, environment_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Step 3+4: point the transport at the metadata lakehouse, publish --------


def transport_properties(meta: dict, namespace: str) -> dict:
    """The Spark properties the listener needs. transport.folder
    is relative to the lakehouse's Files/, and must stay in step with
    lineage_folder in NB_LineageSnapshot (default Files/lineage_events)."""
    return {
        "spark.extraListeners":
            "io.openlineage.spark.agent.OpenLineageSparkListener",
        "spark.openlineage.transport.type":      "onelake",
        "spark.openlineage.transport.folder":    "lineage_events",
        "spark.openlineage.transport.workspace": meta["workspace_id"],
        "spark.openlineage.transport.lakehouse": meta["lakehouse_id"],
        "spark.openlineage.namespace":           namespace,
        # The logical-plan facet is huge and unused by the parser; column
        # lineage across datasets is exactly what the parser consumes.
        "spark.openlineage.facets.spark.logicalPlan.disabled":   "true",
        "spark.openlineage.columnLineage.datasetLineageEnabled": "true",
        # Flush interval of the transport's background writer. A killed
        # session loses at most this many seconds of events.
        "spark.openlineage.transport.flushIntervalSeconds":      "5",
        # Spark silently drops listener events when this queue is full, and
        # the default 10000 has overflowed under the nine-way fan-out.
        "spark.scheduler.listenerbus.eventqueue.shared.capacity": "100000",
    }


def merge_spark_properties(staged: dict, wanted: dict) -> tuple:
    """The staged sparkCompute body with `wanted` merged over its
    sparkProperties. Returns (body, changed_keys). GET, merge and PATCH the whole
    body back, so pool/runtime settings pass through untouched."""
    body = dict(staged)
    props = dict(body.get("sparkProperties") or {})
    changed = [k for k, v in wanted.items() if props.get(k) != v]
    props.update(wanted)
    body["sparkProperties"] = props
    return body, changed


def configure_transport(workspace_id: str, environment_id: str) -> list:
    staged = fabric_request(
        "GET", f"/workspaces/{workspace_id}/environments/{environment_id}"
               f"/staging/sparkcompute").json()
    body, changed = merge_spark_properties(
        staged, transport_properties(meta, openlineage_namespace))
    if not changed:
        print("Spark properties: already correct")
        return []
    fabric_request(
        "PATCH", f"/workspaces/{workspace_id}/environments/{environment_id}"
                 f"/staging/sparkcompute", json=body)
    print(f"Spark properties: updated {len(changed)} key(s): {changed}")
    return changed


def publish_and_wait(workspace_id: str, environment_id: str,
                     timeout_s: int = 1800) -> str:
    """Trigger the publish and poll until it lands. Full-mode library publishes
    take minutes; the state lives on the environment item itself."""
    fabric_request("POST", f"/workspaces/{workspace_id}/environments/"
                           f"{environment_id}/staging/publish")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        env = fabric_request(
            "GET", f"/workspaces/{workspace_id}/environments/{environment_id}"
        ).json()
        state = (((env.get("properties") or {}).get("publishDetails") or {})
                 .get("state") or "unknown").lower()
        if state in ("success", "failed", "cancelled"):
            print(f"Publish: {state}")
            return state
        print(f"  publish state: {state}, waiting...")
        time.sleep(30)
    raise RuntimeError(f"Publish did not finish within {timeout_s}s.")


changed_properties = configure_transport(WORKSPACE_ID, environment_id)

publish_state = "skipped"
if publish_environment and (changed_properties
                            or jar_state.startswith("uploaded")):
    publish_state = publish_and_wait(WORKSPACE_ID, environment_id)
    if publish_state != "success":
        raise RuntimeError(f"Environment publish ended in state "
                           f"'{publish_state}', nothing was rolled out.")
elif publish_environment:
    print("Publish: nothing staged that is not already published, skipping.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Step 5: rollout ---------------------------------------------------------
# Targeted attach (the default): notebooks matched by naming convention get the
# Environment; everything else, including notebooks that deliberately run under
# other environments, stays untouched. Two caveats worth knowing:
#   - attaching replaces a notebook's environment, so the collector environment
#     must carry every library those ETL notebooks need, in this repo the
#     kidoma wheel;
#   - the attach rewrites each notebook's definition via
#     notebookutils.notebook.updateDefinition, which shows up as a git diff.
#     Try one notebook first on a fresh deployment.
# The workspace-default mechanism stays available for workspaces where no other
# environment is meant to be active: nothing is rewritten and future notebooks
# are covered automatically.

# The module's own notebooks never get the collector environment: they read the
# lineage, and emitting events about the metadata tables would only add noise.
MODULE_NOTEBOOKS = {"NB_LineageSetup", "NB_LineageSnapshot",
                    "NB_LineageSemanticModels"}


def find_etl_notebooks(patterns: list, workspace_id: str) -> list:
    """Display names of this workspace's notebooks matching any of the
    case-insensitive regexes, module notebooks excluded."""
    if not patterns:
        return []
    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
    matched = []
    for nb in (notebookutils.notebook.list(workspaceId=workspace_id) or []):
        name = (nb.get("displayName") if isinstance(nb, dict)
                else getattr(nb, "displayName", None)) or ""
        if name and name not in MODULE_NOTEBOOKS \
                and any(rx.search(name) for rx in compiled):
            matched.append(name)
    matched.sort()
    print(f"Pattern match: {len(matched)} notebook(s) {matched}")
    return matched


def _environment_id_of(nb: dict) -> str | None:
    """The environment GUID a notebook definition currently carries, or None.

    Takes the parsed definition, so a notebook is never held as a string and a
    dict at the same time.
    """
    deps = (nb.get("metadata") or {}).get("dependencies") or {}
    eid = ((deps.get("environment") or {}).get("environmentId") or "").strip()
    return eid.lower() or None


def _with_environment(nb: dict, environment_id: str,
                      environment_workspace_id: str) -> str:
    """The definition, serialized, with the environment attached.

    getDefinition returns ipynb JSON whose notebook-level metadata is the same
    object the git .py format writes into its metadata header, and the attachment
    lives at metadata.dependencies.environment. Writing it there works on both
    runtimes. updateDefinition's environmentId and environmentWorkspaceId
    arguments do not: they exist only in the Spark runtime, and on the Python
    kernel they raise TypeError for every notebook in the rollout.
    """
    deps = nb.setdefault("metadata", {}).setdefault("dependencies", {})
    deps["environment"] = {"environmentId": environment_id,
                           "workspaceId": environment_workspace_id}
    return json.dumps(nb, separators=(",", ":"))


def _definition(name: str, workspace_id: str) -> tuple:
    """(parsed definition, payload size in bytes), with the source string freed.

    Fabric has no API that patches a notebook's metadata alone, so attaching an
    environment means reading, patching and writing the whole definition. Doing
    one notebook at a time and releasing it before the next keeps the cost at the
    largest single notebook rather than the sum of the rollout.

    The size is returned so the rollout can print it next to the elapsed time.
    Each call is a long running operation and takes seconds regardless of size,
    so the two numbers together tell you whether a slow rollout is the API floor
    or a large definition. getDefinition returns ipynb, which includes cell
    outputs, so a notebook that has run display(df) can be far larger than its
    source.
    """
    content = notebookutils.notebook.getDefinition(name, workspaceId=workspace_id)
    size = len(content)
    nb = json.loads(content)
    del content
    return nb, size


def attach_to_notebooks(names: list, workspace_id: str, environment_id: str,
                        force: bool = False) -> dict:
    """Attach the environment to each listed notebook, one failure at a time.

    A notebook carries only one environment, so attaching replaces what is there.
    One that already has a different environment is skipped and reported rather
    than overwritten; force, from force_environment_attachment, is the opt-in to
    replace it anyway. A notebook with no environment yet is attached either way.

    The outcome is read back from the stored definition rather than taken from
    the return value, so a write that did not take is reported as a failure.

    Sequential on purpose: each notebook is one to three long running Fabric
    operations, and running them concurrently risks throttling for a rollout that
    happens rarely.
    """
    results = {}
    want = str(environment_id).strip().lower()
    started = time.monotonic()
    for name in names:
        t0, size = time.monotonic(), 0
        try:
            nb, size = _definition(name, workspace_id)
            before = _environment_id_of(nb)
            if before == want:
                results[name] = "already attached"
            elif before and not force:
                results[name] = (f"skipped: keeps environment {before} - set "
                                 f"force_environment_attachment = True to replace")
            else:
                payload = _with_environment(nb, environment_id, workspace_id)
                del nb
                ok = notebookutils.notebook.updateDefinition(
                    name=name, workspaceId=workspace_id, content=payload)
                del payload
                stored, _ = _definition(name, workspace_id)
                after = _environment_id_of(stored)
                del stored
                if after == want:
                    results[name] = ("attached" if not before
                                     else f"attached (replaced {before})")
                elif not ok:
                    results[name] = "failed: updateDefinition returned False"
                else:
                    results[name] = f"failed: definition still reports {after}"
        except Exception as e:
            results[name] = f"failed: {type(e).__name__}: {e}"
        print(f"  {name}: {results[name]}"
              f"  ({time.monotonic() - t0:.0f}s, definition {size / 1024:.0f} KB)")
    print(f"  rollout: {len(names)} notebook(s) in "
          f"{time.monotonic() - started:.0f}s")
    return results


# Disabled means disabled: no notebook listing, no definition reads, nothing
# rewritten, the attachments are expected to arrive with the deployment.
if attach_environment_to_notebooks:
    targets = find_etl_notebooks(attach_notebook_patterns, WORKSPACE_ID)
else:
    targets = []
    print("Rollout: per-notebook attach is off "
          "(attach_environment_to_notebooks = False). Notebook attachments are "
          "expected to come from their git definitions via the deployment; this "
          "run only ensures and publishes the Environment itself.")

rollout = {"enabled": attach_environment_to_notebooks,
           "matched": targets, "notebooks": {}}
if targets:
    print(f"Attaching {environment_name} to {len(targets)} notebook(s):")
    rollout["notebooks"] = attach_to_notebooks(
        targets, WORKSPACE_ID, environment_id,
        force=force_environment_attachment)
    rollout["skipped"] = [n for n, r in rollout["notebooks"].items()
                          if r.startswith("skipped")]
    if rollout["skipped"]:
        print(f"\n  {len(rollout['skipped'])} notebook(s) kept the environment "
              f"they already had and will emit no lineage: {rollout['skipped']}")
        print(f"  Either add the OpenLineage listener and JAR to the environment "
              f"they already use, or set force_environment_attachment = True to "
              f"move them onto {environment_name} - after putting the libraries "
              f"they import on it.")
    if [n for n, r in rollout["notebooks"].items() if r.startswith("attached")]:
        print("\n  Sync this workspace back to git now. The attachment lives in "
              "each notebook's definition, so committing it makes every future "
              "deployment apply it for free, and leaving it uncommitted means "
              "the next deployment republishes those notebooks without an "
              "environment and undoes this run.")
elif attach_environment_to_notebooks and attach_notebook_patterns:
    print(f"Rollout: no notebook matched {attach_notebook_patterns}. Check the "
          "names against notebookutils.notebook.list().")
if attach_environment_to_notebooks and not targets:
    print("Rollout: nothing attached. The environment is published and ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Step 6: verify ----------------------------------------------------------
# The published (not staged) state is what sessions actually get. A wrong
# transport GUID is the invisible failure this notebook exists to prevent, so
# a mismatch here fails the run loudly.


def verify_published(workspace_id: str, environment_id: str) -> list:
    """Mismatches between the published spark properties and what the metadata
    lakehouse discovery says they should be. Empty list = correct."""
    published = fabric_request(
        "GET", f"/workspaces/{workspace_id}/environments/{environment_id}"
               f"/sparkcompute").json()
    props = published.get("sparkProperties") or {}
    problems = []
    for key, want in transport_properties(meta, openlineage_namespace).items():
        got = props.get(key)
        if got != want:
            problems.append(f"{key}: published {got!r}, expected {want!r}")
    return problems


def events_folder_status() -> str:
    """Whether the transport has ever delivered anything. Informational: a
    fresh deployment has no events until the first ETL run with the
    environment attached."""
    folder = (f"abfss://{meta['workspace_id']}@onelake.dfs.fabric.microsoft.com/"
              f"{meta['lakehouse_id']}/Files/lineage_events")
    try:
        entries = notebookutils.fs.ls(folder)
        partitions = [e for e in entries if e.name.startswith("dt=")]
        return f"{len(partitions)} dt= partition(s)" if partitions else "empty"
    except Exception:
        return "missing (created on the first event)"


problems = verify_published(WORKSPACE_ID, environment_id)
events_status = events_folder_status()

print(f"\nEvents folder: {events_status}")
if events_status in ("empty", "missing (created on the first event)"):
    print("  Expected on a fresh deployment. After the next ETL run with the "
          "environment attached, rerun this cell. If it is still empty, no "
          "notebook has the environment (check the rollout step).")

attach_failures = [n for n, r in rollout["notebooks"].items()
                   if str(r).startswith("failed")]
status = {
    "status": ("error" if problems
               else "partial" if attach_failures else "ok"),
    "environment": environment_name, "environment_id": environment_id,
    "jar": jar_state,
    "properties_changed": changed_properties,
    "publish": publish_state,
    "rollout": rollout,
    "attach_failures": attach_failures,
    "events_folder": events_status,
    "problems": problems,
}
print(status)
if problems:
    raise RuntimeError(f"NB_LineageSetup: published environment does not match "
                       f"the discovered metadata lakehouse: {problems}")
if attach_failures:
    print(f"Warning: {len(attach_failures)} notebook(s) failed to attach "
          f"(partial run): {attach_failures}")

notebookutils.notebook.exit(json.dumps(status))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
