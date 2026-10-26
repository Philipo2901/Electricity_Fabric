# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d8a807cb-0836-437f-826c-416c5c8f05d1",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "1cfeab7c-4df3-48cb-8407-c31a3bb8a7e5",
# META       "known_lakehouses": [
# META         {
# META           "id": "d8a807cb-0836-437f-826c-416c5c8f05d1"
# META         },
# META         {
# META           "id": "abb261c6-e1bd-4a24-bc3d-ff740faa8a87"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "25081f93-ca7f-b2d2-4f46-5aaab3cb5aa6",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Imports

# CELL ********************

import json
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# For logging purpose
start_time = datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load helper functions

# CELL ********************

%run "NB_HelperFunctions"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters

# PARAMETERS CELL ********************

data_model_name = "Sales"
parent_audit_id = 0
parent_run_id = None
master_run_id = get_item_id()

refresh_sql_enpoint_enabled = True
is_debug_mode = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 1) Get DataProductConfiguration

# CELL ********************

data_product_configuration_df = read_sql_endpoint( 
    f"SELECT D.Name AS DataProduct, T.Notebook, T.Dependencies, T.TimeoutPerCellInSeconds, T.Retry, T.RetryIntervalInSeconds, T.IsActive \
    FROM [meta].[DataProductConfiguration] T \
    LEFT JOIN [meta].[DataProduct] D ON D.DataProductID = T.DataProductID \
    WHERE D.Name = '{data_model_name}' AND IsActive = 'true'"
    )

if is_debug_mode:
    display(data_product_configuration_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2) Load entities to gold  

# MARKDOWN ********************

# ## Create DAG

# CELL ********************

default_timeout_per_cell_in_seconds = 360
default_retry = 0
default_retry_interval_in_seconds = 0

activities = []
for index, config in data_product_configuration_df.iterrows():
    activities.append(
        {
            "name": config.Notebook, # activity name, must be unique
            "path": config.Notebook, # notebook path
            "args": {
                "is_debug_mode": is_debug_mode
            },
            "timeoutPerCellInSeconds": (default_timeout_per_cell_in_seconds if config.TimeoutPerCellInSeconds is None else config.TimeoutPerCellInSeconds), 
            "retry": (default_retry if config.Retry is None else config.Retry),
            "retryIntervalInSeconds": (default_retry_interval_in_seconds if config.RetryIntervalInSeconds is None else config.RetryIntervalInSeconds),
            "dependencies": ([] if config.Dependencies is None else config.Dependencies.split(","))
        }
    )

DAG = {
    "activities": activities,
    "timeoutInSeconds": 43200, # max timeout for the entire pipeline, default to 12 hours
    "concurrency": 10 # max number of notebooks to run concurrently, default to unlimited
}

if is_debug_mode:
    print(activities)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validate and run DAG

# CELL ********************

#Validate DAG
notebookutils.notebook.validateDAG(DAG)

#Run DAG
try:
    runs = notebookutils.notebook.runMultiple(DAG)
except Exception as e:
    runs = e.result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3) Refresh SQL Endpoint

# CELL ********************

if refresh_sql_enpoint_enabled:
    refresh_sql_endpoint("LH_Gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# For logging purpose
end_time = datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 4) Logging

# CELL ********************

logging_helper = LoggingHelper()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add Audit entry for this notebook

# CELL ********************

item_id = notebookutils.runtime.context["currentNotebookId"]
item_name = notebookutils.runtime.context["currentNotebookName"]
run_id = notebookutils.runtime.context["activityId"]
workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
workspace_name = notebookutils.runtime.context["currentWorkspaceName"]

audit_entry = Audit (
        parent_audit_id = parent_audit_id, 
        run_id = run_id, 
        parent_run_id = parent_run_id, 
        master_run_id = master_run_id,
        item_name = item_name, 
        item_type = "Notebook",
        item_id = item_id,
        workspace_id = workspace_id,
        workspace_name = workspace_name,
        component_name = "Silver2Gold", 
        source = 'LH_Bronze',
        sink = 'LH_Silver',
        start_time = start_time, 
        end_time = end_time
    )

logging_helper.add_audit_entry(audit_entry)
audit_id = logging_helper.get_audit_entry_by_run_id_and_item_id(run_id, item_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add Audit entries for child notebooks

# CELL ********************

audit_entries = []
for key, value in runs.items():
    item_name = [activity for activity in locals()['DAG']["activities"] if activity['name'] == key][0]['path']
    item_id = get_item_id_by_name(item_name)
    if 'exception' in value and value['exception'] is not None:
        audit_entries.append(
            Audit(
                parent_audit_id = int(audit_id), 
                run_id = run_id, #when using the runMultiple() function, the same runid of the orchestrating notebook is used
                parent_run_id = run_id, 
                master_run_id = master_run_id,
                item_name = item_name, 
                item_type = "Notebook",
                item_id = item_id,
                workspace_id = workspace_id,
                workspace_name = workspace_name,
                component_name = "Silver2Gold", 
                start_parameter = None,
                source_object = None,
                source = "LH_Silver",
                sink = f"LH_Gold/{key}",
                source_type = "Lakehouse",
                sink_type = "Lakehouse",
                start_time = start_time, #is not accurate
                end_time = None,
                has_failed = True,
                error_message = str(value  ['exception'])[0:3900]
            )
        )
    else:
        metrics = json.loads(value["exitVal"].replace("'", "\""))
        audit_entries.append(
            Audit(
                parent_audit_id = int(audit_id), 
                run_id = run_id, #when using the runMultiple() function, the same runid of the orchestrating notebook is used
                parent_run_id = run_id, 
                master_run_id = master_run_id,
                item_name = item_name, 
                item_type = "Notebook",
                item_id = item_id,
                workspace_id = workspace_id,
                workspace_name = workspace_name,
                component_name = "Silver2Gold", 
                start_parameter = None,
                source_object = metrics.get("source_object"),
                source = metrics.get("source"),
                sink = f"LH_Gold/{key}",
                source_type = "Lakehouse",
                sink_type = "Lakehouse",
                start_time = datetime.strptime(metrics["start_time"], "%Y-%m-%d %H:%M:%S"),
                end_time = datetime.strptime(metrics["end_time"], "%Y-%m-%d %H:%M:%S"),
                duration_in_seconds = metrics["execution_time"],
                inserts = int(metrics.get('numTargetRowsInserted') or 0),
                updates = int(metrics.get('numTargetRowsUpdated') or 0),
                deletes = int(metrics.get('numTargetRowsDeleted') or 0)
            )
        )

if audit_entries:
    logging_helper.add_audit_entries(audit_entries)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3) Raise Error if child notebook failed

# CELL ********************

class DAGExecutionError(Exception):
    pass

failed_notebooks = []
for key in runs.keys():
    if runs[key]["exception"] != None:
        failed_notebooks.append(key)
if failed_notebooks:
    raise DAGExecutionError(f"Notebooks {failed_notebooks} failed!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
