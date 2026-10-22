# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f5df3ca3-6568-4c3b-80e3-6f4ceb6a2d6f",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "1cfeab7c-4df3-48cb-8407-c31a3bb8a7e5",
# META       "known_lakehouses": [
# META         {
# META           "id": "f5df3ca3-6568-4c3b-80e3-6f4ceb6a2d6f"
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

string_date = "2026-01-11"
data_source = "AdventureWorks"
parent_audit_id = 0
parent_run_id = None
master_run_id = get_item_id()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 1) Get TransformConfiguration

# CELL ********************

transform_configuration_df = read_sql_endpoint( 
    f"SELECT D.Name AS DataSource, T.Entity, T.IdentifierColumns \
    FROM [meta].[TransformConfiguration] T \
    LEFT JOIN [meta].[DataSource] D ON D.DataSourceID = T.DataSourceID \
    WHERE D.Name = '{data_source}' AND IsActive = 'true'"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2) Load entities to silver  

# CELL ********************

activities = []
for index, config in transform_configuration_df.iterrows():
    activities.append(
        {
            "name": config.Entity, # activity name, must be unique
            "path": "02_NB_MergeEntityIntoSilverTables", # notebook path
            "args": {
                "data_source": config.DataSource,
                "entity": config.Entity,
                "identifier_columns": config.IdentifierColumns,
                "string_date": string_date
            },
            "timeoutPerCellInSeconds": 90, # max timeout for each cell, default to 90 seconds
            "retry": 0, # max retry times, default to 0
            "retryIntervalInSeconds": 0, # retry interval, default to 0 seconds
            "dependencies": [] # list of activity names that this activity depends on
        }
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG = {
    "activities": activities,
    "timeoutInSeconds": 43200, # max timeout for the entire pipeline, default to 12 hours
    "concurrency": 10 # max number of notebooks to run concurrently, default to unlimited
}

try:
    runs = notebookutils.notebook.runMultiple(DAG)
except Exception as e:
    runs = e.result

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

# # 3) Logging

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
        component_name = "Bronze2Silver", 
        start_parameter = f"data_source: {data_source}, string_date: {string_date}",
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
    item_id = get_item_id_by_name("02_NB_MergeEntityIntoSilverTables")
    transform_config = transform_configuration_df[transform_configuration_df["Entity"] == key]
    print(transform_config["DataSource"].iloc[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(audit_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

audit_entries = []
for key, value in runs.items():
    item_id = get_item_id_by_name("02_NB_MergeEntityIntoSilverTables")
    transform_config = transform_configuration_df[transform_configuration_df["Entity"] == key]
    data_source = transform_config["DataSource"].iloc[0]
    entity = transform_config["Entity"].iloc[0]
    identifier_columns = transform_config["IdentifierColumns"].iloc[0]
    if 'exception' in value and value['exception'] is not None:
        audit_entries.append(
            Audit(
                parent_audit_id = int(audit_id), 
                run_id = run_id, #when using the runMultiple() function, the same runid of the orchestrating notebook is used
                parent_run_id = run_id, 
                master_run_id = master_run_id,
                item_name = "02_NB_MergeEntityIntoSilverTables", 
                item_type = "Notebook",
                item_id = item_id,
                workspace_id = workspace_id,
                workspace_name = workspace_name,
                component_name = "Bronze2Silver", 
                start_parameter = f"data_source: {data_source}, entity: {entity}, identifier_columns: {identifier_columns}, string_date: {string_date}",
                source_object = key,
                source = "LH_Bronze",
                sink = "LH_Silver",
                source_type = "Lakehouse",
                sink_type = "Lakehouse",
                start_time = datetime.now(), 
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
                item_name = "02_NB_MergeEntityIntoSilverTables", 
                item_type = "Notebook",
                item_id = item_id,
                workspace_id = workspace_id,
                workspace_name = workspace_name,
                component_name = "Bronze2Silver", 
                start_parameter = f"data_source: {data_source}, entity: {entity}, identifier_columns: {identifier_columns}, string_date: {string_date}",
                source_object = key,
                source = f'LH_Bronze/{metrics["source"]}',
                sink = metrics["sink"],
                source_type = "Lakehouse",
                sink_type = "Lakehouse",
                start_time = datetime.strptime(metrics["start_time"], "%Y-%m-%d %H:%M:%S"),
                end_time = datetime.strptime(metrics["end_time"], "%Y-%m-%d %H:%M:%S"),
                duration_in_seconds = metrics["execution_time"],
                inserts = int(metrics.get('numTargetRowsInserted') or 0), # added 0 if no numTargetRowsInserted
                updates = int(metrics.get('numTargetRowsUpdated') or 0), # added 0 if no numTargetRowsUpdated
                deletes = int(metrics.get('numTargetRowsDeleted') or 0) # added 0 if no numTargetRowsDeleted
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

# # 4) Raise Error if child notebook failed

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
