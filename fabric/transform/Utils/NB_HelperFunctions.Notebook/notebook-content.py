# Fabric notebook source


# CELL ********************

from delta.tables import DeltaTable
from datetime import datetime, date
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sempy.fabric as fabric
from dataclasses import dataclass, asdict, astuple
from pyspark.sql.types import IntegerType, BooleanType, FloatType, StringType, StructType, StructField, TimestampType, NullType
from pyspark.sql import DataFrame, SparkSession
import pandas as pd
import struct
import sqlalchemy
import pyodbc
import time
from enum import Enum
from typing import Tuple, Dict, Any
import urllib
from sqlalchemy import create_engine, text
import datetime as dt
from decimal import Decimal

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vl = notebookutils.variableLibrary.getLibrary("Variables_Environment_Config")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Fabric Util functions

# CELL ********************

def get_item_id() -> str:
    """
    Returns the ID of the current Fabric item (artifact) where this code is executing.
    
    Returns:
        str: The unique identifier of the current item.
    """
    return fabric.get_artifact_id()

def get_item_id_by_name(name: str) -> str:
    """
    Retrieves the ID of a Fabric item by its display name within the ETL workspace.
    
    Args:
        name (str): The display name of the item to find.
    
    Returns:
        str: The unique identifier of the item.
    
    Raises:
        IndexError: If no item with the given name is found.
    """
    items = fabric.list_items(workspace=vl.ETLWorkspaceName)


    item_row = items[items['Display Name'] == name]
    item_id = item_row['Id'].iloc[0]
    return item_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_destination_abfs_path(workspace_name, destination_lakehouse):
    workspace_id = fabric.resolve_workspace_id(workspace_name)
    lakehouse_id = notebookutils.lakehouse.get(destination_lakehouse, workspace_id).id
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Get Connection to Meta (Fabric or Azure) and Logging DB

# CELL ********************

def get_database_connection():
    """
    Returns a database connection engine based on the source type.
    
    :variable library databasetype: 'fabric' or 'azure'
    :return: SQLAlchemy engine
    """
    databasetype = vl.databasetype

    if databasetype == "fabric":
        return _get_fabric_engine()
    
    elif databasetype == "azure":
        return _get_azure_engine()
    
    else:
        raise ValueError("Invalid source. Use 'fabric' or 'azure'.")


def _get_fabric_engine():
    metadatabase_name = vl.metadatabase_name
    workspace_id = fabric.resolve_workspace_id(vl.DataWorkspaceName)
    items = fabric.list_items(workspace=workspace_id)
    sqldb_id = items[(items['Display Name'] == metadatabase_name) & (items['Type'] == 'SQLDatabase')]['Id'].iloc[0]
    
    fabric_rest_client = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/sqldatabases/{sqldb_id}").json()['properties']
    
    connection_string = (
        f"Driver=ODBC Driver 18 for SQL Server;"
        f"Server={fabric_rest_client['serverFqdn']};"
        f"Database={fabric_rest_client['databaseName']};"
        f"TrustServerCertificate=No"
    )
    
    token_bytes = notebookutils.credentials.getToken("https://analysis.windows.net/powerbi/api").encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    
    params = urllib.parse.quote_plus(connection_string)
    conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
    
    return create_engine(conn_str, connect_args={"attrs_before": {SQL_COPT_SS_ACCESS_TOKEN: token_struct}})


def _get_azure_engine():    
    params = urllib.parse.quote_plus(
        f"Driver=ODBC Driver 18 for SQL Server;"
        f"Server={vl.metadatabase_server};"
        f"Database={vl.metadatabase_name};"
        f"Authentication=ActiveDirectoryServicePrincipal;"

        f"UID={vl.sp_client_id};"
        f"PWD={notebookutils.credentials.getSecret(vl.keyvault_url, vl.kv_secretname)};" # if key vault does not allow access from all networks a private endpoint is required
        f"Authority Id={vl.tenant_id};"
    )
    
    conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
    return create_engine(conn_str)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_sql_endpoint(query: str):
    engine = get_database_connection()
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
            return df
    except Exception as e:
        print("Query failed:", e)
        return pd.DataFrame()  # Return empty DataFrame on failure

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_sql_endpoint(query: str, values=None):
    engine = get_database_connection()
    try:
        with engine.begin() as conn:  # Transaction is automatically committed
            if values is None:
                conn.exec_driver_sql(query)
            elif isinstance(values, list):
                if all(isinstance(i, tuple) for i in values):
                    for val in values:
                        conn.exec_driver_sql(query, val)
                else:
                    raise ValueError("Values for executemany must be a list of tuples.")
            else:
                if isinstance(values, tuple):
                    conn.exec_driver_sql(query, values) # Ensure single values are tuples
                else:
                    conn.exec_driver_sql(query, (values,)) # Convert single values to tuple
            print("Write was successful.")
    except Exception as e:
        print("Write failed:", e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Logging

# CELL ********************

@dataclass
class Audit:
    parent_audit_id: int
    run_id: str
    parent_run_id: str
    master_run_id: str
    item_name: str
    item_type: str
    item_id: str
    workspace_id: str
    workspace_name: str
    component_name: str
    start_time: datetime
    end_time: datetime
    run_url: str = None
    item_url: str = None
    duration_in_seconds: int = 0
    start_parameter: str = None
    source_object: str = None
    source: str = None
    sink: str = None
    source_type: str = None
    sink_type: str = None
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    has_failed: bool = False
    error_code: str = None
    error_message: str = None
    failure_type: str = None
    created_on: datetime = datetime.now()
    updated_on: datetime = None

    def __post_init__(self):
        self.duration_in_seconds = (int((self.end_time - self.start_time).total_seconds())) if self.end_time else None
        self.item_url = f'https://app.powerbi.com/groups/{self.workspace_id}/synapsenotebooks/{self.item_id}'
        self.run_url = f'https://app.powerbi.com/workloads/de-ds/sparkmonitor/{self.item_id}/{self.run_id}'

    def get_dataframe(self) -> DataFrame :
        schema = StructType([
            StructField('ParentAuditID', IntegerType(), False),
            StructField('RunId', StringType(), False),
            StructField('ParentRunId', StringType(), True),
            StructField('MasterRunId', StringType(), False),
            StructField('ItemName', StringType(), False),
            StructField('ItemType', StringType(), False),
            StructField('ItemId', StringType(), False),
            StructField('WorkspaceId', StringType(), False),
            StructField('WorkspaceName', StringType(), False),
            StructField('ComponentName', StringType(), False),
            StructField('StartTime', TimestampType(), False),
            StructField('EndTime', TimestampType(), True),
            StructField('RunURL', StringType(), True),
            StructField('ItemURL', StringType(), True),
            StructField('DurationInSeconds', IntegerType(), True),
            StructField('StartParameter', StringType(), True),
            StructField('SourceObject', StringType(), True),
            StructField('Source', StringType(), True),
            StructField('Sink', StringType(), True),
            StructField('SourceType', StringType(), True),
            StructField('SinkType', StringType(), True),
            StructField('Inserts', IntegerType(), False),
            StructField('Updates', IntegerType(), False),
            StructField('Deletes', IntegerType(), False),
            StructField('HasFailed', BooleanType(), False),
            StructField('ErrorCode', StringType(), True),
            StructField('ErrorMessage', StringType(), True),
            StructField('FailureType', StringType(), True),
            StructField('CreatedOn', TimestampType(), False),
            StructField('UpdatedOn', TimestampType(), True)
        ])

        return spark.createDataFrame([astuple(self)], schema)

class LoggingHelper():
    def add_audit_entry(self, audit_entry: str):
        sql = """
            INSERT INTO [log].[Audit] (
                ParentAuditID, RunId, ParentRunId, MasterRunId, ItemName, ItemType, ItemId, WorkspaceId,
                WorkspaceName, ComponentName, StartTime, EndTime, RunURL, ItemURL, DurationInSeconds,
                StartParameter, SourceObject, Source, Sink, SourceType, SinkType, Inserts, Updates, Deletes,
                HasFailed, ErrorCode, ErrorMessage, FailureType, CreatedOn, UpdatedOn
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        values = (
            audit_entry.parent_audit_id,
            audit_entry.run_id,
            audit_entry.parent_run_id,
            audit_entry.master_run_id,
            audit_entry.item_name,
            audit_entry.item_type,
            audit_entry.item_id,
            audit_entry.workspace_id,
            audit_entry.workspace_name,
            audit_entry.component_name,
            audit_entry.start_time,
            audit_entry.end_time,
            audit_entry.run_url,
            audit_entry.item_url,
            audit_entry.duration_in_seconds,
            audit_entry.start_parameter,
            audit_entry.source_object,
            audit_entry.source,
            audit_entry.sink,
            audit_entry.source_type,
            audit_entry.sink_type,
            audit_entry.inserts,
            audit_entry.updates,
            audit_entry.deletes,
            audit_entry.has_failed,
            audit_entry.error_code,
            audit_entry.error_message,
            audit_entry.failure_type,
            audit_entry.created_on,
            audit_entry.updated_on
        )

        write_sql_endpoint(sql, values)

    def add_audit_entries(self, audit_entries: list):
        sql = """
            INSERT INTO [log].[Audit] (
                ParentAuditID, RunId, ParentRunId, MasterRunId, ItemName, ItemType, ItemId, WorkspaceId,
                WorkspaceName, ComponentName, StartTime, EndTime, RunURL, ItemURL, DurationInSeconds,
                StartParameter, SourceObject, Source, Sink, SourceType, SinkType, Inserts, Updates, Deletes,
                HasFailed, ErrorCode, ErrorMessage, FailureType, CreatedOn, UpdatedOn
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        values = [
            (
                audit.parent_audit_id,
                audit.run_id,
                audit.parent_run_id,
                audit.master_run_id,
                audit.item_name,
                audit.item_type,
                audit.item_id,
                audit.workspace_id,
                audit.workspace_name,
                audit.component_name,
                audit.start_time,
                audit.end_time,
                audit.run_url,
                audit.item_url,
                audit.duration_in_seconds,
                audit.start_parameter,
                audit.source_object,
                audit.source,
                audit.sink,
                audit.source_type,
                audit.sink_type,
                audit.inserts,
                audit.updates,
                audit.deletes,
                audit.has_failed,
                audit.error_code,
                audit.error_message,
                audit.failure_type,
                audit.created_on,
                audit.updated_on,
            )
            for audit in audit_entries
        ]
        write_sql_endpoint(sql, values)


    def get_audit_entry_by_run_id_and_item_id(self, run_id, item_id):
        df = read_sql_endpoint(f"SELECT MAX(AuditID) AuditID FROM log.Audit WHERE RunId = '{run_id}' AND ItemId = '{item_id}'")
        return df["AuditID"].iloc[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_source_entities():
    all_dataframes = {name: obj for name, obj in globals().items() if isinstance(obj, DataFrame)}
    filtered_dfs = [df for df in dataframes if df.startswith("df_") and df != "df_transformed"]

    def to_camel_case(name):
        words = name[3:].split("_")  # "df_" entfernen und bei "_" splitten
        return "".join(word.capitalize() for word in words)  # CamelCase erzeugen

    camel_case_list = [to_camel_case(df) for df in filtered_dfs]
    comma_separated = ", ".join(camel_case_list)

    return comma_separated

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # SQL Endpoint

# CELL ********************


from datetime import datetime
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sempy.fabric as fabric
from dataclasses import dataclass, asdict, astuple
from pyspark.sql.types import IntegerType, BooleanType, FloatType, StringType, StructType, StructField, TimestampType
from pyspark.sql import DataFrame
import pandas as pd
import struct
import sqlalchemy
import pyodbc
import time
def pad_or_truncate_string(input_string, length, pad_char=' '):
    # Truncate if the string is longer than the specified length
    if len(input_string) > length:
        return input_string[:length]
    # Pad if the string is shorter than the specified length
    return input_string.ljust(length, pad_char)


def refresh_sql_endpoint(lakehouse_name):
    workspace_id = fabric.resolve_workspace_id(vl.DataWorkspaceName)
    lakehouse_id = notebookutils.lakehouse.get(lakehouse_name, workspace_id).id
    fabric_lakehouse = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()

    sql_endpoint_id = fabric_lakehouse['properties']['sqlEndpointProperties']['id']

    uri = f"/v1.0/myorg/lhdatamarts/{sql_endpoint_id}"
    payload = {"commands":[{"$type":"MetadataRefreshExternalCommand"}]}
    response = fabric.FabricRestClient().post(uri, json=payload)
    
    data = json.loads(response.text)
    progress_state = data["progressState"]
    print(f"Sync state: {progress_state}")

    status_uri = f"/v1.0/myorg/lhdatamarts/{sql_endpoint_id}/batches/{data['batchId']}"
    
    while progress_state == 'inProgress' :
        time.sleep(1)
        status_response_data = fabric.FabricRestClient().get(status_uri).json()
        progress_state = status_response_data["progressState"]
        print(f"Sync state: {progress_state}")

    if progress_state == 'success':
        table_details = [
            {
            'tableName': table['tableName'],
            'warningMessages': table.get('warningMessages', []),
            'lastSuccessfulUpdate': table.get('lastSuccessfulUpdate', 'N/A'),
            'tableSyncState':  table['tableSyncState'],
            'sqlSyncState':  table['sqlSyncState']
            }
            for table in status_response_data['operationInformation'][0]['progressDetail']['tablesSyncStatus']
        ]
        print("Extracted Table Details:")
        for detail in table_details:
            print(f"Table: {pad_or_truncate_string(detail['tableName'],30)}   Last Update: {detail['lastSuccessfulUpdate']}  tableSyncState: {detail['tableSyncState']}   Warnings: {detail['warningMessages']}")        

    if progress_state == 'failure':
        raise Exception(status_response_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Quality

# CELL ********************

def has_duplicate_entries(
        df: DataFrame,
        identifier_columns: list[str],
        raise_error: bool = True
    ) -> bool:
    """
    Check for uniqueness in the DataFrame based on the specified identifier columns.

    Args:
        df (DataFrame): The DataFrame to check.
        identifier_columns (List[str]): Columns to check for uniqueness.
        raise_error (bool): If True, raises ValueError when duplicates are found.

    Returns:
        bool: True if duplicates exist, else False.

    Raises:
        ValueError: If `identifier_columns` is empty, missing in df, or duplicates are detected
                    and `raise_error=True`.
    """

    total_count = df.count()
    distinct_count = df.dropDuplicates(identifier_columns).count()
    
    has_duplicates = distinct_count < total_count

    if raise_error & has_duplicates:
        print(f"[Info] Duplicate entries found based on columns: [{', '.join(identifier_columns)}]")
        print(f"[Info] Show the first 20 duplicate lines:")
        df.select(*identifier_columns).groupBy(*identifier_columns).count().filter("count > 1").show(20, truncate=False)
      
        raise ValueError(
                    "Duplicate entries found in DataFrame:\n"
                    f"\tidentifier_columns={identifier_columns}\n"
                    f"\ttotal_count={total_count}\n"
                    f"\tdistinct_count={distinct_count}"
                )

    else:
        return has_duplicates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Delta Table Operations

# CELL ********************

def create_or_alter_table(
    df: DataFrame,
    delta_table_path: str,
    partition_column: str = None
    ) -> DeltaTable:
    """
    Create or update a Delta Table at the specified ABFS path using the schema of the provided DataFrame.

    If the Delta Table does not exist, it will be created (optionally partitioned) according to the DataFrame schema.
    If the table already exists, any new columns present in the DataFrame but missing in the table are added in the correct order.
    Columns with NullType will be ignored.

    Args:
        df (DataFrame): The Spark DataFrame to use as the schema source.
        delta_table_path (str): The ABFS path prefix of the destination table.
        partition_column (str, optional): Comma-separated column name(s) to define partitions when creating a new table. Defaults to None.

    Returns:
        DeltaTable: A reference to the DeltaTable at the specified location.
    """
    delta_table_exists = notebookutils.fs.exists(delta_table_path + "/_delta_log")

    # Create table without partitions
    if not delta_table_exists and partition_column is None:
        print(f"[{datetime.now()}] Destination table doesn't exist. Create table...")
        (
            DeltaTable.createIfNotExists(df.sparkSession)
            .location(delta_table_path)
            .addColumns(df.schema)
            .execute()
        )

        print(f"[{datetime.now()}] Create table finished.")

    # Create table with partitions
    elif not delta_table_exists:
        print(f"[{datetime.now()}] Destination table doesn't exist. Create table with partitions...")

        # Convert partition_column to a list whether it's single or multiple columns
        partition_cols = [col.strip() for col in partition_column.split(',')]
        missing_columns = [col for col in partition_cols if col not in df.columns]

        if missing_columns:
            raise KeyError(
                f"Following partition columns are not in DataFrame: {', '.join(missing_columns)}. "
                "Please validate the 'partition_column' parameter."
            )
            
        (
            DeltaTable.createIfNotExists(df.sparkSession)
            .location(delta_table_path)
            .addColumns(df.schema)
            .partitionedBy(partition_cols)
            .execute()
        )

        print(f"[{datetime.now()}] Create table with partitions finished.")

    #Detect new columns and add to destination table (ignore NullType columns)
    else:
        print(f"[{datetime.now()}] Table exists. Checking for new columns...")
        
        delta_table = DeltaTable.forPath(df.sparkSession, delta_table_path)
        source_columns = [col for col in df.columns
                          if not isinstance(df.schema[col].dataType, NullType)]
        source_columns_lower = [col.lower() for col in source_columns]
        delta_columns_lower = [col.lower() for col in delta_table.toDF().columns]

        # check for new columns
        new_cols = [col for col, col_lower in zip(source_columns, source_columns_lower)
            if col_lower not in delta_columns_lower]

        if len(new_cols) == 0:
            print(f"[Info] No new columns found.")

        for idx, column in enumerate(new_cols):
            d_type = dict(df.dtypes)[column]
            if df.columns.index(column) == 0:
                alter_statement = f"ALTER TABLE delta.`{delta_table_path}` ADD COLUMN {column} {d_type} FIRST"
            else:
                previous_col = df.columns[df.columns.index(column) - 1]
                alter_statement = f"ALTER TABLE delta.`{delta_table_path}` ADD COLUMN {column} {d_type} AFTER {previous_col}"
            print(f"Excute: {alter_statement}")
            df.sparkSession.sql(alter_statement)

        print(f"[{datetime.now()}] Adding new columns finished.")

    return DeltaTable.forPath(df.sparkSession, delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_exit_value(spark: SparkSession, delta_table: DeltaTable, start_time: datetime, source: str="LH_Silver", sink: str="LH_Gold") -> dict[str, any]:
    """
    Retrieves the latest Delta Lake operation metrics since a given start time and augments them with execution metadata.

    Args:
        spark (SparkSession): The active SparkSession.
        delta_table (DeltaTable): The DeltaTable object to query history from.
        source (str): A string identifying the data source.
        sink (str): A string identifying the data sink.
        start_time (datetime): The UTC datetime object representing the start of the execution window.

    Returns:
        Dict[str, Any]: A dictionary containing operation metrics and augmented execution metadata.
    """
    history = (
        delta_table.history()
        .filter(F.col("timestamp") >= start_time)
        .filter(F.col("operation").isin("MERGE", "WRITE", "UPDATE", "DELETE"))
        .orderBy(F.col("version"), ascending=False)
        .limit(1)
    )
    history_collected = history.collect()
    if len(history_collected) > 0:
        history = history_collected[0].operationMetrics
    else:
        history = {}

    history["start_time"] = start_time
    history["end_time"] = datetime.now()
    history["execution_time"] = int((history["end_time"] - history["start_time"]).total_seconds())

    history["start_time"] = datetime.strftime(history["start_time"], "%Y-%m-%d %H:%M:%S")
    history["end_time"] = datetime.strftime(history["end_time"], "%Y-%m-%d %H:%M:%S")

    history["source"] = source
    history["sink"] = sink
    history["item_name"] = notebookutils.runtime.context["currentNotebookName"]
    history["item_id"] = notebookutils.runtime.context["currentNotebookId"]
    return history

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_metrics_of_delta_table(delta_table: DeltaTable, version: int) -> str:
    """Retrieves merge operation metrics for a specific version of a Delta Table.
    Args: 
        delta_table (DeltaTable): The DeltaTable object to query.
        version (int): The specific Delta Lake version (commit number) to retrieve metrics from.
    Returns:
        str: A dictionary-like string containing operation metrics such as the number of 
            inserted, updated, and deleted rows.
    """
    history = delta_table.history() \
        .filter(F.col("operation").isin("MERGE", "WRITE")) \
        .filter(f"version == {version}")
    return history.collect()[0].operationMetrics

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class WriteMode(Enum):
    APPEND = "append"
    UPSERT = "upsert"
    UPSERT_WITH_DELETE = "upsert_with_delete"
    OVERWRITE = "overwrite"


def write_delta_table(
    df: DataFrame, 
    identifier_columns: str,
    destination_path: str, 
    mode: WriteMode = WriteMode.UPSERT,
    exclude_columns_from_update: str = None, 
    audit_id: int = None
) -> tuple[DeltaTable, int]:
    """
    Merge a PySpark DataFrame into a Delta Table stored at the given ABFS path.

    Args:
        df (DataFrame): Source DataFrame to merge into the target Delta Table.
        identifier_columns (str): Comma-separated column names used to match rows (primary key/unique constraint).
        destination_path (str): ABFS path to the Delta Table.
        mode (WriteMode): Types of write (upsert, upsert_with_delete, append, overwrite)
        audit_id (int, optional): Audit ID to annotate data; if provided, meta columns are added.
        exclude_columns_from_update (str, optional): Comma-separated column names to exclude from update logic.

    Returns:
        Tuple[DeltaTable, int or None, dict or None]:
            - DeltaTable: The DeltaTable object after (or before, for dry run) the merge.
            - int or None: The Delta Table version after the merge (or None if no new merge occurred).

    Raises:
        Exception: If the source DataFrame is None.

    Notes:
        - Duplicate rows in the source DataFrame (as per the identifier columns) will raise an exception before merging.
        - All meta and identifier columns are excluded from update statements.
    """
    if df is None:
       raise Exception("Given source data frame is None") 
    
    # ----------- Add meta columns -----------  
    meta_prefix = "Meta"
    df = (df
        .withColumn(f"{meta_prefix}UpdatedAt", F.lit(datetime.now()))
        #.withColumn(f"{meta_prefix}AuditID", F.lit(audit_id))
    )

    # ----------- Create or update delta table -----------
    print(f"[{datetime.now()}] Starting create or alter table...")
    delta_table = create_or_alter_table(df, destination_path)
    print(f"[{datetime.now()}] Create or alter table finished.")

    max_version_before_write = (delta_table
        .history()
        .agg(F.max("version")).collect()[0][0]
    )
    print(f"[INFO] Delta table version before write: {max_version_before_write}")

    if mode in (WriteMode.UPSERT, WriteMode.UPSERT_WITH_DELETE):
        # Prepare exclude column list
        if exclude_columns_from_update is None:
            exclude_columns_from_update_list = []
        else:
            exclude_columns_from_update_list = [col.strip() for col in exclude_columns_from_update.split(',')]

        # Prepare identifier column list
        if not identifier_columns:
            # Include all df columns except exclusion columns and meta columns
            identifier_columns_list = [
                c for c in df.columns
                if c not in exclude_columns_from_update_list and not c.startswith(meta_prefix)
            ]
        else:
            identifier_columns_list = [col.strip() for col in identifier_columns.split(',')]
            missing = [c for c in identifier_columns_list if c not in df.columns]
            if missing:
                raise ValueError(f"Identifier columns missing in df: {missing}")

        # ----------- Check duplicate entries -----------
        print(f"[{datetime.now()}] Starting duplicate entries check...")
        has_duplicate_entries(df = df, identifier_columns = identifier_columns_list)
        print(f"[{datetime.now()}] Duplicate entries check finished.")

        # ----------- Prepare merge -----------
        print(f"[{datetime.now()}] Starting prepare merge..")
        exclude = set(identifier_columns_list) | set(exclude_columns_from_update_list)
        
        other_columns_list = [
            c for c in df.columns
            if c not in exclude and not c.startswith(meta_prefix)
        ]

        other_columns_list_with_meta = [
            c for c in df.columns
            if c not in exclude
        ]

        merge_conditions = " AND ".join(
            f"Source.{c} = Target.{c}" for c in identifier_columns_list
        )
        
        # If only identifier columns are present this condition is set, as there are no necessary updates
        matched_conditions = " OR ".join(
            f"!(Source.{c} <=> Target.{c})" for c in other_columns_list
        ) if other_columns_list else "1=0"

        # Create dictionary of update columns
        update_dict = {f"Target.{column}": f"Source.{column}" for column in other_columns_list_with_meta}
        insert_dict = {f"Target.{column}": f"Source.{column}" for column in df.columns}

        # Prepare merge_builder
        merge_builder = (delta_table
            .alias('Target')
            .merge(
                source= df.alias('Source'),
                condition=merge_conditions
            )
            .whenMatchedUpdate(
                condition=matched_conditions,
                set=update_dict
            ) 
            .whenNotMatchedInsert(
                values=dict(insert_dict)
            ) 
        )

        if mode is WriteMode.UPSERT_WITH_DELETE:
            merge_builder = merge_builder.whenNotMatchedBySourceDelete()

        print(f"[{datetime.now()}] Prepare merge finished.")
        

        # ----------- Execute merge -----------
        print(f"[{datetime.now()}] Starting merge execute...")
        merge_builder.execute()
        print(f"[{datetime.now()}] Merge execute finished.")

    elif mode is WriteMode.APPEND:
        # ----------- Execute append -----------
        print(f"[{datetime.now()}] Starting write with append...")
        df.write.mode("append").save(destination_path)
        print(f"[{datetime.now()}] Write with append executed.")

    elif mode is WriteMode.OVERWRITE:
        # ----------- Execute overwrite -----------
        print(f"[{datetime.now()}] Starting write with overwrite...")
        df.write.mode("overwrite").save(destination_path)
        print(f"[{datetime.now()}] Write with overwrite executed.")


    delta_table = DeltaTable.forPath(df.sparkSession, destination_path)

    max_version_after_write = (delta_table
        .history()
        .filter(F.col("operation").isin("MERGE", "WRITE"))
        .agg(F.max("version")).collect()[0][0]
    )
    
    # Check if merge happend
    if max_version_before_write >= max_version_after_write:
        post_version = None
        print(f"[INFO] Delta table version after write: {max_version_before_write}")
        print("[INFO] No new rows found for merging. (post_version = None)")
    else:
        post_version = max_version_after_write
        print(f"[INFO] Delta table version after write: {max_version_after_write}")
        print(f"[INFO] Delta table updated. (post_version = {post_version})")

    return delta_table, post_version

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Change Data Feed

# CELL ********************

from pyspark.errors import AnalysisException
import pyspark.sql.types as T
class ChangeDataFeed:

    def __init__(
        self, spark: SparkSession, target_entity: str, target_entity_table_path: str
    ):
        """The constructor of the CDF Helper only takes

        Args:
            target_entity (str): Target Entity Name of the Delta Table
            target_entity_table_path (str): Path to Target Entity
        """
        self.spark = spark
        self.metadata_path = target_entity_table_path + "/_cdf_metadata"
        self.target_entity = target_entity

        # check if the meta delta Table exists and if not create it.
        delta_table_exists = notebookutils.fs.exists(self.metadata_path)
        if delta_table_exists:

            # Init metadata table
            self.metadata_table = DeltaTable.forPath(self.spark, self.metadata_path)
            self.is_first_load = False

        if not delta_table_exists:
            self.is_first_load = True
            

        # Init version dict for later updating
        self.version_dict = {}

        

    def get_latest_source_version(self, source_entity_table_path: str) -> int:
        """Get most recent version number of a Source Delta Table

        Args:
            source_entity_table_path (str): Path to the most recent version number

        Returns:
            int: Most recent version number
        """
        return (
            DeltaTable.forPath(self.spark, source_entity_table_path)
            .history()
            .select(F.max(F.col("version")))
            .collect()[0][0]
        )

    def get_source_table_creation_time(self, source_entity_table_path: str) -> dt.datetime:
        """Get creation Time of Delta Table.

        Args:
            source_entity_table_path (str): Path to the Delta Table.

        Returns:
            atetime: Creation Time
        """
        return (
            DeltaTable.forPath(self.spark, source_entity_table_path)
            .history()
            .orderBy(F.col("version").asc())
            .select("timestamp")
            .collect()[0][0]
        )

    def get_last_processed_version(
        self, source_entity: str
    ) -> tuple[int | None, dt.datetime | None]:
        """Get the last processed version of a specified source Table.

        Args:
            source_entity (str): source entity name

        Returns:
            tuple[int | None, dt.datetime | None]: Returns version number and processing time
        """
        query_result = (
            self.metadata_table.toDF()
            .where(f"sourceEntity = '{source_entity}'")
            .select(
                F.max(F.col("versionProcessed")).alias("versionProcessed"),
                F.max(F.col("createdAt")).alias("createdAt"),
            )
            .collect()[0]
        )
        return query_result["versionProcessed"], query_result["createdAt"]

    def load_with_cdf_all_commits(
        self, source_entity: str, source_entity_table_path: str
    ) -> DataFrame:
        """Load change data feed Dataframe with all new commit Versions per row.
        Returns an empty dataframe with the schema of the source table if there has been no new version
        compared to the metadata table.
        Returns a dataframe containing the entire source table in the following cases:
            - There is no entry for the source table in the metadata table
            - The source table has been recreated since the timestamp `last_processed_at` in the metadata table
            - The starting version given by the metadata table is not available anymore (i.e. due to vacuum)

        Args:
            source_entity (str): source entity name
            source_entity_table_path (str): source entity

        Returns:
            DataFrame:  Returns the Dataframe
        """
        latest_source_version = self.get_latest_source_version(source_entity_table_path)
        source_table_creation_time = self.get_source_table_creation_time(
            source_entity_table_path
        )
        if self.is_first_load:
            last_processed_version = None
            last_processed_at = None
        else:
            last_processed_version, last_processed_at = self.get_last_processed_version(
                source_entity
            )

        # Load entire table if there is no entry for the source table in the metadata table
        if last_processed_version is None:
            df = self.spark.read.format("delta").load(source_entity_table_path)

        # Load entire table if the source table has been recreated since the timestamp `last_processed_at` in the metadata table
        elif last_processed_at < source_table_creation_time:
            self.metadata_table.delete(f"sourceEntity = '{source_entity}'")
            df = self.spark.read.format("delta").load(source_entity_table_path)

        # Return an empty table if there has been no new version compared to the metadata table
        elif latest_source_version == last_processed_version:
            df = self.spark.createDataFrame(
                [],
                schema=DeltaTable.forPath(self.spark, source_entity_table_path)
                .toDF()
                .schema,
            ).withColumn(
                "_change_type"
                ,F.lit(None).cast("string")
            )

        # Otherwise load only the changes since the last version
        else:
            # Try-Except needed for the case that the needed startingVersion is not available anymore (i.e. due to vacuum)
            # in that case also return the entire table
            try:
                print(f"Starting version: {last_processed_version + 1}")
                print(f"Ending Version: {latest_source_version}")
                df = (
                    self.spark.read.format("delta")
                    .option("readChangeFeed", "true")
                    .option("startingVersion", last_processed_version + 1)
                    .option("endingVersion", latest_source_version)
                    .load(source_entity_table_path)
                )
            except AnalysisException:
                self.metadata_table.delete(f"sourceEntity = '{source_entity}'")
                df = self.spark.read.format("delta").load(source_entity_table_path)

        # Update version dict with latest source version and return dataframe
        self.version_dict[source_entity] = latest_source_version
        return df

    def load_with_cdf_cleaned(
        self,
        source_entity: str,
        source_entity_table_path: str,
        identifier_cols: list[str],
        change_types: list[str] = ["update_postimage", "insert"],
        drop_internal_columns: bool = True,
    ) -> DataFrame:
        """Load change data feed Dataframe with most recent entry per row

        Args:
            source_entity (str): source entity name
            source_entity_table_path (str): path to source table
            identifier_cols (list[str]): list of identifier columns for table
            change_types (list[str], optional): Specifies change types to return. Defaults to ['update_postimage', 'insert'].
            drop_internal_columns (bool, optional): Whether internal CDF columns are supposed to be dropped. Defaults to True.

        Returns:
            DataFrame: Returns the Dataframe
        """

        df = self.load_with_cdf_all_commits(source_entity, source_entity_table_path)
        if "_commit_version" in df.columns:
            window = (
                Window()
                .partitionBy(*identifier_cols)
                .orderBy(F.col("_commit_version").desc())
            )
            df_cleaned = (
                df.where(F.col("_change_type").isin(change_types))
                .withColumn("_max", F.first(F.col("_commit_version")).over(window))
                .where("_max = _commit_version")
            )
            if drop_internal_columns:
                df_cleaned = df_cleaned.drop(
                    "_max", "_commit_version", "_change_type", "_commit_timestamp"
                )
            return df_cleaned
        else:
            return df

    def update_cdf_metadata_table_for_entity(
        self, source_entity: str, latest_source_version: int
    ) -> None:
        """Update the meta table with new processing

        Args:
            source_entity (str): source entity name
            latest_source_version (int): latest version processed
        """
        temp_schema = T.StructType(
            [
                T.StructField("sourceEntity", T.StringType(), False),
                T.StructField("versionProcessed", T.IntegerType(), False),
                T.StructField("createdAt", T.TimestampType(), False),
            ]
        )
        df_temp = self.spark.createDataFrame(
            [
                (source_entity, latest_source_version, dt.datetime.utcnow()),
            ],
            schema=temp_schema,
        )

        df_temp.write.mode("append").format("delta").save(self.metadata_path)

    def update_cdf_metadata_table(self) -> None:
        """
        Update the meta table with new Version numbers for all source entities in the version_dict
        and create meta data table if it does not exist
        """
        if self.is_first_load:
            metadata_schema = T.StructType(
                    [
                        T.StructField("sourceEntity", T.StringType(), False),
                        T.StructField("versionProcessed", T.IntegerType(), False),
                        T.StructField("createdAt", T.TimestampType(), False),
                    ]
                )

            (
                DeltaTable.createIfNotExists(self.spark)
                .location(self.metadata_path)
                .addColumns(metadata_schema)
                .execute()
            )
        for source_entity, latest_source_version in self.version_dict.items():
            self.update_cdf_metadata_table_for_entity(
                source_entity, latest_source_version
            )

        # reinitalize version_dict
        self.version_dict = {}

    def purge_entity_from_metatable(self, source_entity: str) -> str:
        """Purge the entity from the metadata table

        Args:
            source_entity (str): source entity name
        """
        self.metadata_table.delete(f"sourceEntity = '{source_entity}'")

        return "source entity purged from metadata table"

    def get_version_dict(self) -> dict:
        """Get the version dict

        Returns:
            dict: version dict
        """
        return self.version_dict

    def set_version_dict(self, version_dict: dict) -> None:
        """Set the version dict

        Args:
            version_dict (dict): version dict
        """
        self.version_dict = version_dict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Default Value Helper

# CELL ********************

class DefaultHelperException(Exception):
    """Exception class used in the DefaultHelper class
    """
    def __init__(self, message: str):
        super().__init__(message)    
        
class DefaultHelper:
    """A collection of methods to help with default values.
    """
    def __init__(self, spark: SparkSession) -> None :
       self.spark = spark
       self.default_values = {
            'tinyint': 0,
            'smallint': 0,
            'int': 0,
            'bigint': 0,
            'float': 0.0,
            'double': 0.0,
            'decimal': Decimal("0"),
            'string': 'N/A',
            'date': date(1900, 1, 1),
            'timestamp': datetime(1900, 1, 1, 0, 0, 0),
            'boolean': False,
        }
    def set_default_for_dtypes(self, dtypes: list[tuple[str, any]]) -> None:
        """Set default values for one ore more datatypes. Note: for decimal(p,s) use 'decimal' as key and e.g. Decimal("-1.0") as the value.
        Args:
            dtypes (list[tuple[str, any]]): A list of tuples of (dtype_as_a_string, default_value). E.g. [('string', 'unknown')]
        """
        for entry in dtypes:
            self.set_default_for_dtype(entry[0], entry[1])
    def set_default_for_dtype(self, dtype: str, default: any) -> None:
        """Set default value for a dataype. Note: for decimal(p,s) use 'decimal' as key and e.g. Decimal("-1.0") as the value.
        Args:
            dtype (str): datatype as a string
            default (any): Default value to be used for the datatype
        """
        self.default_values[dtype]= default
    #
    # Unknown row
    #        
    def add_unknown_row(self, df: DataFrame) -> DataFrame:
        """Adds a row of default values to the dataframe.
        Args:
            df (DataFrame): Source dataframe
        Returns:
            DataFrame: The resulting dataframe with an added row of defaults
        """
        dataTypeList = [dtype for name, dtype in df.dtypes]
        df_dummy = self.spark.createDataFrame(data=[tuple(map(self._create_unknown_entry, dataTypeList)),], schema=df.schema)
        return df.unionByName(df_dummy)
    def _create_unknown_entry(self, data_type: str) -> any:
        # Decimal appears as 'decimal(p,s)', but we want to use just 'decimal' in our dictionary:
        if data_type.startswith('decimal'):
            data_type = 'decimal'
        if not data_type in self.default_values.keys():
            raise DefaultHelperException(f"Default value for {data_type} is missing, please use set_default_for_dtype method to add the default value for this datatype.")
        
        return self.default_values[data_type]
    #
    # Fill NA
    #
    def fill_na(self, df: DataFrame, cols: list[str] = None) -> DataFrame:
        """Fill na values based on the data type for whole dataframe or specific columns.
        Args:
            df (DataFrame): Source dataframe
            cols (list): list of the columns we would like to fill na
        Returns:
            DataFrame: The resulting dataframe without na values for the whole dataframe or for specific columns
        """
        if not cols:
            cols = df.columns
            
        for col in cols:
            data_type = [t[1] for t in df.dtypes if t[0] == col][0]
            # Decimal appears as 'decimal(p,s)', but we want to use just 'decimal' in our dictionary:
            if data_type.startswith('decimal'):
                data_type = 'decimal'
            if data_type in self.default_values:
                replacement_value = self.default_values[data_type]
                df = df.withColumn(col, F.when(df[col].isNull(), replacement_value).otherwise(df[col]))
            else:
                raise DefaultHelperException(f"Default value for {data_type} is missing, please use set_default_for_dtype method to add the default value for this datatype.")
                
        return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
