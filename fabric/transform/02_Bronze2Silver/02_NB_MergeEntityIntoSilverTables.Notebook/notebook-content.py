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
# META     }
# META   }
# META }

# MARKDOWN ********************

# #  Spark Configurations

# CELL ********************

spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)

#https://blog.fabric.microsoft.com/en-us/blog/optimizing-spark-compute-for-medallion-architectures-in-microsoft-fabric?ft=All
spark.conf.set("spark.sql.parquet.vorder.enabled","false")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","false")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Imports

# CELL ********************

from delta.tables import *
import pyspark.sql.functions as F
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_time = datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_HelperFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters

# PARAMETERS CELL ********************

data_source = "AdventureWorks"
entity = "SalesLTSalesOrderHeader"
string_date = "2026-01-11 11:53:38"
identifier_columns = "SalesOrderID"
# This cell is generated from runtime parameters. Learn more: https://go.microsoft.com/fwlink/?linkid=2161015
string_date = "2026-05-12 16:56:57"
identifier_columns = "AddressID"
entity = "SalesLTAddress"
data_source = "AdventureWorks"


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

# CELL ********************

date = datetime.strptime(string_date, '%Y-%m-%d %H:%M:%S')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 1) Read from bronze files section

# CELL ********************

def get_source_path():
    source_directory_delta_path = "Files/%s/%s/Delta/Year=%s/Month=%s/Day=%s/Hour=%s/Minute=%s" \
            % (data_source, entity, date.strftime("%Y"), date.strftime("%m"), date.strftime("%d"), date.strftime("%H"), date.strftime("%M"))
    source_directory_full_path = "Files/%s/%s/Full/Year=%s/Month=%s/Day=%s/Hour=%s/Minute=%s" \
            % (data_source, entity, date.strftime("%Y"), date.strftime("%m"), date.strftime("%d"), date.strftime("%H"), date.strftime("%M"))
            
    if mssparkutils.fs.exists(source_directory_delta_path):
        return source_directory_delta_path
    elif mssparkutils.fs.exists(source_directory_full_path):
        return source_directory_full_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_path = get_source_path()
file_format = notebookutils.fs.ls(source_path)[-1].name.split('.')[-1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format(file_format).load(source_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2) Transform

# MARKDOWN ********************

# ## Load transformation code for the cross-datasource transformation
# Cross-datasource transformations are defined in the CustomDataFrame class and can be used like that
# ```
# df_transfromed = CustomDataFrame(df).to_pascal_case("_")
# ```

# CELL ********************

%run 02_NB_SharedTransformation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load transformation code for all data sources
# Each data source has its own transformation notebook. In these, the classes must follow the naming scheme {data_source}Transformation and the functions the naming scheme transform_{entity}

# CELL ********************

%run 02_NB_AdventureWorksTransformation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute transformation
# Check if the class {data_source}Transformation and the function transform_{entity} exist, execute it and use the return value as df_transformed, otherwise set df_tranformed = df

# CELL ********************

if f"{data_source}Transformation" in locals():
    transformation = globals()[f"{data_source}Transformation"](df)
    if f"transform_{entity}" in dir(transformation):
        transform_entity = getattr(transformation, f"transform_{entity}")
        df_transformed = transform_entity()
    else:
        print(f"Function transform_{entity} not implemented")
        df_transformed = df
else:
    print(f"Class {data_source}Transformation not implemented")
    df_transformed = df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3) Data Quality checks

# CELL ********************

#TODO

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 4) Write into silver tables section

# CELL ********************

abfs_path = get_destination_abfs_path(workspace_name=vl.DataWorkspaceName, destination_lakehouse = 'LH_Silver')
delta_table, post_version = write_delta_table(
    df=df_transformed, 
    identifier_columns=identifier_columns,
    destination_path=f"{abfs_path}/Tables/{data_source}/{entity}", 
    mode=WriteMode.UPSERT
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

history = generate_exit_value(spark, delta_table, start_time)

mssparkutils.notebook.exit(history)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
