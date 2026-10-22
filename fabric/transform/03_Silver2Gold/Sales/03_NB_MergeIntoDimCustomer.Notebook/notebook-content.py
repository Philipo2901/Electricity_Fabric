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
# META     }
# META   }
# META }

# CELL ********************

#https://blog.fabric.microsoft.com/en-us/blog/optimizing-spark-compute-for-medallion-architectures-in-microsoft-fabric?ft=All
spark.conf.set("spark.sql.parquet.vorder.enabled","true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.optimizeWrite.binSize","1073741824")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Imports

# CELL ********************

from delta.tables import *
from datetime import datetime
import pyspark.sql.functions as F 
start_time = datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

is_debug_mode = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Constants

# CELL ********************

DESTINATION_SCHEMA = "Sales"
DESTINATION_ENTITY = "DimCustomer"
IDENTIFIER_COLUMNS = "CustomerID"
SURROGATE_KEY = 'CustomerSK'

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

# # 1) Read from silver

# CELL ********************

df_customer = spark.sql("SELECT * FROM LH_Silver.AdventureWorks.SalesLTCustomer")
df_customer_address = spark.sql("SELECT * FROM LH_Silver.AdventureWorks.SalesLTCustomerAddress")
df_address = spark.sql("SELECT * FROM LH_Silver.AdventureWorks.SalesLTAddress")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2) Transform

# CELL ********************

#Get target_path, feature workespace will write to dev workspace (hardcode dev worksapce name, and lakehouse name here)
abfs_path = get_destination_abfs_path(workspace_name=vl.DataWorkspaceName, destination_lakehouse = 'LH_Gold')
delta_table_exists = mssparkutils.fs.exists(f"{abfs_path}/Tables/{DESTINATION_SCHEMA}/{DESTINATION_ENTITY}")
max_identity_column = 0

if delta_table_exists:
    max_identity_column = spark.sql(f"SELECT * FROM LH_Gold.{DESTINATION_SCHEMA}.{DESTINATION_ENTITY}").select(F.max(SURROGATE_KEY)).collect()[0][0] or 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_transformed = df_customer \
    .join(df_customer_address.where("AddressType == 'Main Office'"), df_customer["CustomerID"] == df_customer_address["CustomerID"], "left") \
    .join(df_address, df_customer_address["AddressID"] == df_address["AddressID"], "left") \
    .select( \
        F.lit(max_identity_column + F.monotonically_increasing_id()+1).alias(SURROGATE_KEY), \
        df_customer["CustomerID"], \
        df_customer["Title"], \
        df_customer["FirstName"], \
        df_customer["LastName"], \
        df_customer["CompanyName"], \
        df_customer_address["AddressType"], \
        df_address["AddressLine1"], \
        df_address["AddressLine2"], \
        df_address["City"], \
        df_address["CountryRegion"], \
        df_address["PostalCode"] \
    )

dh = DefaultHelper(spark)
df_transformed = dh.add_unknown_row(df_transformed)
df_transformed = dh.fill_na(df_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if is_debug_mode:
    display(df_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3) Write into gold

# CELL ********************

delta_table, post_version = write_delta_table(
    df=df_transformed, 
    identifier_columns=IDENTIFIER_COLUMNS,
    destination_path=f"{abfs_path}/Tables/{DESTINATION_SCHEMA}/{DESTINATION_ENTITY}", 
    exclude_columns_from_update=SURROGATE_KEY,
    mode=WriteMode.UPSERT
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

history = generate_exit_value(spark, delta_table, start_time)

notebookutils.notebook.exit(history)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
