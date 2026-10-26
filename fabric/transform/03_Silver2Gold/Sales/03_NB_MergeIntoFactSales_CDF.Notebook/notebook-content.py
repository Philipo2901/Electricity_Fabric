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
# import kidoma.transform.defaulthelper as K
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

# # Run CDF Setup Notebook

# CELL ********************

notebookutils.notebook.run(
    "00_NB_SetupSilverSalesData_CDF"
    ,300
    ,arguments={
        "useRootDefaultLakehouse": True
    }
)

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
DESTINATION_ENTITY = "FactSales_CDF"
IDENTIFIER_COLUMNS = "SalesOrderDetailID"
SURROGATE_KEY = 'SalesSK'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 1) Read from silver

# CELL ********************

lh_silver = get_destination_abfs_path(dev_workspace_name='Lakehouse Architecture - Dev', destination_lakehouse = 'LH_Silver')
lh_gold =  get_destination_abfs_path(dev_workspace_name='Lakehouse Architecture - Dev', destination_lakehouse = 'LH_Gold')

destination_path = f"{lh_gold}/Tables/{DESTINATION_SCHEMA}/{DESTINATION_ENTITY}"
## Initialize cdf module
cdf = ChangeDataFeed(spark, DESTINATION_ENTITY, destination_path)

## Read CDF Data and extract all Observation Dates for which there has been an Update
header_path = f"{lh_silver}/Tables/AdventureWorks/SalesLTSalesOrderHeader_CDF"
detail_path = f"{lh_silver}/Tables/AdventureWorks/SalesLTSalesOrderDetail_CDF"
df_sales_order_header_cdf = cdf.load_with_cdf_all_commits("SalesLTSalesOrderHeader_CDF", header_path)

df_sales_order_detail_cdf = cdf.load_with_cdf_all_commits("SalesLTSalesOrderDetail_CDF", detail_path)

filter_column = "SalesOrderID"
## _change_type is missing if there are no new rows
is_change_type_missing = any(
    "_change_type" not in df.columns
    for df in [df_sales_order_header_cdf, df_sales_order_detail_cdf]
)
if is_change_type_missing:
    print("Full load triggered")

    df_sales_order_header = spark.read.load(header_path)
    df_sales_order_detail = spark.read.load(detail_path)

else:
    df_sales_ids = (
        df_sales_order_detail_cdf.select(filter_column)
        .union(
            df_sales_order_header_cdf.select(filter_column)
        ).distinct()
    )
    
    df_sales_order_header = (
        spark.read.load(header_path)
        .join(
            df_sales_ids
            ,filter_column
            ,"inner"
        )
    )
    df_sales_order_detail = (
        spark.read.load(detail_path)
        .join(
            df_sales_ids
            ,filter_column
            ,"inner"
        )
    )





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2) Transform

# CELL ********************

df_dim_customer = spark.sql("SELECT * FROM LH_Gold.Sales.DimCustomer")
df_dim_product = spark.sql("SELECT * FROM LH_Gold.Sales.DimProduct")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Get target_path, feature workespace will write to dev workspace (hardcode dev worksapce name, and lakehouse name here)
abfs_path = get_destination_abfs_path(dev_workspace_name='Lakehouse Architecture - Dev', destination_lakehouse = 'LH_Gold')
delta_table_exists = mssparkutils.fs.exists(f"{abfs_path}/Tables/{DESTINATION_SCHEMA}/{DESTINATION_ENTITY}/_delta_log")
max_identity_column = 0

if delta_table_exists:
    max_identity_column = spark.sql(f"SELECT * FROM LH_Gold.{DESTINATION_SCHEMA}.{DESTINATION_ENTITY}").select(F.max(SURROGATE_KEY)).collect()[0][0] or 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_transformed = df_sales_order_header.alias("soh") \
    .join(df_sales_order_detail.alias("sod"), F.col("sod.SalesOrderID") == F.col("soh.SalesOrderID"), "left") \
    .join(df_dim_customer.alias("dc"), F.col("dc.CustomerID") == F.col("soh.CustomerID"), "left") \
    .join(df_dim_product.alias("dp"), F.col("dp.ProductID") == F.col("sod.ProductID"), "left") \
    .select(
        F.lit(max_identity_column + F.monotonically_increasing_id()+1).alias(SURROGATE_KEY), \
        "sod.SalesOrderDetailID", \
        "soh.SalesOrderID",
        "dc.CustomerSK", \
        "dp.ProductSK", \
        "soh.OrderDate", \
        "soh.DueDate", \
        "soh.ShipDate", \
        F.col("soh.OnlineOrderFlag").alias("IsOnlineOrder"), \
        F.col("sod.OrderQty").alias("OrderQuantity"),\
        "sod.UnitPrice", \
        "sod.UnitPriceDiscount", \
        "sod.LineTotal")



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
    destination_path=destination_path, 
    exclude_columns_from_update=SURROGATE_KEY,
    mode=WriteMode.UPSERT
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cdf.update_cdf_metadata_table()

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
