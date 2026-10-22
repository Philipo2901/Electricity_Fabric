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

# MARKDOWN ********************

# # Imports

# CELL ********************

import numpy as np
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import explode, to_date, col, date_format, dayofweek,dayofmonth, dayofyear, weekofyear, \
    quarter, last_day, concat, lit, to_timestamp, current_date, cast
from pyspark.sql.types import IntegerType
from datetime import datetime
from delta.tables import *
import time

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

# CELL ********************

# For logging purpose
start_time = datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Constants

# CELL ********************

DESTINATION_SCHEMA = "Shared"
DESTINATION_ENTITY = "DimTimeOfDay"
IDENTIFIER_COLUMNS = 'TimeOfDaySK'

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

# # 1) Create dataframe

# CELL ********************

#Define parameters of the date table
dummy_start_timestamp = '00:00:00.000'
dummy_end_timestamp =  '23:59:59.999'


#Create Pandas Dataframe with all times of day
list_timestamp= pd.date_range(dummy_start_timestamp, dummy_end_timestamp, freq='s').tolist()
df_timestamp = pd.DataFrame (list_timestamp, columns = ['TimeStamp'])

#Create Spark DataFrame
df_timestamp = spark.createDataFrame(df_timestamp)

#Add further columns to Time of Day Table
df_transformed = df_timestamp.withColumn(IDENTIFIER_COLUMNS, date_format(to_timestamp(df_timestamp.TimeStamp),'HHmmss')) \
    .withColumn('TimeOfDayStringHHMMSS',date_format(df_timestamp.TimeStamp,'HH:mm:ss')) \
    .withColumn('TimeOfDayStringHHMM',date_format(df_timestamp.TimeStamp,'HH:mm')) \
    .withColumn('HourOfDay',date_format(df_timestamp.TimeStamp,'HH')) \
    .withColumn('MinuteOfDay',date_format(df_timestamp.TimeStamp,'mm')) \
    .withColumn('SecondOfDay',date_format(df_timestamp.TimeStamp,'ss')) \
    .withColumn('CreateDate', lit(dt.datetime.now())) \
    .drop('TimeStamp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2) Write into gold tables

# CELL ********************

abfs_path = get_destination_abfs_path(workspace_name=vl.DataWorkspaceName, destination_lakehouse = 'LH_Gold')
delta_table, post_version = write_delta_table(
    df=df_transformed, 
    identifier_columns=IDENTIFIER_COLUMNS,
    destination_path=f"{abfs_path}/Tables/{DESTINATION_SCHEMA}/{DESTINATION_ENTITY}", 
    exclude_columns_from_update='CreateDate',
    mode=WriteMode.UPSERT
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3) Return logging data

# CELL ********************

history = generate_exit_value(spark, delta_table, start_time)

notebookutils.notebook.exit(history)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
