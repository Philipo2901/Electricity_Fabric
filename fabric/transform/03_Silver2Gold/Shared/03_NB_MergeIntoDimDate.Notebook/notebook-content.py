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
import datetime as dt
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import explode, to_date, col, date_format, dayofweek,dayofmonth, dayofyear, weekofyear, \
    quarter, last_day, concat, lit, current_date, when
from pyspark.sql.types import IntegerType
import time
from datetime import datetime
from delta.tables import *

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
DESTINATION_ENTITY = "DimDate"
IDENTIFIER_COLUMNS = 'DateSK'

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
start_year = 2000
future_years = 30

today = dt.date.today()
start_date = str(start_year) + '-01-01'
end_date = today + relativedelta(years=future_years)

#Create DataFrame
df_date = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])
df_date = df_date.selectExpr("explode(sequence(to_date(start_date), to_date(end_date))) AS TempDate")

#Change the column order so that DateKey is #1
df_date = df_date.withColumn(IDENTIFIER_COLUMNS, date_format(df_date.TempDate,'yyyyMMdd').cast(IntegerType())).withColumn('Date', df_date.TempDate)
df_date = df_date.drop('TempDate')

# #Add further date information
df_transformed = df_date.withColumn('Year', date_format(df_date.Date,'yyyy')) \
    .withColumn('Quarter', quarter(df_date.Date)) \
    .withColumn('QuarterShortName', concat(lit("Q"),quarter(df_date.Date))) \
    .withColumn('QuarterName', concat(lit("Quarter "),quarter(df_date.Date))) \
    .withColumn('Month', date_format(df_date.Date,'MM')) \
    .withColumn('MonthShortName', date_format(df_date.Date,'MMM')) \
    .withColumn('MonthName', date_format(df_date.Date,'MMMM')) \
    .withColumn('MonthSortName', concat(date_format(df_date.Date,'yyyy') , lit("-"),date_format(df_date.Date,'MM')))\
    .withColumn('BusinessDay', when(dayofweek(df_date.Date).isin (1,7),0).otherwise(1)) \
    .withColumn('LastDayOfMonth', last_day(df_date.Date)) \
    .withColumn('Day', date_format(df_date.Date,'dd')) \
    .withColumn('DayOfYear',  dayofyear(df_date.Date)) \
    .withColumn('DayOfMonth',  dayofmonth(df_date.Date)) \
    .withColumn('DayOfWeek',  dayofweek(df_date.Date)) \
    .withColumn('CreateDate', lit(dt.datetime.now()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2) Write into gold tables

# CELL ********************

abfs_path = get_destination_abfs_path(workspace_name=vl.DataWorkspaceName, destination_lakehouse = 'LH_Gold')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
