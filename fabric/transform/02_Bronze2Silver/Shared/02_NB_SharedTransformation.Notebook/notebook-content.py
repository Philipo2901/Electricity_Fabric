# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

class CustomDataFrame(DataFrame):
    def __init__(self, df: DataFrame):
        super().__init__(df._jdf, df.sparkSession)

    def to_upper_camel_case(self):
        for column_name in self.columns:
            new_column_name = column_name[0].upper() + column_name[1:]
            self = self.withColumnRenamed(column_name, new_column_name)
        return CustomDataFrame(self)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
