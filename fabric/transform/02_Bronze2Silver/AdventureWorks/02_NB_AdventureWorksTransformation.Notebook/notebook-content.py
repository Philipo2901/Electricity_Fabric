# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

class AdventureWorksTransformation():
    def __init__(self, df: DataFrame):
            self.df = df

    def transform_SalesLTCustomer(self):
        return CustomDataFrame(self.df) \
            .to_upper_camel_case() \
            .drop("PasswordHash") \
            .drop("PasswordSalt") \
            .drop("Rowguid")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
