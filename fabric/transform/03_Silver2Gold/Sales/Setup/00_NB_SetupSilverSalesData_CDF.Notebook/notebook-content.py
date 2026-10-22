# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "abb261c6-e1bd-4a24-bc3d-ff740faa8a87",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "1cfeab7c-4df3-48cb-8407-c31a3bb8a7e5",
# META       "known_lakehouses": [
# META         {
# META           "id": "abb261c6-e1bd-4a24-bc3d-ff740faa8a87"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Setup: Populate Silver Sales Tables (CDF Demo)
# 
# This notebook populates `SalesLTSalesOrderHeader` and `SalesLTSalesOrderDetail` in LH_Silver with random data.
# 
# **Change Data Feed (CDF) is enabled** on both tables so downstream Gold notebooks can track incremental changes.
# 
# **Behavior:**
# - **First run:** Creates the tables with an initial batch of orders.
# - **Subsequent runs:** Updates some existing rows (e.g. status, ship date) and inserts new orders — simulating incremental changes for the CDF pattern.

# CELL ********************

from delta.tables import DeltaTable
from datetime import datetime, timedelta
from pyspark.sql.types import *
import pyspark.sql.functions as F
import random
import uuid

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

# ---------- Table Configuration ----------
SILVER_SCHEMA = "AdventureWorks"
HEADER_TABLE = "SalesLTSalesOrderHeader_CDF"
DETAIL_TABLE = "SalesLTSalesOrderDetail_CDF"

# ---------- Volume Control ----------
NUM_NEW_ORDERS = 10            # Number of new orders to insert per run
NUM_DETAILS_PER_ORDER = (2, 6) # Min/max detail lines per order
NUM_UPDATES = 5                # Number of existing orders to update per run

# ---------- Value Sets (realistic IDs from AdventureWorks) ----------
CUSTOMER_IDS = [29485, 29531, 29568, 29584, 29612, 29638, 29644, 29653, 29660, 29736,
                29781, 29796, 29847, 29877, 29922, 29929, 29932, 29938, 29957, 29975,
                29982, 30019, 30025, 30027, 30033, 30072, 30089, 30102, 30113]

PRODUCT_IDS = [707, 708, 711, 712, 714, 715, 716, 722, 738, 739, 742, 747, 748,
               780, 781, 782, 783, 784, 793, 794, 795, 797, 798, 801, 809, 835,
               836, 838, 858, 859, 860, 864, 865, 867, 869, 870, 873, 875, 876,
               877, 880, 881, 883, 884, 885, 889, 892, 894, 904, 905, 909, 917,
               918, 924, 925, 935, 937, 938, 939, 940, 945, 949, 951, 957, 958,
               959, 962, 963, 965, 966, 967, 968, 969, 970, 971, 972, 973, 974,
               975, 976, 977, 978, 979, 983, 984, 985, 986, 987, 988, 992, 996, 998, 999]

# Unit prices keyed by product category ranges (simplified)
PRODUCT_PRICE_MAP = {
    (707, 716): (5.39, 29.99),    # Accessories
    (722, 748): (202.33, 818.70), # Components - Frames
    (780, 809): (672.29, 1466.01),# Bikes
    (835, 838): (356.90, 858.90), # Components - Headsets
    (858, 877): (1.37, 72.00),    # Components - Small parts
    (880, 896): (29.69, 602.35),  # Components - Brakes/Chains
    (904, 951): (23.48, 242.99),  # Clothing
    (957, 999): (112.99, 1430.44),# Components - High-end
}

ADDRESS_IDS = [637, 639, 640, 642, 643, 649, 651, 652, 653, 659, 669,
               989, 992, 993, 999, 1014, 1019, 1020, 1026, 1034, 1038,
               1058, 1061, 1086, 1090, 1092, 1098, 1102]

SHIP_METHODS = ["CARGO TRANSPORT 5"]

DISCOUNT_OPTIONS = [0.00, 0.02, 0.05, 0.10, 0.15, 0.40]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Helper Functions

# CELL ********************

def get_unit_price(product_id: int) -> float:
    """Get a reasonable unit price for a product based on its category range."""
    for (low, high), (price_low, price_high) in PRODUCT_PRICE_MAP.items():
        if low <= product_id <= high:
            return round(random.uniform(price_low, price_high), 4)
    return round(random.uniform(20.0, 500.0), 4)


def generate_orders(start_order_id: int, start_detail_id: int, num_orders: int):
    """Generate random header and detail rows."""
    headers = []
    details = []
    
    current_detail_id = start_detail_id
    now = datetime.now()
    
    for i in range(num_orders):
        order_id = start_order_id + i
        customer_id = random.choice(CUSTOMER_IDS)
        address_id = random.choice(ADDRESS_IDS)
        
        order_date = now - timedelta(days=random.randint(1, 30))
        due_date = order_date + timedelta(days=12)
        ship_date = order_date + timedelta(days=random.randint(5, 10))
        
        num_lines = random.randint(*NUM_DETAILS_PER_ORDER)
        sub_total = 0.0
        
        order_details = []
        for _ in range(num_lines):
            product_id = random.choice(PRODUCT_IDS)
            unit_price = get_unit_price(product_id)
            order_qty = random.randint(1, 25)
            discount = random.choice(DISCOUNT_OPTIONS)
            line_total = round(order_qty * unit_price * (1 - discount), 6)
            sub_total += line_total
            
            order_details.append((
                order_id,
                current_detail_id,
                order_qty,
                product_id,
                unit_price,
                discount,
                line_total,
                str(uuid.uuid4()),
                order_date,
                now
            ))
            current_detail_id += 1
        
        tax_amt = round(sub_total * 0.08, 4)
        freight = round(sub_total * 0.025, 4)
        total_due = round(sub_total + tax_amt + freight, 4)
        
        headers.append((
            order_id,
            2,
            order_date,
            due_date,
            ship_date,
            5,
            random.choice([True, False]),
            f"SO{order_id}",
            f"PO{random.randint(10000000, 99999999)}",
            f"10-4020-{random.randint(0, 999):06d}",
            customer_id,
            address_id,
            address_id,
            random.choice(SHIP_METHODS),
            None,
            round(sub_total, 4),
            tax_amt,
            freight,
            total_due,
            None,
            str(uuid.uuid4()),
            ship_date,
            now
        ))
        
        details.extend(order_details)
    
    return headers, details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Schemas

# CELL ********************

header_schema = StructType([
    StructField("SalesOrderID", IntegerType(), False),
    StructField("RevisionNumber", IntegerType(), True),
    StructField("OrderDate", TimestampType(), True),
    StructField("DueDate", TimestampType(), True),
    StructField("ShipDate", TimestampType(), True),
    StructField("Status", IntegerType(), True),
    StructField("OnlineOrderFlag", BooleanType(), True),
    StructField("SalesOrderNumber", StringType(), True),
    StructField("PurchaseOrderNumber", StringType(), True),
    StructField("AccountNumber", StringType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("ShipToAddressID", IntegerType(), True),
    StructField("BillToAddressID", IntegerType(), True),
    StructField("ShipMethod", StringType(), True),
    StructField("CreditCardApprovalCode", StringType(), True),
    StructField("SubTotal", DoubleType(), True),
    StructField("TaxAmt", DoubleType(), True),
    StructField("Freight", DoubleType(), True),
    StructField("TotalDue", DoubleType(), True),
    StructField("Comment", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("MetaUpdatedAt", TimestampType(), True),
])

detail_schema = StructType([
    StructField("SalesOrderID", IntegerType(), False),
    StructField("SalesOrderDetailID", IntegerType(), False),
    StructField("OrderQty", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("UnitPriceDiscount", DoubleType(), True),
    StructField("LineTotal", DoubleType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("MetaUpdatedAt", TimestampType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Resolve Paths & Determine State

# CELL ********************

lh_silver = get_destination_abfs_path(dev_workspace_name='Lakehouse Architecture - Dev', destination_lakehouse='LH_Silver')
header_path = f"{lh_silver}/Tables/{SILVER_SCHEMA}/{HEADER_TABLE}"
detail_path = f"{lh_silver}/Tables/{SILVER_SCHEMA}/{DETAIL_TABLE}"

header_exists = mssparkutils.fs.exists(header_path)
detail_exists = mssparkutils.fs.exists(detail_path)

print(f"Header table exists: {header_exists}")
print(f"Detail table exists: {detail_exists}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Generate & Write Data

# CELL ********************

if header_exists and detail_exists:
    # ============================================================
    # INCREMENTAL RUN: Update existing rows + Insert new rows
    # ============================================================
    print("Tables exist — performing incremental load (updates + inserts)...")
    
    # Read current max IDs
    df_header_current = spark.read.format("delta").load(header_path)
    df_detail_current = spark.read.format("delta").load(detail_path)
    
    max_order_id = df_header_current.agg(F.max("SalesOrderID")).collect()[0][0]
    max_detail_id = df_detail_current.agg(F.max("SalesOrderDetailID")).collect()[0][0]
    
    # --- 1) UPDATE existing orders (simulate status change, new ship date) ---
    existing_order_header_ids = [row.SalesOrderID for row in df_header_current.select("SalesOrderID").orderBy(F.rand()).limit(NUM_UPDATES).collect()]
    existing_order_detail_ids = [row.SalesOrderID for row in df_header_current.select("SalesOrderID").orderBy(F.rand()).limit(NUM_UPDATES).collect()]
    

    now = datetime.now()
    
    # Update headers: change Status and ModifiedDate
    dt_header = DeltaTable.forPath(spark, header_path)
    for oid in existing_order_header_ids:
        new_ship_date = now + timedelta(days=random.randint(1, 5))
        dt_header.update(
            condition=F.col("SalesOrderID") == oid,
            set={
                "Status": F.lit(random.choice([4, 5, 6])),
                "ShipDate": F.lit(new_ship_date),
                "RevisionNumber": F.col("RevisionNumber") + 1,
                "ModifiedDate": F.lit(now),
                "MetaUpdatedAt": F.lit(now)
            }
        )
    
    # Update details: change OrderQty and recalculate LineTotal for affected orders
    dt_detail = DeltaTable.forPath(spark, detail_path)
    for oid in existing_order_detail_ids:
        detail_ids = [row.SalesOrderDetailID for row in df_detail_current.filter(F.col("SalesOrderID") == oid).select("SalesOrderDetailID").orderBy(F.rand()).limit(2).collect()]
        for did in detail_ids:
            new_qty = random.randint(1, 30)
            # Get current price info
            row = df_detail_current.filter(F.col("SalesOrderDetailID") == did).select("UnitPrice", "UnitPriceDiscount").collect()[0]
            new_line_total = round(new_qty * row.UnitPrice * (1 - row.UnitPriceDiscount), 6)
            dt_detail.update(
                condition=F.col("SalesOrderDetailID") == did,
                set={
                    "OrderQty": F.lit(new_qty),
                    "LineTotal": F.lit(new_line_total),
                    "ModifiedDate": F.lit(now),
                    "MetaUpdatedAt": F.lit(now)
                }
            )
    
    print(f"  Updated {len(existing_order_header_ids)} existing orders.")
    
    # --- 2) INSERT new orders ---
    new_headers, new_details = generate_orders(max_order_id + 1, max_detail_id + 1, NUM_NEW_ORDERS)
    
    df_new_headers = spark.createDataFrame(new_headers, schema=header_schema)
    df_new_details = spark.createDataFrame(new_details, schema=detail_schema)
    
    df_new_headers.write.format("delta").mode("append").save(header_path)
    df_new_details.write.format("delta").mode("append").save(detail_path)
    
    print(f"  Inserted {NUM_NEW_ORDERS} new orders with {len(new_details)} detail lines.")

else:
    # ============================================================
    # INITIAL RUN: Create tables with CDF enabled
    # ============================================================
    print("Tables do not exist — creating initial load...")
    
    START_ORDER_ID = 71774
    START_DETAIL_ID = 110616
    INITIAL_NUM_ORDERS = 30
    
    headers, details = generate_orders(START_ORDER_ID, START_DETAIL_ID, INITIAL_NUM_ORDERS)
    
    df_headers = spark.createDataFrame(headers, schema=header_schema)
    df_details = spark.createDataFrame(details, schema=detail_schema)
    
    # Write with CDF enabled
    df_headers.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.enableChangeDataFeed", "true") \
        .save(header_path)
    
    df_details.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.enableChangeDataFeed", "true") \
        .save(detail_path)
    
    print(f"  Created {HEADER_TABLE} with {len(headers)} rows.")
    print(f"  Created {DETAIL_TABLE} with {len(details)} detail lines.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Verify

# CELL ********************

existing_order_detail_ids

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_h = spark.read.format("delta").load(header_path)
df_d = spark.read.format("delta").load(detail_path)

print(f"Header rows: {df_h.count()}")
print(f"Detail rows: {df_d.count()}")

display(df_h.orderBy(F.col("SalesOrderID").desc()).limit(5))
display(df_d.orderBy(F.col("SalesOrderDetailID").desc()).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show CDF is enabled
dt = DeltaTable.forPath(spark, header_path)
print("Header table properties:")
print(dt.detail().select("properties").collect()[0][0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
