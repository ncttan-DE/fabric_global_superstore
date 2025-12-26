# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0e584ec2-ac1b-4527-a56a-135ad6afbedf",
# META       "default_lakehouse_name": "lh_global_superstore",
# META       "default_lakehouse_workspace_id": "14bc441d-af57-4449-94e9-848ed4994395",
# META       "known_lakehouses": [
# META         {
# META           "id": "0e584ec2-ac1b-4527-a56a-135ad6afbedf"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### 1. Import

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 2. Read RAW CSV (Bronze)

# CELL ********************

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("Files/raw/Global_Superstore2.csv")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3. Inspect RAW

# CELL ********************

display(raw_df)
raw_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4. Rename columns → snake_case (Silver standard)

# CELL ********************

silver_df = (
    raw_df
    .withColumnRenamed("Row ID", "row_id")
    .withColumnRenamed("Order ID", "order_id")
    .withColumnRenamed("Order Date", "order_date")
    .withColumnRenamed("Ship Date", "ship_date")
    .withColumnRenamed("Ship Mode", "ship_mode")
    .withColumnRenamed("Customer ID", "customer_id")
    .withColumnRenamed("Customer Name", "customer_name")
    .withColumnRenamed("Segment", "segment")
    .withColumnRenamed("City", "city")
    .withColumnRenamed("State", "state")
    .withColumnRenamed("Country", "country")
    .withColumnRenamed("Postal Code", "postal_code")
    .withColumnRenamed("Market", "market")
    .withColumnRenamed("Region", "region")
    .withColumnRenamed("Product ID", "product_id")
    .withColumnRenamed("Category", "category")
    .withColumnRenamed("Sub-Category", "sub_category")
    .withColumnRenamed("Product Name", "product_name")
    .withColumnRenamed("Sales", "sales")
    .withColumnRenamed("Quantity", "quantity")
    .withColumnRenamed("Discount", "discount")
    .withColumnRenamed("Profit", "profit")
    .withColumnRenamed("Shipping Cost", "shipping_cost")
    .withColumnRenamed("Order Priority", "order_priority")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 5. Convert DATE (dd-MM-yyyy)

# CELL ********************

silver_df = (
    silver_df
    .withColumn("order_date", to_date(col("order_date"), "dd-MM-yyyy"))
    .withColumn("ship_date", to_date(col("ship_date"), "dd-MM-yyyy"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 6. Cast numeric columns (VERY IMPORTANT)

# CELL ********************

silver_df = (
    silver_df
    .withColumn("sales", col("sales").cast("double"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("discount", col("discount").cast("double"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 7. Silver-level Data Quality Rules

# CELL ********************

silver_df = (
    silver_df
    .filter(col("order_id").isNotNull())
    .filter(col("order_date").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("product_id").isNotNull())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 8. Add technical column (ingestion_ts)

# CELL ********************

from pyspark.sql.functions import current_timestamp

silver_df = (
    silver_df.withColumn("ingestion_ts", current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 9. Add HASH column (change Detection)

# CELL ********************

from pyspark.sql.functions import sha2, concat_ws

silver_incremental_df = silver_df.withColumn(
    "record_hash",
    sha2(
        concat_ws(
            "||",
            col("order_id"),
            col("product_id"),
            col("sales"),
            col("quantity"),
            col("discount"),
            col("profit")
        ),
        256
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 10. create temp view (for merge)

# CELL ********************

silver_incremental_df.createOrReplaceTempView("silver_staging")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 11. Run check

# CELL ********************

table_exists = spark.catalog.tableExists("silver_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 12. CDC MERGE LOGIC

# CELL ********************

if not table_exists:
    silver_incremental_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_orders")
else:
    spark.sql("""
        MERGE INTO silver_orders tgt
        USING silver_staging src
        ON tgt.order_id = src.order_id
           AND tgt.product_id = src.product_id

        WHEN MATCHED
         AND tgt.record_hash <> src.record_hash
        THEN UPDATE SET *

        WHEN NOT MATCHED
        THEN INSERT *
    """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### POST-RUN VALIDATION

# CELL ********************

display(
    spark.sql("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT order_id) AS total_orders,
            COUNT(DISTINCT product_id) AS total_products,
            MAX(ingestion_ts) AS last_ingestion
        FROM silver_orders
    """)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
