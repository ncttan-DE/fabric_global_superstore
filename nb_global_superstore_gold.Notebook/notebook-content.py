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

# #### 1. Load Silver table

# CELL ********************

from pyspark.sql.functions import col, row_number, year, month, dayofmonth
from pyspark.sql.window import Window

silver_df = spark.table("silver_orders")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 2. DIMENSION TABLES

# MARKDOWN ********************

# ###### dim_customer

# CELL ********************

window_customer = Window.partitionBy("customer_id").orderBy(col("ingestion_ts").desc())

dim_customer_stg = (
    silver_df
    .select(
        "customer_id",
        "customer_name",
        "segment",
        "ingestion_ts"
    )
    .withColumn("rn", row_number().over(window_customer))
    .filter(col("rn") == 1)
    .drop("rn")
)

dim_customer_stg.createOrReplaceTempView("dim_customer_stg")

if not spark.catalog.tableExists("dim_customer"):
    dim_customer_stg.write.format("delta").mode("overwrite").saveAsTable("dim_customer")
else:
    spark.sql("""
        MERGE INTO dim_customer tgt
        USING dim_customer_stg src
        ON tgt.customer_id = src.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### dim_product

# CELL ********************

window_product = Window.partitionBy("product_id").orderBy(col("ingestion_ts").desc())

dim_product_stg = (
    silver_df
    .select(
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "ingestion_ts"
    )
    .withColumn("rn", row_number().over(window_product))
    .filter(col("rn") == 1)
    .drop("rn")
)

dim_product_stg.createOrReplaceTempView("dim_product_stg")

if not spark.catalog.tableExists("dim_product"):
    dim_product_stg.write.format("delta").mode("overwrite").saveAsTable("dim_product")
else:
    spark.sql("""
        MERGE INTO dim_product tgt
        USING dim_product_stg src
        ON tgt.product_id = src.product_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### dim_location

# CELL ********************

window_location = Window.partitionBy(
    "city", "state", "country"
).orderBy(col("ingestion_ts").desc())

dim_location_stg = (
    silver_df
    .select(
        "city",
        "state",
        "country",
        "region",
        "market",
        "postal_code",
        "ingestion_ts"
    )
    .withColumn("rn", row_number().over(window_location))
    .filter(col("rn") == 1)
    .drop("rn")
)

dim_location_stg.createOrReplaceTempView("dim_location_stg")

if not spark.catalog.tableExists("dim_location"):
    dim_location_stg.write.format("delta").mode("overwrite").saveAsTable("dim_location")
else:
    spark.sql("""
        MERGE INTO dim_location tgt
        USING dim_location_stg src
        ON tgt.city = src.city
           AND tgt.state = src.state
           AND tgt.country = src.country
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### dim_date

# CELL ********************

dim_date_stg = (
    silver_df
    .select("order_date")
    .dropDuplicates()
    .withColumn("year", year("order_date"))
    .withColumn("month", month("order_date"))
    .withColumn("day", dayofmonth("order_date"))
)

dim_date_stg.createOrReplaceTempView("dim_date_stg")

if not spark.catalog.tableExists("dim_date"):
    dim_date_stg.write.format("delta").mode("overwrite").saveAsTable("dim_date")
else:
    spark.sql("""
        MERGE INTO dim_date tgt
        USING dim_date_stg src
        ON tgt.order_date = src.order_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3. FACT TABLE

# CELL ********************

fact_sales_stg = (
    silver_df
    .select(
        "row_id",
        "order_id",
        "order_date",
        "customer_id",
        "product_id",
        "city",
        "sales",
        "quantity",
        "discount",
        "profit",
        "shipping_cost",
        "ship_mode",
        "order_priority",
        "record_hash"
    )
)

fact_sales_stg.createOrReplaceTempView("fact_sales_stg")

if not spark.catalog.tableExists("fact_sales"):
    fact_sales_stg.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("fact_sales")
else:
    spark.sql("""
        MERGE INTO fact_sales tgt
        USING fact_sales_stg src
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

spark.sql("""
SELECT
  (SELECT COUNT(*) FROM dim_customer)  AS dim_customer_rows,
  (SELECT COUNT(*) FROM dim_product)   AS dim_product_rows,
  (SELECT COUNT(*) FROM dim_location)  AS dim_location_rows,
  (SELECT COUNT(*) FROM dim_date)      AS dim_date_rows
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
