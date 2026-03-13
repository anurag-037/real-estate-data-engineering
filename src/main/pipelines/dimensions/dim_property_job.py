from src.main.utility.logging_config import *
from src.main.utility.spark_session import *
from resources import config
from pyspark.sql.functions import max, col
from pyspark.sql.functions import monotonically_increasing_id

spark = spark_session_with_hudi()

def main():

    # =========================================================
    # 1. CONFIGURATION VARIABLES
    # =========================================================
    input_path = config.property_master
    output_path = config.dim_property

    # =========================================================
    # 2. READ
    # =========================================================
    master_df = spark.read.parquet(input_path)
    master_df.show()

    # =========================================================
    # 3. TRANSFORM (initial load)
    # =========================================================
    try:
        dim_existing_df = spark.read.format("hudi").load(output_path)
        max_key = dim_existing_df.agg(max("property_key")).collect()[0][0]
        max_key = max_key if max_key else 0
    except:
        max_key = 0
        dim_existing_df = None

    if dim_existing_df is None:
        initial_dim_df = (
            master_df
            .withColumn("start_date", current_date())
            .withColumn("end_date", lit("9999-12-31"))
            .withColumn("is_current", lit("Y"))
            .withColumn("property_key_temp", monotonically_increasing_id())
            .withColumn("property_key", col("property_key_temp") + max_key + 1)
            .drop("property_key_temp", "updated_date")
            )

        hudi_options = {
            "hoodie.table.name": "dim_property",
            "hoodie.datasource.write.recordkey.field": "property_key",
            "hoodie.datasource.write.precombine.field": "start_date",
            "hoodie.datasource.write.operation": "insert",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE"
        }

        initial_dim_df.show()

        initial_dim_df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("overwrite") \
            .save(output_path)

    # =========================================================
    # 4. TRANSFORM (incremental load)
    # =========================================================
    else:
        current_dim_df = dim_existing_df.filter(col("is_current") == "Y")

        joined_df = master_df.alias("new") \
            .join(current_dim_df.alias("old"),
                  col("new.property_id") == col("old.property_id"),
                  "left")

        changed_df = joined_df.filter(
            (col("old.property_id").isNull()) |  # new property
            (col("new.broker_id") != col("old.broker_id")) |
            (col("new.property_type") != col("old.property_type")) |
            (col("new.bedrooms") != col("old.bedrooms")) |
            (col("new.city") != col("old.city")) |
            (col("new.price") != col("old.price"))
            )\
            .select(
                col("new.property_id").alias("property_id"),
                col("new.broker_id").alias("broker_id"),
                col("new.property_type").alias("property_type"),
                col("new.bedrooms").alias("bedrooms"),
                col("new.city").alias("city"),
                col("new.price").alias("price"),
                col("old.property_id").alias("old_property_id"),
                col("old.broker_id").alias("old_broker_id"),
                col("old.property_type").alias("old_property_type"),
                col("old.bedrooms").alias("old_bedrooms"),
                col("old.city").alias("old_city"),
                col("old.price").alias("old_price"),
                col("new.listing_date").alias("listing_date")
            )

        expired_df = current_dim_df \
            .join(changed_df.select("property_id").distinct(),
                  "property_id",
                  "inner")\
            .withColumn("end_date", current_date()) \
            .withColumn("is_current", lit("N"))

        expired_df = expired_df.select(
            "property_id",
            "broker_id",
            "property_type",
            "bedrooms",
            "city",
            "price",
            "listing_date",
            "start_date",
            "end_date",
            "is_current",
            "property_key"
        )

        new_versions_df = (
            changed_df.select("property_id", "broker_id", "property_type","bedrooms", "city","price", "listing_date" )
            .withColumn("start_date", current_date())
            .withColumn("end_date", lit("9999-12-31"))
            .withColumn("is_current", lit("Y"))
            .withColumn("property_key_temp", monotonically_increasing_id())
            .withColumn("property_key", col("property_key_temp") + max_key + 1)
            .drop("property_key_temp")
            .drop("old_property_id")
            .drop("old_broker_id")
            .drop("old_property_type")
            .drop("old_bedrooms")
            .drop("old_city")
            .drop("old_price")
        )

        final_df = expired_df.unionByName(new_versions_df)

        hudi_options = {
            "hoodie.table.name": "dim_property",
            "hoodie.datasource.write.recordkey.field": "property_key",
            "hoodie.datasource.write.precombine.field": "start_date",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE"
        }
        dim_existing_df.printSchema()
        final_df.printSchema()

        final_df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(output_path)

main()