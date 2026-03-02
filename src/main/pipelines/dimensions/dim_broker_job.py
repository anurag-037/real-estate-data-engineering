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
    input_path = config.broker_master
    output_path = config.dim_broker

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
        max_key = dim_existing_df.agg(max("broker_key")).collect()[0][0]
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
            .withColumn("broker_key_temp", monotonically_increasing_id())
            .withColumn("broker_key", col("broker_key_temp") + max_key + 1)
            .drop("broker_key_temp", "created_at", "updated_at")
            )

        hudi_options = {
            "hoodie.table.name": "dim_broker",
            "hoodie.datasource.write.recordkey.field": "broker_key",
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
                  col("new.broker_id") == col("old.broker_id"),
                  "left")

        changed_df = joined_df.filter(
            (col("old.broker_id").isNull()) |  # new broker
            (col("new.company_name") != col("old.company_name")) |
            (col("new.broker_name") != col("old.broker_name"))
            )\
            .select(
                col("new.broker_id").alias("broker_id"),
                col("new.company_name").alias("company_name"),
                col("new.broker_name").alias("broker_name"),
                col("old.company_name").alias("old_company_name"),
                col("old.broker_name").alias("old_broker_name"),
                col("old.broker_id").alias("old_broker_id")
            )

        expired_df = current_dim_df \
            .join(changed_df.select("broker_id").distinct(),
                  "broker_id",
                  "inner")\
            .withColumn("end_date", current_date()) \
            .withColumn("is_current", lit("N"))

        expired_df = expired_df.select(
            "broker_id",
            "company_name",
            "broker_name",
            "start_date",
            "end_date",
            "is_current",
            "broker_key"
        )

        new_versions_df = (
            changed_df.select("broker_id", "company_name", "broker_name")
            .withColumn("start_date", current_date())
            .withColumn("end_date", lit("9999-12-31"))
            .withColumn("is_current", lit("Y"))
            .withColumn("broker_key_temp", monotonically_increasing_id())
            .withColumn("broker_key", col("broker_key_temp") + max_key + 1)
            .drop("broker_key_temp")
            .drop("old_company_name")
            .drop("old_broker_name")
            .drop("old_broker_id")
        )

        final_df = expired_df.unionByName(new_versions_df)

        hudi_options = {
            "hoodie.table.name": "dim_broker",
            "hoodie.datasource.write.recordkey.field": "broker_key",
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