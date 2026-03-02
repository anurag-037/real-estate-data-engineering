from src.main.utility.logging_config import *
from src.main.utility.spark_session import *
import resources.config as config

spark = spark_session()

def main():

    # =========================================================
    # 1. CONFIGURATION VARIABLES
    # =========================================================
    input_path = config.raw_broker_incremental
    output_path = config.broker_master

    # =========================================================
    # 2. READ
    # =========================================================
    raw_df = spark.read.option("header", True).csv(input_path)

    # =========================================================
    # 3. TRANSFORM
    # =========================================================
    clean_df = (
        raw_df
        .withColumn("broker_id", trim(col("broker_id")))
        .withColumn("broker_name", trim(col("broker_name")))
        .withColumn("company_name", trim(col("company_name")))
        .dropDuplicates(["broker_id"])
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
    )

    # =========================================================
    # 4. Data Quality Check
    # =========================================================
    clean_df = clean_df.filter(col("broker_id").isNotNull())
    clean_df.show()

    # =========================================================
    # 4. WRITE
    # =========================================================
    logger.info(f"Writing curated data to {output_path}")

    clean_df.write \
        .mode("overwrite") \
        .parquet(output_path)

    logger.info("Broker Master Job Completed Successfully")

if __name__ == "__main__":
    main()