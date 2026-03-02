from src.main.utility.logging_config import *
from src.main.utility.spark_session import *
import resources.config as config

spark = spark_session()

def main():

    # =========================================================
    # 1. CONFIGURATION VARIABLES
    # =========================================================
    input_path = config.raw_property
    output_path = config.property_master

    # =========================================================
    # 3. READ
    # =========================================================
    raw_df = spark.read.option("multiLine", "true").json(input_path)

    # =========================================================
    # 3. TRANSFORM
    # =========================================================
    clean_df = (
        raw_df
        .withColumnRenamed("propertyId", "property_id")
        .withColumnRenamed("brokerId", "broker_id")
        .withColumnRenamed("type", "property_type")
        .withColumnRenamed("bedrooms", "bedrooms")
        .withColumnRenamed("city", "city")
        .withColumnRenamed("price", "price")
        .withColumnRenamed("listingDate", "listing_date")
        .dropDuplicates(["property_id"])
        .withColumn("updated_date", current_timestamp())
    )

    # =========================================================
    # 4. Data Quality Check
    # =========================================================
    clean_df = clean_df.filter(col("property_id").isNotNull())

    # =========================================================
    # 4. WRITE
    # =========================================================
    logger.info(f"Writing curated data to {output_path}")

    clean_df.write \
        .mode("overwrite") \
        .parquet(output_path)

    logger.info("Property Master Job Completed Successfully")

if __name__ == "__main__":
    main()