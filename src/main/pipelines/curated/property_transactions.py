from src.main.utility.logging_config import *
from src.main.utility.spark_session import *
import resources.config as config

spark = spark_session()

def main():

    # =========================================================
    # 1. CONFIGURATION VARIABLES
    # =========================================================
    input_path = config.raw_transaction
    output_path = config.property_transactions

    # =========================================================
    # 3. READ
    # =========================================================
    raw_df = spark.read.option("header", True).csv(input_path)

    # =========================================================
    # 3. TRANSFORM
    # =========================================================
    clean_df = (
        raw_df
        .withColumnRenamed("Transaction_ID", "transaction_id")
        .withColumnRenamed("Property_ID", "property_id")
        .withColumnRenamed("SalePrice", "sale_price")
        .withColumnRenamed("SaleDate", "sale_date")
        .withColumnRenamed("TaxAmount", "tax_amount")
        .dropDuplicates(["transaction_id"])
        .withColumn("updated_date", current_timestamp())
    )

    # =========================================================
    # 4. Data Quality Check
    # =========================================================
    clean_df = clean_df.filter(col("transaction_id").isNotNull())

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