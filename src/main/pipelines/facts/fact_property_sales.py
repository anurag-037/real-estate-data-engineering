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
    transactions_path = config.property_transactions
    property_path = config.dim_property
    broker_path = config.dim_broker
    sales_path = config.fact_property_sales

    # =========================================================
    # 2. READ
    # =========================================================
    transactions_df = spark.read.parquet(transactions_path)
    property_df = spark.read.format("hudi").load(property_path).filter("is_current = true")
    broker_df = spark.read.format("hudi").load(broker_path).filter("is_current = true")

    # =========================================================
    # 3. TRANSFORM (initial load)
    # =========================================================

    try:
        fact_existing_df = spark.read.format("hudi").load(sales_path)
    except:
        fact_existing_df = None

    if fact_existing_df is None:
        initial_fact_df = (
            transactions_df
            .join(property_df,"property_id","left")
            .join(broker_df,"broker_id","left")
            .select(
                transactions_df.transaction_id,
                property_df.property_id,
                broker_df.broker_id,
                transactions_df.sale_date,
                transactions_df.sale_price,
                transactions_df.tax_amount
            )
        )

        initial_fact_df = (
            transactions_df.alias("t")
            .join(property_df.alias("p"),
                (col("t.property_id") == col("p.property_id")) & (col("p.is_current") == "Y"), "left")
            .join(broker_df.alias("b"),
                (col("p.broker_id") == col("b.broker_id")) & (col("b.is_current") == "Y"),"left")
            .select(
                col("t.transaction_id"),
                col("p.property_id"),
                col("b.broker_id"),
                col("t.sale_date"),
                col("t.sale_price"),
                col("t.tax_amount"),
                col("t.updated_date")
            )
        )

        hudi_options = {
            "hoodie.table.name": "fact_property_sales",
            "hoodie.datasource.write.recordkey.field": "transaction_id",
            "hoodie.datasource.write.precombine.field": "updated_date",
            "hoodie.datasource.write.operation": "insert",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE"
        }

        initial_fact_df.show()

        initial_fact_df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("overwrite") \
            .save(sales_path)

main()
