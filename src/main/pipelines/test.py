from resources import config
from src.main.utility.spark_session import *

spark = spark_session_with_hudi()
output_path = config.fact_property_sales
dim_existing_df = spark.read.format("hudi").load(output_path)

dim_existing_df.show()