import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *

def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("anurag_spark") \
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark

def spark_session_with_hudi():
    spark = SparkSession.builder.master("local[*]") \
        .appName("anurag_spark") \
        .config("spark.jars.packages",
                "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark
