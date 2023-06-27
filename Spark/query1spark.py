from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("311 Analysis") \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "3g") \
    .getOrCreate()

df = spark.read.csv("s3://clustertuplexproofs231/311_subset.csv", header=True, inferSchema=True)

year_to_investigate = 2019

def extract_month(date):
    date = date.split(" ")[0]
    return int(date.split("/")[0])

def extract_year(date):
    date = date.split(" ")[0]
    return int(date.split("/")[-1])

extract_month_udf = udf(extract_month, IntegerType())
extract_year_udf = udf(extract_year, IntegerType())

df2 = df.withColumn("Month", extract_month_udf(col("Created Date"))) \
    .withColumn("Year", extract_year_udf(col("Created Date"))) \
    .filter(col("Complaint Type").contains("Mosquito")) \
    .filter(col("Year") == year_to_investigate) \
    .select("Month", "Year", "Complaint Type")

df2.show(5)
