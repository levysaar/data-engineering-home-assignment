from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType

spark = SparkSession.builder.appName("Highest Worth").getOrCreate()

stocks_schema = StructType([
    StructField("date", DateType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("ticker", StringType(), True),
])

df_stocks = spark.read.csv("s3://data-engineer-assignment-saarlevy/stocks_data/", header=True, schema=stocks_schema)

# Using SparkSQL
# df_stocks.createOrReplaceTempView("stocks_data")
# df_res = spark.sql("""
# select ticker, avg(close * volume) as value
# from stocks_data
# group by ticker
# order by value desc
# limit 1""")

# Using Spark API
df_with_value = df_stocks.withColumn("daily_value", col("close") * col("volume"))

df_avg_value = df_with_value.groupBy("ticker").agg(
    avg("daily_value").alias("value")
)

df_res = df_avg_value.orderBy(col("value").desc()).limit(1)

df_res.write.option("header", "true").csv("s3://data-engineer-assignment-saarlevy/results/highest_worth.csv")