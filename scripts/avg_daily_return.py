from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType

spark = SparkSession.builder.appName("Average Daily Return").getOrCreate()

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
# select date, avg((close-prev_close)/prev_close) as average_return
# from (
#     select date, ticker, close , lag(close) over(partition by ticker order by date) as prev_close
#     from stocks_data
# )
# group by date""")


# Using Spark API
w = Window.partitionBy("ticker").orderBy("date")

df_with_return = (
    stocks_df
    .withColumn("prev_close", lag("close").over(w))
    .withColumn("daily_return", ((col("close") - col("prev_close")) / col("prev_close")))
)

df_res = df_with_return.groupBy("date").agg(
    avg("daily_return").alias("average_return")
)

df_res.write.option("header", "true").csv("s3://data-engineer-assignment-saarlevy/results/avg_daily_return.csv")