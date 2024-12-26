from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType

spark = SparkSession.builder.appName("Top 30 Day Return").getOrCreate()

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
# with return_30_days as(
# select date, ticker, (close-prev_30_close)/prev_30_close as return_30
# from (select date, ticker, close , lag(close,30) over(partition by ticker order by date) as prev_30_close
# from stocks_data)
# )
# select date, ticker from return_30_days
# order by return_30 desc
# limit 3""")


# Using Spark API
w = Window.partitionBy("tocker").orderBy("date")

df_with_30_return = (
    stocks_df
    .withColumn("prev_30_close", lag("close", 30).over(w))
    .withColumn("return_30", ((col("close") - col("prev_30_close")) / col("prev_30_close")))
)

df_res = df_with_30_return.orderBy(col("return_30").desc()).limit(3).select("date", "ticker")

df_res.write.option("header", "true").csv("s3://data-engineer-assignment-saarlevy/results/top_30_day_return.csv")