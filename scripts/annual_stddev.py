from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType

spark = SparkSession.builder.appName("Annual STDDEV").getOrCreate()

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
# with daily_returns as(
# select date, ticker, (close-prev_close)/prev_close as daily_return
# from (select date, ticker, close , lag(close) over(partition by ticker order by date) as prev_close
# from stocks_data)
# )
# SELECT
# ticker,
#     -- this calculation was taken from the web, I didn't knew what annual stddev is, the assumption is there are 252 trading days per year
#     sqrt(252) * stddev(daily_return) AS annualized_stddev
# FROM
#     daily_returns
# WHERE
#     daily_return IS NOT NULL
# group by ticker
# order by annualized_stddev desc
# limit 1""")


# Using Spark API
w = Window.partitionBy("tocker").orderBy("date")

df_with_return = (
    stocks_df
    .withColumn("prev_close", lag("close").over(w))
    .withColumn("daily_return", ((col("close") - col("prev_close")) / col("prev_close")))
)

df_stddev_per_stock = df_with_return.groupBy("ticker").agg(
    stddev("daily_return").alias("daily_stddev")
)

df_stddev_annual = df_stddev_per_stock.withColumn(
    "annualized_stddev", col("daily_stddev") * sqrt(252)
)

df_res = df_stddev_annual.orderBy(col("annualized_stddev").desc()).limit(1)

df_res.write.option("header", "true").csv("s3://data-engineer-assignment-saarlevy/results/annual_stddev.csv")