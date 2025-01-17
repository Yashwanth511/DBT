from pyspark.sql import SparkSession
from pyspark.sql.functions import split, window, count
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("test3").getOrCreate()

jdbcUrl = "jdbc:mysql://localhost:3306/dbt"
connectionProperties = {"user": "root", "password": "db_pass"}
df = spark.read.jdbc(url=jdbcUrl, table="tweets", properties=connectionProperties)

df1 = df.withColumn("tweet", split(df["value"], "\s+", limit=3).getItem(1)).withColumn(
    "hashtags", split(df["value"], "\s+", limit=3).getItem(2)
)

df1.createOrReplaceTempView("tweets_view")

positive_df = spark.sql("SELECT DISTINCT * FROM tweets_view WHERE topic = 'positive'")

count_df = positive_df.groupBy(window("timestamp", "30 minutes")).agg(
    func.count("tweet").alias("num_tweets"),
    func.sum(func.size(split(func.col("hashtags"), "\\s+"))).alias("num_hashtags"),
)

count_df.show()
