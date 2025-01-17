from pyspark.sql import SparkSession
from pyspark.sql.functions import window
import pyspark.sql.functions as func
from pyspark.sql.functions import split, current_timestamp, col

spark = (
    SparkSession.builder.appName("test4")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    .getOrCreate()
)


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "negative")
    .option("startingOffsets", "earliest")
    .option("includeHeaders", "true")
    .load()
)

print(" Consumer Stream Processing has Started")


df1 = df.selectExpr("CAST(value AS STRING) AS message", "topic")
df2 = df1.selectExpr("CAST(message AS STRING)", "topic")


parsed_df = df2.select(
    func.from_json(df2.message, "msg STRING, count INT").alias("data"), "topic"
)


extracted_df = parsed_df.select("data.*", "topic")


count_df = (
    extracted_df.withColumn(
        "hashtags", split(col("msg"), "#").getItem(2).alias("hashtags")
    )
    .withColumn("timestamp", current_timestamp())
    .groupBy(window("timestamp", "30 minutes"))
    .agg(
        func.sum("count").alias("count_of_hashtags"),
        func.count("*").alias("count_of_tweets"),
    )
)

query = (
    count_df.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
