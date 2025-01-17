from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf, col, current_timestamp
import mysql.connector
import re
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test3").getOrCreate()


hostname = "localhost"
user = "root"
password = "db_pass!"  # your password in mysql
database = "dbt"  # your database in mysql
table_name = "tweets"

# made a change to a file


def drop_table_if_exists(tablename):
    db = mysql.connector.connect(
        host=hostname, user=user, password=password, database=database
    )
    cursor = db.cursor()
    sql = f"DROP TABLE IF EXISTS {tablename}"
    cursor.execute(sql)
    db.close()


def remove_digits(s):
    return "".join(c for c in s if not c.isdigit())


def remove_punctuations(s):
    return re.sub(r"[^\w\s]", "", s)


@udf(returnType=StringType())
def extract_hashtags(s):
    hashtags = re.findall(r"#(\w+)", s)
    return " #".join(hashtags)


@udf(returnType=StringType())
def preprocess(s):
    news = remove_digits(s)
    news = remove_punctuations(news)
    return news


schema = StructType(
    [
        StructField("value", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("hashtags", StringType(), True),
        StructField("timestamp", StringType(), True),  # New column added
    ]
)


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "positive,negative,neutral")
    .option("startingOffsets", "earliest")
    .option("includeHeaders", "true")
    .load()
)

df = df.selectExpr("CAST(value AS STRING)", "topic").withColumn(
    "hashtags", extract_hashtags(col("value"))
)

df = df.withColumn("value", preprocess(col("value")))


db_target_properties = {"user": user, "password": password}


def foreach_batch_function(df, epoch_id):
    print("Writing to database")
    df = df.withColumn("timestamp", current_timestamp())
    df.write.mode("append").jdbc(
        url=f"jdbc:mysql://{hostname}/{database}",
        table=table_name,
        properties=db_target_properties,
    )


drop_table_if_exists(table_name)
out = df.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start()
out.awaitTermination()
