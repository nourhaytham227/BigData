from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType


spark = SparkSession.builder \
    .appName("KafkaToDB") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')



schema = StructType() \
    .add("ID", IntegerType()) \
    .add("Name", StringType()) \
    .add("Certificate", StringType()) \
    .add("Duration", IntegerType()) \
    .add("Genre", StringType()) \
    .add("Rating", FloatType()) \
    .add("Metascore", IntegerType()) \
    .add("Revenue", IntegerType())



df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "output_topic") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")


def write_to_oracle(batch_df, batch_id):

    df = batch_df \
        .withColumn("Name_Length", length(col("Name"))) \
        .withColumn("Rating_Rounded", round(col("Rating"), 0))


    df.write \
        .format("jdbc") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("url", "jdbc:oracle:thin:@//localhost:1521/free") \
        .option("dbtable", "MOVIES") \
        .option("user", "nouri") \
        .option("password", "123") \
        .mode("append") \
        .save()

    agg_df = df.groupBy("Genre").agg(
    count("*").alias("movie_count"),
    round(avg("Rating"), 1).alias("avg_rating"),
    sum("Revenue").alias("total_revenue")
)


    agg_df.write \
        .format("jdbc") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("url", "jdbc:oracle:thin:@//localhost:1521/free") \
        .option("dbtable", "MOVIE_STATS") \
        .option("user", "nouri") \
        .option("password", "123") \
        .mode("append") \
        .save()



oracle_query = df.writeStream \
    .outputMode("append") \
    .trigger(processingTime="1 second") \
    .foreachBatch(write_to_oracle) \
    .option("checkpointLocation", "C:/tmp/checkpoints/oracle_movie_sink") \
    .start()

oracle_query.awaitTermination()
