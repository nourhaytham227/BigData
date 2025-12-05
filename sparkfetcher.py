from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType


spark = SparkSession.builder \
    .appName("KafkaToDB") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')


schema = StructType() \
    .add("ID", IntegerType()) \
    .add("Name", StringType()) \
    .add("Certificate", StringType()) \
    .add("Duration", IntegerType()) \
    .add("Genre", StringType()) \
    .add("Rating", FloatType()) \
    .add("Metascore", IntegerType()) \
    .add("Revenue", IntegerType())

#reading from kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "test2") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")\



def write_to_oracle(batch_df, batch_id):
    #Analysis
    df = batch_df \
        .withColumn("NAME_LENGTH", length(col("Name"))) \
        .withColumn("RATING_ROUNDED", round(col("Rating"), 0))  
    #write to db
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:oracle:thin:@//localhost:1521/free") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("user", "nouri") \
        .option("password", "123") \
        .option("dbtable", "MOVIES") \
        .mode("append") \
        .save()

    print("wrote " + str(df.count()) + " rows to MOVIES.")


movies_query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_oracle) \
    .start()


#aggregation analysis
agg = df.groupBy("Genre").agg(
    count("*").alias("MOVIE_COUNT"),
    round(avg("Rating"), 1).alias("AVG_RATING"),
    sum("Revenue").alias("TOTAL_REVENUE")
)


def write_stats_to_oracle(batch_df, batch_id):

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:oracle:thin:@//localhost:1521/free") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("user", "nouri") \
        .option("password", "123") \
        .option("dbtable", "MOVIE_STATS") \
        .mode("overwrite") \
        .save()
    print("wrote " + str(batch_df.count()) + " rows to MOVIE_STATS.")


stats_query = agg.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_stats_to_oracle) \
    .start()

movies_query.awaitTermination()
stats_query.awaitTermination()