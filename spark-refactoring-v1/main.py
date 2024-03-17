import subprocess
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import ( 
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

import pyspark.sql.functions  as F 

from session import spark
from schema import upbit_schema, bybit_schema


spark.init_session("CoinStreaming")
spark_session = next(spark.session)


events_upbit = (
        spark_session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker1:9092")
        .option("subscribe", "upbit")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("encoding", "UTF-8")
        .load()
)


df_upbit = events_upbit.select(
    F.from_json(F.col("value").cast("string"), upbit_schema).alias("value")
)
df_upbit = df_upbit.select("value.*")


events_bybit = (
    spark_session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("subscribe", "bybit")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("encoding", "UTF-8")
    .load()
)


df_bybit = events_bybit.select(
    F.from_json(F.col("value").cast("string"), bybit_schema).alias("value")
)

df_bybit = df_bybit.select("value.*")

# Main Data
windowed_df_upbit = df_upbit \
    .withColumn("current_time", F.expr("current_timestamp()")) \
    .withColumn("latency", (F.col("current_time").cast("long") - F.col("isotime").cast("long"))) \
    .withWatermark("isotime", "3 seconds") \
    .groupBy(F.window("isotime", "3 seconds"), "symbol") \
    .agg(F.first("guid").alias("guid"), F.first("isotime").alias("timestamp"), F.first("data").alias("data"), F.first("exchangeRate").alias("exchangeRate"))


windowed_df_upbit \
  .withWatermark("timestamp", "10 minute") \
  .dropDuplicates(["guid", "timestamp"])


windowed_df_bybit = df_bybit \
    .withColumn("current_time", F.expr("current_timestamp()")) \
    .withColumn("latency", (F.col("current_time").cast("long") - F.col("isotime").cast("long"))) \
    .withWatermark("isotime", "3 seconds") \
    .groupBy(F.window("isotime", "3 seconds"), "symbol") \
    .agg(F.first("guid").alias("guid"), F.first("isotime").alias("timestamp"), F.first("data").alias("data"), F.first("exchangeRate").alias("exchangeRate"))


windowed_df_bybit \
  .withWatermark("timestamp", "10 minute") \
  .dropDuplicates(["guid", "timestamp"])


windowed_upbit_delay = windowed_df_upbit.select("timestamp", "latency", "window")
windowed_bybit_delay = windowed_df_bybit.select("timestamp", "latency", "window")

timezone_delta = timedelta(hours=9)
utc_plus_9 = timezone(timezone_delta)
current_datetime = datetime.now().astimezone(utc_plus_9).strftime("%Y-%m-%d")

upbit_output_path = f"s3a://spark-s3-streaming/v1/upbit/{current_datetime}"
bybit_output_path = f"s3a://spark-s3-streaming/v1/bybit/{current_datetime}"

upbit_latency_path = f"s3a://kafka-monitoring/v1/latency/upbit/{current_datetime}"
bybit_latency_path = f"s3a://kafka-monitoring/v1/latency/bybit/{current_datetime}"


query_upbit = (
    windowed_df_upbit.writeStream.outputMode("append")
    .format("parquet")
    .trigger(processingTime="3 seconds")
    .option("path", upbit_output_path)
    .option("checkpointLocation", "checkpoint_upbit")
    .start()
)


query_bybit = (
    windowed_df_bybit.writeStream.outputMode("append")
    .format("parquet")
    .trigger(processingTime="3 seconds")
    .option("path", bybit_output_path)
    .option("checkpointLocation", "checkpoint_bybit")
    .start()
)


query_upbit_delay = (
    windowed_upbit_delay.writeStream.outputMode("append")
    .format("json")
    .option("path", upbit_latency_path)
    .option("checkpointLocation", "checkpoint_upbit_latency")
    .start()
)


query_bybit_delay = (
    windowed_bybit_delay.writeStream.outputMode("append")
    .format("json")
    .option("path", bybit_latency_path)
    .option("checkpointLocation", "checkpoint_bybit_latency")
    .start()
)


spark_session.streams.awaitAnyTermination()