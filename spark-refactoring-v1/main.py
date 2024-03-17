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
        .option("kafka.bootstrap.servers", "kafka:9092")
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

df_upbit \
  .withWatermark("isotime", "10 minute") \
  .dropDuplicates("guid", "isotime")

events_bybit = (
    spark_session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
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

df_bybit \
  .withWatermark("isotime", "10 minute") \
  .dropDuplicates("guid", "isotime")

# Main Data
windowed_df_upbit = df_upbit \
    .withWatermark("isotime", "3 seconds") \
    .groupBy(F.window("isotime", "3 seconds"), "symbol") \
    .agg(F.first("isotime").alias("timestamp"), F.first("data").alias("data"), F.first("exchangeRate").alias("exchangeRate"))

windowed_df_bybit = df_bybit \
    .withWatermark("isotime", "3 seconds") \
    .groupBy(F.window("isotime", "3 seconds"), "symbol") \
    .agg(F.first("isotime").alias("timestamp"), F.first("data").alias("data"), F.first("exchangeRate").alias("exchangeRate"))


# Delay monitoring
# df_upbit_with_delay = df_upbit.withColumn("event_processing_delay", F.expr("(current_timestamp()- isotime)"))
# df_bybit_with_delay = df_bybit.withColumn("event_processing_delay", F.expr("(current_timestamp()- isotime)"))


# 현재 시간을 구하는 함수
# now = datetime.now(timezone(timedelta(hours=9)))
# current_time = round(now.timestamp() * 1000)

# 현재 시간에서 이벤트 시간을 빼서 지연 시간을 계산한 후 밀리초로 변환
# df_upbit_with_delay = df_upbit.withColumn("current_time", F.lit(current_time)) \
#     .withColumn("event_processing_delay", (F.col("current_time") - F.col("timestamp")).cast("bigint"))


# df_bybit_with_delay = df_bybit.withColumn("current_time", F.lit(current_time)) \
#     .withColumn("event_processing_delay", (F.col("current_time").cast("integer") - F.col("timestamp").cast("bigint")) )


windowed_upbit_delay = df_upbit \
     .withColumn("current_time", F.expr("current_timestamp()")) \
    .withColumn("latency", (F.col("current_time") - F.col("isotime"))) \
    .withWatermark("isotime", "30 seconds") \
    .groupBy(F.window("isotime", "1 minute")) \
    .agg(F.max("latency").alias("max_latency"))


windowed_bybit_delay = df_bybit \
    .withColumn("current_time", F.expr("current_timestamp()")) \
    .withColumn("latency", (F.col("current_time").cast("long") - F.col("isotime").cast("long"))) \
    .withWatermark("isotime", "20 seconds") \
    .groupBy(F.window("isotime", "1 minute")) \
    .agg(F.max("latency").alias("max_latency"))


timezone_delta = timedelta(hours=9)
utc_plus_9 = timezone(timezone_delta)
current_datetime = datetime.now().astimezone(utc_plus_9).strftime("%Y-%m-%d")

upbit_output_path = f"s3a://spark-s3-streaming/upbit/{current_datetime}"
bybit_output_path = f"s3a://spark-s3-streaming/bybit/{current_datetime}"

upbit_latency_path = f"s3a://kafka-monitoring/latency/upbit/{current_datetime}"
bybit_latency_path = f"s3a://kafka-monitoring/latency/bybit/{current_datetime}"


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