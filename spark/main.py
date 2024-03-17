import subprocess
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import from_json, col

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("StockStreaming")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("symbol", StringType(), nullable=True),
            StructField("stck_prpr", DoubleType(), nullable=True),
            StructField("prdy_vrss", DoubleType(), nullable=True),
            StructField("stck_oprc", DoubleType(), nullable=True),
            StructField("stck_hgpr", DoubleType(), nullable=True),
            StructField("stck_lwpr", DoubleType(), nullable=True),
            StructField("per", DoubleType(), nullable=True),
            StructField("vol_tnrt", DoubleType(), nullable=True),
            StructField("d250_lwpr_vrss_prpr_rate", DoubleType(), nullable=True),
        ]
    )

    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092")
        .option("subscribe", "kStock")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("enconding", "UTF-8")
        .load()
    )

    value_df = events.select(
        from_json(col("value").cast("string"), schema).alias("value")
    )

    value_df = value_df.select("value.*")

    filtered_df = value_df.filter(
        (col("vol_tnrt") >= 1) & (col("d250_lwpr_vrss_prpr_rate") <= 50)
    )

    current_datetime = datetime.now().strftime("%Y-%m-%d")
    output_data_path = f"hdfs:///stock/data/{current_datetime}"
    output_worth_path = f"hdfs:///stock/worth/{current_datetime}_worth"
    subprocess.run(["hadoop", "fs", "-mkdir", "-p", output_data_path])
    subprocess.run(["hadoop", "fs", "-mkdir", "-p", output_worth_path])

    query_data = (
        value_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", output_data_path)
        .option("checkpointLocation", "checkpoint")
        .start()
    )

    query_worth = (
        filtered_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", output_worth_path)
        .option("checkpointLocation", "checkpoint")
        .start()
    )

    spark.streams.awaitTermination()
