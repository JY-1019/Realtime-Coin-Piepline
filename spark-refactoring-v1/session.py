import os
import json
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


class SparkStreamingSession:
    def __init__(self,):
        self._session = None
        
    def init_session(self, app_name : str):
        self._app_name = app_name
        current_path = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(current_path, "config.json"), "r") as config_file:
            config = json.load(config_file)
            aws_access_key_id = config["aws_access_key_id"]
            aws_secret_access_key = config["aws_secret_access_key"]

        conf = (
            SparkConf()
            .setAppName(self._app_name) 
            .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
            .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
            .set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
            .set("spark.sql.shuffle.partitions", "3")
            .set("spark.driver.port", "8080")
            .set("spark.driver.bindAddress", "127.0.0.1")
            .setMaster("local[*]") 
        )

        self._session = SparkSession.builder.config(conf=conf).getOrCreate()

    def get_session(self,):
        if self._session is None:
            raise Exception("must be called 'init_app'")
        spark_session = None
        try:
            return self._session
        finally:
            spark_session.close()

    @property
    def session(self):
        return self.get_session()
    
spark = SparkStreamingSession()
