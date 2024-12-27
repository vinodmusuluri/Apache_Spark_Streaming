from consumers.base_consumer import BaseConsumer
from pyspark.sql import SparkSession

class KafkaConsumer(BaseConsumer):
    def __init__(self, config, spark):
        super().__init__(config)
        self.spark = spark

    def read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['brokers']) \
            .option("subscribe", self.config['topic']) \
            .option("startingOffsets", "earliest") \
            .load()
