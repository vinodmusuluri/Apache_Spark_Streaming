import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_json, lit
from pyspark.sql import DataFrame

class Transformer:
    def __init__(self):
        self.spark = SparkSession.builder.appName("DataTransformer").getOrCreate()
        self.logger = logging.getLogger("Transformer")

   

    def transform_messages(self, messages, message_type):
        try:
            # Convert the input messages to DataFrame and cast key/value to String
            df = messages.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                        .withColumnRenamed("key", "messageId") \
                        .withColumnRenamed("value", "body")

            # Perform the transformation
            transformed_df = df.select(
                col("message_key").alias("SEQUENCENUMBER"),
                current_timestamp().alias("CreatedDateTime"),
                current_timestamp().alias("UpdatedDateTime"),
                col("message_value").alias("Json_Payload")
            )

            return transformed_df
        except Exception as e:
            self.logger.error(f"Transformation error for {message_type}: {e}")
            return None

    def transform_sqs(self, messages):
        try:
            # Convert SQS messages to an RDD (no need to dump them to JSON strings here)
            rdd = self.spark.sparkContext.parallelize([json.dumps(msg) for msg in messages])
            df = self.spark.read.json(rdd)

           
            # Apply transformations to the DataFrame using the common logic
            transformed_df = df.select(
                lit("body.type").alias("Event"),
                lit("body.object.uri").alias("AccountId"),
                col("MessageId").alias("SEQUENCENUMBER"),
                current_timestamp().alias("CreatedDateTime"),
                current_timestamp().alias("UpdatedDateTime"),
                col("body").alias("Json_Payload")
            )

            #transformed_df.show(truncate=False)
            return transformed_df

        except Exception as e:
            self.logger.error(f"Transformation error for SQS: {e}")
            return None

    def transform_kafka(self, messages):
        return self.transform_messages(messages, "Kafka")

 