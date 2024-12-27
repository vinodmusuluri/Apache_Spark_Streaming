import logging
import boto3
import json
import sys
import os
import zipfile

# Configure logging
logger = logging.getLogger("ApacheSparkStreaming")
logging.basicConfig(level=logging.INFO)

# Step 1: Dynamically find the path to the .zip file
def find_zip_in_tmp():
    for dirpath, dirnames, filenames in os.walk('/tmp'):
        for filename in filenames:
            if filename == 'Apache_Spark_Streaming.zip':
                return os.path.join(dirpath, filename)
    return None

# Find the zip file
zip_file_path = find_zip_in_tmp()

if not zip_file_path:
    raise FileNotFoundError("Apache_Spark_Streaming.zip not found in the /tmp directory")

# Step 2: Unzip the file to a known location
extract_dir = '/tmp/Apache_Spark_Streaming'  # Directory to extract the files
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

# Step 3: Verify the files inside the extracted directory
unzipped_folder = os.path.join(extract_dir, 'Apache_Spark_Streaming')  # The nested folder
if not os.path.exists(unzipped_folder):
    raise FileNotFoundError(f"The expected folder 'Apache_Spark_Streaming' is not found in {extract_dir}")

# Step 4: Add the extracted directory to sys.path
sys.path.append(unzipped_folder)

# Step 5: Verify by printing the files in the unzipped folder
print("Files in the unzipped folder:", os.listdir(unzipped_folder))

from pyspark.sql import SparkSession
from consumers.kafka_consumer import KafkaConsumer
from consumers.sqs_consumer import SQSConsumer
from transformers.transformer import Transformer
from writer.snowflake_writer import SnowflakeWriter


def load_global_properties():
    s3 = boto3.client('s3')
    bucket_name = 'aws-glue-reltio-bucket'
    key = 'snowflake-jars/Apache_Spark_Streaming/config/global_properties.py'

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        global_properties_code = obj['Body'].read().decode('utf-8')
        local_vars = {}
        exec(global_properties_code, {}, local_vars)
        return local_vars['GLOBAL_PROPERTIES']
    except Exception as e:
        logger.error(f"Error loading GLOBAL_PROPERTIES: {e}")
        raise

def load_config():
    s3 = boto3.client('s3')
    bucket_name = 'aws-glue-reltio-bucket'
    key = 'snowflake-jars/Apache_Spark_Streaming/config/queue_config.json'

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise

def initialize_spark(global_properties):
    spark = SparkSession.builder \
        .appName(global_properties['spark']['app_name']) \
        .getOrCreate()
    spark.sparkContext.setLogLevel(global_properties['spark']['log_level'])
    return spark

def main():
    try:
        logger.info("Starting the application...")
        GLOBAL_PROPERTIES = load_global_properties()
        config = load_config()
        spark = initialize_spark(GLOBAL_PROPERTIES)
        transformer = Transformer()
        snowflake_writer = SnowflakeWriter(GLOBAL_PROPERTIES['snowflake'])  # Initialize Snowflake writer once

        for queue in config['queues']:
            if queue['type'] == 'kafka':
                # Read and transform Kafka data
                consumer = KafkaConsumer(queue, spark)
                raw_data = consumer.read_stream()
                logger.info("Raw Kafka data fetched.")
                
                transformed_data = transformer.transform_messages(raw_data)
                logger.info("Kafka data transformed.")
                
                # Write Kafka-transformed data to Snowflake
                snowflake_writer.write(transformed_data)
                logger.info("Kafka data written to Snowflake.")
                
            elif queue['type'] == 'sqs':
                # Read and transform SQS data
                consumer = SQSConsumer(queue)
                raw_data = consumer.read_stream()
                logger.info("Raw SQS data fetched.")
                
                transformed_data = transformer.transform_sqs(raw_data)
                logger.info("SQS data transformed.")
                
                # Write SQS-transformed data to Snowflake
                snowflake_writer.write(transformed_data)
                logger.info("SQS data written to Snowflake.")

                # Delete processed messages from SQS
                sqs_client = boto3.client('sqs', region_name=queue['region'])
                for message in raw_data:
                    try:
                        receipt_handle = message['ReceiptHandle']
                        sqs_client.delete_message(
                            QueueUrl=queue['queue_url'],
                            ReceiptHandle=receipt_handle
                        )
                        logger.info(f"Message {message['MessageId']} deleted successfully.")
                    except Exception as e:
                        logger.error(f"Failed to delete message {message['MessageId']}: {e}")
            else:
                raise ValueError(f"Unsupported queue type: {queue['type']}")

        logger.info("Application completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
