import time
import boto3
import logging

from consumers.base_consumer import BaseConsumer

class SQSConsumer(BaseConsumer):
    def read_stream(self):
        sqs = boto3.client('sqs', region_name=self.config['region'])
        while True:  # This keeps polling the SQS queue continuously
            try:
                # Receiving messages with long polling enabled
                response = sqs.receive_message(
                    QueueUrl=self.config['queue_url'],
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,  # Enable long polling
                    AttributeNames=['All'],
                    MessageAttributeNames=['All']
                )

                # Extract messages from the response
                messages = response.get('Messages', [])
                if not messages:
                    logging.info("No messages received from SQS queue.")
                    time.sleep(5)  # Optional sleep before trying again
                    continue

                return messages  # Return the messages for further processing

            except Exception as e:
                logging.error(f"Error receiving messages from SQS: {e}")
                time.sleep(5)  # Optional sleep before retrying in case of error
