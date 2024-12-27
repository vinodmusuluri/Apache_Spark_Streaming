import logging

from writer.base_writer import BaseWriter

class SnowflakeWriter(BaseWriter):
    def __init__(self, snowflake_config):
        self.config = snowflake_config
        self.logger = logging.getLogger("SnowflakeWriter")

    def write(self, df):
        try:
            df.write \
                .format("snowflake") \
                .options(**self.config)\
                .option("dbtable", "RELTIO_ENTITY_EVENTS") \
                .mode("append") \
                .save()
            self.logger.info("Data successfully written to Snowflake.")
        except Exception as e:
            self.logger.error(f"Error writing to Snowflake: {e}")

