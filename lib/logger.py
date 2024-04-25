from pyspark.sql import SparkSession


class Log4J:
    def __init__(self, spark: SparkSession):
        log4j = spark._jvm.org.apache.log4j
        root_logger_name = "hackaboss.spark.examples"
        self.logger = log4j.LogManager.getLogger(root_logger_name)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)
