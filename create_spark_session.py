from pyspark.sql import SparkSession
import logging.config

# passing the path to logging.config where logging.configuration file is defined
logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('create_spark_session')
def create_session(appname, envn):
    try:
        logger.info("get_spark_session method started...")
        if envn == "DEV":
            master = "local"
        else:
            master = "Yarn"

        logger.info(f"Master is {master}")
        spark = SparkSession.builder.master(master).appName(appname).getOrCreate()
    except Exception as e:
        logger.error("An error occured in the get_spark_session object ====",str(e))
        raise
    else:
        logger.info("Spark Session Created Successfully")

    return spark
