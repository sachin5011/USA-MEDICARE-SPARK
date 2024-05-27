import logging.config

# passing the path to logging.config where logging.configuration file is defined
logging.config.fileConfig('Properties/configuration/logging.config')

loggers = logging.getLogger("Validate")
def get_current_date(spark):
    try:
        loggers.warning("Started get_current_date method...")
        output = spark.sql("""SELECT current_date""")
        loggers.warning("Validating the current date : "+ str(output.collect()))
    except Exception as e:
        loggers.error("An error occurred in get_current_date method : ",str(e))
        raise

    else:
        loggers.warning("Validation Successful.. Go Forward")