import logging.config

# passing the path to logging.config where logging.configuration file is defined
logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger("Ingest")

def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning("Load file method started....")

        if file_format == "parquet":
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == "csv":
            df = spark.read.format(file_format).option(header=header).option(inferSchema=inferSchema).load(file_dir)

    except Exception as e:
        logger.error("An error occured at load_files===", str(e))
        raise
    else:
        logger.warning("Loading file completed")

    return df

def display_df(df, df_show):
    df_show = df.show()
    return df_show

def count_df(df, dfName):
    try:
        logger.warning(f"Here is the count of records in the df {dfName}")

        df_count = df.count()

    except Exception as e:
        raise

    else:
        logger.warning(f"Number of the records present in the {df} are :: {df_count}")

    return df_count