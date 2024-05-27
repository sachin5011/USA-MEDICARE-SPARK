import os

import get_environment_varriables as gav
from create_spark_session import create_session
from validate import get_current_date
import logging
import logging.config
import sys
from ingest import load_files, display_df, count_df

# passing the path to logging.config where logging.configuration file is defined
logging.config.fileConfig('Properties/configuration/logging.config')

def main():
    try:
        logging.info("Main method started...")
        envn = gav.envn
        appname = gav.appname
        logging.info("Calling the spark object")
        spark = create_session(appname, envn)

        logging.info("Validating spark object")
        get_current_date(spark)

        file_format = ""
        olap_path = gav.source_olap
        for file in os.listdir(olap_path):
            print(f"Current file is {file}")
            file_dir = os.path.join(olap_path, file)
            # print(file_dir)
            if file.endswith(".parquet"):
                file_format = "parquet"
                header = "NA"
                inferSchema = 'NA'

            elif file.endswith(".csv"):
                file_format = "csv"
                header = gav.header
                inferSchema = gav.inferSchema

            logging.info(f"Reading file which is of {file_format}")

            df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)
            logging.info(f"Displaying the dataframe {df_city}")
            display_df(df_city, 'df_city')
            logging.info("Validating the dataframe....")
            count_df(df_city, 'df_city')
    except Exception as e:
        logging.error("An error occured while calling main() please chaeck the trace===", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
    logging.info("Application completed successfully..")