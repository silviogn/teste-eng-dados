import findspark

path_to_spark = "/Tools/spark-2.2.0-bin-hadoop2.7"
findspark.init(path_to_spark)

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from src.scripts import data_format_conversion, data_fields_conversion, data_deduplication, write_converted_data

# Path to the raw data
INPUT_USERS_DATA_FILE = "../data/input/users/load.csv"

# Path to the converted data
OUTPUT_USERS_CONVERTED_DATA_FILE = "../data/output/users/converted/users.parquet"

# Path to the final output data
OUTPUT_USERS_PROCESSED_DATA_FILE = "../data/output/users/processed/users_deduplicated.parquet"

# Path to the configuration file
CONFIG_TYPES_MAPPING = "../config/types_mapping.json"

if __name__ == '__main__':

    # Initialises the spark environment
    conf = SparkConf().setMaster("local[*]")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("cognitivo.ai_test").getOrCreate()

    # Executes the JOB
    # Load and convert the data from csv to parquet
    data_format_conversion(spark, INPUT_USERS_DATA_FILE, OUTPUT_USERS_CONVERTED_DATA_FILE)

    # Performs the data deduplication task
    df_users_deduplicated = data_deduplication(spark, OUTPUT_USERS_CONVERTED_DATA_FILE)

    # Performs the conversion of the fields
    df_users_converted = data_fields_conversion(spark, df_users_deduplicated, CONFIG_TYPES_MAPPING)

    # Persists the data in the output directory
    write_converted_data(df_users_converted, OUTPUT_USERS_PROCESSED_DATA_FILE)