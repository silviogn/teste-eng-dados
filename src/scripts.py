import json

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def data_format_conversion(spark: SparkSession, input_file: str, output_file: str) -> None:
    """
    Convert the raw data in CSV format to Parquet data format.
    :param spark:
    :param input_file:
    :param output_file:
    :return:
    """
    df_users_csv = spark.read.csv(input_file, header=True)
    write_parquet_data_frame(df_users_csv, output_file)


def data_deduplication(spark: SparkSession, input_file: str) -> DataFrame:
    """
    Load the data from parquet data file and returns the deduplicated records.
    :param spark:
    :param input_file:
    :return:
    """
    df_users = spark.read.parquet(input_file)
    df_users.createOrReplaceTempView("users")
    return spark.sql("SELECT users.* FROM users INNER JOIN (SELECT id, MAX(update_date) AS update_date "
                     "FROM users GROUP BY id) AS U ON U.id = users.id AND U.update_date = users.update_date")


def data_fields_conversion(spark: SparkSession, df_users: DataFrame, configuration_file: str) -> DataFrame:
    """
    Load the configuration file data. After, cast each field according to the configuration.
    :param spark:
    :param df_users:
    :param configuration_file:
    :return:
    """
    with open(configuration_file, 'r') as json_config_file:
        fields_conversion = json.load(json_config_file)

    for field, data_type in fields_conversion.items():
        df_users = df_users.withColumn(field, df_users[field].cast(data_type))
    return df_users


def write_converted_data(df_users: DataFrame, output_file: str) -> None:
    """
    Save the converted data.
    :param df_users:
    :param output_file:
    :return:
    """
    write_parquet_data_frame(df_users, output_file)


def write_parquet_data_frame(data_frame: DataFrame, output_file: str) -> None:
    """
    Writes the data of a data frame to a Parquet file.
    :param data_frame:
    :param output_file:
    :return:
    """
    data_frame.write.parquet(output_file, mode="overwrite")