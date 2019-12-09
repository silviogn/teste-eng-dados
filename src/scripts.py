import json

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def data_format_conversion(spark: SparkSession, input_file: str, output_file: str) -> None:
    """
    Converte os dados que estão no formato CSV para Parquet.
    :param spark:
    :param input_file:
    :param output_file:
    :return:
    """
    df_users_csv = spark.read.csv(input_file, header=True)
    write_parquet_data_frame(df_users_csv, output_file)


def data_deduplication(spark: SparkSession, input_file: str) -> DataFrame:
    """
    Método de deduplicação de dados.
    :param spark:
    :param input_file:
    :return:
    """
    df_users = spark.read.parquet(input_file)
    df_users.createOrReplaceTempView("users")
    return spark.sql("SELECT users.id, users.name, users.email, users.phone, users.address, users.age, "
                     "users.create_date, users.update_date FROM users INNER JOIN (SELECT id, MAX(update_date) "
                     "AS update_date "
                     "FROM users GROUP BY id) AS U ON U.id = users.id AND U.update_date = users.update_date")


def data_fields_conversion(spark: SparkSession, df_users: DataFrame, configuration_file: str) -> DataFrame:
    """
    Faz leitura do arquivo de configuração e em base as configurações especificadas
    converte os campos.
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
    Persiste os dados convertidos.
    :param df_users:
    :param output_file:
    :return:
    """
    write_parquet_data_frame(df_users, output_file)


def write_parquet_data_frame(data_frame: DataFrame, output_file: str) -> None:
    """
    Escreve os dados de um data frame para um arquivo no formato Parquet.
    :param data_frame:
    :param output_file:
    :return:
    """
    data_frame.write.parquet(output_file, mode="overwrite")