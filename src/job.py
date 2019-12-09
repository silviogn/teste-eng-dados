import findspark

path_to_spark = "/Tools/spark-2.2.0-bin-hadoop2.7"
findspark.init(path_to_spark)

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from src.scripts import data_format_conversion, data_fields_conversion, data_deduplication, write_converted_data

# Caminho para os dados originais.
INPUT_USERS_DATA_FILE = "../data/input/users/load.csv"

# Caminho para os dados convertidos.
OUTPUT_USERS_CONVERTED_DATA_FILE = "../data/output/users/converted/users.parquet"

# Caminho para os dados deduplicados e convertidos.
OUTPUT_USERS_PROCESSED_DATA_FILE = "../data/output/users/processed/users_deduplicated.parquet"

# Caminho para o arquivo de configuração.
CONFIG_TYPES_MAPPING = "../config/types_mapping.json"

if __name__ == '__main__':

    # Inicializa o Ambiente do Spark
    conf = SparkConf().setMaster("local[*]")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("cognitivo.ai_test").getOrCreate()

    # Inicio da execução do Job
    # Carrega e converte os dados no formato CSV para o formato parquet.
    data_format_conversion(spark, INPUT_USERS_DATA_FILE, OUTPUT_USERS_CONVERTED_DATA_FILE)

    # Execução da tarefa de dedupilcação de dados.
    df_users_deduplicated = data_deduplication(spark, OUTPUT_USERS_CONVERTED_DATA_FILE)

    # Conversão dos tipos dos campos especificados.
    df_users_converted = data_fields_conversion(spark, df_users_deduplicated, CONFIG_TYPES_MAPPING)

    # Persiste os dados processados.
    write_converted_data(df_users_converted, OUTPUT_USERS_PROCESSED_DATA_FILE)

    

