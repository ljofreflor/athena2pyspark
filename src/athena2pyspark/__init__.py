'''
Created on 24-10-2017

@author: lnjofre
'''
# TODO: implementar coverade de la libreria
# TODO: implementar ambientes de la libreria

from exceptions import UnboundLocalError
import os.path
import re
import time

import boto3
from py4j.protocol import Py4JJavaError
import pyspark
from pyspark.sql import dataframe
from pyspark.sql.utils import AnalysisException

from athena2pyspark.athena_sql import queryByName
from athena2pyspark.config import aws_access_key_id, aws_secret_access_key,\
    get_spark_session
from athena2pyspark.config import paths, result_folder_temp, partition_by


class cell(object):
    def __init__(self):
        pass


def get_query_from_app(cell_list, spark, param):
    # leer la matriz de configuracion
    database = "prod_{flag}".format(**param)
    query = "select distinct "
    matriz_de_configuracion = queryByName(
        "sql/matriz_de_configuracion", args=param)

    matriz_de_configuracion_df = run_query(query=matriz_de_configuracion,
                                           database=database, s3_output=result_folder_temp, spark=spark)
    pass


def get_dataframe(path_query, spark):
    u"""por alguna razon desconocida glue no acepta el protocolo s3n, por otra razon las aplicaciones
    locales no aceptan el protocolo s3 agregando un ; al final de la url, por lo que manejamos la excepcion
    para ambos casos."""

    try:
        return spark.read.format("com.databricks.spark.csv") \
            .options(header=True, inferschema=True) \
            .csv(path_query)  # version s3
    except:
        return spark.read.format("com.databricks.spark.csv") \
            .options(header=True, inferschema=True) \
            .csv(str(path_query).replace("s3://", "s3n://"))  # version s3n


def run_create_table(query, database, s3_output):
    athena = boto3.client('athena', region_name='us-east-1',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

    s3 = boto3.client('s3', region_name='us-east-1',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        }
    )
    print('Execution ID: ' + response['QueryExecutionId'])
    # return s3_output + response['QueryExecutionId'] + '.csv'

    file_path = s3_output + response['QueryExecutionId'] + '.csv'
    metadata_file_path = file_path + '.metadata'

    # patron para extraer el nombre del bucket
    bucket_pttrn = re.compile(r"s3://[^/]+/")

    Bucket = bucket_pttrn.findall(metadata_file_path)[
        0].replace("s3://", "").replace("/", "")
    Key = metadata_file_path.replace(
        bucket_pttrn.findall(metadata_file_path)[0], "")
    s3.delete_object(Bucket=Bucket, Key=Key)

    return file_path


def run_query(query, database, s3_output, spark):

    athena = boto3.client('athena', region_name='us-east-1',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

    s3 = boto3.client('s3', region_name='us-east-1',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        }
    )
    print('Execution ID: ' + response['QueryExecutionId'])
    query_id = response['QueryExecutionId']
    status = 'RUNNING'
    while status != 'SUCCEEDED':
        status = athena.get_query_execution(QueryExecutionId=query_id)[
            'QueryExecution']['Status']['State']
        assert(status != 'FAILED')
        assert(status != 'CANCELLED')
        time.sleep(5)

    file_path = s3_output + response['QueryExecutionId'] + '.csv'
    metadata_file_path = file_path + '.metadata'

    # patron para extraer el nombre del bucket
    bucket_pttrn = re.compile(r"s3://[^/]+/")

    Bucket = bucket_pttrn.findall(metadata_file_path)[
        0].replace("s3://", "").replace("/", "")
    Key = metadata_file_path.replace(
        bucket_pttrn.findall(metadata_file_path)[0], "")
    s3.delete_object(Bucket=Bucket, Key=Key)

    return file_path


def get_ddl(df, database, table, s3_input):
    columns = df.columns
    # lo pasamos a pandas pero no traemos nada, simplemente generamos este array vacio
    # para obtener los tipos de datos de las columnas.

    fields = ",\n".join(map(lambda x:  x + " string", columns))

    create_database = "CREATE DATABASE IF NOT EXISTS %s;" % (database)
    create_table = \
        """CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (%s)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     WITH SERDEPROPERTIES (
     'serialization.format' = '1',
     'field.delim' = ','
     ) LOCATION '%s'
     TBLPROPERTIES ('has_encrypted_data'='false');""" % (database, table, fields, s3_input)

    return create_database, create_table


def get_create_table(query):
    client = boto3.client('athena', region_name='us-east-1',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

    response = client.start_query_execution(
        QueryString=query
    )
    print('Execution ID: ' + response['QueryExecutionId'])
    # return s3_output + response['QueryExecutionId'] + '.csv'


def repair_table(database, table, spark):
    athena = boto3.client('athena', region_name='us-east-1',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

    query = "MSCK REPAIR TABLE " + table

    response = run_query(query=query, database=database,
                         s3_output=result_folder_temp, spark=spark)  # correr la query


class Job(object):
    def __init__(self, sql_querys_path):
        # esta libreria requiere de una ruta en donde se encuentren las
        # consultas sql y asi poder ser reutilizable para cualquier set de
        # querys
        self.sql_querys_path = sql_querys_path

    def run(self, branch, flag, queryName, spark, sql_path="", partition_by_id_com=False, param={}):

        database = branch + "_" + flag
        # asociar la bandera a la ruta de resultados
        path_result = paths[queryName].format(**{'flag': flag})

        query = queryByName(
            queryName, sql_path=self.sql_querys_path).format(**param)

        path_query = run_query(query=query, database=database,
                               s3_output=result_folder_temp, spark=spark)  # correr la query

        df = get_dataframe(path_query=path_query, spark=spark)

        # guardar el parquet en la ruta dada por la configuracion
        try:
            assert(partition_by_id_com)
            df.write.mode("overwrite").partitionBy(
                partition_by[queryName]).parquet(path_result + "id_com=" + str(param['id_com']))
        except AssertionError:
            df.write.mode("overwrite").partitionBy(
                partition_by[queryName]).parquet(path_result)

        # jorge: repara las particiones de la tabla
        repair_table(database=database, table=queryName, spark=spark)

        return path_query
