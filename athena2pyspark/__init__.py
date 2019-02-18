'''
Created on 24-10-2017

@author: lnjofre
'''
# TODO: implementar coverade de la libreria
# TODO: implementar ambientes de la libreria
# TODO: probar en python3.6


import os.path
import re
import time
try:
    from urlparse import urlparse
except ModuleNotFoundError:
    import urllib.parse as urlparse

import zipfile

from athena2pyspark.config import get_spark_session
import boto3

from .custom_utils import deprecated


class athena2pyspark(object):

    def __init__(self):
        pass

    def set_region_name(self, region_name):
        self.region_name = region_name

    def set_profile(self, profile):
        import ConfigParser
        Config = ConfigParser.ConfigParser()
        Config.read(os.environ['HOME'] + "/.aws/credentials")
        self.aws_access_key_id = Config.get(profile, "aws_access_key_id")
        self.aws_secret_access_key = Config.get(
            profile, "aws_secret_access_key")

    def set_spark_session(self, spark):
        self.spark = spark

        try:
            aws_access_key_id = self.spark.conf.get("fs.s3n.awsAccessKeyId")
            aws_secret_access_key = self.spark.conf.get(
                "fs.s3n.awsSecretAccessKey")

            self.aws_access_key_id = aws_access_key_id
            self.aws_secret_access_key = aws_secret_access_key
        except:
            # en Rol no existen las credenciales asi que pasamos de largo
            pass

    @deprecated(reason="las credenciales deben pasarse por medio del sparkSession")
    def set_credentials(self, aws_secret_access_key, aws_access_key_id):
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_access_key_id = aws_access_key_id

    def queryByName(self, query_file_name, sql_path, args=None):
        '''
        este es un ejemplo
        :param query_file_name:
        :param args: diccionario con los parametros de la query
        '''

        try:
            # version local de la libreria
            mop_base = os.path.join(sql_path, "sql", query_file_name + ".sql")
            sql_file = open(mop_base, "r").read()
        except IOError:
            # version zip de la libreria
            try:
                zf = zipfile.ZipFile(sql_path)
                sql_file = zf.open(os.path.join(
                    sql_path, query_file_name + ".sql")).read()
            except IOError:
                # version egg de la libreria
                basename = os.path.basename(sql_path)
                sql_path = re.sub(".egg/.*", ".egg", sql_path)
                zf = zipfile.ZipFile(sql_path)
                sql_file = zf.open(os.path.join(basename,
                                                'sql',
                                                query_file_name + ".sql")).read()

        if args is not None:
            sql_file = sql_file.format(**args)

        return sql_file

    def get_dataframe(self, path_query):
        u"""por alguna razon desconocida glue no acepta el protocolo s3n, por otra razon las aplicaciones
        locales no aceptan el protocolo s3 agregando un ; al final de la url, por lo que manejamos la excepcion
        para ambos casos."""

        reader = self.spark.read.format("com.databricks.spark.csv") \
            .options(header=True, inferschema=True)

        try:
            return reader.csv(path_query)  # version s3
        except:
            # version s3n
            return reader.csv(str(path_query).replace("s3://", "s3n://"))

    def run_create_table(self, query, database, s3_output):
        athena = boto3.client('athena',
                              region_name=self.region_name,
                              aws_access_key_id=self.aws_access_key_id,
                              aws_secret_access_key=self.aws_secret_access_key)

        s3 = boto3.client('s3', region_name=self.region_name,
                          aws_access_key_id=self.aws_access_key_id,
                          aws_secret_access_key=self.aws_secret_access_key)

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

    def run_query(self, query, database, s3_output):

        try:
            athena = boto3.client('athena', region_name=self.region_name,
                                aws_access_key_id=self.aws_access_key_id,
                                aws_secret_access_key=self.aws_secret_access_key)

            s3 = boto3.client('s3', region_name=self.region_name,
                            aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key)
        except AttributeError:
            # si no existen credenciales se esta ejecutando por Rol de AWS
            athena = boto3.client('athena', region_name=self.region_name)

            s3 = boto3.client('s3', region_name=self.region_name)


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

    def get_json(self, file_path):
        s3 = boto3.resource('s3', region_name=self.region_name,
                            aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key)

        import unidecode
        import io
        import csv
        key_url = urlparse(file_path)

        bucket = key_url.netloc
        key = key_url.path[1:]

        obj = s3.Object(bucket, key)
        result = unicode(unidecode.unidecode(
            obj.get()['Body'].read().decode('utf-8')))

        result_dict = list(csv.DictReader(
            io.StringIO(result)))

        """
        result_dict = list(csv.DictReader(
            io.StringIO(result.replace('"', ''))))"""

        return result_dict

    @deprecated("no es necesario crear tablas al vuelo, puede usar las vistas de athena")
    def get_ddl(self, df, database, table, s3_input):
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

    def get_create_table(self, query):
        client = boto3.client('athena', region_name=self.region_name,
                              aws_access_key_id=self.aws_access_key_id,
                              aws_secret_access_key=self.aws_secret_access_key)

        response = client.start_query_execution(
            QueryString=query
        )
        print('Execution ID: ' + response['QueryExecutionId'])
        # return s3_output + response['QueryExecutionId'] + '.csv'

    def repair_table(self, database, table):
        query = "MSCK REPAIR TABLE " + table
        self.run_query(query=query,
                       database=database,
                       s3_output=self.result_folder)  # correr la query

    def set_result_folder(self, result_folder):
        """setear la carpeta temporal en donde se almacenan los resultados"""
        self.result_folder = result_folder


class Job(object):

    def __init__(self, spark):
        self.spark = spark
        # objeto de comunicacion entre athena & spark local
        self.ath = athena2pyspark(self.spark)

    def set_sql_querys_path(self, sql_querys_path):
        """si se desea cambiar la ruta de las querys on the fly"""
        self.sql_querys_path = sql_querys_path

    def set_result_folder(self, result_folder):
        self.result_folder = result_folder
        self.ath.set_result_folder(result_folder=self.result_folder)

    def set_s3_tables_path(self, s3_tables_path):
        self.s3_tables_path = s3_tables_path

    def set_field_partitions(self, field_partition):
        self.field_partition = field_partition

    def run(self, database, query_name,  partition_by=None, param={}):

        from py4j.protocol import Py4JJavaError

        param[partition_by]

        # asociar la bandera a la ruta de resultados
        path_result = self.s3_tables_path[query_name].format(**param)

        query = self.ath.queryByName(query_file_name=query_name,
                                     sql_path=self.sql_querys_path).format(**param)

        # correr la query
        path_query = self.ath.run_query(query=query,
                                        database=database,
                                        s3_output=self.result_folder)

        df = self.ath.get_dataframe(path_query=path_query)

        # guardar el parquet en la ruta dada por la configuracion

        writer = df.write.mode("overwrite").partitionBy(
            self.field_partition[query_name])

        writer.parquet(path_result.replace("s3", "s3n") +
                       partition_by + "=" + param[partition_by])
        try:
            assert(partition_by)
            writer.parquet(path_result +
                           partition_by + "=" + param[partition_by])
        except Py4JJavaError:
            writer.parquet(path_result.replace("s3", "s3n") +
                           partition_by + "=" + param[partition_by])
        except AssertionError:
            writer.parquet(path_result)

        # reparar la tabla despues de actualizar el archivo en s3
        self.ath.repair_table(database=database,
                              table=query_name)
