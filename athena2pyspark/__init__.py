'''
Created on 24-10-2017

@author: lnjofre
'''

import boto3
import re
from  athena2pyspark.config import aws_access_key_id, aws_secret_access_key

import pyspark
from pyspark.sql import dataframe
from pyspark.sql.utils import AnalysisException
import time
from py4j.protocol import Py4JJavaError

def get_dataframe(path_query, spark):
    
    u"""por alguna razon desconocida glue no acepta el protocolo s3n, por otra razon las aplicaciones
    locales no aceptan el protocolo s3 agregando un ; al final de la url, por lo que manejamos la excepcion
    para ambos casos."""
    
    while True:  
        try:
            return spark.read.format("com.databricks.spark.csv") \
                .options(header=True, inferschema=True) \
                .load(path_query) # version s3
        except AnalysisException:
            try :
                return spark.read.format("com.databricks.spark.csv") \
                    .options(header=True, inferschema=True) \
                    .csv(str(path_query).replace("s3://", "s3n://")) # version s3n
            except AnalysisException:
                pass
            
            except Py4JJavaError:
                time.sleep(1)


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
    
    bucket_pttrn = re.compile(r"s3://[^/]+/") # patron para extraer el nombre del bucket

    Bucket = bucket_pttrn.findall(metadata_file_path)[0].replace("s3://","").replace("/","")
    Key = metadata_file_path.replace(bucket_pttrn.findall(metadata_file_path)[0],"")
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
        status = athena.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
        time.sleep(5)
    
    file_path = s3_output + response['QueryExecutionId'] + '.csv'
    metadata_file_path = file_path + '.metadata'
    
    bucket_pttrn = re.compile(r"s3://[^/]+/") # patron para extraer el nombre del bucket

    Bucket = bucket_pttrn.findall(metadata_file_path)[0].replace("s3://","").replace("/","")
    Key = metadata_file_path.replace(bucket_pttrn.findall(metadata_file_path)[0],"")
    s3.delete_object(Bucket=Bucket, Key=Key)

    return file_path

def get_ddl(df, database, table, s3_input):
    columns = df.columns
    # lo pasamos a pandas pero no traemos nada, simplemente generamos este array vacio
    # para obtener los tipos de datos de las columnas.
    
    fields = ",\n".join(map(lambda x:  x  + " string", columns))
    
    create_database = "CREATE DATABASE IF NOT EXISTS %s;" % (database)
    create_table = \
        """CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (%s)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     WITH SERDEPROPERTIES (
     'serialization.format' = '1',
     'field.delim' = ','
     ) LOCATION '%s'
     TBLPROPERTIES ('has_encrypted_data'='false');""" % ( database, table, fields, s3_input )
    
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
    