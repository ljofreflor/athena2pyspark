'''
Created on 24-10-2017

@author: lnjofre
'''


import boto3

from  athena2pyspark.config import aws_access_key_id, aws_secret_access_key

import pyspark
from pyspark.sql import dataframe
from pyspark.sql.utils import AnalysisException
import time

def get_dataframe(path_query, spark):
    while True:
        try:
            return spark.read.format("com.databricks.spark.csv") \
                .option("header", "true") \
                .option("inferschema", "true") \
                .csv(path_query.replace("s3://", "s3n://"))
        except AnalysisException:
            time.sleep(1)
            pass
        
def run_query(query, database, s3_output, spark):

    client = boto3.client('athena', region_name='us-east-1', 
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
    
    response = client.start_query_execution(
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
    df = get_dataframe( s3_output + response['QueryExecutionId'] + '.csv', spark)
    
    return s3_output + response['QueryExecutionId'] + '.csv'

def get_ddl(df, database, table, s3_input):
    columns = df.columns
    # lo pasamos a pandas pero no traemos nada, simplemente generamos este array vacio
    # para obtener los tipos de datos de las columnas.
    void_df = df.limit(1).toPandas()
    
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
    