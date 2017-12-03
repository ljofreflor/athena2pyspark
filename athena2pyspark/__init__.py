'''
Created on 24-10-2017

@author: lnjofre
'''

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 pyspark-shell'

from  athena2pyspark.config import aws_access_key_id, aws_secret_access_key

from pyspark.sql import dataframe
import boto3
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.utils import AnalysisException
import time

sc = SparkContext.getOrCreate()
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_access_key_id)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)

sqlContext = SQLContext(sc)

def get_dataframe(path_query):
    while True:
        try:
            return sqlContext.read.format("com.databricks.spark.csv") \
                .option("header", "true") \
                .option("inferschema", "true") \
                .csv(path_query.replace("s3://", "s3n://"))
        except AnalysisException:
            time.sleep(1)
            pass
        
def run_query(query, database, s3_output):

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
    df = get_dataframe( s3_output + response['QueryExecutionId'] + '.csv')
    
    return s3_output + response['QueryExecutionId'] + '.csv'

def get_ddl(df, database, table, s3_input):
    columns = df.columns
    fields = ",".join(map(lambda x: "'" + x + "'" + " string", columns))
    
    create_database = "CREATE DATABASE IF NOT EXISTS %s;" % (database)
    create_table = \
        """CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (%s)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
     WITH SERDEPROPERTIES (
     'serialization.format' = '1'
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
    