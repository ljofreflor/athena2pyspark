'''
Created on 24-10-2017

@author: lnjofre
'''

import os
from pyspark.sql import dataframe
import string
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 pyspark-shell'

import boto3
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.utils import AnalysisException
import time

sc = SparkContext.getOrCreate()

aws_access_key_id='AKIAJYICQU2XCXFLACWA'
aws_secret_access_key='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'
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_access_key_id)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)

sqlContext = SQLContext(sc)

def get_dataframe(path_query):
    while True:
        try:
            path_query = "s3n://leonardo.exalitica.com/boto3/query_1/e2f22be7-70bc-4c1f-b23b-8fa2f5692579.csv"
            return sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").option("mode","DROPMALFORMED").load(path_query)
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
    
    return df

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
    
    