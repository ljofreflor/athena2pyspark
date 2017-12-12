'''
Created on 29-11-2017

@author: lnjofre
'''

import os
import boto3
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext

aws_access_key_id='AKIAJYICQU2XCXFLACWA'
aws_secret_access_key='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'

def getLocalSparkSession():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.11.86,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'
    spark = SparkSession.builder.master("local").getOrCreate()
    spark.conf.set("fs.s3n.awsAccessKeyId", aws_access_key_id)
    spark.conf.set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)
    spark.conf.set("fs.s3.awsAccessKeyId", aws_access_key_id)
    spark.conf.set("fs.s3.awsSecretAccessKey", aws_secret_access_key)
    
    return spark