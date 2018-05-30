'''
Created on 29-11-2017

@author: lnjofre
'''

import os
import boto3


def get_spark_session(aws_access_key_id, aws_secret_access_key, args):
    from pyspark.context import SparkContext
    from pyspark.sql.context import SQLContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext

    if args['mode'] == "local":

        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
        spark = SparkSession.builder.master("local").getOrCreate()
        spark.conf.set("fs.s3n.awsAccessKeyId", aws_access_key_id)
        spark.conf.set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)
        spark.conf.set("fs.s3.awsAccessKeyId", aws_access_key_id)
        spark.conf.set("fs.s3.awsSecretAccessKey", aws_secret_access_key)

    elif args['mode'] == "glue":

        sc = SparkContext().getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session

    return spark
