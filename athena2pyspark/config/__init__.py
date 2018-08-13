'''
Created on 29-11-2017

@author: lnjofre
'''


class AthenaContext(object):
    def __init__(self, sparkContext):
        raise NotImplementedError


def get_spark_session(args, aws_access_key_id=None, aws_secret_access_key=None):
    import os
    import boto3
    from pyspark.context import SparkContext
    from pyspark.sql.context import SQLContext
    from pyspark.sql.session import SparkSession

    if (aws_access_key_id is None) or (aws_secret_access_key is None) and args['mode'] == "local":
        aws_access_key_id = 'AKIAJJ3QVXBBL4ZDQHEQ'
        aws_secret_access_key = '+XxkZE3vY2p6IkItaoGzJE2+UsTPmY36udmqA73sC'

    if args['mode'] == "local":
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
        spark = SparkSession.builder.master("local").getOrCreate()
        spark.conf.set("fs.s3n.awsAccessKeyId", aws_access_key_id)
        spark.conf.set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)
        spark.conf.set("fs.s3.awsAccessKeyId", aws_access_key_id)
        spark.conf.set("fs.s3.awsSecretAccessKey", aws_secret_access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3.access.key", aws_access_key_id)
        spark._jsc.hadoopConfiguration().set("fs.s3.secret.key", aws_secret_access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3n.access.key", aws_access_key_id)
        spark._jsc.hadoopConfiguration().set("fs.s3n.secret.key", aws_secret_access_key)

    elif args['mode'] == "glue":
        from awsglue.context import GlueContext
        sc = SparkContext().getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session

    return spark
