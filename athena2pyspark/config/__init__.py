'''
Created on 29-11-2017

@author: lnjofre
'''
from pyspark import SparkConf


def get_spark_session(args,
                      profile=None,
                      aws_access_key_id=None,
                      aws_secret_access_key=None):
    import os
    import boto3
    from pyspark.context import SparkContext
    from pyspark.sql.context import SQLContext
    from pyspark.sql.session import SparkSession
    import ConfigParser

    if args['mode'] == "local":

        Config = ConfigParser.ConfigParser()
        Config.read(os.environ['HOME'] + "/.aws/credentials")
        aws_access_key_id = Config.get(profile, "aws_access_key_id")
        aws_secret_access_key = Config.get(profile, "aws_secret_access_key")

        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

        spark = SparkSession.builder.master("local").getOrCreate()

        # TODO: por que hice esto?
        if aws_access_key_id is None:
            aws_access_key_id = spark.conf.get("fs.s3n.awsAccessKeyId")

        # TODO: por que hice esto otro?
        if aws_secret_access_key is None:
            aws_secret_access_key = spark.conf.get("fs.s3n.awsSecretAccessKey")

        spark.conf.set("fs.s3n.awsAccessKeyId", aws_access_key_id)
        spark.conf.set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)
        spark.conf.set("fs.s3.awsAccessKeyId", aws_access_key_id)
        spark.conf.set("fs.s3.awsSecretAccessKey", aws_secret_access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3.access.key", aws_access_key_id)
        spark._jsc.hadoopConfiguration().set("fs.s3.secret.key", aws_secret_access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3n.access.key", aws_access_key_id)
        spark._jsc.hadoopConfiguration().set("fs.s3n.secret.key", aws_secret_access_key)

        # todo: hacer andar el conector de athena sobre spark
        # spark.conf.set("spark.jars", "./AthenaJDBC41_2.0.2.jar")

    elif args['mode'] == "glue":
        from awsglue.context import GlueContext
        sc = SparkContext().getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session

    return spark
