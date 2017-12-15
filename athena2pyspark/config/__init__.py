'''
Created on 29-11-2017

@author: lnjofre
'''

import os

import boto3
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession


aws_access_key_id = 'AKIAJYICQU2XCXFLACWA'
aws_secret_access_key = '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'
result_folder_temp = "s3://athena2pyspark.temp/temp/"


def getLocalSparkSession():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.11.86,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'
    spark = SparkSession.builder.master("local").getOrCreate()
    spark.conf.set("fs.s3n.awsAccessKeyId", aws_access_key_id)
    spark.conf.set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)
    spark.conf.set("fs.s3.awsAccessKeyId", aws_access_key_id)
    spark.conf.set("fs.s3.awsSecretAccessKey", aws_secret_access_key)

    return spark


paths = {
    "afinidad_marca": "s3://cencosud.exalitica.com/prod/bandera/dinamicas/afinidad_marca/",
    "afinidad_subclase": "s3://cencosud.exalitica.com/prod/bandera/dinamicas/afinidad_subclase/",
    "ciclo_recompra": "s3://cencosud.exalitica.com/prod/bandera/dinamicas/ciclo_recompra/",
    "objetivo_item": "s3://cencosud.exalitica.com/prod/bandera/dinamicas/ciclo_recompra/",
    "party_rubro": "s3://cencosud.exalitica.com/prod/bandera/metricas/party_rubro"

}
