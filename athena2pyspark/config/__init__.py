'''
Created on 29-11-2017

@author: lnjofre
'''

import os

import boto3
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession

from awsglue.context import GlueContext


aws_access_key_id = 'AKIAJYICQU2XCXFLACWA'
aws_secret_access_key = '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'
result_folder_temp = "s3://athena2pyspark.temp/temp/"


def get_spark_session(args):

    if args['mode'] == "local":
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages mysql:mysql-connector-java:5.1.38,com.amazonaws:aws-java-sdk:1.11.86,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'
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


paths = {
    "afinidad_marca": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/afinidad_marca/",
    "afinidad_subclase": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/afinidad_subclase/",
    "ciclo_recompra": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/ciclo_recompra/",
    "cons_hab_des": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/cons_hab_des/",
    "consumo_habitual": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/consumo_habitual/",
    "objetivo_item": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/objetivo_item/",
    "party_rubro": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/party_rubro/",
    "party_subrubro": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/party_subrubro/",
    "prepriorizacion_filtrada": "s3://cencosud.exalitica.com/prod/{flag}/listado/prepriorizacion_filtrada/",
    "prioriza_cliente": "s3://cencosud.exalitica.com/prod/{flag}/prioriza_cliente/",
    "priorizacion":  "s3://cencosud.exalitica.com/prod/{flag}/priorizacion/",
    "prepriorizacion": "s3://cencosud.exalitica.com/prod/{flag}/prepriorizacion/",
    "listado": "s3://cencosud.exalitica.com/prod/{flag}/listado/",
    "up_sell":  "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/up_sell/"
}

partition_by = {
    "afinidad_marca": [],
    "afinidad_subclase": [],
    "ciclo_recompra": [],
    "cons_hab_des": [],
    "consumo_habitual": [],
    "objetivo_item": [],
    "party_rubro": [],
    "party_subrubro": [],
    "prepriorizacion_filtrada": ['col2', 'col1'],
    "prioriza_cliente": [],
    "priorizacion":  [],
    "prepriorizacion": ['col2', 'col1'],
    "listado": [],
    "up_sell": []

}
