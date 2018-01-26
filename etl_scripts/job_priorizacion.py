'''
Created on 09-01-2018

@author: leonardo.jofre@exalitica.com

para transportar a glue
aws s3 cp ./etl_scripts/job_priorizacion.py s3://cencosud.exalitica.com/prod/etl_scripts/job_priorizacion.py
'''

# TODO: llevar la logica de librerias a un pipeline de bitbucket

"""
el formato de los argumentos deben ser los siguientes
args = {'id_com': '262', 'flag': 'jumbo', 'ncelda':'7','mode': 'glue'} # para glue
args = {'id_com': '262', 'flag': 'jumbo','ncelda':'7', 'mode': 'local'} # para local y databricks
"""

import sys

import mysql.connector

from athena2pyspark import job
from athena2pyspark.config import get_spark_session
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(sys.argv, ['id_com', 'flag', 'mode', 'ncelda'])

spark = get_spark_session(args)

# prepriorizacion
job(branch="prod", flag="jumbo", queryName="prepriorizacion",
    partition_by_id_com=True, spark=spark, param=args)

# priorizacion
job(branch="prod", flag="jumbo", queryName="priorizacion",
    partition_by_id_com=True, spark=spark, param=args)

# listado
path_query = job(branch="prod", flag="jumbo", queryName="listado",
                 spark=spark, partition_by_id_com=True, param=args)


# TODO: guardar el resultado de la priorizacion en una tabla de athena
cols = ["PARTY_ID", "PROMO_ID", "COMM_CHANNEL_CD", "CODIGO_SIEBEL", "CODIGO_MOTOR",
        "COMMUNICATION_ID", "PAGE_ID", "DATOS_DE_CONTACTO", "CORRELATIVO", "GRUPO"]

listado = spark.read.option("header", False).csv(path_query).toDF(*cols)

table = "LISTADO_COM_{id_com}".format(**args)

listado.write.format('jdbc').options(
    url='jdbc:mysql://172.26.216.22/JUMBO',
    driver='com.mysql.jdbc.Driver',
    dbtable=table,
    user='root',
    password='cencosud2015').mode('append').save()

con = mysql.connector.connect(user='root', password='cencosud2015',
                              host='172.26.216.22', database='JUMBO_WEB')

c = con.cursor()
c.execute(
    "UPDATE JUMBO_WEB.comunicacion SET ID_ESTADO=6 WHERE ID_COM={id_com}".format(**args))
con.commit()
