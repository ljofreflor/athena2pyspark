'''
Created on 09-01-2018

@author: leonardo.jofre@exalitica.com
'''
# aws s3 cp ./etl_scripts/job_priorizacion.py
# s3://cencosud.exalitica.com/prod/etl_scripts/job_priorizacion.py
"""
el formato de los argumentos deben ser los siguientes
args = {'id_com': '262', 'flag': 'jumbo', 'mode': 'glue'} # para glue
args = {'id_com': '262', 'flag': 'jumbo', 'mode': 'local'} # para local y databricks
"""

import sys

from athena2pyspark import job
from athena2pyspark.config import get_spark_session
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(sys.argv, ['id_com', 'flag', 'mode'])

try:
    spark = get_spark_session(args)
except UnboundLocalError:
    # de ecurrir una excepcion significa que se esta corriendo en glue y el spark ya
    # esta seteado
    pass
except ValueError:
    pass


"""
job(branch="prod", flag="jumbo", queryName="prepriorizacion",
    partition_by_id_com=True, spark=spark, param=args)
"""

job(branch="prod", flag="jumbo", queryName="priorizacion",
    partition_by_id_com=True, spark=spark, param=args)


path_query = job(branch="prod", flag="jumbo", queryName="listado",
                 spark=spark, partition_by_id_com=False, param=args)


# TODO: guardar el resultado de la priorizacion en una tabla de athena
cols = ["PARTY_ID", "PROMO_ID", "COMM_CHANNEL_CD", "CODIGO_SIEBEL", "CODIGO_MOTOR",
        "COMMUNICATION_ID", "PAGE_ID", "DATOS_DE_CONTACTO", "CORRELATIVO", "GRUPO"]

listado = spark.read.csv(path_query).toDF(*cols)

table = "LISTADO_COM_{id_com}".format(**args)


listado.write.format('jdbc').options(
    url='jdbc:mysql://cencosud-mariadb-preprod.cindgoz7oqnp.us-east-1.rds.amazonaws.com/JUMBO',
    driver='com.mysql.jdbc.Driver',
    dbtable=table,
    user='root',
    password='cencosud2015').mode('append').save()
