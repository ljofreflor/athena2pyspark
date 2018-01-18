'''
Created on 09-01-2018

@author: leonardo.jofre@exalitica.com
'''
# aws s3 cp ./etl_scripts/job_priorizacion.py
# s3://cencosud.exalitica.com/prod/etl_scripts/job_priorizacion.py
from athena2pyspark import job
from athena2pyspark.config import get_spark_session

try:
    # corre local (true) o glue (falso)
    spark = get_spark_session(True)
except UnboundLocalError:
    # de ecurrir una excepcion significa que se esta corriendo en glue y el spark ya
    # esta seteado
    pass
except ValueError:
    pass

args = {'id_com': '262', 'flag': 'jumbo'}

job(branch="prod", flag="jumbo", queryName="prepriorizacion",
    partition_by_id_com=True, spark=spark, param=args)

path_query = job(branch="prod", flag="jumbo", queryName="priorizacion",
                 partition_by_id_com=True, spark=spark, param=args)

cols = ["PARTY_ID", "PROMO_ID", "COMM_CHANNEL_CD", "CODIGO_SIEBEL", "CODIGO_MOTOR",
        "COMMUNICATION_ID", "PAGE_ID", "DATOS_DE_CONTACTO", "CORRELATIVO", "GRUPO"]
priorizacion = spark.read.csv(path_query).toDF(*cols)

table = "LISTADO_COM_{id_com}".format(**args)

priorizacion.write.format('jdbc').options(
    url='jdbc:mysql://cencosud-mariadb-preprod.cindgoz7oqnp.us-east-1.rds.amazonaws.com/JUMBO',
    driver='com.mysql.jdbc.Driver',
    dbtable=table,
    user='root',
    password='cencosud2015').mode('append').save()
