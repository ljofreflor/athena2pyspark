"""
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

spark = glueContext.spark_session

"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import coalesce
from pyspark.sql.types import *

from athena2pyspark import run_query
from athena2pyspark.athena_sql import querybyByName
from athena2pyspark.athena_sql.dinamicas import producto_nuevo
from athena2pyspark.config import result_folder_temp, getLocalSparkSession

"""
spark = getLocalSparkSession()
"""

sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


#query_str = querybyByName("sql/jumbo/dinamicas/producto_nuevo")

args = getResolvedOptions(sys.argv, ['subclase', 'marca', 'lift'])

subclase, marca, lift = (args['subclase'], args['marca'], args['lift'])

print(subclase, marca, lift)

query_str = producto_nuevo(subclase, marca, lift)

path_query = run_query(query=query_str, database="prod_jumbo",
                       s3_output=result_folder_temp, spark=spark)

args = getResolvedOptions(sys.argv, ['ID_COM'])
print "El subrubro es: ", args['ID_COM']

source_df = spark.read.format('jdbc').options(
    url="jdbc:mysql://cencosud-mariadb-preprod.cindgoz7oqnp.us-east-1.rds.amazonaws.com:3306/JUMBO",
    driver="com.mysql.jdbc.Driver",
    dbtable="PROD_CORR",
    user="root",
    password="cencosud2015").load()
