# job_dinamica_producto_nuevo.py

import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from athena2pyspark import run_query
from athena2pyspark.athena_sql import querybyByName
from athena2pyspark.config import result_folder_temp, getLocalSparkSession

spark = getLocalSparkSession()

"""
sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
"""

query_str = querybyByName("sql/producto_nuevo")
args = getResolvedOptions(sys.argv, ['id_com', 'lift'])
subclase, marca, lift = (args['subclase'], args['marca'], args['lift'])

path_query = run_query(query=query_str, database="prod_jumbo",
                       s3_output=result_folder_temp, spark=spark)

print "El subrubro es: ", args['ID_COM']