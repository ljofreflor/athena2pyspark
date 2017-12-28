# job_dinamica_producto_nuevo.py

"""
from athena2pyspark.config import getLocalSparkSession
spark = getLocalSparkSession()
"""

import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from athena2pyspark import run_query
from athena2pyspark.athena_sql import querybyByName
from athena2pyspark.config import result_folder_temp

"""
spark = getLocalSparkSession()
"""
sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


query_str = querybyByName("sql/producto_nuevo")
args = getResolvedOptions(sys.argv, ['id_com'])
id_com = args['id_com']

path_query = run_query(query=query_str, database="prod_jumbo",
                       s3_output=result_folder_temp, spark=spark)


# el resultado de la query debe quedar seteado

print "El subrubro es: ", args['id_com']