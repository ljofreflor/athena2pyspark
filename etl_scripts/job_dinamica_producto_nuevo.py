# job_dinamica_producto_nuevo.py
# aws s3 cp ./etl_scripts/job_dinamica_producto_nuevo.py
# s3://cencosud.exalitica.com/prod/etl_scripts/job_dinamica_producto_nuevo.py

import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from athena2pyspark import run_query
from athena2pyspark.athena_sql import queryByName
from athena2pyspark.config import result_folder_temp, getLocalSparkSession

"""
id_com = 180

spark = getLocalSparkSession()
"""

sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


args = getResolvedOptions(sys.argv, ['id_com'])
id_com = args['id_com']


query_str = queryByName(
    query_file_name="sql/producto_nuevo", args={'id_com': id_com})

path_query = run_query(query=query_str, database="prod_jumbo",
                       s3_output=result_folder_temp, spark=spark)
