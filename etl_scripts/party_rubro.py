
# aws s3 cp  ./etl_scripts/party_rubro.py
# s3://cencosud.exalitica.com/prod/etl_scripts/party_rubro.py

from awsglue.context import GlueContext
from pyspark.context import SparkContext

from athena2pyspark import get_dataframe
from athena2pyspark import run_query
from athena2pyspark.athena_sql import queryByName
from athena2pyspark.config import getLocalSparkSession, result_folder_temp, paths

flag = 'prod_jumbo'  # setear la bandera
# asociar la bandera a la ruta de resultados
path_party_rubro = paths['party_rubro'].format(**{'flag': flag})

"""
spark = getLocalSparkSession()  # corre local

"""

sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

query = queryByName("sql/party_rubro")  # obtener la query

path_query = run_query(query=query, database=flag,
                       s3_output=result_folder_temp, spark=spark)  # correr la query

df = get_dataframe(path_query=path_query, spark=spark)  # leer el csv con spark

# guardar el parquet en la ruta dada por la configuracion
df.write.mode("overwrite").parquet(path_party_rubro)


# particionar el dataframe por lo solicitado
