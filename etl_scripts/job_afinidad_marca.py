from awsglue.context import GlueContext
from pyspark.context import SparkContext

from athena2pyspark import run_query, get_dataframe
from athena2pyspark.athena_sql import querybyByName
from athena2pyspark.config import result_folder_temp


# TODO: parametrizar por bandera.
# TODO: parametrizar por aplicacion local o job
# TODO: abstraer el concepto para generar el grafo de dependencia.

sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

"""
from athena2pyspark.config import getLocalSparkSession

spark = getLocalSparkSession()
"""

query_afinidad_marca = querybyByName("sql/afinidad_marca")

query_afinidad_subclase = querybyByName("sql/afinidad_subclase")

query_afinidad_marca_path = run_query(query=query_afinidad_marca,
                                      s3_output=result_folder_temp,
                                      database="prod_jumbo",
                                      spark=spark)

query_afinidad_subclase_path = run_query(query=query_afinidad_subclase,
                                         s3_output=result_folder_temp,
                                         database="prod_jumbo",
                                         spark=spark)

df_afinidad_marca = get_dataframe(
    path_query=query_afinidad_marca_path, spark=spark)
df_afinidad_subclase = get_dataframe(
    path_query=query_afinidad_subclase_path, spark=spark)

df_afinidad_marca.repartition(1).write.mode("Overwrite").parquet(
    "s3://cencosud.exalitica.com/prod/jumbo/dinamicas/afinidad_marca/")
df_afinidad_subclase.repartition(1).write.mode("Overwrite").parquet(
    "s3://cencosud.exalitica.com/prod/jumbo/dinamicas/afinidad_subrubro/")
