from athena2pyspark.athena_sql import querybyByName
from athena2pyspark import run_query, get_dataframe
from athena2pyspark.config import result_folder_temp, getLocalSparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

"""
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

df_afinidad_marca = get_dataframe(path_query=query_afinidad_marca_path, spark=spark)
df_afinidad_subclase = get_dataframe(path_query=query_afinidad_subclase_path, spark=spark)

df_afinidad_marca.write.parquet("s3://cencosud.exalitica.com/prod/jumbo/dinamicas/afinidad_marca/")
df_afinidad_subclase.write.parquet("s3://cencosud.exalitica.com/prod/jumbo/dinamicas/afinidad_subrubro/")

pass