from athena2pyspark.athena_sql import querybyByName
from athena2pyspark import run_query
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from athena2pyspark import get_dataframe
from athena2pyspark.config import getLocalSparkSession


sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

"""
spark = getLocalSparkSession() # corre local
"""
query = querybyByName("sql/party_rubro")

temp_folder = "s3://athena2pyspark.temp/temp/"

path_query = run_query(query=query, database="prod_easy", s3_output=temp_folder, spark=spark)

df = get_dataframe(path_query=path_query, spark=spark)

# particionar el dataframe por lo solicitado

