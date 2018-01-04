from athena2pyspark import run_query, get_dataframe
from athena2pyspark.athena_sql import queryByName
from athena2pyspark.config import getLocalSparkSession, result_folder_temp

id_com = 180  # de donde venga

spark = getLocalSparkSession()

# todo: implementar esto
query = queryByName("sql/matriz_configuracion",
                    select={'dinamica': 'cons_hab', 'id_com': 180})


csv_path = run_query(query=query, database="prod_jumbo",
                     s3_output=result_folder_temp, spark=spark)

df = get_dataframe(path_query=csv_path, spark=spark)

pass
