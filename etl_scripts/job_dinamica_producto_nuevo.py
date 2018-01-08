# job_dinamica_producto_nuevo.py
# aws s3 cp ./etl_scripts/job_dinamica_producto_nuevo.py
# s3://cencosud.exalitica.com/prod/etl_scripts/job_dinamica_producto_nuevo.py

import sys

from athena2pyspark import run_query, get_dataframe
from athena2pyspark.athena_sql import queryByName
from athena2pyspark.config import result_folder_temp, getLocalSparkSession

id_com = 180
"""

args = getResolvedOptions(sys.argv, ['id_com'])
id_com = args['id_com']

"""

spark = getLocalSparkSession(True)

query_str = queryByName(
    query_file_name="sql/producto_nuevo", args={'id_com': id_com})

path_query_producto_nuevo = run_query(query=query_str, database="prod_jumbo",
                                      s3_output=result_folder_temp, spark=spark)

# dataframe con la dinamica de producto nuevo con el id_com=180
df_producto_nuevo = get_dataframe(
    path_query=path_query_producto_nuevo, spark=spark)

# dataframe de los clientes (template)

path_query_clientes = run_query(query="select * from template",
                                database="prod_jumbo", s3_output=result_folder_temp, spark=spark)

df_clientes = get_dataframe(
    path_query=path_query_clientes, spark=spark)

# dataframe de las ofertas (ofertas)

path_query_ofertas = run_query(query="select * from ofertas",
                               database="prod_jumbo", s3_output=result_folder_temp, spark=spark)

df_clientes = get_dataframe(
    path_query=path_query_ofertas, spark=spark)


# todo: jorge, acá hay que hacer el join que se necesita
