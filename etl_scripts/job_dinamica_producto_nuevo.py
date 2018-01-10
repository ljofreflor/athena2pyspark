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
# todo: tiene muchas celdas y muchas ofertas para un cliente y deberia ser uno
df_producto_nuevo = get_dataframe(
    path_query=path_query_producto_nuevo, spark=spark)
# dataframe de los clientes (template)
path_query_clientes = run_query(query="select * from clientes",
                                database="prod_jumbo", s3_output=result_folder_temp, spark=spark)
clientes = get_dataframe(
    path_query=path_query_clientes, spark=spark)
# dataframe de las ofertas (ofertas)
path_query_ofertas = run_query(query="select * from ofertas",
                               database="prod_jumbo", s3_output=result_folder_temp, spark=spark)
df_ofertas = get_dataframe(
    path_query=path_query_ofertas, spark=spark)
# todo: jorge, aca hay que hacer el join que se necesita
clientes.createOrReplaceTempView("clientes")

df_ofertas.createOrReplaceTempView("ofertas")


# (sirve para cualquier comunicacion)
df_producto_nuevo.createOrReplaceTempView("df_producto_nuevo")
listado_final = spark.sql("""
SELECT
a.party_id, a.id_oferta as promo_id, b.canal as comm_channel_cd, b.cod_siebel as codigo_siebel, a.id_oferta as codigo_motor, a.id_com as communication_id, a.id_celda as page_id, IF(c.nec_end_email !="", c.nec_end_email, c.jumbo_cl_ind_email) AS datos_de_contacto, a.id_oferta as correlativo, 0 as grupo 
FROM df_producto_nuevo a
LEFT JOIN ofertas b
ON a.id_com = b.id_com
LEFT JOIN clientes c
ON a.party_id = c.party_id
""")
# TODO: setear la ruta donde se guarda el listado
# listado_final.write.mode("overwrite").parquet("s3")
