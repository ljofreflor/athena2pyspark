from datetime import datetime
import time

from awsglue.context import GlueContext
import mysql.connector
from pyspark.context import SparkContext
from pyspark.ml import PipelineModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.util import MLUtils
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from athena2pyspark import get_dataframe
import athena2pyspark as ath
from athena2pyspark.config import result_folder_temp

"""
spark = getLocalSparkSession()
"""

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# Parametros temporales
sem_ref = "2017_46"
sem_ref_modelos = "2017_16"
mes = "201710"

# Logica modelos a scorear en MySQL
# Integrar inserts y updates a tabla bitacora para tomar un correlativo
pre_url = spark.read.format('jdbc').options(
    url="jdbc:mysql://cencosud-mariadb-preprod.cindgoz7oqnp.us-east-1.rds.amazonaws.com:3306/JUMBO",
    driver="com.mysql.jdbc.Driver",
    dbtable="INFO_MODELOS_ITER_BIT",
    user="root",
    password="cencosud2015").load()

corr = 0

while corr != -1:
    try:
        corr = pre_url.where("STATUS = 'F' AND SEM_REF = '" +
                             sem_ref_modelos + "'").select("CORR").limit(1).head()[0]
    except TypeError:
        corr = -1

    con = mysql.connector.connect(user='root', password='cencosud2015',
                                  host='cencosud-mariadb-preprod.cindgoz7oqnp.us-east-1.rds.amazonaws.com', database='JUMBO')

    c = con.cursor()
    c.execute(
        """UPDATE JUMBO.INFO_MODELOS_ITER_BIT SET STATUS = 'S' WHERE CORR = """ + str(corr))
    con.commit()
    con.close()

    # Script para score
    parametrica = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(
        "s3://cencosud.exalitica.com/prepod/semanas_parametricas_spark_v2.csv")
    a = parametrica.where("N_KEY = '" + sem_ref +
                          "'").select("CODMES").head()[0]

    def strToDate(fecha):
        deit = time.mktime(datetime.strptime(
            '{}'.format(fecha), '%Y%m').timetuple())
        fec_tstmp = long("{0:.0f}".format(deit))
        return fec_tstmp

    fecha_inicio = datetime.utcfromtimestamp(
        strToDate(str(a))).strftime("%Y-%m-%d")
    act_date = str(parametrica.where("N_KEY = '" + sem_ref +
                                     "'").select("fec_fin").head()[0])[0:10]

    # Query genera tablon Athena
    query_score = """
    select
    party_id,
    corr,
    features,
    vector_1,
    prom_meses_dist,
    temp_min,
    temp_max,
    precipitaciones,
    coalesce(nva_rec,recencia) as recencia,
    '{0}' as n_key
    from
    -- /13. join baul2_sample con data5 -> df4/
    (select
    baul2_sample.party_id,
    baul2_sample.corr,
    baul2_sample.features,
    baul2_sample.rec,
    baul2_sample.vector_1,
    baul2_sample.srm,
    baul2_sample.prom_meses_dist,
    baul2_sample.recencia+(date_diff('day',cast('{1}' as date),cast('{2}' as date))) as recencia,
    baul2_sample.id_loc_pref_frec_jumbo,
    baul2_sample.temp_min,
    baul2_sample.temp_max,
    baul2_sample.precipitaciones,
    data5.nva_rec
    from
    -- /12. join output5 y temp -> baul2_sample/
    (select
    output5.*,
    f.temp_min,
    f.temp_max,
    f.precipitaciones
    from
    -- /11. join output4 y clientes_global -> output5/
    (select
    output4.*,
    coalesce(e.id_loc_pref_frec_jumbo,'J511') as id_loc_pref_frec_jumbo
    from
    -- /10. join output3 y data_lp -> output4/
    (select
    output3.*,
    coalesce(d.prom_meses_dist,0) as prom_meses_dist,
    coalesce(d.recencia,365) as recencia
    from
    -- /9. join aux3 y srm -> output3/
    (select
    aux3.*,
    c.srm
    from
    -- /8. join clientes_baul, baul2_sparse y variables cruzadas y reemplazando nulls-> aux3/
    (select
    a.party_id,
    coalesce(b.corr,{3}) as corr,
    coalesce(b.features,'(12,[9,10,11],[90.0,30.0,90.0])') as features,
    coalesce(b.rec_cp,90) as rec,
    h.vector_1
    from
    clientes_baul as a
    left join
    (select * from baul2_sparse where corr = {3}) as b
    on
    a.party_id=b.party_id
    left join
    variables_cruzadas as h
    on
    a.party_id=h.party_id) as aux3
    left join
    srm as c
    on
    aux3.corr=c.corr) as output3
    left join
    (select *
    from data_lp
    where srm in
    (select srm from srm where corr = {3})) as d
    on
    output3.party_id=d.party_id) as output4
    left join
    clientes_global as e
    on
    output4.party_id=e.party_id) as output5
    left join
    temp as f
    on
    output5.id_loc_pref_frec_jumbo=f.location_id) as baul2_sample
    left join
    -- generando tabla data5 con recencia actual a partir de fuente
    (select
    data3.*,
    date_diff('day',cast(act_rec as date),cast(fec_act as date)) as nva_rec
    from
    (select
    party_id,
    corr,
    max(cast(tran_start_dt as date)) as act_rec,
    max(n_key) as n_key,
    cast('{2}' as date) as fec_act
    from
    (select
    a.*,
    b.corr
    from
    fuente as a
    left join
    item_corr as b
    on
    a.item_id=b.prod_hier
    where
    b.corr={3}) as data2
    group by
    party_id,
    corr) as data3) as data5
    on
    baul2_sample.party_id=data5.party_id) as df2""".format(sem_ref, fecha_inicio, act_date, corr)

    ruta_base = ath.run_query(query=query_score,
                              s3_output=result_folder_temp,
                              database="score",
                              spark=spark)

    base = get_dataframe(path_query=ruta_base, spark=spark)

    def base_map(dic_row):
        return (int(dic_row['party_id']), int(dic_row['corr']), str(dic_row['n_key']), Vectors.parse(str(dic_row['vector_1'])), Vectors.parse(str(dic_row['features'])), float(dic_row['prom_meses_dist']), float(dic_row['recencia']), float(dic_row['temp_min']), float(dic_row['temp_max']), float(dic_row['precipitaciones']))

    base_to_score = base.rdd.map(base_map).toDF(["PARTY_ID", "CORR", "N_KEY", "VECTOR_1",
                                                 "FEATURES", "PROM_MESES_DIST", "RECENCIA", "TEMP_MIN", "TEMP_MAX", "PRECIPITACIONES"])

    pipeline_model = PipelineModel.load(
        "s3://cencosud.exalitica.com/prepod/jumbo/modelos_iter/2017_16/" + str(corr) + "")

    base_to_score_ml = MLUtils.convertVectorColumnsToML(base_to_score)

    model = pipeline_model.transform(base_to_score_ml)

    firstelement = udf(lambda v: float(v[1]), FloatType())

    result = model.select("PARTY_ID", "CORR", firstelement(
        "probability")).toDF("PARTY_ID", "CORR", "SCORE")

    # Se quita .mode("overwrite") por lentitud
    result.repartition(1).write.mode("append").parquet(
        "s3://pablo.exalitica.com/cencosud/jumbo/score/" + sem_ref + "/")

    con = mysql.connector.connect(user='root', password='cencosud2015',
                                  host='cencosud-mariadb-preprod.cindgoz7oqnp.us-east-1.rds.amazonaws.com', database='JUMBO')

    c = con.cursor()
    c.execute(
        """UPDATE JUMBO.INFO_MODELOS_ITER_BIT SET STATUS = 'K' WHERE CORR = """ + str(corr))
    con.commit()
    con.close()
