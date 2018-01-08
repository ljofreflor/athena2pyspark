# aws s3 cp  ./etl_scripts/score.py
# s3://cencosud.exalitica.com/prod/etl_scripts/score.py

from datetime import datetime
import sys
import time

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import mysql.connector
from pyspark.context import SparkContext
from pyspark.ml import PipelineModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.util import MLUtils
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from athena2pyspark import get_dataframe
import athena2pyspark as ath
from athena2pyspark.config import result_folder_temp, getLocalSparkSession

spark = getLocalSparkSession(False)


# Parametros banderas
args = getResolvedOptions(sys.argv, ['bandera'])
#args = {'bandera': 'jumbo'}

if args['bandera'] == 'jumbo':
    loc_pref = 'J511'
    features = '(12,[9,10,11],[90.0,30.0,90.0])'
    rec = '90'
    lp = 'SRM'
    rec_lp = '365'
elif args['bandera'] == 'paris':
    loc_pref = 'P511'
    features = '(17,[12,13,14,15,16],[365.0,30.0,90.0,180.0,365.0])'
    rec = '365'
    lp = 'SRM'
    rec_lp = '365'
else:
    loc_pref = 'E511'
    features = '(17,[12,13,14,15,16],[2190.0,30.0,90.0,180.0,365.0])'
    rec = '2190'
    lp = 'RM'
    rec_lp = '2190'

# Parametros temporales
sem_ref = "2017_52"
sem_ref_modelos = "2017_16"
mes = "201712"

# Logica modelos a scorear en MySQL
# Integrar inserts y updates a tabla bitacora para tomar un correlativo
pre_url = spark.read.format('jdbc').options(
    url="jdbc:mysql://cencosud-mariadb-preprod.cindgoz7oqnp.us-east-1.rds.amazonaws.com:3306/{0}".format(
        args['bandera'].upper()),
    driver="com.mysql.jdbc.Driver",
    dbtable="INFO_MODELOS_ITER_BIT",
    user="root",
    password="cencosud2015").load()

con = mysql.connector.connect(user='root', password='cencosud2015',
                              host='cencosud-mariadb-preprod.cindgoz7oqnp.us-east-1.rds.amazonaws.com', database='{0}'.format(args['bandera'].upper()))

#check_sem_score = pre_url.where("SEM_REF_SCORE = '" +sem_ref + "'").select("SEM_REF_SCORE").limit(1).head()[0]

# if str(check_sem_score) != sem_ref:
#    c = con.cursor()
#    c.execute("""INSERT INTO {0}.INFO_MODELOS_ITER_BIT
#        SELECT
#        DISTINCT CORR,
#        N_MUESTRA,
#        ROC_Prob1,
#        GINI_Prob1,
#        ROC_Pred,
#        GINI_Pred,
#        SEM_REF AS SEM_REF_MODEL,
#        'F' AS STATUS,
#        '{1}' as SEM_REF_SCORE
#        FROM {0}.INFO_MODELOS_ITER
#        WHERE SEM_REF = '{2}'
#        LIMIT 10""".format(args['bandera'].upper(),sem_ref,sem_ref_modelos)
#    con.commit()
#    con.close()

corr = 0

while corr != -1:
    try:
        corr = pre_url.where("STATUS = 'F' AND SEM_REF_SCORE = '" +
                             sem_ref + "'").select("CORR").limit(1).head()[0]
    except TypeError:
        corr = -1

    con = mysql.connector.connect(user='root', password='cencosud2015',
                                  host='cencosud-mariadb-preprod.cindgoz7oqnp.us-east-1.rds.amazonaws.com', database='{0}'.format(args['bandera'].upper()))

    c = con.cursor()
    c.execute("""UPDATE {0}.INFO_MODELOS_ITER_BIT SET STATUS = 'S' WHERE CORR = {1} AND SEM_REF_SCORE = '{2}'""".format(
        args['bandera'].upper(), str(corr), sem_ref))

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
    baul2_sample.id_loc_pref_frec_{4},
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
    coalesce(e.id_loc_pref_frec_{4},'{5}') as id_loc_pref_frec_{4}
    from
    -- /10. join output3 y data_lp -> output4/
    (select
    output3.*,
    coalesce(d.prom_meses_dist,0) as prom_meses_dist,
    coalesce(d.recencia,{9}) as recencia
    from
    -- /9. join aux3 y srm -> output3/
    (select
    aux3.*,
    c.srm
    from
    -- /8. join clientes_baul, baul2_sparse y variables cruzadas y reemplazando nulls-> aux3/
    (select
    a.party_id,
    a.vector_1,
    coalesce(b.corr,{3}) as corr,
    coalesce(b.features,'{6}') as features,
    coalesce(b.rec_cp,{7}) as rec
    from
    variables_cruzadas as a
    left join
    (select * from baul2_sparse where corr = {3}) as b
    on
    a.party_id=b.party_id) as aux3
    left join
    srm as c
    on
    aux3.corr=c.corr) as output3
    left join
    (select *
    from data_lp
    where {8} in
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
    output5.id_loc_pref_frec_{4}=f.location_id) as baul2_sample
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
    baul2_sample.party_id=data5.party_id) as df2""".format(sem_ref, fecha_inicio, act_date, corr, args['bandera'].lower(), loc_pref, features, rec, lp, rec_lp)

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
        "s3://cencosud.exalitica.com/prepod/{0}/modelos_iter/2017_16/".format(args['bandera'].lower()) + str(corr) + "")

    base_to_score_ml = MLUtils.convertVectorColumnsToML(base_to_score)

    model = pipeline_model.transform(base_to_score_ml)

    firstelement = udf(lambda v: float(v[1]), FloatType())

    result = model.select("PARTY_ID", firstelement(
        "probability")).toDF("party_id","score")

    result.repartition(1).write.mode("overwrite").parquet(
        "s3://pablo.exalitica.com/cencosud/{0}/score/n_key={1}/corr={2}".format(args['bandera'].lower(),sem_ref,corr))

    con = mysql.connector.connect(user='root', password='cencosud2015',
                                  host='cencosud-mariadb-preprod.cindgoz7oqnp.us-east-1.rds.amazonaws.com', database='{0}'.format(args['bandera'].upper()))

    c = con.cursor()
    c.execute(
        """UPDATE {0}.INFO_MODELOS_ITER_BIT SET STATUS = 'K' WHERE CORR = {1} AND SEM_REF_SCORE = '{2}'""".format(args['bandera'].upper(), str(corr), sem_ref))
    con.commit()
    con.close()

    try:
        corr = pre_url.where("STATUS = 'F' AND SEM_REF_SCORE = '" +
                             sem_ref + "'").select("CORR").limit(1).head()[0]
    except TypeError:
        corr = -1
