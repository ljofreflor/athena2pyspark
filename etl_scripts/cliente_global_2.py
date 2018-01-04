from datetime import datetime
import sys
import time

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import *
from pyspark.sql import *
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import LongType

from athena2pyspark.config import getLocalSparkSession


# COMMAND ----------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

spark = glueContext.spark_session
spark = getLocalSparkSession()


sc_clientes = sc.textFile(
    "s3://cencosud.exalitica.com/prod/datos/entrada/comunes/CL_MOTOR_ENTRADA_CLIENTES.DAT.gz")


# COMMAND ----------

rdd_clientes = sc_clientes.map(lambda y: y.split("|"))

# COMMAND ----------

df_clientes = rdd_clientes.map(lambda x: (int(x[0]), x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19], x[20], x[21], x[22], x[23], x[24], x[25], x[26], x[27], x[28], x[29], x[30], x[31], x[32], x[33], x[34], x[35], x[36], x[37], x[38],
                                          x[39], x[40], x[41], x[42], x[43], x[44], x[45], x[46], x[47], x[48], x[49], x[50], x[51], x[52], x[53], x[54], x[55], x[56], x[57], x[58], x[59], x[60], x[61], x[62], x[63], x[64], x[65], x[66], x[67], x[68], x[69], x[70], x[71], int(x[72]), int(x[73]), int(x[74]), int(x[75]), int(x[76]), int(x[77])))

# COMMAND ----------

schema = StructType([StructField("PARTY_ID", IntegerType(), True),
                     StructField("NEC_FEC_INSCRIPCION", StringType(), True),
                     StructField("NAME_CLI", StringType(), True),
                     StructField("SURNAME_CLI", StringType(), True),
                     StructField("NEC_APELLIDO_MATERNO", StringType(), True),
                     StructField("MAIL_CLI", StringType(), True),
                     StructField("NEC_END_EMAIL", StringType(), True),
                     StructField("PARIS_CL_IND_EMAIL", StringType(), True),
                     StructField("EASY_CL_IND_EMAIL", StringType(), True),
                     StructField("JUMBO_CL_IND_EMAIL", StringType(), True),
                     StructField("DIRECTION_CLI", StringType(), True),
                     StructField("GEO_REF_LAT", StringType(), True),
                     StructField("GEO_REF_LONG", StringType(), True),
                     StructField("EDAD_CLI", StringType(), True),
                     StructField("NEC_REGION", StringType(), True),
                     StructField("COMUNA_CLI", StringType(), True),
                     StructField("EC_CLI", StringType(), True),
                     StructField("NEC_GENERO", StringType(), True),
                     StructField("ID_LOC_PREF_FREC_JUMBO", StringType(), True),
                     StructField("ID_LOC_PREF_GAST_JUMBO", StringType(), True),
                     StructField("ID_LOC_PREF_FREC_PARIS", StringType(), True),
                     StructField("ID_LOC_PREF_GAST_PARIS", StringType(), True),
                     StructField("ID_LOC_PREF_FREC_EASY", StringType(), True),
                     StructField("ID_LOC_PREF_GAST_EASY", StringType(), True),
                     StructField("SP_CLI", StringType(), True),
                     StructField("DEU_CLI", StringType(), True),
                     StructField("CUPO_TO_CLI", StringType(), True),
                     StructField("CUPO_DIS_CLI", StringType(), True),
                     StructField("FREC_US_CLI", StringType(), True),
                     StructField("PTO_CENCO", StringType(), True),
                     StructField("PARIS_CL", StringType(), True),
                     StructField("JUMBO_CL", StringType(), True),
                     StructField("EASY_CL", StringType(), True),
                     StructField("SEGMENT_VALUE_CD_PARIS", StringType(), True),
                     StructField("SEGMENT_VALUE_CD_Jumbo", StringType(), True),
                     StructField("SEGMENT_VALUE_CD_easy", StringType(), True),
                     StructField("SEGMENT_VALUE_CD_CENCOSUD",
                                 StringType(), True),
                     StructField("SUB_SEGMENT_VALUE_CD_JUMBO",
                                 StringType(), True),
                     StructField("SEGMENT_VALUE_CD_TARJETA",
                                 StringType(), True),
                     StructField("SOFISTICACION_PARIS", StringType(), True),
                     StructField("SOFIStICACION_EASY", StringType(), True),
                     StructField("SOFITICACION_SUPER", StringType(), True),
                     StructField("COBERTURA_JUMBO_CL", StringType(), True),
                     StructField("GSE", StringType(), True),
                     StructField("TARJETA_HAB", StringType(), True),
                     StructField("MEDIO_PAGO_PREF_PARIS", StringType(), True),
                     StructField("MEDIO_PAGO_PREF_EASY", StringType(), True),
                     StructField("MEDIO_PAGO_PREF_JUMBO", StringType(), True),
                     StructField("EDAD_1HIJO", StringType(), True),
                     StructField("EDAD_2HIJO", StringType(), True),
                     StructField("EDAD_3HIJO", StringType(), True),
                     StructField("EDAD_4HIJO", StringType(), True),
                     StructField("SEG_FIDELIDAD_JUMBO", StringType(), True),
                     StructField("SEG_FIDELIDAD_PARIS", StringType(), True),
                     StructField("SEG_FIDELIDAD_EASY", StringType(), True),
                     StructField("SUP_IND_COMPRA_VEGANO", StringType(), True),
                     StructField("SUP_IND_COMPRA_SIN_GLUTEN",
                                 StringType(), True),
                     StructField("SUP_IND_COMPRA_SIN_AZUCAR",
                                 StringType(), True),
                     StructField("SUP_IND_COMPRA_ORGANICO",
                                 StringType(), True),
                     StructField("SUP_SEG_CLIENTE_VEGANO", StringType(), True),
                     StructField("SUP_SEG_COMPRA_SIN_GLUTEN",
                                 StringType(), True),
                     StructField("SUP_SEG_COMPRA_SIN_AZUCAR",
                                 StringType(), True),
                     StructField("SUP_SEG_COMPRA_ORGANICO",
                                 StringType(), True),
                     StructField("SUP_JUM_ACTIVITY_CREDITCARD_CD",
                                 StringType(), True),
                     StructField("SUP_JUM_ACTIVITY_CHAIN_CD",
                                 StringType(), True),
                     StructField("SUP_JUM_SEGMENT_ACTIVITY_CD",
                                 StringType(), True),
                     StructField("TPD_PAR_ACTIVITY_CREDITCARD_CD",
                                 StringType(), True),
                     StructField("TPD_PAR_ACTIVITY_CHAIN_CD",
                                 StringType(), True),
                     StructField("TPD_PAR_SEGMENT_ACTIVITY_CD",
                                 StringType(), True),
                     StructField("TMH_EASY_ACTIVITY_CREDITCARD_CD",
                                 StringType(), True),
                     StructField("TMH_EASY_ACTIVITY_CHAIN_CD",
                                 StringType(), True),
                     StructField("TMH_EASY_SEGMENT_ACTIVITY_CD",
                                 StringType(), True),
                     StructField("JU_MAIL", IntegerType(), True),
                     StructField("PA_MAIL", IntegerType(), True),
                     StructField("EA_MAIL", IntegerType(), True),
                     StructField("JU_POS", IntegerType(), True),
                     StructField("PA_POS", IntegerType(), True),
                     StructField("EA_POS", IntegerType(), True)])

# COMMAND ----------

clientes = spark.createDataFrame(df_clientes, schema)

# COMMAND ----------

clientes_lc = clientes.toDF(*[c.lower() for c in clientes.columns])

# COMMAND ----------

clientes_lc.printSchema()

# COMMAND ----------

clientes_lc.repartition(20).write.mode("overwrite").parquet(
    "s3://cencosud.exalitica.com/prod/datos/entrada/comunes/clientes/")
