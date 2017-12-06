
from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# spark = getLocalSparkSession()

import athena2pyspark as ath
from athena2pyspark.athena_sql.dinamicas import producto_nuevo
import boto3
import re

s3 = boto3.client('s3')

# parametros de la funcion
file_name = "CL_MOTOR_SALIDA_PA_LISTADO_20171129"
s3_output = "s3://leonardo.exalitica.com/boto3/prueba_producto_nuevo2/"
query = producto_nuevo(subclase=110209, marca='2717', lift=8)
path_file = ath.run_query(query = query, database = "prod_jumbo", s3_output = s3_output,spark=spark)

# extraemos de la url el bucket y el key del archivo generado
bucket_pttrn = re.compile(r"s3://[^/]+/") # patron para extraer el nombre del bucket
file_pttrn = re.compile(r"[^/]*csv") # patron para extraer el key

Bucket = bucket_pttrn.findall(path_file)[0].replace("s3://","").replace("/","")
Key = path_file.replace(bucket_pttrn.findall(path_file)[0],"")
FileName = file_pttrn.findall(path_file)

#renombrar el archivo generado.
copy_source = {
    'Bucket': Bucket,
    'Key': Key
}

s3.copy(copy_source, Bucket, 'boto3/prueba_producto_nuevo2/'.replace(FileName[0], "") + file_name + '.csv')
s3.delete_object(Bucket=Bucket, Key=Key)
