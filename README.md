
athena2pyspark
==

“La documentación es como el sexo; cuando es bueno, es muy, muy bueno, y cuando es malo, es mejor que nada” 
-- Dick Brandon

Usted está frente athena2pyspark una api creada por exalítica para la manipulación de consultas en sql desde s3 basado en athena. El objetivo de esta librería es no tener futuro sin servidores y sin cluster EMR. Cuide esta librería, apoye reportando bugs, definiendo casos de usos, requerimientos en los issues y disfrútela.

Este código está siendo probado constantemente en distintos ambientes, de forma local, en jobs de glue en databricks y endpoints de prueba.

Instalación
==

Si quiere usarla en un job de glue, debe apuntar a s3://library.exalitica.com/athena2pyspark.zip


```python
import athena2pyspark as ath 
from athena2pyspark.config import getLocalSparkSession

spark = getLocalSparkSession() # se demora un poco porque esta creado el SparkSession ...
```

por ejemplo, tenemos la función que retorna la consulta de producto nuevo

Hacer una query y que te retorne un dataframe
==

Podemos usar los recursos de athena para obtener querys basado en sql, esto nos permite hacer querys gigantes de forma server-less


```python
s3_output = "s3://leonardo.exalitica.com/glue_example/query_examples_select_all/"
path_dataframe = ath.run_query(query = "select * from baul_2 limit 10", 
                               database = "ljofre", 
                               s3_output = s3_output,
                               spark = spark)
```

    Execution ID: 61fb6cdd-7fbb-4bc3-8ec2-3c056435281e



```python
df = ath.get_dataframe(path_query=path_dataframe, spark=spark)
df.show()
```

    +---------+-----+-----+------+-------+----+---------+-----------------+-----+------+-------+----+----+-----+-------+
    | party_id| corr|cv_us|cv_u4s|cv_u12s|m_us|    m_u4s|           m_u12s|cp_us|cp_u4s|cp_u12s| rec|tu4s|tu12s|  n_key|
    +---------+-----+-----+------+-------+----+---------+-----------------+-----+------+-------+----+----+-----+-------+
    |100138097|20579|    0|     0|      1|   0|      0.0|        1234.4538|    0|   0.0|    1.0|91.0|30.0| 90.0|2016_10|
    |100096250|21516|    0|     1|      1|   0|1258.8236|        1258.8236|    0|   2.0|    2.0|19.0|30.0| 90.0|2016_10|
    |100318587| 1828|    0|     0|      4|   0|      0.0|6231.932400000001|    0|   0.0|   24.0|54.0|30.0| 11.0|2016_10|
    |100270997| 8926|    0|     1|      1|   0|1170.5882|        1170.5882|    0|   1.0|    1.0|23.0|30.0| 90.0|2016_10|
    |100521809| 9090|    0|     0|      1|   0|      0.0|        2099.1597|    0|   0.0|    2.0|65.0|30.0| 90.0|2016_10|
    |100473725| 7820|    0|     0|      1|   0|      0.0|          587.395|    0|   0.0|    1.0|51.0|30.0| 90.0|2016_10|
    |100720101| 2613|    0|     1|      1|   0|9977.3109|        9977.3109|    0|  0.72|   0.72|16.0|30.0| 90.0|2016_10|
    |100655707|28515|    0|     2|      2|   0| 779.8319|         779.8319|    0|   2.0|    2.0|30.0| 4.0|  4.0|2016_10|
    |100914163|  216|    0|     0|      1|   0|      0.0|       13103.3612|    0|   0.0|  1.525|36.0|30.0| 90.0|2016_10|
    |100838668|23346|    0|     0|      1|   0|      0.0|        1032.7731|    0|   0.0|    1.0|86.0|30.0| 90.0|2016_10|
    +---------+-----+-----+------+-------+----+---------+-----------------+-----+------+-------+----+----+-----+-------+
    


Dinámica producto nuevo
==

Podemos obtener el dataframe de dinámica de producto nuevo


```python
from athena2pyspark.athena_sql.dinamicas import producto_nuevo 
# “El código nunca miente, los comentarios sí” -- Ron Jeffries

#todo: agregar codigo_siebel
producto_nuevo_query = producto_nuevo(subclase=110209, marca='2717', lift=8) # creamos la query
```


```python
s3_output = "s3://leonardo.exalitica.com/boto3/query_examples_dinamica_producto_nuevo/"

path_producto_nuevo = ath.run_query(query = producto_nuevo_query, 
                                    database = "ljofre", 
                                    s3_output = s3_output, 
                                    spark = spark)

df_producto_nuevo = ath.get_dataframe(path_query=path_producto_nuevo, spark = spark)

df_producto_nuevo.limit(10).show()
```

    Execution ID: 74d03414-c9ea-49a3-93c9-a2623accea8e



    ---------------------------------------------------------------------------

    AssertionError                            Traceback (most recent call last)

    <ipython-input-7-6c2b37ea03df> in <module>()
          4                                     database = "ljofre",
          5                                     s3_output = s3_output,
    ----> 6                                     spark = spark)
          7 
          8 df_producto_nuevo = ath.get_dataframe(path_query=path_producto_nuevo, spark = spark)


    ~/athena2pyspark/athena2pyspark/__init__.py in run_query(query, database, s3_output, spark)
         94         status = athena.get_query_execution(QueryExecutionId=query_id)[
         95             'QueryExecution']['Status']['State']
    ---> 96         assert(status != 'FAILED')
         97         assert(status != 'CANCELLED')
         98         time.sleep(5)


    AssertionError: 


Generar el "create table" a partir del dataframe ya creado
==

Es util registrar este dataframe dentro de un catálogo para poder seguir haciendo consultas dentro de athena: 

Hay algunas cosas importantes que considerar antes de automatizar la lectura de tablas: Hay que considerar que dentro de la carpeta solo debe estar el archivo que tiene la información. Athena genera un archivo .metadata que debe ser borrado antes de que se haga la lectura.


```python
path_producto_nuevo
```




    's3://leonardo.exalitica.com/boto3/query_examples_dinamica_producto_nuevo/2a994aeb-0847-4556-9dd3-940f277bb2a0.csv'




```python
s3_input = path_producto_nuevo
create_database, create_table = ath.get_ddl(df=df_producto_nuevo,
                                            database="ljofre",
                                            table="nueva_tabla_de_ejemplo",
                                            s3_input=s3_output)
```


```python
print(create_database)
```

    CREATE DATABASE IF NOT EXISTS ljofre;



```python
print(create_table)
```

    CREATE EXTERNAL TABLE IF NOT EXISTS ljofre.nueva_tabla_de_ejemplo (party_id string,
    promo_id string,
    comm_channel_cd string,
    codigo_siebel string,
    codigo_motor string,
    communication_id string,
    page_id string,
    datos_de_contacto string,
    correlativo string,
    grupo string)
         ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
         WITH SERDEPROPERTIES (
         'serialization.format' = '1',
         'field.delim' = ','
         ) LOCATION 's3://leonardo.exalitica.com/boto3/query_examples_dinamica_producto_nuevo/'
         TBLPROPERTIES ('has_encrypted_data'='false');


Ejecutar create table y dejar registrada la tabla en Athena
==

“Cuando trabajo en un problema nunca pienso sobre la elegancia, sólo sobre cómo resolverlo. Pero cuando he acabado, si la solución no es elegante, sé que es incorrecta” 
-- R. Buckminster Fuller


```python
from athena2pyspark.config import aws_access_key_id, aws_secret_access_key

import boto3

client = boto3.client('athena', region_name='us-east-1', 
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

response = client.start_query_execution(
    QueryString=create_table,
    QueryExecutionContext={
        'Database': "ljofre"
        },
    ResultConfiguration={
            'OutputLocation': s3_output,
            }
    )
print('Execution ID: ' + response['QueryExecutionId'])
```

Hacer un listado a partir de la prepriorizacion
==


```python
print(create_table)
```


```python
import boto3
```


```python
boto3.__version__
```




    '1.4.8'


