
athena2pyspark
==

“La documentación es como el sexo; cuando es bueno, es muy, muy bueno, y cuando es malo, es mejor que nada” 
-- Dick Brandon

Usted está frente athena2pyspark una api creada por exalítica para la manipulación de consultas en sql desde s3 basado en athena. El objetivo de esta librería es no tener un futuro sin servidores y sin cluster EMR. Cuide esta librería, apoye reportando bugs y disfrútela.

Este código está siendo probado constantemente en distintos ambientes, de forma local y en jobs de glue ...

Instalación
==
Si usted quiere probar la librería localmente no tiene más que instalarla

pip install git+https://ljofre-exalitica@bitbucket.org/exalitica-team/athena2pyspark.git

Si quiere usarla en un job de glue, debe apuntar a s3://library.exalitica.com/athena2pyspark.zip


```python
import athena2pyspark as ath 
from athena2pyspark.config import getLocalSparkSession

spark = getLocalSparkSession() # se demora un poco porque esta creado el SparkSession ...
```


```python
spark
```





            <div>
                <p><b>SparkSession - in-memory</b></p>
                
        <div>
            <p><b>SparkContext</b></p>

            <p><a href="http://192.168.1.39:4040">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v2.2.0</code></dd>
              <dt>Master</dt>
                <dd><code>local</code></dd>
              <dt>AppName</dt>
                <dd><code>pyspark-shell</code></dd>
            </dl>
        </div>
        
            </div>
        



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

    Execution ID: af777a26-4708-4d02-aef9-ea60e588d2ad



```python
df = ath.get_dataframe(path_query=path_dataframe, spark=spark)
df.show()
```

    +---------+-----+-----+------+-------+----+--------+----------+-----+------+-------+----+----+-----+-------+
    | party_id| corr|cv_us|cv_u4s|cv_u12s|m_us|   m_u4s|    m_u12s|cp_us|cp_u4s|cp_u12s| rec|tu4s|tu12s|  n_key|
    +---------+-----+-----+------+-------+----+--------+----------+-----+------+-------+----+----+-----+-------+
    |100015028| 8926|    0|     0|      2|   0|     0.0| 2336.1345|    0|   0.0|    2.0|37.0|  30| 37.0|2016_10|
    |100067924| 3757|    0|     0|      5|   0|     0.0| 2928.7397|    0|   0.0|    5.0|47.0|  30|  7.0|2016_10|
    |100200278| 4394|    0|     1|      1|   0|637.8151|  637.8151|    0|   1.0|    1.0|15.0|  30| 90.0|2016_10|
    |100246523|32142|    0|     0|      2|   0|     0.0|  1907.563|    0|   0.0|    2.0|57.0|  30| 12.0|2016_10|
    |100397603|19801|    0|     0|      1|   0|     0.0|10923.5294|    0|   0.0|    2.0|71.0|  30| 90.0|2016_10|
    |100444594|11798|    0|     1|      1|   0|2092.437|  2092.437|    0|   1.0|    1.0|26.0|  30| 90.0|2016_10|
    |100586799|20825|    0|     1|      1|   0|6007.563|  6007.563|    0|   1.0|    1.0|20.0|  30| 90.0|2016_10|
    |100644377|33342|    0|     1|      5|   0|455.4621| 4153.7809|    0|  0.35|  3.087|31.0|  30| 13.0|2016_10|
    |100783909|36235|    0|     0|      1|   0|     0.0| 2008.4034|    0|   0.0|    1.0|86.0|  30| 90.0|2016_10|
    |100844927|26286|    0|     0|      1|   0|     0.0| 2683.1932|    0|   0.0|  0.915|83.0|  30| 90.0|2016_10|
    +---------+-----+-----+------+-------+----+--------+----------+-----+------+-------+----+----+-----+-------+
    


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
                                    database = "prod_jumbo", 
                                    s3_output = s3_output, 
                                    spark = spark)

df_producto_nuevo = ath.get_dataframe(path_query=path_producto_nuevo, spark = spark)

df_producto_nuevo.limit(10).show()
```

    Execution ID: 2a994aeb-0847-4556-9dd3-940f277bb2a0
    +---------+--------+---------------+-------------+------------+----------------+-------+--------------------+-----------+-----+
    | party_id|promo_id|comm_channel_cd|codigo_siebel|codigo_motor|communication_id|page_id|   datos_de_contacto|correlativo|grupo|
    +---------+--------+---------------+-------------+------------+----------------+-------+--------------------+-----------+-----+
    |180039513|     300|              1|      PopCorn|         300|               1|      1|HECTOR.HOBAICA@GM...|      61009|    0|
    |127228094|     300|              1|      PopCorn|         300|               1|      1|Josefaundez01@gma...|      61009|    0|
    |107562342|     300|              1|      PopCorn|         300|               1|      1|  PDONOSOP@GMAIL.COM|      61009|    0|
    |167626019|     300|              1|      PopCorn|         300|               1|      1|     PHUMERES@CSM.CL|      61009|    0|
    |169905581|     300|              1|      PopCorn|         300|               1|      1|    Rbullard@itau.cl|      61009|    0|
    |151828781|     300|              1|      PopCorn|         300|               1|      1| Rosyrener@gmail.com|      61009|    0|
    |176966390|     300|              1|      PopCorn|         300|               1|      1|IVONNEMC8@HOTMAIL...|      61009|    0|
    |154872394|     300|              1|      PopCorn|         300|               1|      1| WMCLAUDIO@GMAIL.COM|      61009|    0|
    |121682849|     300|              1|      PopCorn|         300|               1|      1|PATRIVERI@HOTMAIL...|      61009|    0|
    |143086980|     300|              1|      PopCorn|         300|               1|      1|INGRIDFREIRE.LORE...|      61009|    0|
    +---------+--------+---------------+-------------+------------+----------------+-------+--------------------+-----------+-----+
    


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
