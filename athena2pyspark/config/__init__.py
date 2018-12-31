'''
Created on 29-11-2017

@author: lnjofre
'''
from pyspark import SparkConf


def get_spark_session(args,
                      profile=None,
                      aws_access_key_id=None,
                      aws_secret_access_key=None):
    import os
    import boto3
    from pyspark.context import SparkContext
    from pyspark.sql.context import SQLContext
    from pyspark.sql.session import SparkSession
    import ConfigParser

    if args['mode'] == "local":

        Config = ConfigParser.ConfigParser()
        Config.read(os.environ['HOME'] + "/.aws/credentials")
        aws_access_key_id = Config.get(profile, "aws_access_key_id")
        aws_secret_access_key = Config.get(profile, "aws_secret_access_key")

        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

        spark = SparkSession.builder.master("local").getOrCreate()

        # TODO: por que hice esto?
        if aws_access_key_id is None:
            aws_access_key_id = spark.conf.get("fs.s3n.awsAccessKeyId")

        # TODO: por que hice esto otro?
        if aws_secret_access_key is None:
            aws_secret_access_key = spark.conf.get("fs.s3n.awsSecretAccessKey")

        spark.conf.set("fs.s3n.awsAccessKeyId", aws_access_key_id)
        spark.conf.set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)
        spark.conf.set("fs.s3.awsAccessKeyId", aws_access_key_id)
        spark.conf.set("fs.s3.awsSecretAccessKey", aws_secret_access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3.access.key", aws_access_key_id)
        spark._jsc.hadoopConfiguration().set("fs.s3.secret.key", aws_secret_access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3n.access.key", aws_access_key_id)
        spark._jsc.hadoopConfiguration().set("fs.s3n.secret.key", aws_secret_access_key)

        # todo: hacer andar el conector de athena sobre spark
        # spark.conf.set("spark.jars", "./AthenaJDBC41_2.0.2.jar")

    elif args['mode'] == "glue":
        from awsglue.context import GlueContext
        sc = SparkContext().getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session

    return spark
<<<<<<< HEAD


paths = {
    "afinidad_marca": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/afinidad_marca/",
    "afinidad_subclase": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/afinidad_subclase/",
    "ciclo_recompra": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/ciclo_recompra/",
    "cons_hab_des": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/cons_hab_des/",
    "consumo_habitual": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/consumo_habitual/",
    "matriz_de_configuracion": "s3://cencosud.exalitica.com/prod/{flag}/matriz_de_configuracion/",
    "objetivo_item": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/objetivo_item/",
    "party_rubro": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/party_rubro/",
    "party_subrubro": "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/party_subrubro/",
    "prepri_consumo_habitual": "s3://cencosud.exalitica.com/prod/{flag}/metricas/consumo_habitual/",
    "prepri_ciclo_recompra": "s3://cencosud.exalitica.com/prod/{flag}/metricas/ciclo_recompra/",
    "prepri_cons_hab_des": "s3://cencosud.exalitica.com/prod/{flag}/metricas/cons_hab_des/",
    "prepri_objetivo_item": "s3://cencosud.exalitica.com/prod/{flag}/metricas/objetivo_item/",
    "prepri_party_rubro": "s3://cencosud.exalitica.com/prod/{flag}/metricas/party_rubro/",
    "prepri_party_subrubro": "s3://cencosud.exalitica.com/prod/{flag}/metricas/party_subrubro/",
    "prepri_producto_nuevo": "s3://cencosud.exalitica.com/prod/{flag}/metricas/producto_nuevo/",
    "prepri_propension": "s3://cencosud.exalitica.com/prod/{flag}/metricas/propension/",
    "prepri_sensibilidad_precio": "s3://cencosud.exalitica.com/prod/{flag}/metricas/sensibilidad_precio/",
    "prepri_cross_sell": "s3://cencosud.exalitica.com/prod/{flag}/metricas/cross_sell/",
    "prepri_up_sell": "s3://cencosud.exalitica.com/prod/{flag}/metricas/up_sell/",
    "prepriorizacion_filtrada": "s3://cencosud.exalitica.com/prod/{flag}/listado/prepriorizacion_filtrada/",
    "prioriza_cliente": "s3://cencosud.exalitica.com/prod/{flag}/prioriza_cliente/",
    "priorizacion":  "s3://cencosud.exalitica.com/prod/{flag}/priorizacion/",
    "prepriorizacion": "s3://cencosud.exalitica.com/prod/{flag}/prepriorizacion/",
    "listado": "s3://cencosud.exalitica.com/prod/{flag}/listado/",
    "up_sell":  "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/up_sell/",
    "cross_sell":  "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/cross_sell/",
    "sensibilidad_precio":  "s3://cencosud.exalitica.com/prod/{flag}/dinamicas/sensibilidad_precio/",
    "templ":  "s3://cencosud.exalitica.com/prod/{flag}/auxiliares/templ/",
    "mc":  "s3://cencosud.exalitica.com/prod/{flag}/auxiliares/mc/"
}

partition_by = {
    "afinidad_marca": [],
    "afinidad_subclase": [],
    "ciclo_recompra": [],
    "cons_hab_des": [],
    "cross_sell": [],
    "consumo_habitual": [],
    "matriz_de_configuracion": [],
    "objetivo_item": [],
    "party_rubro": [],
    "party_subrubro": [],
    "prepri_consumo_habitual": [],
    "prepri_ciclo_recompra": [],
    "prepri_cons_hab_des": [],
    "prepri_objetivo_item": [],
    "prepri_party_rubro": [],
    "prepri_party_subrubro": [],
    "prepri_producto_nuevo": [],
    "prepri_propension": [],
    "prepri_sensibilidad_precio": [],
    "prepri_cross_sell": [],
    "prepri_up_sell": [],
    "prepriorizacion_filtrada": ['col2', 'col1'],
    "prioriza_cliente": [],
    "priorizacion":  [],
    "prepriorizacion": ['col2', 'col1'],
    "listado": [],
    "sensibilidad_precio": [],
    "up_sell": [],
    "templ": [],
    "mc": []

}
=======
>>>>>>> desacoplar_mop_glue
