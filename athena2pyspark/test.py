u'''
Created on 27-11-2017

@author: lnjofre

El problema fundamental del mantenimiento de un programa es que arreglar un defecto tiene una sustancial 
(20-50%) probabilidad de introducir otro. Por tanto, el proceso completo es dar dos pasos adelante y uno hacia atras 
-- Fred Brooks

El testing de componentes puede ser muy efectivo para mostrar la presencia de errores, pero absolutamente inadecuado para demostrar su ausencia
-- Edsger Dijkstra
'''
import unittest
from athena2pyspark import get_dataframe, run_query, get_ddl, run_create_table
from athena2pyspark.config import getLocalSparkSession

spark = getLocalSparkSession()

class Test(unittest.TestCase):

    def test_RunQuery(self):
        s3_output = "s3://leonardo.exalitica.com/boto3/query_1/"
        df = run_query(query = "select * from baul_2 limit 1000", 
                       database = "ljofre", 
                       s3_output = s3_output, 
                       spark=spark)
        return df
    
    def test_GetDDL(self):
        s3_output = "s3://leonardo.exalitica.com/boto3/query_1/"
        file_location = run_query(query = "select * from baul_2 limit 1000", database = "ljofre", s3_output = s3_output, spark=spark)
        df = get_dataframe(file_location, spark=spark)
        ddl_create_database, ddl_create_table = get_ddl(df=df, database="ljofre", table="test_table",s3_input=s3_output)
        return ddl_create_database, ddl_create_table
    
    def test_CreateTable(self):
        """  """
        s3_output = "s3://leonardo.exalitica.com/boto3/query_1/"
        file_location = run_query(query = "select * from baul_2 limit 1000", 
                                  database = "ljofre", 
                                  s3_output = s3_output, 
                                  spark=spark)
        
        df = get_dataframe(file_location, spark=spark)
        _, ddl_create_table = get_ddl(df=df, database="ljofre", table="test_table_2",s3_input=s3_output)
        run_create_table(query = ddl_create_table, database = "ljofre", s3_output=s3_output)