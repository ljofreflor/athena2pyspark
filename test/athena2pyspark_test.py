'''
Created on 27-11-2017

@author: lnjofre
'''
import unittest
from athena2pyspark import get_dataframe, run_query, get_ddl


class Test(unittest.TestCase):


    def testRunQuery_00001(self):
        s3_output = "s3://leonardo.exalitica.com/boto3/query_1/"
        df = run_query(query = "select * from baul_2 limit 1000", database = "ljofre", s3_output = s3_output)
        return df
    
    def testGetDDL(self):
        s3_output = "s3://leonardo.exalitica.com/boto3/query_1/"
        df = run_query(query = "select * from baul_2 limit 1000", database = "ljofre", s3_output = s3_output)
        ddl_create_database, ddl_create_table = get_ddl(df=df, database="ljofre", table="test_table",s3_input=s3_output)
        return ddl_create_database, ddl_create_table


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()