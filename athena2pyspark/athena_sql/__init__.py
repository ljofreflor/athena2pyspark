'''
Created on 01-12-2017

@author: lnjofre
'''
import os.path
import zipfile
import pdb 



def querybyByName(query_file_name, args = None):
    '''
    este es un ejemplo
    :param query_file_name:
    :param args: diccionario con los parametros de la query
    '''
    
    project_base = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    
    try:       
        mop_base = os.path.join(project_base, "athena2pyspark", "athena_sql", "mop-glue", query_file_name + ".sql")
        sql_file = open(mop_base, "r").read()
    except IOError:
        pdb.set_trace()
        zf = zipfile.ZipFile(mop_base)
        print mop_base
        sql_file = zf.open(os.path.join("athena2pyspark/athena_sql/mop-glue", query_file_name + ".sql"))
        pass  
    
    if args is not None:
        sql_file = sql_file.format(args)
        
    return sql_file
