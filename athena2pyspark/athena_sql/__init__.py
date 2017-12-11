'''
Created on 01-12-2017

@author: lnjofre
'''
import os.path 

def querybyByName(query_file_name, args = None):
    '''
    este es un ejemplo
    :param query_file_name:
    :param args: diccionario con los parametros de la query
    '''
    mop_base = os.path.join(os.path.dirname(__file__), "mop-glue", query_file_name + '.sql')    
    

    sql_file = open(mop_base, "r").read()
    
    if args is not None:
        sql_file = sql_file.format(args)
