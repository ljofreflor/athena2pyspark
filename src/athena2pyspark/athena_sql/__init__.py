'''
Created on 01-12-2017

@author: lnjofre
'''
import os.path
import zipfile
#import pdb


def queryByName(query_file_name, sql_path, args=None):
    '''
    este es un ejemplo
    :param query_file_name:
    :param args: diccionario con los parametros de la query
    '''

    try:
        mop_base = os.path.join(sql_path, query_file_name + ".sql")
        sql_file = open(mop_base, "r").read()
    except IOError:
        zf = zipfile.ZipFile(sql_path)
        sql_file = zf.open(os.path.join(
            sql_path, query_file_name + ".sql")).read()
        pass

    if args is not None:
        sql_file = sql_file.format(**args)

    return sql_file
