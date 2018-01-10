'''
Created on 10-01-2018

@author: lnjofre
'''
from athena2pyspark import job

job(branch="prod", flag="jumbo", queryName="prepriorizacion", local=True)
