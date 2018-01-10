'''
Created on 09-01-2018

@author: leonardo.jofre@exalitica.com
'''
from athena2pyspark import job

# este job actualizara la tabla de prepriorizacion en athena
job(branch="prod", flag="jumbo", queryName="prepriorizacion", local=True)
