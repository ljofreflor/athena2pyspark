
from pyspark.ml.linalg import Vectors
from pyspark_knn.ml.classification import KNNClassifier

from athena2pyspark.config import get_spark_session


u'''
Created on 27-11-2017

@author: lnjofre

El problema fundamental del mantenimiento de un programa es que arreglar un defecto tiene una sustancial 
(20-50%) probabilidad de introducir otro. Por tanto, el proceso completo es dar dos pasos adelante y uno hacia atras 
-- Fred Brooks

El testing de componentes puede ser muy efectivo para mostrar la presencia de errores, pero absolutamente inadecuado para demostrar su ausencia
-- Edsger Dijkstra
'''

args = {'flag': 'jumbo', 'mode': 'local'}
spark = get_spark_session(aws_access_key_id="AKIAJLLN4ZKCDQ2R4UCQ",
                          aws_secret_access_key="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",
                          args=args)

training = spark.createDataFrame([
    [Vectors.dense([0.2, 0.9]), 0.0],
    [Vectors.dense([0.2, 1.0]), 0.0],
    [Vectors.dense([0.2, 0.1]), 1.0],
    [Vectors.dense([0.2, 0.2]), 1.0],
], ['features', 'label'])

test = spark.createDataFrame([
    [Vectors.dense([0.1, 0.0])],
    [Vectors.dense([0.3, 0.8])]
], ['features'])

knn = KNNClassifier(k=1, topTreeSize=1, topTreeLeafSize=1,
                    subTreeLeafSize=1, bufferSizeSampleSize=[1, 2, 3])  # bufferSize=-1.0,

model = knn.fit(training)

pass
