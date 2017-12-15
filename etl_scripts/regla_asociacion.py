'''
Created on 14-12-2017

@author: lnjofre
'''
from athena2pyspark.config import getLocalSparkSession
from pyspark.ml.fpm import FPGrowth

spark = getLocalSparkSession()

df = spark.createDataFrame([
    (0, [1, 2, 5]),
    (1, [1, 2, 3, 5]),
    (2, [1, 2])
], ["id", "items"])

fpGrowth = FPGrowth(itemsCol="items", minSupport=0.5, minConfidence=0.6)

model = fpGrowth.fit(df)

model.freqItemsets.show()

model.associationRules.show()

model.transform(df).show()
