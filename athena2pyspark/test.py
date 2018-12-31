
from athena2pyspark.config import get_spark_session

spark = get_spark_session(args={'mode': 'local'}, profile="default")
pass
