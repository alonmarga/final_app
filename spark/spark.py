import sys
from operator import add

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("exportDataApp")\
        .master("local[*]")\
        .config("hive.metastore.uris", "thrift://localhost:9083")\
        .config("spark.sql.warehouse.dir","/users/hive/warehouse")\
        .enableHiveSupport()\
        .getOrCreate()

df = spark.sql("select * from new_test1111")
df.show()
spark.stop()