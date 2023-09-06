from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("exportDataApp") \
        .master("local[*]") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

# Directory containing CSV files
directory_path = '/home/alonm/final_app/output_by_date'

# Define HDFS path
hdfs_path = 'hdfs://localhost:8020/spark_3108'

# Read all CSV files in the directory into a single DataFrame
df = spark.read.csv(directory_path + "/*.csv", header=True, inferSchema=True)

# Convert 'purchase_time' to timestamp and extract year, month, and day
df = df.withColumn("purchase_time", F.to_timestamp("purchase_time")) \
        .withColumn("year", F.year("purchase_time")) \
        .withColumn("month", F.month("purchase_time")) \
        .withColumn("day", F.dayofmonth("purchase_time"))

# Cast columns to proper types
df = df.withColumn("product_price", df["product_price"].cast("float"))
df = df.withColumn("total", df["total"].cast("float"))

# Save the DataFrame to HDFS with partitioning
df.write.partitionBy("year", "month", "day").mode("overwrite").parquet(hdfs_path)
