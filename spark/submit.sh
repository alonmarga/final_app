export SPARK_MASTER_URL=spark://spark-master:7077

SPARK_PYTHON=python3  spark-submit \
            --master ${SPARK_MASTER_URL} \
            ./spark.py