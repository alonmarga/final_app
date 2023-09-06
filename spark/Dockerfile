FROM bde2020/spark-base:3.3.0-hadoop3.3

ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077

COPY . /

CMD ["/bin/bash", "/submit.sh"]