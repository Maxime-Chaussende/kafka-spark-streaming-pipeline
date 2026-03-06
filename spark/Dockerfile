FROM spark:3.5.0-scala2.12-java17-python3-ubuntu

USER root

RUN mkdir -p /tmp/ivy /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chmod -R 777 /tmp/ivy /home/spark/.ivy2

WORKDIR /app

COPY spark_job.py .

CMD ["sh", "-c", "sleep 20 && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.jars.ivy=/tmp/ivy /app/spark_job.py"]