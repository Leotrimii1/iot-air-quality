FROM apache/spark:3.5.1

USER root
ENV CASS_DRIVER_NO_CYTHON=1
RUN pip install --no-cache-dir --prefer-binary cassandra-driver scikit-learn

WORKDIR /app
COPY src/processor/ /app/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app"
ENV PATH="/opt/spark/bin:${PATH}"

# Spark packages for Kafka and Cassandra
# Note: Match the Spark image version to avoid package resolution issues
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1", \
     "processor.py"]
