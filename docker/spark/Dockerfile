# PySpark 3.5 with Iceberg, the AWS S3 client and the Kafka source baked in, so jobs start
# without downloading anything and run the same way locally and on a cloud VM.
# Python 3.11 is the newest version PySpark 3.5 officially supports; Debian 12 still ships the
# Java 17 runtime Spark 3.5 is built for.
FROM python:3.11-slim-bookworm

ARG ICEBERG=1.6.1
ARG SPARK=3.5.3
ARG MAVEN=https://repo1.maven.org/maven2

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# pytest is here so the Spark unit tests run in the same image as the jobs (they need Java).
RUN pip install --no-cache-dir pyspark==${SPARK} pytest==8.3.3
RUN cd "$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
 && for jar in \
      org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG}/iceberg-spark-runtime-3.5_2.12-${ICEBERG}.jar \
      org/apache/iceberg/iceberg-aws-bundle/${ICEBERG}/iceberg-aws-bundle-${ICEBERG}.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK}/spark-sql-kafka-0-10_2.12-${SPARK}.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK}/spark-token-provider-kafka-0-10_2.12-${SPARK}.jar \
      org/apache/spark/spark-connect_2.12/${SPARK}/spark-connect_2.12-${SPARK}.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; \
    do curl -fsSLO "${MAVEN}/${jar}"; done

RUN useradd --create-home --uid 1000 spark
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir . && mkdir -p /checkpoints && chown -R spark /app /checkpoints

USER spark
