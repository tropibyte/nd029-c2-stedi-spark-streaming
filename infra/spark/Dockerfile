# ---------------------------------------------------------------------------
# Drop-in replacement for docker.io/bitnami/spark:3-debian-10
#
# Bitnami deleted every legacy Spark tag from Docker Hub in 2025 (the archive
# namespace bitnamilegacy/spark now only carries 4.0.0-debian-12), so the image
# named in the course docker-compose.yaml can no longer be pulled by anyone.
#
# This image reproduces the two things the course actually depends on:
#   1. the /opt/bitnami/spark prefix, so every submit-*.sh / .cmd script in the
#      starter repo runs verbatim with no edits, and
#   2. the SPARK_MODE=master|worker contract used by docker-compose.yaml.
#
# It additionally bakes the Kafka structured-streaming connector onto the
# default classpath so a run can never be broken by Maven Central being slow,
# unreachable, or shadowed by the bind-mounted (initially empty) Ivy cache.
# ---------------------------------------------------------------------------
FROM apache/spark-py:v3.3.0

USER root

# Must match the Spark version of the base image; the submit scripts pass the
# same coordinate via --packages, which then resolves instantly from the cache.
ARG SPARK_KAFKA_PKG=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0

RUN set -eux; \
    mkdir -p /opt/bitnami; \
    ln -s /opt/spark /opt/bitnami/spark

# Resolve the connector through Spark itself rather than hard-coding transitive
# versions, then promote only the four jars Spark does not already ship. Copying
# the whole Ivy tree would put duplicate/older Hadoop and Scala jars on the
# classpath and break the driver.
RUN set -eux; \
    echo 'print("classpath warm-up")' > /tmp/warm.py; \
    HOME=/root /opt/spark/bin/spark-submit \
        --master 'local[1]' \
        --packages "${SPARK_KAFKA_PKG}" \
        /tmp/warm.py > /tmp/warm.log 2>&1 || { cat /tmp/warm.log; exit 1; }; \
    cp /root/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-*.jar \
       /root/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-*.jar \
       /root/.ivy2/jars/org.apache.kafka_kafka-clients-*.jar \
       /root/.ivy2/jars/org.apache.commons_commons-pool2-*.jar \
       /opt/spark/jars/; \
    rm -rf /tmp/warm.py /tmp/warm.log

# Bitnami set HOME to the Spark prefix, which is what makes ~/.ivy2 land inside
# the repo-mounted ./spark/jars folder. Preserve that behaviour.
ENV HOME=/opt/spark \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    SPARK_LOG_DIR=/home/workspace/spark/logs \
    SPARK_IDENT_STRING=spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

COPY entrypoint.sh /opt/entrypoint-stedi.sh
RUN chmod +x /opt/entrypoint-stedi.sh

# Root, because the daemons write their .out logs into a Windows bind mount.
ENTRYPOINT ["/opt/entrypoint-stedi.sh"]
