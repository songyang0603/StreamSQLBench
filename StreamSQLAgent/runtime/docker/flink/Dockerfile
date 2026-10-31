ARG FLINK_BASE_IMAGE=flink:1.20.5-scala_2.12-java17
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG KAFKA_CONNECTOR_SHA256=b860724fd97d7da0901bfc3859680442f4c1aa2b35ab944407558b393a1cf93e
FROM ${FLINK_BASE_IMAGE}
ARG KAFKA_CONNECTOR_SHA256
COPY vendor/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/flink-sql-connector-kafka.jar
RUN echo "${KAFKA_CONNECTOR_SHA256}  /opt/flink/lib/flink-sql-connector-kafka.jar" \
    | sha256sum --check --strict
