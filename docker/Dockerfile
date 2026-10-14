# Image dùng chung cho ws-producer, raw-archive, stream-candles (và chạy batch nếu muốn).
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
RUN [ -d "$JAVA_HOME" ] || ln -s /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17-openjdk-arm64

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Tải sẵn connector Kafka cho Spark để container khởi động không cần mạng tới Maven
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]').config('spark.jars.packages', \
'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4').getOrCreate().stop()"

COPY config ./config
COPY src ./src
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1
