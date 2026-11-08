FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Tai connector ngay luc build de luc do E3 khong phu thuoc Maven Central.
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.repositories', 'https://maven-central.storage-download.googleapis.com/maven2/') \
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4') \
.getOrCreate().stop()"

COPY config ./config
COPY src ./src
COPY experiments ./experiments
COPY deploy/e3-azure ./deploy/e3-azure

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1
