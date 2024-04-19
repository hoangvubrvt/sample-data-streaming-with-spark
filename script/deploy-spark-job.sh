#!/bin/bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1  --master spark://localhost:7077 ../spark_stream.py                              [10:11:24]
