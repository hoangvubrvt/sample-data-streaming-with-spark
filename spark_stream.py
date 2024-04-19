import logging
import uuid

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructField, StringType, StructType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}; 
     """)
    logging.info("Keyspace created")


def generate_uuid():
    return str(uuid.uuid4())


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            postcode TEXT,
            dob TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
    """)

    logging.info("Table created")


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel('INFO')
        logging.info('Spark connection created successfully!')
    except Exception as e:
        logging.error(f"Spark connection creation failed: {e}")

    return s_conn


def connect_to_kafka(spark_connection):
    try:
        df = spark_connection.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()

        logging.info('Kafka connection created successfully!')
        return df
    except Exception as e:
        logging.error(f"Kafka connection creation failed: {e}")
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        return cluster.connect()
    except Exception as e:
        logging.error(f"Cassandra connection creation failed: {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('gender', StringType(), False),
        StructField('address', StringType(), False),
        StructField('postcode', StringType(), False),
        StructField('email', StringType(), False),
        StructField('username', StringType(), False),
        StructField('dob', StringType(), False),
        StructField('registered_date', StringType(), False),
        StructField('phone', StringType(), False),
        StructField('picture', StringType(), False)
    ])

    generate_uuid_udf = udf(generate_uuid, StringType())

    sel = (spark_df.selectExpr("CAST(value AS STRING)")
           .select(from_json(col('value'), schema).alias('data'))
           .select('data.*')
           .withColumn("id", generate_uuid_udf()))

    return sel


if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            streaming_query = (selection_df.writeStream.format('org.apache.spark.sql.cassandra')
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()
