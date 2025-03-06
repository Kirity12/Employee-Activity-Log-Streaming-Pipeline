from cassandra.cluster import Cluster, Session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, unix_timestamp, hour, from_json, when, split
from pyspark.sql.types import (
    IntegerType,
    StringType,
    BooleanType,
    StructField,
    StructType,
)
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType
import ipaddress
import os
import logging


logging.basicConfig(
    filename="spark-logs/spark_logs.log",  # Log file path
    level=logging.INFO,  # Set the log level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
)

try:
    os.makedirs("data")
except OSError as e:
    pass

try:
    os.makedirs("checkpoint")
except OSError as e:
    pass

""" 
# spark-submit --master spark:localhost:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 .\spark_stream.py
# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 spark_stream.py

{"user_data":{"index":254,"Login Timestamp":"2020-02-03 12:47:40.149","User ID":-3265178323424568512,"Round-Trip Time [ms]":"\"NaN\"","IP Address":"193.234.39.57","Country":"NO","Region":"-","City":"-","ASN":50989,"User Agent String":"Mozilla/5.0  (X11; CrOS armv7l 5978.98.0) AppleWebKit/537.36 (KHTML, like Gecko Chrome/69.0.3497.17.19.95 Safari/537.36","Browser Name and Version":"Chrome 69.0.3497.17.19","OS Name and Version":"Chrome OS 5978.98.0","Device Type":"desktop","Login Successful":true,"Is Attack IP":false,"Is Account Takeover":false}}

data = {'index': 14999,
        'Login Timestamp': '2020-02-03 15:41:05.461', 
        'User ID': -2705667229215491307, 
        'Round-Trip Time [ms]': nan, 
        'IP Address': '51.175.44.10', 
        'Country': 'NO', 
        'Region': 'Viken', 
        'City': 'Fredrikstad', 
        'ASN': 29695, 
        'User Agent String': 'Mozilla/5.0  (X11; CrOS x86_64 13505.73.0) AppleWebKit/537.36 (KHTML, like Gecko Chrome/72.0.3626.63.109 Safari/537.36', 
        'Browser Name and Version': 'Chrome 72.0.3626.63', 
        'OS Name and Version': 'Chrome OS 13505.73.0', 
        'Device Type': 'desktop', 
        'Login Successful': True, 
        'Is Attack IP': False, 
        'Is Account Takeover': False}
"""


def create_spark_connection():
    s_conn = None
    try:
        s_conn: SparkSession = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            )
            .config("spark.cassandra.connection.host", "cassandra:9042")
            .getOrCreate()
        )
        s_conn.sparkContext.setLogLevel("INFO")
        logging.info("Spark Submit Application: Spark connection created successfully!")
    except Exception as e:
        logging.error(
            f"Spark Submit Application: Couldn't create the spark session due to exception {e}"
        )

    return s_conn


def create_keyspace(session: Session):
    logging.info(f"Spark Submit Application: Cassandra Creating Table")
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """
        )
        logging.info("Spark Submit Application: Keyspace created successfully!")
    except Exception as e:
        logging.info(
            f"Spark Submit Application: Session did not create keyspace because: {e}"
        )


def create_table(session: Session):
    logging.info(f"Spark Submit Application: Cassandra Creating Table")
    try:
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS spark_streams.users_created (
                                                                    user_id bigint,
                                                                    login_hour int,
                                                                    ip_address int,
                                                                    country text,
                                                                    asn int,
                                                                    user_agent_string text,
                                                                    device_type text,
                                                                    login_successful boolean, 
                                                                    is_attack_ip boolean,     
                                                                    is_account_takeover int,
                                                                    browser_name text,
                                                                    os_name text,
                                                                    PRIMARY KEY (user_id, login_hour)
                                                                );

            """
        )
        print("User Login data table created successfully!")
    except Exception as e:
        logging.error(
            f"Spark Submit Application: Session did not create table because: {e}"
        )


def connect_to_kafka(spark_conn: SparkSession):
    spark_df = None
    logging.info(f"Spark Submit Application: Initializing Kafka Connection")
    try:
        spark_df: DataFrame = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            # .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "users_created")
            .option("startingOffsets", "earliest")
            .load()
        )
        spark_df.printSchema()
        logging.info(
            "Spark Submit Application: Kafka connected and DataFrame created successfully"
        )
    except Exception as e:
        logging.warning(
            f"Spark Submit Application: Kafka DataFrame could not be created because: {e}"
        )

    return spark_df


def create_cassandra_connection():
    cas_session = None
    logging.info(f"Spark Submit Application: Initializing Cassandra Connection")
    try:
        cluster = Cluster(["cassandra"], load_balancing_policy=None)
        cas_session = cluster.connect()
        logging.info(
            f"Spark Submit Application: Session Created and Cassandra connected"
        )
        return cas_session
    except Exception as e:
        logging.error(
            f"Spark Submit Application: Could not create Cassandra connection due to {e}"
        )
        return None


def process_spark_df(df: DataFrame):
    logging.info(f"Spark Submit Application: Starting to Process Spark Kafka DF")
    try:
        schema = StructType(
            [
                StructField("index", IntegerType(), True),
                StructField("Login Timestamp", StringType(), True),
                StructField("User ID", StringType(), True),
                StructField("Round-Trip Time [ms]", StringType(), True),
                StructField("IP Address", StringType(), True),
                StructField("Country", StringType(), True),
                StructField("Region", StringType(), True),
                StructField("City", StringType(), True),
                StructField("ASN", IntegerType(), True),
                StructField("User Agent String", StringType(), True),
                StructField("Browser Name and Version", StringType(), True),
                StructField("OS Name and Version", StringType(), True),
                StructField("Device Type", StringType(), True),
                StructField("Login Successful", BooleanType(), True),
                StructField("Is Attack IP", BooleanType(), True),
                StructField("Is Account Takeover", BooleanType(), True),
            ]
        )

        wrapped_data = df.select(
            from_json(col("value").cast("string"), schema).alias("user_data")
        )

        data = wrapped_data.select(
            "user_data.index",
            "user_data.Login Timestamp",
            "user_data.User ID",
            "user_data.Round-Trip Time [ms]",
            "user_data.IP Address",
            "user_data.Country",
            "user_data.Region",
            "user_data.City",
            "user_data.ASN",
            "user_data.User Agent String",
            "user_data.Browser Name and Version",
            "user_data.OS Name and Version",
            "user_data.Device Type",
            "user_data.Login Successful",
            "user_data.Is Attack IP",
            "user_data.Is Account Takeover",
        )

        # Step 1: Calculate the exact hour of the day (convert Login Timestamp to hour)
        data = data.withColumn(
            "Login Hour",
            hour(
                unix_timestamp(col("Login Timestamp"), "yyyy-MM-dd HH:mm:ss.SSS").cast(
                    "timestamp"
                )
            ),
        )
        # Step 2: Converting Booleans to Integers (True=1, False=0)
        data = data.withColumn(
            "Is Account Takeover", col("Is Account Takeover").cast(IntegerType())
        )
        data = data.withColumn("Is Attack IP", col("Is Attack IP").cast(IntegerType()))
        data = data.withColumn(
            "Login Successful", col("Login Successful").cast(IntegerType())
        )

        data = data.withColumn(
            "Is Account Takeover",
            when(col("Is Account Takeover").isNull(), True).otherwise(
                col("Is Account Takeover").cast(BooleanType())
            ),
        )
        data = data.withColumn(
            "Login Successful",
            when(col("Login Successful").isNull(), True).otherwise(
                col("Login Successful").cast(BooleanType())
            ),
        )
        data = data.withColumn(
            "Is Attack IP",
            when(col("Is Attack IP").isNull(), False).otherwise(
                col("Is Attack IP").cast(BooleanType())
            ),
        )

        data = data.withColumn(
            "Browser Name",
            when(
                col("Browser Name and Version").isNotNull(),
                split(col("Browser Name and Version"), " ")[0],
            ).otherwise("Other"),
        )
        data = data.withColumn(
            "OS Name",
            when(
                col("OS Name and Version").isNotNull(),
                split(col("OS Name and Version"), " ")[0],
            ).otherwise("Other"),
        )

        # Step 3: Dropping unneeded columns
        data = data.drop(
            "Round-Trip Time [ms]",
            "Region",
            "City",
            "index",
            "Login Timestamp",
            "Browser Name and Version",
            "OS Name and Version",
        )

        # # Step 4: Converting Strings to Integers (Using StringIndexer for categorical features)

        # indexer_user_agent = StringIndexer(inputCol="User Agent String", outputCol="User Agent String Index")
        # indexer_browser = StringIndexer(inputCol="Browser Name and Version", outputCol="Browser Name and Version Index")
        # indexer_os = StringIndexer(inputCol="Browser Name", outputCol="Browser Name Index")
        # indexer_browser = StringIndexer(inputCol="OS Name", outputCol="OS Name Index")

        # # Apply the indexers
        # data = indexer_user_agent.fit(data).transform(data)
        # data = indexer_browser.fit(data).transform(data)
        # data = indexer_os.fit(data).transform(data)
        # pipeline = Pipeline(stages=[indexer_os, indexer_browser])

        # # Fit and transform the data
        # data = pipeline.fit(data).transform(data)

        # data = data.drop("Browser Name", "OS Name")

        # Step 5: Converting IP Addresses to Integers
        def ip_to_int(ip):
            return int(ipaddress.ip_address(ip))

        ip_to_int_udf = udf(ip_to_int, LongType())
        data = data.withColumn("IP Address", ip_to_int_udf(col("IP Address")))

        data = data.select(
            col("User ID").cast("bigint").alias("user_id"),  # Convert User ID to bigint
            col("Login Hour").alias(
                "login_hour"
            ),  # Extract hour from the Login Timestamp
            col("IP Address").alias("ip_address"),  #  integer
            col("Country").alias("country"),  # Keep country as text
            col("ASN").alias("asn"),  #
            col("User Agent String").alias(
                "user_agent_string"
            ),  # Keep User Agent String as text
            col("Device Type").alias("device_type"),  # Keep Device Type as text
            col("Login Successful").alias("login_successful"),  #  boolean
            col("Is Attack IP").alias("is_attack_ip"),  #  to boolean
            col("Is Account Takeover").alias("is_account_takeover"),  #  to int
            col("Browser Name").alias(
                "browser_name"
            ),  # Keep Browser Name and Version as text
            col("OS Name").alias("os_name"),  # Keep OS Name and Version as text
        )

        data = data.select(
            col("user_id").cast("bigint").alias("user_id"),  # Cast user_id to bigint
            col("login_hour").cast("int").alias("login_hour"),
            col("ip_address").cast("int").alias("ip_address"),  # Cast ip_address to int
            col("country").cast("string").alias("country"),
            col("asn").cast("int").alias("asn"),
            col("user_agent_string").cast("string").alias("user_agent_string"),
            col("device_type").cast("string").alias("device_type"),
            col("login_successful").cast("boolean").alias("login_successful"),
            col("is_attack_ip").cast("boolean").alias("is_attack_ip"),
            col("is_account_takeover").cast("int").alias("is_account_takeover"),
            col("browser_name").cast("string").alias("browser_name"),
            col("os_name").cast("string").alias("os_name"),
        )

        logging.info(f"Spark Submit Data Processed successfully")
        return data

    except Exception as e:
        logging.error(f"Spark Submit Application: Could not process data due to: {e}")


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:

        # Connect to Kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = process_spark_df(spark_df)

        # streaming_query = selection_df.writeStream \
        #                             .format("json") \
        #                             .trigger(processingTime="10 seconds") \
        #                             .option("checkpointLocation", "checkpoint/") \
        #                             .option("path", "data/") \
        #                             .outputMode("append") \
        #                             .start()
        # streaming_query.awaitTermination()

        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            print(
                "========================================XXXXXXXXX========================================"
            )
            print("Streaming is being started...")
            print(
                "========================================XXXXXXXXX========================================"
            )

            try:
                streaming_query = (
                    selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .option("keyspace", "spark_streams")
                    .option("table", "users_created")
                    .start()
                )
                logging.info("Streaming query started successfully!")
            except Exception as e:
                logging.error(f"Streaming query failed due to: {e}")
            streaming_query.awaitTermination()
