from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, from_json
from pyspark.sql.types import IntegerType
import ipaddress

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaCassandraIntegration") \
                            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
                            .config("spark.jars", "/path/to/jars") \
                            .config("spark.cassandra.connection.host", "localhost") \
                            .getOrCreate()

# Load the data (assuming it's in CSV format)
data = spark.read.csv("your_data.csv", header=True, inferSchema=True)

# Step 1: Calculate the exact hour of the day (convert Login Timestamp to hour)
data = data.withColumn('Login Hour', hour(unix_timestamp('Login Timestamp', 'yyyy-MM-dd HH:mm:ss').cast('timestamp')))

# Step 2: Converting Booleans to Integers (True=1, False=0)
data = data.withColumn('Is Account Takeover', col('Is Account Takeover').cast(IntegerType()))
data = data.withColumn('Is Attack IP', col('Is Attack IP').cast(IntegerType()))
data = data.withColumn('Login Successful', col('Login Successful').cast(IntegerType()))

# Step 3: Dropping unneeded columns
data = data.drop("Round-Trip Time [ms]", "Region", "City", "Login Timestamp", "index")

# Step 4: Converting Strings to Integers (Using StringIndexer for categorical features)
from pyspark.ml.feature import StringIndexer

indexer_user_agent = StringIndexer(inputCol="User Agent String", outputCol="User Agent String Index")
indexer_browser = StringIndexer(inputCol="Browser Name and Version", outputCol="Browser Name and Version Index")
indexer_os = StringIndexer(inputCol="OS Name and Version", outputCol="OS Name and Version Index")

# Apply the indexers
data = indexer_user_agent.fit(data).transform(data)
data = indexer_browser.fit(data).transform(data)
data = indexer_os.fit(data).transform(data)

# Step 5: Converting IP Addresses to Integers
def ip_to_int(ip):
    return int(ipaddress.ip_address(ip))

# Register a UDF for IP conversion
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

ip_to_int_udf = udf(ip_to_int, LongType())
data = data.withColumn('IP Address', ip_to_int_udf(col('IP Address')))