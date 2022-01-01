from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# create a StructType for the Kafka redis-server topic which has all changes made to Redis
redisMessageSchema = StructType([
    StructField("key", StringType()),
    StructField("value", StringType()),
    StructField("expiredType", StringType()),
    StructField("expiredValue", StringType()),
    StructField("existType", StringType()),
    StructField("ch", StringType()),
    StructField("incr", BooleanType()),
    StructField("zSetEntries", ArrayType(StructType([
        StructField("element", StringType()),
        StructField("score", StringType())
    ])))
])

# create a StructType for the Customer JSON that comes from Redis
customerJSONSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType()),
])

# create a spark application object
spark = SparkSession.builder.appName("customer-birthday").getOrCreate()
# set the spark log level to WARN
spark.sparkContext.setLogLevel('WARN')

# using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# set startingOffsets to earliest - this includes all events from the topic, including those that were
# published before you started the spark stream
redisServerRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# cast the value column in the streaming dataframe as a STRING
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

"""
The redisServerStreamingDF dataframe has a column named "value", whose entries are JSON-like strings. They look like this:
+------------+
| value      |
+------------+
|{"key":"Q3..|
+------------+

Those JSON objects have the following format:
{
  "key":"Q3VzdG9tZXI=",
  "existType":"NONE",
  "Ch":false,
  "Incr":false,
  "zSetEntries":[{
  "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
  "Score":0.0
  }],
  "zsetEntries":[{
  "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
  "score":0.0
  }]
}

(Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)

Parse those JSON fields out into separte columns, having the following schema:
+------------+-----+-----------+------------+---------+-----+-----+-----------------+
|         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
+------------+-----+-----------+------------+---------+-----+-----+-----------------+
|U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
+------------+-----+-----------+------------+---------+-----+-----+-----------------+

Then store them in a temporary view called RedisSortedSet
"""
redisServerStreamingDF.withColumn("value", from_json("value", redisMessageSchema)) \
    .select(
    col("key"),
    col("value"),
    col('value.existType'),
    col('value.Ch'),
    col('value.Incr'),
    col('value.zSetEntries'),
) \
    .createOrReplaceTempView("RedisSortedSet")

# execute a sql statement against a temporary view, which statement takes the "element" key from the 0th element
# in the arrays of the zSetEntries column and create a column called encodedCustomer
# it's easier to select the nth element of rows that have arrays in them using SparkSQL syntax
zSetEntriesEncodedStreamingDF = spark.sql("select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet")

# Create a new column, called decodedCustomer that is the base64 DE-CODED version of the encodedCustomer column, then
# cast it as a string
zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn("decodedCustomer", unbase64(
    zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string"))

# parse the JSON-like strings in decodedCustomer into separate columns
# and create a temporary view called CustomerRecords
zSetDecodedEntriesStreamingDF \
    .withColumn("customer", from_json("decodedCustomer", customerJSONSchema)) \
    .select(col("customer.*")) \
    .createOrReplaceTempView("CustomerRecords")

# create a dataframe selecting emails and birthdays from the CustomerRecords table
# where email and birthday are not null
emailAndBirthDayStreamingDF = spark.sql(
    "SELECT email, birthDay FROM CustomerRecords WHERE email IS NOT NULL AND birthDay IS NOT NULL"
)

# create a dataframe containing just the birth YEAR (using the split function) and the customers email
# then sink that stream to console output in append mode
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email",
    split(
        emailAndBirthDayStreamingDF.birthDay,
        "-"
    ).getItem(0).alias("birthYear")) \
    .writeStream.outputMode("append").format("console").start().awaitTermination()

"""
The output should look like this:
+--------------------+-----               
| email         |birthYear|
+--------------------+-----
|Gail.Spencer@test...|1963|
|Craig.Lincoln@tes...|1962|
|  Edward.Wu@test.com|1961|
|Santosh.Phillips@...|1960|
|Sarah.Lincoln@tes...|1959|
|Sean.Howard@test.com|1958|
|Sarah.Clark@test.com|1957|
+--------------------+-----

Run the python script by running the command from the terminal:
/home/workspace/submit-redis-kafka-streaming.sh
Verify the data looks correct
"""
