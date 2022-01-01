"""
This script is a combination of sparkpyrediskafkastreamtoconsole.py and sparkpyeventskafkastreamtoconsole.py
To see more descriptive inline comments, take a look at those scripts
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, FloatType

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

# create a schema that matches the risk score events that are generated by the STEDI application
stediEventSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])

spark = SparkSession.builder.appName("customer-risk-birthdays").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

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

zSetEntriesEncodedStreamingDF = spark.sql("select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet")

zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn("decodedCustomer", unbase64(
    zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string"))

zSetDecodedEntriesStreamingDF \
    .withColumn("customer", from_json("decodedCustomer", customerJSONSchema)) \
    .select(col("customer.*")) \
    .createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark.sql(
    "SELECT email, birthDay FROM CustomerRecords WHERE email IS NOT NULL AND birthDay IS NOT NULL"
)

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email",
    split(
        emailAndBirthDayStreamingDF.birthDay,
        "-"
    ).getItem(0).alias("birthYear")
)


stediRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stedi-events")                  \
    .option("startingOffsets", "earliest")\
    .load()

stediStreamingDF = stediRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

stediStreamingDF.withColumn("value", from_json("value", stediEventSchema)) \
    .select(
#     col("key"), - this is the key for this row
#     col("value"), - this is the JSON containing the data for this row
    col('value.customer'),
    col('value.score'),
    col('value.riskDate'),
).createOrReplaceTempView("CustomerRisk")

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

customerStediRiskAndBirthday = emailAndBirthYearStreamingDF.join(customerRiskStreamingDF, expr( """
   email=customer
"""
))

# this is how you'd write the resulting joined dataframe out to console
# customerStediRiskAndBirthday.writeStream.outputMode("append").format("console").start().awaitTermination()

# sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application
customerStediRiskAndBirthday.selectExpr("cast(customer as string) as key", "to_json(struct(*)) as value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "risk-scores-and-birthdays")\
    .option("checkpointLocation", "/tmp/kafkacheckpoint")\
    .option("failOnDataLoss", "false")\
    .start()\
    .awaitTermination()


# it should look like this:
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
