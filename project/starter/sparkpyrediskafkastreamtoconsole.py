"""
Validation script 1 of 2: customer birth years from the Redis change stream.

Reads the `redis-server` Kafka topic, which carries every write made to Redis,
and unwraps the two layers of encoding the Redis source connector applies:

    Kafka value (string)
      -> JSON envelope  {key, existType, ch, incr, zSetEntries[...]}
           key                     base64 of the Redis key name ("Customer")
           zSetEntries[0].element  base64 of the sorted-set member
             -> customer JSON      {customerName, email, phone, birthDay}

and prints email + birth year to the console so the data can be eyeballed
before the join in sparkpykafkajoin.py is trusted.

Every Redis sorted set STEDI touches arrives on this topic, not just `Customer`
-- `User` and `RapidStepTest` rows land here too. Those parse into a Customer
row of all-nulls, which is why the null filter below is load-bearing rather
than merely defensive.

Submit with: project/starter/submit-redis-kafka-streaming.sh
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# Schema of the Kafka redis-server topic. Spark cannot infer a schema for a
# streaming source, so the envelope the connector publishes is declared in full,
# including the fields that only appear for non-sorted-set writes (value,
# expiredType, expiredValue) -- they parse to null for ZADD traffic.
redisMessageSchema = StructType([
    StructField("key", StringType()),
    StructField("value", StringType()),
    StructField("expiredType", StringType()),
    StructField("expiredValue", StringType()),
    StructField("existType", StringType()),
    StructField("ch", BooleanType()),
    StructField("incr", BooleanType()),
    StructField("zSetEntries", ArrayType(
        StructType([
            StructField("element", StringType()),
            StructField("score", StringType())
        ])
    ))
])

# Schema of the base64-decoded sorted-set member for the Customer key.
customerJSONSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

spark = SparkSession.builder.appName("stedi-redis-customer-birthyear").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# startingOffsets=earliest so customers created before this job started are
# included; without it the stream only sees customers registered from now on.
redisServerRawStreamingDF = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Kafka hands back key/value as binary; only the value carries the payload.
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr(
    "cast(key as string) key",
    "cast(value as string) value"
)

# +------------+                +------------+-----+---------+-----------------+
# | value      |   from_json    |         key|value|existType|      zSetEntries|
# |{"key":"Q3..|  ----------->  |U29ydGVkU2V0| null|     NONE|[[dGVzdDI=, 0.0]]|
# +------------+                +------------+-----+---------+-----------------+
(
    redisServerStreamingDF
    .withColumn("value", from_json("value", redisMessageSchema))
    .select(col("value.*"))
    .createOrReplaceTempView("RedisSortedSet")
)

# Selecting the 0th array element is far easier in SQL against a view than with
# the DataFrame API, which is why the starter routes through a temp view here.
zSetEntriesEncodedStreamingDF = spark.sql(
    "select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet"
)

# unbase64 returns binary, so it is cast back to a string to recover the JSON:
# [7B 22 73 74 61 7...]  ->  {"customerName":"...","email":"..."}
zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn(
    "customer", unbase64(zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string")
)

(
    zSetDecodedEntriesStreamingDF
    .withColumn("customer", from_json("customer", customerJSONSchema))
    .select(col("customer.*"))
    .createOrReplaceTempView("CustomerRecords")
)

# Rows sourced from the User and RapidStepTest sorted sets parse to all-nulls
# against customerJSONSchema, so they are discarded here.
emailAndBirthDayStreamingDF = spark.sql(
    "select email, birthDay from CustomerRecords "
    "where email is not null and birthDay is not null"
)

# birthDay arrives as yyyy-MM-dd; the graph plots the year alone.
emailAndBirthYearStreamingDF = (
    emailAndBirthDayStreamingDF
    .withColumn("birthYear", split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0))
    .select("email", "birthYear")
)

# +--------------------+---------+
# |               email|birthYear|
# +--------------------+---------+
# |Gail.Spencer@test...|     1963|
# +--------------------+---------+
(
    emailAndBirthYearStreamingDF
    .writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination()
)
