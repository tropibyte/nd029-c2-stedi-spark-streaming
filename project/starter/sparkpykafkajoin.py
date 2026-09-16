"""
The project deliverable: join customer birth years to customer risk scores and
publish the result for the STEDI risk graph.

    redis-server  --(base64 x2, JSON)-->  emailAndBirthYearStreamingDF
                                                    |
                                                    |  join on email = customer
                                                    v
    stedi-events  --(JSON)----------->    customerRiskStreamingDF
                                                    |
                                                    v
                                      customer-risk topic (JSON)
                                                    |
                                                    v
                                      STEDI risk graph at :4567

The sink topic name is the one declared in stedi-application/application.conf
(kafka.riskTopic=customer-risk), which is mounted into the STEDI container, so
the producer and the consumer are configured from the same file.

Output payload, as consumed by public/risk-graph.js (which plots
customerRisk.birthYear on x and customerRisk.score on y):

    {"customer":"Santosh.Fibonnaci@test.com",
     "score":"28.5",
     "email":"Santosh.Fibonnaci@test.com",
     "birthYear":"1963"}

Submit with: project/starter/submit-event-kafkajoin.sh
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

# ---------------------------------------------------------------------------
# Schemas. Spark cannot infer a schema for a streaming source, so all three are
# declared explicitly.
# ---------------------------------------------------------------------------

# The redis-server envelope published for every Redis write. `value`,
# `expiredType` and `expiredValue` only appear for non-sorted-set operations;
# they parse to null for the ZADD traffic this job cares about.
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

# The base64-decoded sorted-set member, for the Customer key.
customerJSONSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

# The stedi-events business event. riskDate is typed as a string because the
# application emits a full ISO-8601 instant, which Spark 3's stricter date
# parser would resolve to null under DateType.
stediEventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])

spark = SparkSession.builder.appName("stedi-risk-score-by-birth-year").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Streaming dataframe 1: email + birth year, out of the Redis change stream.
# ---------------------------------------------------------------------------

# startingOffsets=earliest matters more here than anywhere else: a customer is
# written to Redis once, at registration. Reading from `latest` would miss every
# existing customer and the join would produce nothing.
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

# Taking the 0th element of an array is much simpler in SQL against a view than
# through the DataFrame API.
zSetEntriesEncodedStreamingDF = spark.sql(
    "select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet"
)

# unbase64 yields binary, so cast back to a string to recover the customer JSON.
zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn(
    "customer", unbase64(zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string")
)

(
    zSetDecodedEntriesStreamingDF
    .withColumn("customer", from_json("customer", customerJSONSchema))
    .select(col("customer.*"))
    .createOrReplaceTempView("CustomerRecords")
)

# STEDI also writes the User and RapidStepTest sorted sets, and both arrive on
# this same topic. They parse to all-null Customer rows, so they are filtered
# out here rather than being carried into the join.
emailAndBirthDayStreamingDF = spark.sql(
    "select email, birthDay from CustomerRecords "
    "where email is not null and birthDay is not null"
)

emailAndBirthYearStreamingDF = (
    emailAndBirthDayStreamingDF
    .withColumn("birthYear", split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0))
    .select("email", "birthYear")
)

# ---------------------------------------------------------------------------
# Streaming dataframe 2: email + risk score, out of the STEDI business events.
# ---------------------------------------------------------------------------

stediEventsRawStreamingDF = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "stedi-events")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

stediEventsStreamingDF = stediEventsRawStreamingDF.selectExpr(
    "cast(value as string) value"
)

(
    stediEventsStreamingDF
    .withColumn("value", from_json("value", stediEventsSchema))
    .select(col("value.*"))
    .createOrReplaceTempView("CustomerRisk")
)

customerRiskStreamingDF = spark.sql(
    "select customer, score from CustomerRisk where customer is not null"
)

# ---------------------------------------------------------------------------
# The join: risk score and birth year in one row, keyed on the email address.
# ---------------------------------------------------------------------------
#
# A stream-stream inner join. Both sides keep their rows in state, so a risk
# score arriving minutes after the customer record still finds its match -- the
# customer is written to Redis at registration, but the first risk score cannot
# exist until four assessments later.
riskScoreByBirthYear = customerRiskStreamingDF.join(
    emailAndBirthYearStreamingDF,
    expr("customer = email")
)

# ---------------------------------------------------------------------------
# Sink to the topic the STEDI graph subscribes to.
# ---------------------------------------------------------------------------
#
# score is cast to a string so the emitted JSON matches the documented contract
# exactly ("score":"28.5"). risk-graph.js coerces both axes with a unary +, so
# the string values plot correctly on the scatter chart.
#
# to_json over a struct of the four columns produces the single `value` column
# that the Kafka sink requires.
riskScoreByBirthYearJSON = riskScoreByBirthYear.select(
    to_json(
        expr("struct(customer, cast(score as string) as score, email, birthYear)")
    ).alias("value")
)

# awaitTermination() is what keeps the application running continuously rather
# than exiting once the first batch is written.
(
    riskScoreByBirthYearJSON
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("topic", "customer-risk")
    .option("checkpointLocation", "/home/workspace/spark/checkpoints/kafkajoin")
    .outputMode("append")
    .start()
    .awaitTermination()
)
