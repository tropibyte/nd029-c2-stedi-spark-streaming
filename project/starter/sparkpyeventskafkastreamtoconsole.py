"""
Validation script 2 of 2: customer risk scores from the STEDI business events.

Reads the `stedi-events` Kafka topic, which the STEDI application writes to
whenever a customer completes an assessment and has four or more step tests on
file. Unlike the redis-server topic, this payload is plain JSON with no base64
layer:

    {"customer":"Jason.Mitra@test.com","score":7.0,"riskDate":"2020-09-14T07:54:06.417Z"}

Prints customer + score to the console so the risk side of the join in
sparkpykafkajoin.py can be verified independently.

Submit with: project/starter/submit-event-kafkastreaming.sh
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

# Schema of the Kafka stedi-events topic.
#
# riskDate is deliberately typed as a string rather than DateType: the
# application emits a full ISO-8601 instant ("2020-09-14T07:54:06.417Z"), and
# Spark 3's stricter date parser resolves that to null instead of a date. The
# field is not used downstream, so a string keeps the record faithful without
# introducing a silent null.
stediEventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])

spark = SparkSession.builder.appName("stedi-events-customer-risk").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# startingOffsets=earliest so risk scores emitted before this job started are
# included rather than skipped.
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

# The topic is keyed with a Long by the application, so only the value is cast.
stediEventsStreamingDF = stediEventsRawStreamingDF.selectExpr(
    "cast(value as string) value"
)

# +------------+                 +------------+-----+-----------+
# | value      |    from_json    |    customer|score|   riskDate|
# |{"custom"...|   ----------->  |"sam@tes"...| -1.4| 2020-09...|
# +------------+                 +------------+-----+-----------+
(
    stediEventsStreamingDF
    .withColumn("value", from_json("value", stediEventsSchema))
    .select(col("value.*"))
    .createOrReplaceTempView("CustomerRisk")
)

customerRiskStreamingDF = spark.sql(
    "select customer, score from CustomerRisk where customer is not null"
)

# +--------------------+-----+
# |            customer|score|
# +--------------------+-----+
# |Spencer.Davis@tes...|  8.0|
# +--------------------+-----+
(
    customerRiskStreamingDF
    .writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination()
)
