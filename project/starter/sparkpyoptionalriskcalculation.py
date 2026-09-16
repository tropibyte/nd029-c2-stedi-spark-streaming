"""
Optional extra 1: recompute the fall-risk score in Spark from the raw step tests.

The required project takes STEDI's risk score as given. This script derives it
independently, straight from the RapidStepTest sorted set that arrives on the
same `redis-server` topic, implementing the Java pseudocode from the project
brief:

    currentTestAverageScore  = (d(most recent) + d(2nd most recent)) / 2
    previousTestAverageScore = (d(3rd most recent) + d(4th most recent)) / 2
    riskScore                = (previousTestAverageScore - currentTestAverageScore) / 1000

where d(test) = stopTime - startTime, and a customer with fewer than four tests
on file has no score.

Why foreachBatch
----------------
"The last four tests per customer" is a ranked window, and Structured Streaming
refuses row_number() over an ordered window on a streaming DataFrame. The step
tests are therefore accumulated to a Parquet history and the ranking is done on
that static view once per micro-batch, which also keeps the query plan flat
instead of growing a union lineage batch after batch.

Submit with: project/starter/submit-optional-calculate-score.sh
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, first, from_json, row_number, unbase64, round as spark_round
)
from pyspark.sql.types import (
    ArrayType, BooleanType, LongType, StringType, StructField, StructType
)

# Both of these live under /home/workspace, the folder docker-compose mounts
# into the master AND the worker, because the driver and the executors run in
# different containers. A /tmp path looks fine until the driver tries to read
# back what an executor wrote and fails with "Unable to infer schema for
# Parquet": each container has its own private /tmp.
HISTORY_PATH = "/home/workspace/spark/tmp/step-test-history"

# The redis-server envelope, as published for every Redis write.
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

# A RapidStepTest member, which nests the whole customer record:
# {"startTime":1789584935382,"stopTime":1789585042382,"testTime":107000,
#  "totalSteps":30,"customer":{"customerName":...,"email":...,"birthDay":...}}
rapidStepTestSchema = StructType([
    StructField("startTime", LongType()),
    StructField("stopTime", LongType()),
    StructField("testTime", LongType()),
    StructField("totalSteps", LongType()),
    StructField("customer", StructType([
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]))
])

spark = SparkSession.builder.appName("stedi-optional-risk-calculation").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

rawDF = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Envelope -> base64 member -> RapidStepTest JSON.
stepTestDF = (
    rawDF
    .selectExpr("cast(value as string) value")
    .withColumn("value", from_json("value", redisMessageSchema))
    .select(col("value.zSetEntries")[0]["element"].alias("encoded"))
    .withColumn("stepTest", from_json(unbase64(col("encoded")).cast("string"),
                                      rapidStepTestSchema))
    .select(
        col("stepTest.customer.email").alias("email"),
        col("stepTest.startTime").alias("startTime"),
        col("stepTest.stopTime").alias("stopTime")
    )
    # Customer and User records also land on this topic; they have no startTime.
    .filter(col("email").isNotNull() & col("startTime").isNotNull()
            & col("stopTime").isNotNull())
)


def score_batch(batch_df, batch_id):
    """Append this micro-batch to history, then rescore every customer."""
    if batch_df.isEmpty():
        return

    batch_df.write.mode("append").parquet(HISTORY_PATH)

    history = spark.read.parquet(HISTORY_PATH).dropDuplicates(["email", "startTime"])

    # Rank each customer's tests newest first and keep the most recent four.
    mostRecentFour = (
        history
        .withColumn("duration", col("stopTime") - col("startTime"))
        .withColumn(
            "rank",
            row_number().over(Window.partitionBy("email").orderBy(col("startTime").desc()))
        )
        .filter(col("rank") <= 4)
    )

    # One row per customer, with the four durations in their own columns.
    widened = (
        mostRecentFour
        .groupBy("email")
        .pivot("rank", [1, 2, 3, 4])
        .agg(first("duration"))
    )

    # A fourth test is what makes a score possible at all, so its absence is the
    # "fewer than four rapid step tests on file" case from the brief.
    scored = (
        widened
        .filter(col("4").isNotNull())
        .withColumn("currentAverage", (col("1") + col("2")) / 2)
        .withColumn("previousAverage", (col("3") + col("4")) / 2)
        .withColumn(
            "computedScore",
            spark_round((col("previousAverage") - col("currentAverage")) / 1000, 2)
        )
        .select("email", "currentAverage", "previousAverage", "computedScore")
        .orderBy("email")
    )

    print("===== batch %d: risk recomputed from RapidStepTest =====" % batch_id)
    scored.show(40, truncate=True)


(
    stepTestDF
    .writeStream
    .foreachBatch(score_batch)
    .outputMode("append")
    .option("checkpointLocation", "/home/workspace/spark/checkpoints/optional-risk-calculation")
    .start()
    .awaitTermination()
)
