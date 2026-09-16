"""
Optional extra 2: audit STEDI's risk scores against an independent calculation.

Consumes both topics at once:

  * `redis-server`  -> the raw RapidStepTest records, from which the fall-risk
                       score is recomputed using the pseudocode in the brief;
  * `stedi-events`  -> the score the STEDI application itself published.

and prints them side by side with a verdict, so the business event can be
checked rather than trusted.

A caveat worth stating, because it shapes how the verdict should be read: STEDI
computes a score at the instant a customer's assessment completes, against the
four tests on file *at that moment*. This script always scores the four most
recent tests. While a customer keeps testing, the two can legitimately disagree
simply because they are looking at different windows -- so the sign agreement
column matters more than exact equality, and a customer who has stopped testing
should converge to an exact match.

Submit with: project/starter/submit-optional-risk-quality.sh
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    abs as spark_abs, col, first, from_json, row_number, signum, unbase64,
    round as spark_round, when
)
from pyspark.sql.types import (
    ArrayType, BooleanType, FloatType, LongType, StringType, StructField, StructType
)

# Both of these live under /home/workspace, the folder docker-compose mounts
# into the master AND the worker, because the driver and the executors run in
# different containers. A /tmp path looks fine until the driver tries to read
# back what an executor wrote and fails with "Unable to infer schema for
# Parquet": each container has its own private /tmp.
STEP_HISTORY = "/home/workspace/spark/tmp/quality-step-history"
SCORE_HISTORY = "/home/workspace/spark/tmp/quality-score-history"

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

stediEventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])

spark = SparkSession.builder.appName("stedi-optional-risk-quality").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def kafka_stream(topic):
    return (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:19092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("cast(value as string) value")
    )


# --------------------------------------------------------------------------
# Stream 1: raw step tests out of the Redis change feed.
# --------------------------------------------------------------------------
stepTestDF = (
    kafka_stream("redis-server")
    .withColumn("value", from_json("value", redisMessageSchema))
    .select(col("value.zSetEntries")[0]["element"].alias("encoded"))
    .withColumn("stepTest", from_json(unbase64(col("encoded")).cast("string"),
                                      rapidStepTestSchema))
    .select(
        col("stepTest.customer.email").alias("email"),
        col("stepTest.startTime").alias("startTime"),
        col("stepTest.stopTime").alias("stopTime")
    )
    .filter(col("email").isNotNull() & col("startTime").isNotNull()
            & col("stopTime").isNotNull())
)

# --------------------------------------------------------------------------
# Stream 2: the scores STEDI published.
# --------------------------------------------------------------------------
reportedScoreDF = (
    kafka_stream("stedi-events")
    .withColumn("value", from_json("value", stediEventsSchema))
    .select(
        col("value.customer").alias("email"),
        col("value.score").alias("reportedScore"),
        col("value.riskDate").alias("riskDate")
    )
    .filter(col("email").isNotNull())
)


def computed_scores():
    """Score every customer with four or more tests from the step-test history."""
    history = spark.read.parquet(STEP_HISTORY).dropDuplicates(["email", "startTime"])
    mostRecentFour = (
        history
        .withColumn("duration", col("stopTime") - col("startTime"))
        .withColumn(
            "rank",
            row_number().over(Window.partitionBy("email").orderBy(col("startTime").desc()))
        )
        .filter(col("rank") <= 4)
    )
    widened = (
        mostRecentFour
        .groupBy("email")
        .pivot("rank", [1, 2, 3, 4])
        .agg(first("duration"))
    )
    return (
        widened
        .filter(col("4").isNotNull())
        .withColumn(
            "computedScore",
            spark_round(
                (((col("3") + col("4")) / 2) - ((col("1") + col("2")) / 2)) / 1000, 2
            )
        )
        .select("email", "computedScore")
    )


def latest_reported():
    """Keep only each customer's most recent published score."""
    scores = spark.read.parquet(SCORE_HISTORY)
    return (
        scores
        .withColumn(
            "rank",
            row_number().over(Window.partitionBy("email").orderBy(col("riskDate").desc()))
        )
        .filter(col("rank") == 1)
        .select("email", "reportedScore", "riskDate")
    )


def collect_step_tests(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.write.mode("append").parquet(STEP_HISTORY)


def audit_scores(batch_df, batch_id):
    """Append this batch of published scores, then compare the two sides."""
    if batch_df.isEmpty():
        return
    batch_df.write.mode("append").parquet(SCORE_HISTORY)

    try:
        computed = computed_scores()
    except Exception:
        # The step-test history has not been written yet on the first batches.
        print("===== batch %d: waiting for step-test history =====" % batch_id)
        return

    comparison = (
        latest_reported()
        .join(computed, "email", "inner")
        .withColumn("delta", spark_round(col("reportedScore") - col("computedScore"), 2))
        .withColumn(
            "verdict",
            when(spark_abs(col("delta")) < 0.01, "MATCH")
            .when(signum(col("reportedScore")) == signum(col("computedScore")),
                  "same direction, different window")
            .otherwise("DISAGREES")
        )
        .select("email", "reportedScore", "computedScore", "delta", "verdict")
        .orderBy("email")
    )

    print("===== batch %d: STEDI score vs independently computed score =====" % batch_id)
    comparison.show(40, truncate=False)


(
    stepTestDF
    .writeStream
    .foreachBatch(collect_step_tests)
    .outputMode("append")
    .option("checkpointLocation", "/home/workspace/spark/checkpoints/optional-quality-steps")
    .start()
)

(
    reportedScoreDF
    .writeStream
    .foreachBatch(audit_scores)
    .outputMode("append")
    .option("checkpointLocation", "/home/workspace/spark/checkpoints/optional-quality-scores")
    .start()
)

spark.streams.awaitAnyTermination()
