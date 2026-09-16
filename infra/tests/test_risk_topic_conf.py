#!/usr/bin/env python3
"""
Tests for the sink-topic resolution in sparkpykafkajoin.py.

The join reads kafka.riskTopic out of the STEDI application's own
application.conf so the producer and the consumer cannot drift apart. That
makes the parse worth testing: if it silently fell back to the default while
the config said something else, the job would publish to a topic the graph is
not listening to and the graph would simply stay empty.

Run inside the Spark container, where pyspark is importable:

    docker compose exec -T spark python3 \
        /home/workspace/infra/tests/test_risk_topic_conf.py
"""
import glob
import os
import sys
import tempfile

# pyspark is not on the default python3 path in the Apache Spark image -- it
# ships under $SPARK_HOME/python with py4j zipped beside it. Putting those on
# sys.path here keeps the test runnable as plain `python3 <file>` rather than
# needing spark-submit just to exercise a config parser.
SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/spark")
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
for _py4j in glob.glob(os.path.join(SPARK_HOME, "python", "lib", "py4j-*-src.zip")):
    sys.path.insert(0, _py4j)

JOIN_SCRIPT = "/home/workspace/project/starter/sparkpykafkajoin.py"
REAL_CONF = "/home/workspace/stedi-application/application.conf"

# Execute only the part of the join script above the SparkSession, so the
# helpers can be exercised without starting a Spark application.
source = open(JOIN_SCRIPT, encoding="utf-8").read()
namespace = {}
exec(compile(source[:source.index("spark = SparkSession.builder")],
             JOIN_SCRIPT, "exec"), namespace)
risk_topic_from_conf = namespace["risk_topic_from_conf"]

failures = []


def check(label, conf_text, expected):
    if conf_text is None:
        actual = risk_topic_from_conf("/no/such/file.conf")
    else:
        handle, path = tempfile.mkstemp(suffix=".conf")
        os.write(handle, conf_text.encode())
        os.close(handle)
        try:
            actual = risk_topic_from_conf(path)
        finally:
            os.unlink(path)
    ok = actual == expected
    print("  [%s] %-46s -> %r" % ("PASS" if ok else "FAIL", label, actual))
    if not ok:
        failures.append("%s (expected %r)" % (label, expected))


print("sink topic resolution")

actual = risk_topic_from_conf(REAL_CONF)
print("  [%s] %-46s -> %r"
      % ("PASS" if actual == "customer-risk" else "FAIL",
         "the real application.conf", actual))
if actual != "customer-risk":
    failures.append("real application.conf")

check("missing file falls back", None, "customer-risk")

check("riskTopic in another block is ignored",
      'redis{ host=redis\n  riskTopic=WRONG\n}\n'
      'kafka{\n  broker="kafka:19092"\n  riskTopic=my-topic\n}\n',
      "my-topic")

# The reason the block is brace-matched rather than regex-delimited: a pattern
# that stops at the first "}" would never reach riskTopic here.
check("nested block before the key",
      'kafka{\n  ssl { enabled=true\n    truststore { path="/x" }\n  }\n'
      '  riskTopic=nested-topic\n}\n',
      "nested-topic")

check("nested block and a decoy together",
      'redis{ riskTopic=WRONG }\n'
      'kafka{\n  sasl { mechanism=PLAIN }\n  riskTopic = "quoted-topic"\n}\n',
      "quoted-topic")

check("no kafka block falls back", 'redis{ host=redis }\n', "customer-risk")

check("kafka block without riskTopic falls back",
      'kafka{ broker="kafka:19092" }\n', "customer-risk")

check("unbalanced braces fall back rather than hang",
      'kafka{\n  riskTopic=truncated\n', "customer-risk")

print()
if failures:
    print("FAILED: %s" % "; ".join(failures))
    sys.exit(1)
print("All sink-topic resolution checks passed.")
