# Evaluate Human Balance with Spark Streaming — solution

The STEDI risk graph was empty because nothing was publishing to the topic it
subscribes to. This project fills that gap: a Spark Structured Streaming
application joins the customer records flowing out of Redis to the risk scores
published by STEDI, and writes the combined record to a new Kafka topic that the
graph consumes.

![Data flow](screenshots/data-flow-diagram.png)

## The two encodings

The difficulty of this project is that the two inputs are encoded very
differently.

**`stedi-events`** is plain JSON — parse and go:

```json
{"customer":"Eric.Ahmed@test.com","score":-4.5,"riskDate":"2026-09-16T19:00:08.632Z"}
```

**`redis-server`** carries every write made to Redis, wrapped twice. The Redis
key name is base64, and the sorted-set member is a base64-encoded JSON document:

```json
{"key":"Q3VzdG9tZXI=","existType":"NONE","ch":false,"incr":false,
 "zSetEntries":[{"element":"eyJjdXN0b21lck5hbWUiOiJTdGVhZHkgU2VuaW9yIiwi...","score":0.0}],
 "zsetEntries":[{"element":"eyJjdXN0b21lck5hbWUiOiJTdGVhZHkgU2VuaW9yIiwi...","score":0.0}]}
```

`Q3VzdG9tZXI=` decodes to `Customer`, and the element decodes to
`{"customerName":"Steady Senior","email":"steady@stedi.fit","phone":"8015551212","birthDay":"1901-01-01"}`.

Three details in that payload shape the code:

1. **Not every message is a customer.** STEDI also writes the `User` and
   `RapidStepTest` sorted sets, and they arrive on the same topic. They parse
   against the customer schema into rows of all nulls, so
   `where email is not null and birthDay is not null` is load-bearing, not
   defensive.
2. **`zSetEntries` and `zsetEntries` are the same list under two spellings.**
   Only one is parsed.
3. **`unbase64` returns binary**, so it has to be cast back to a string before
   `from_json` can read it.

## The three required scripts

| Script | What it does | Log |
| --- | --- | --- |
| `sparkpyrediskafkastreamtoconsole.py` | `redis-server` → decode → email + birth year, to console | `spark/logs/redisstream.log` |
| `sparkpyeventskafkastreamtoconsole.py` | `stedi-events` → parse → customer + score, to console | `spark/logs/eventstream.log` |
| `sparkpykafkajoin.py` | joins both on the email address, sinks JSON to `customer-risk` | `spark/logs/kafkajoin.log` |

The join is a **stream-stream inner join**. Both sides keep their rows in state,
which is what the data demands: a customer is written to Redis once, at
registration, but their first risk score cannot exist until four assessments
later. Every source therefore reads with `startingOffsets=earliest` — on
`latest` the customer records would already have gone past and the join would
produce nothing at all.

The sink payload is exactly the documented contract, and exactly what
`public/risk-graph.js` reads (it plots `customerRisk.birthYear` on x and
`customerRisk.score` on y):

```json
{"customer":"Neeraj.Spencer@test.com","score":"-3.0","email":"Neeraj.Spencer@test.com","birthYear":"1955"}
```

`score` is cast to a string so the JSON matches the documented format. That is
safe here because Chart.js 2.9 coerces scale values with a unary `+`, so the
string plots as a number — verified in the bundled `Chart.js`, not assumed.

The topic name comes from `stedi-application/application.conf`
(`kafka.riskTopic=customer-risk`), which is mounted into the STEDI container and
loaded with `-Dconfig.file`, so the producer and the consumer are configured from
the same file.

## Going past the rubric

### The score is checked, not trusted

`sparkpyoptionalriskcalculation.py` recomputes the fall-risk score from scratch,
straight from the raw `RapidStepTest` records on the `redis-server` topic,
following the pseudocode in the brief:

```
currentTestAverageScore  = (d(most recent) + d(2nd most recent)) / 2
previousTestAverageScore = (d(3rd most recent) + d(4th most recent)) / 2
riskScore                = (previousTestAverageScore - currentTestAverageScore) / 1000
```

"The last four tests per customer" is a ranked window, and Structured Streaming
rejects `row_number()` over an ordered window on a streaming DataFrame. The step
tests are therefore accumulated to Parquet and ranked on that static view inside
`foreachBatch`, which also keeps the plan flat instead of growing a union
lineage batch after batch.

`sparkpyoptionalriskquality.py` then puts the two side by side. Every customer
agrees to the decimal:

```
+--------------------------+-------------+-------------+-----+-------+
|email                     |reportedScore|computedScore|delta|verdict|
+--------------------------+-------------+-------------+-----+-------+
|Ashley.Fibonnaci@test.com |26.5         |26.5         |0.0  |MATCH  |
|David.Anderson@test.com   |33.5         |33.5         |0.0  |MATCH  |
|Jerry.Abram@test.com      |-19.0        |-19.0        |0.0  |MATCH  |
|Neeraj.Anandh@test.com    |30.0         |30.0         |0.0  |MATCH  |
...
```

One caveat is worth stating because it shapes how that table should be read:
STEDI scores a customer against the four tests on file *at the moment the
assessment completes*, while this script always scores the four most recent. A
customer still testing can legitimately disagree; one who has stopped converges
to an exact match. The verdict column distinguishes the two cases rather than
calling a window difference a defect.

### The environment itself

Four of the nine course images can no longer be pulled by anyone. They were
replaced rather than worked around, and the replacements are documented,
reproducible and tested — see **[ENVIRONMENT.md](ENVIRONMENT.md)**. The
short version:

* `gcr.io/simulation-images/*` now refuses anonymous pulls, taking the STEDI
  application and the Redis source connector with it.
* `bitnami/spark:3-debian-10` was deleted from Docker Hub in 2025.

The Redis source connector was reimplemented in `infra/redis-source/`, with
`test_bridge.py` asserting its output against the base64 strings printed in the
project README, so compatibility is proven rather than hoped for. The Spark
image reproduces the `/opt/bitnami/spark` prefix and the `SPARK_MODE` contract,
so every starter script runs unmodified.

The jobs are also submitted with `--master spark://spark:7077`. The original
scripts pass no `--master` at all, which silently runs the job in the driver's
own `local[*]` JVM and never touches the cluster.

## Running it

```bash
docker compose up -d
```

Then start the simulated population (this is the toggle on the timer page; the
endpoint is unauthenticated):

```bash
curl -X POST http://localhost:4567/simulation
```

STEDI creates 30 customers immediately and begins publishing risk scores about
four minutes later, once each customer has four assessments on file.

```bash
bash project/starter/submit-event-kafkajoin.sh          # the deliverable
bash project/starter/submit-redis-kafka-streaming.sh    # validator: birth years
bash project/starter/submit-event-kafkastreaming.sh     # validator: risk scores
bash project/starter/submit-optional-calculate-score.sh # optional extra
bash project/starter/submit-optional-risk-quality.sh    # optional extra
```

The graph is at <http://localhost:4567/risk-graph.html>; the Spark master UI, which
shows the applications running on the cluster, is at <http://localhost:8080>.

## Deliverables

| Rubric item | Where |
| --- | --- |
| `sparkpykafkajoin.py` | `project/starter/sparkpykafkajoin.py` |
| `sparkpyeventskafkastreamtoconsole.py` | `project/starter/sparkpyeventskafkastreamtoconsole.py` |
| `sparkpyrediskafkastreamtoconsole.py` | `project/starter/sparkpyrediskafkastreamtoconsole.py` |
| `kafkajoin.log` | `spark/logs/kafkajoin.log` |
| `eventstream.log` | `spark/logs/eventstream.log` |
| `redisstream.log` | `spark/logs/redisstream.log` |
| Spark master + worker logs | `spark/logs/spark-spark-org.apache.spark.deploy.*.out` |
| `stedi-application/application.conf` | `stedi-application/application.conf` |
| Two screenshots of the working graph | `screenshots/stedi-risk-graph-1.png`, `screenshots/stedi-risk-graph-2.png` |
