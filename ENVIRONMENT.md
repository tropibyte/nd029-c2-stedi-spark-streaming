# Environment repair notes

The course `docker-compose.yaml` builds a nine-container STEDI ecosystem. As of
September 2026, **four of those nine images can no longer be pulled by anyone**,
so the environment as shipped does not start. This file records what broke, how
it was verified, and exactly what replaced it, so a reviewer can tell the
difference between the project work and the scaffolding needed to run it.

Nothing in `project/starter/` was changed to accommodate the substitutions. The
topics, the payload formats and the Spark APIs are identical to the originals.

## What is broken, and the evidence

### 1. The three `gcr.io/simulation-images/*` images are private

```
$ docker pull gcr.io/simulation-images/stedi
Error response from daemon: error from registry: Unauthenticated request.
Unauthenticated requests do not have permission
"artifactregistry.repositories.downloadArtifacts" on resource
"projects/simulation-images/locations/us/repositories/gcr.io"
```

This affects `stedi`, `kafka-connect-redis-source`, `banking-simulation` and
`trucking-simulation`. It is an authorisation failure, not a missing tag: the
whole Google Artifact Registry repository now refuses anonymous reads. The
Artifact Registry hostname (`us-docker.pkg.dev/simulation-images/gcr.io/stedi`)
fails identically.

### 2. `bitnami/spark:3-debian-10` was deleted

```
$ docker compose pull
Error response from daemon: failed to resolve reference
"docker.io/bitnami/spark:3-debian-10": not found
```

Bitnami withdrew its legacy Docker Hub catalogue in August 2025. The archive
namespace `bitnamilegacy/spark` exists but carries only `4.0.0-debian-12` tags —
no Debian 10 build, and no Spark 3.x build at all.

## What replaced each one

| Original | Replacement | Notes |
| --- | --- | --- |
| `gcr.io/simulation-images/stedi` | `scmurdock/stedi:latest` | The course author's own Docker Hub copy of the same `stedi.jar`. Verified below. |
| `gcr.io/simulation-images/kafka-connect-redis-source` | `infra/redis-source/` (local build) | Reimplemented; see below. |
| `docker.io/bitnami/spark:3-debian-10` | `infra/spark/` (local build) | Apache Spark 3.3.0 in a Bitnami-compatible layout. |
| `banking-simulation`, `trucking-simulation` | dropped | Private, and used only by the lesson exercises. Neither publishes to `redis-server`, `stedi-events` or `customer-risk`, so the project is unaffected. |

### The STEDI application

`scmurdock/stedi:latest` is not a guess. Unpacking the jar shows the same
application the project describes:

* `Main-Class: com.getsimplex.steptimer.service.WebAppRunner`, listening on 4567;
* `KafkaRiskTopicProducerActor` publishes to **`stedi-events`**;
* `KafkaConsumerUtil` reads `KAFKA_BROKER` / `KAFKA_RISK_TOPIC` from the
  environment, falling back to `kafka.broker` / `kafka.riskTopic` in
  `application.conf`;
* `JedisClient` resolves `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` the same way;
* the jar's own bundled `application.conf` already contains
  `kafka{ riskTopic=customer-risk }`, which is the value the rubric asks for.

Because both an environment variable and a config file are honoured,
`stedi-application/application.conf` is mounted into the container and loaded
with `-Dconfig.file`. The file the rubric asks to be submitted is therefore the
file the application actually runs on, not a decorative copy.

### The Redis source connector

`infra/redis-source/` is a small Python service that replaces the Kafka Connect
Redis source. Every public `kafka-connect-redis` image on Docker Hub is a
*sink*, so there was nothing to substitute.

It tails the Redis command log with `MONITOR` and republishes each write to the
`redis-server` topic in the connector's exact envelope — base64 key, base64
`zSetEntries[].element`, and the duplicated `zSetEntries` / `zsetEntries`
spellings the project README calls out.

`MONITOR` was chosen over keyspace notifications deliberately: a keyspace event
says only *that* a key changed, so the member just added would have to be
inferred by diffing the sorted set. `MONITOR` carries the full argument vector,
making the published payload an exact record of the write.

`infra/redis-source/test_bridge.py` asserts the output against the base64
strings printed in the project README, so compatibility is checked rather than
assumed:

```
$ docker run --rm --entrypoint python \
    -v "$PWD/infra/redis-source:/t" -w /t stedi/redis-source:1.0 /t/test_bridge.py
...
All contract checks passed - payload matches the documented connector.
```

Every sorted set STEDI writes is republished, not just `Customer` — `User` and
`RapidStepTest` included. That is faithful to the original connector, and it is
what keeps the project's "filter out the rows where email is null" step
meaningful.

### The Spark cluster

`infra/spark/` builds on `apache/spark-py:v3.3.0` and reproduces the two things
the course depends on:

1. **`/opt/bitnami/spark` still exists**, as a symlink to `/opt/spark`, so every
   `submit-*.sh` and `submit-*.cmd` path works unchanged.
2. **`SPARK_MODE=master|worker`** is honoured by `infra/spark/entrypoint.sh`.

The entrypoint starts the daemons through Spark's own `sbin/start-master.sh` and
`sbin/start-worker.sh` with `SPARK_LOG_DIR=/home/workspace/spark/logs`, rather
than running `spark-class` in the foreground. That makes Spark itself write

```
spark/logs/spark-spark-org.apache.spark.deploy.master.Master-1-spark.out
spark/logs/spark-spark-org.apache.spark.deploy.worker.Worker-1-spark-worker-1.out
```

which is the artifact the rubric asks for, produced by Spark rather than
reconstructed from `docker logs` afterwards. The worker log contains the
required line:

```
26/09/16 18:53:29 INFO Worker: Successfully registered with master spark://spark:7077
```

The Kafka connector jars are baked onto the default classpath at build time, so
a run cannot be broken by Maven Central being slow or unreachable — or by the
bind-mounted (initially empty) Ivy cache shadowing a cached resolution.

## Other changes, and why

* **Spark 3.3.0, not 3.0.0.** `apache/spark-py` publishes no 3.0.x tag. The
  `--packages` coordinate in the submit scripts tracks the image
  (`spark-sql-kafka-0-10_2.12:3.3.0`); nothing else was affected.

* **`--master spark://spark:7077` added to the submit scripts.** The originals
  pass no `--master`, so the job silently runs in the driver's own `local[*]`
  JVM and never touches the cluster. Submitting to the standalone master is what
  makes "the Spark application successfully executes on the Spark cluster" true.

* **Worker sized at 4 cores / 5G**, up from the course's 1 core / 1G. With one
  core the master can only give resources to one application at a time, so the
  join and the two console validators cannot run together.

* **`docker exec -i`, not `-it`.** A TTY injects carriage returns and control
  codes into the teed log files.

* **`export MSYS_NO_PATHCONV=1`** in each submit script. Git Bash on Windows
  rewrites container-side absolute paths, turning `/opt/bitnami/spark/...` into
  `C:/Program Files/Git/opt/bitnami/spark/...` and failing the exec. The
  variable is ignored by every non-MSYS shell.

* **`redisstream.log`, not `redis-kafka.log`.** The original script wrote a
  filename the rubric does not list.

* **`kafka-setup` service added.** Declares `redis-server`, `stedi-events` and
  `customer-risk` up front so partition counts are deterministic rather than
  left to auto-creation. Note that `--if-not-exists` cannot be combined with
  `--bootstrap-server` on Kafka 2.5, so an existing topic is tolerated
  explicitly instead.

* **Spark state lives under `/home/workspace`, never `/tmp`.** The driver and
  the executors run in different containers, so a `/tmp` path appears to work
  until the driver tries to read back what an executor wrote and fails with
  `Unable to infer schema for Parquet`. Each container has its own `/tmp`.
