#!/bin/bash
# Submits sparkpyeventskafkastreamtoconsole.py to the Spark standalone cluster and tees the
# driver output to spark/logs/eventstream.log -- the filename the rubric asks for.
#
# Differences from the original starter script, all forced by the environment:
#   * Compose v2 names containers with hyphens, not underscores.
#   * --master spark://spark:7077 so the job genuinely runs on the cluster
#     instead of in the local[*] JVM of the driver.
#   * The connector version tracks the Spark version of the image (3.3.0). The
#     jars are also baked into the image, so this resolves from cache offline.
#   * docker exec gets -i but not -t: a TTY injects carriage returns and control
#     codes into the teed log file.
set -euo pipefail

# Git Bash on Windows rewrites container-side absolute paths into Windows
# paths, turning /opt/bitnami/... into C:/Program Files/Git/opt/bitnami/...
# and failing the exec. Ignored by every non-MSYS shell.
export MSYS_NO_PATHCONV=1

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SPARK_CONTAINER="${SPARK_CONTAINER:-nd029-c2-apache-spark-and-spark-streaming-starter-spark-1}"
LOG="$REPO_ROOT/spark/logs/eventstream.log"

mkdir -p "$(dirname "$LOG")"

docker exec -i "$SPARK_CONTAINER" /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    --total-executor-cores 1 \
    --executor-memory 1g \
    /home/workspace/project/starter/sparkpyeventskafkastreamtoconsole.py 2>&1 | tee "$LOG"
