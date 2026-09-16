#!/usr/bin/env bash
# Reproduces the Bitnami SPARK_MODE contract on top of apache/spark-py.
#
# Deliberately starts the daemons through Spark's own sbin/start-*.sh rather
# than running spark-class in the foreground. That is what makes Spark write
#   $SPARK_LOG_DIR/spark-<ident>-org.apache.spark.deploy.<role>.<Role>-1-<host>.out
# which is the exact artifact the project rubric asks to be submitted -- so the
# log is genuinely produced by Spark instead of being reconstructed afterwards
# from `docker logs`.
set -euo pipefail

export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export SPARK_LOG_DIR="${SPARK_LOG_DIR:-/home/workspace/spark/logs}"
export SPARK_IDENT_STRING="${SPARK_IDENT_STRING:-spark}"
export SPARK_PID_DIR="${SPARK_PID_DIR:-/tmp}"

mkdir -p "$SPARK_LOG_DIR"

wait_for_tcp() {
    local host="$1" port="$2" tries=0
    until (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; do
        tries=$((tries + 1))
        if [ "$tries" -ge 180 ]; then
            echo "entrypoint: timed out waiting for ${host}:${port}" >&2
            exit 1
        fi
        sleep 1
    done
    exec 3<&- 3>&-
}

role_log() {  # $1 = master|worker -> the .out path Spark will create
    local role="$1" cls
    case "$role" in
        master) cls="org.apache.spark.deploy.master.Master" ;;
        worker) cls="org.apache.spark.deploy.worker.Worker" ;;
    esac
    echo "${SPARK_LOG_DIR}/spark-${SPARK_IDENT_STRING}-${cls}-1-$(hostname).out"
}

case "${SPARK_MODE:-}" in
    master)
        logfile="$(role_log master)"
        # --host must be the compose service name, otherwise the master
        # advertises spark://<container-id>:7077 and workers dialling
        # spark://spark:7077 are rejected with a master-URL mismatch.
        "$SPARK_HOME/sbin/start-master.sh" \
            --host "$(hostname)" --port 7077 --webui-port 8080
        ;;
    worker)
        master_url="${SPARK_MASTER_URL:-spark://spark:7077}"
        hostport="${master_url#spark://}"
        wait_for_tcp "${hostport%%:*}" "${hostport##*:}"
        logfile="$(role_log worker)"
        "$SPARK_HOME/sbin/start-worker.sh" "$master_url" --webui-port 8081
        ;;
    *)
        exec "$@"
        ;;
esac

# Hold the container open and mirror the daemon's own log onto stdout, so both
# `docker logs` and the on-disk .out file tell the same story.
for _ in $(seq 1 30); do [ -f "$logfile" ] && break; sleep 1; done
exec tail -F "$logfile"
