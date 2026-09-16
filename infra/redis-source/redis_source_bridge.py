#!/usr/bin/env python3
"""
Redis -> Kafka source bridge (replacement for the Udacity
`gcr.io/simulation-images/kafka-connect-redis-source` connector).

Why this exists
---------------
The course docker-compose.yaml pulls a bespoke Kafka Connect image from
Udacity's private `simulation-images` Google registry. That project now rejects
anonymous pulls ("Unauthenticated requests do not have permission
artifactregistry.repositories.downloadArtifacts"), so the connector can no
longer be obtained. Every public `kafka-connect-redis` image on Docker Hub is a
*sink*, not a source, so there is nothing to swap in.

What it does
------------
Streams the Redis command log via MONITOR and republishes every write as the
exact envelope the course connector emitted, so the project's Spark code -- and
the base64/JSON decoding lesson it is built to teach -- is completely unchanged:

    {"key":"Q3VzdG9tZXI=",
     "existType":"NONE",
     "ch":false,
     "incr":false,
     "zSetEntries":[{"element":"eyJjdXN0b21lck5hbWUiOi...","score":0.0}],
     "zsetEntries":[{"element":"eyJjdXN0b21lck5hbWUiOi...","score":0.0}]}

`key` is the base64 of the Redis key name and `element` is the base64 of the
sorted-set member, matching the connector byte for byte.

MONITOR is used rather than keyspace notifications on purpose: a keyspace event
reports only *that* a key changed, so the member just added would have to be
guessed by diffing the set. MONITOR carries the full argument vector, so the
published payload is an exact record of the write that occurred.

Note that every sorted set STEDI touches is republished, not just `Customer`.
That is faithful to the original connector, and it is what makes the project's
"JSON parsing will set non-existent fields to null, so select just the fields
you want where they are not null" step meaningful.
"""
import base64
import json
import logging
import os
import socket
import sys
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

LOG_FORMAT = "%(asctime)s %(levelname)-5s redis-source | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stdout)
log = logging.getLogger("redis-source")

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:19092")
REDIS_TOPIC = os.environ.get("REDIS_TOPIC", "redis-server")

# Redis write commands worth republishing. Sorted-set writes carry the customer
# payload the project needs; the rest are emitted with a `value` field so the
# topic mirrors the original connector's superset schema.
ZSET_COMMANDS = {"ZADD"}
STRING_COMMANDS = {"SET", "SETEX", "PSETEX", "GETSET", "APPEND"}
ZADD_FLAGS = {"NX", "XX", "GT", "LT", "CH", "INCR"}


def b64(text):
    """Base64 a unicode string exactly the way the Java connector did."""
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def unescape(token):
    """
    Undo Redis' sdscatrepr() quoting.

    MONITOR renders every argument as a double-quoted C-style literal, so the
    embedded customer JSON arrives with its own quotes backslash-escaped.
    Getting this wrong silently corrupts the payload, so each escape Redis can
    emit is handled explicitly rather than with a blanket codec.
    """
    simple = {
        "n": "\n",
        "r": "\r",
        "t": "\t",
        "a": "\a",
        "b": "\b",
        "\\": "\\",
        '"': '"',
    }
    out = []
    i = 0
    while i < len(token):
        ch = token[i]
        if ch != "\\" or i + 1 >= len(token):
            out.append(ch)
            i += 1
            continue
        nxt = token[i + 1]
        if nxt == "x" and i + 3 < len(token):
            try:
                out.append(chr(int(token[i + 2:i + 4], 16)))
                i += 4
                continue
            except ValueError:
                pass
        out.append(simple.get(nxt, nxt))
        i += 2
    return "".join(out)


def split_args(payload):
    """Tokenise the quoted argument vector trailing a MONITOR line."""
    args = []
    i = 0
    n = len(payload)
    while i < n:
        if payload[i] != '"':
            i += 1
            continue
        i += 1
        start = i
        while i < n:
            if payload[i] == "\\":
                i += 2
                continue
            if payload[i] == '"':
                break
            i += 1
        args.append(unescape(payload[start:i]))
        i += 1
    return args


def parse_monitor_line(line):
    """Turn a MONITOR line into (COMMAND, [args]); the prefix is ts + [db addr]."""
    close = line.find("]")
    if close == -1:
        return None, []
    args = split_args(line[close + 1:])
    if not args:
        return None, []
    return args[0].upper(), args[1:]


def envelope(key, zset_entries=None, value=None):
    """Build the connector-compatible payload for one Redis write."""
    payload = {
        "key": b64(key),
        "existType": "NONE",
        "ch": False,
        "incr": False,
    }
    if value is not None:
        payload["value"] = b64(value)
    if zset_entries is not None:
        # The original connector emitted the same list under two spellings; the
        # project README calls this out explicitly and tells students to parse
        # only one of them. Reproduced so the starter schema still fits.
        payload["zSetEntries"] = zset_entries
        payload["zsetEntries"] = list(zset_entries)
    return payload


def build_messages(command, args):
    """Yield one envelope per logical write in a Redis command."""
    if command in ZSET_COMMANDS and len(args) >= 3:
        key = args[0]
        rest = args[1:]
        while rest and rest[0].upper() in ZADD_FLAGS:
            rest = rest[1:]
        entries = []
        for i in range(0, len(rest) - 1, 2):
            try:
                score = float(rest[i])
            except ValueError:
                continue
            entries.append({"element": b64(rest[i + 1]), "score": score})
        if entries:
            yield envelope(key, zset_entries=entries)
    elif command in STRING_COMMANDS and len(args) >= 2:
        yield envelope(args[0], value=args[-1])


def connect_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
                retries=5,
                linger_ms=10,
            )
            log.info("connected to kafka at %s", KAFKA_BROKER)
            return producer
        except NoBrokersAvailable:
            log.warning("kafka at %s not ready yet, retrying in 5s", KAFKA_BROKER)
            time.sleep(5)


def open_monitor():
    """Open a raw RESP connection and put it into MONITOR mode."""
    while True:
        try:
            sock = socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=10)
            sock.sendall(b"MONITOR\r\n")
            stream = sock.makefile("r", encoding="utf-8", errors="replace", newline="\n")
            ack = stream.readline().strip()
            if not ack.startswith("+OK"):
                raise RuntimeError("unexpected MONITOR reply: %r" % ack)
            sock.settimeout(None)
            log.info("streaming redis command log from %s:%s", REDIS_HOST, REDIS_PORT)
            return sock, stream
        except (OSError, RuntimeError) as exc:
            log.warning("redis at %s:%s not ready (%s), retrying in 5s",
                        REDIS_HOST, REDIS_PORT, exc)
            time.sleep(5)


def main():
    producer = connect_producer()
    published = 0
    while True:
        sock, stream = open_monitor()
        try:
            for raw in stream:
                line = raw.strip()
                if not line.startswith("+"):
                    continue
                command, args = parse_monitor_line(line[1:])
                if not command:
                    continue
                for message in build_messages(command, args):
                    producer.send(REDIS_TOPIC, key=command, value=message)
                    published += 1
                    if published <= 5 or published % 25 == 0:
                        log.info("published %d event(s); latest %s on key %r",
                                 published, command, args[0] if args else "?")
        except OSError as exc:
            log.warning("redis connection lost (%s); reconnecting", exc)
        finally:
            producer.flush()
            try:
                stream.close()
                sock.close()
            except OSError:
                pass
        time.sleep(2)


if __name__ == "__main__":
    main()
