#!/usr/bin/env python3
"""
Contract tests for the Redis -> Kafka source bridge.

The fixture is not invented: it is the exact ZADD and the exact resulting
payload printed in the project README, so a pass here means the replacement
bridge is byte-for-byte compatible with the connector the course shipped.

Run with:  docker run --rm -v "$PWD:/t" -w /t stedi/redis-source:1.0 -m pytest -q
or simply: python test_bridge.py
"""
import json

from redis_source_bridge import (
    b64,
    build_messages,
    parse_monitor_line,
    split_args,
    unescape,
)

# The customer JSON exactly as the README stores it in Redis.
CUSTOMER_JSON = (
    '{"customerName":"Sam Test","email":"sam.test@test.com",'
    '"phone":"8015551212","birthDay":"2001-01-03"}'
)

# The base64 values the README says the connector publishes.
README_KEY_B64 = "Q3VzdG9tZXI="
README_ELEMENT_B64 = (
    "eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20i"
    "LCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ=="
)

# What Redis MONITOR actually emits for that ZADD: every argument rendered by
# sdscatrepr(), so the inner double quotes arrive backslash-escaped.
MONITOR_LINE = (
    '1600000000.123456 [0 172.18.0.1:57944] "zadd" "Customer" "0" "'
    + CUSTOMER_JSON.replace('"', '\\"')
    + '"'
)

failures = []


def check(label, actual, expected):
    if actual == expected:
        print("  PASS  %s" % label)
    else:
        print("  FAIL  %s\n          expected: %r\n          actual:   %r"
              % (label, expected, actual))
        failures.append(label)


print("base64 helpers")
check("redis key base64 matches README", b64("Customer"), README_KEY_B64)
check("customer element base64 matches README", b64(CUSTOMER_JSON), README_ELEMENT_B64)

print("MONITOR unescaping")
check("escaped quotes survive round trip",
      unescape(CUSTOMER_JSON.replace('"', '\\"')), CUSTOMER_JSON)
check("hex escapes decode", unescape("a\\x41b"), "aAb")
check("control escapes decode", unescape("a\\nb\\tc"), "a\nb\tc")
check("argument vector tokenises",
      split_args('"zadd" "Customer" "0"'), ["zadd", "Customer", "0"])

print("MONITOR line parsing")
command, args = parse_monitor_line(MONITOR_LINE)
check("command is ZADD", command, "ZADD")
check("key argument", args[0], "Customer")
check("score argument", args[1], "0")
check("member argument is intact JSON", args[2], CUSTOMER_JSON)
check("member parses as JSON", json.loads(args[2])["email"], "sam.test@test.com")

print("published envelope")
messages = list(build_messages(command, args))
check("one envelope produced", len(messages), 1)
envelope = messages[0]
check("key field", envelope["key"], README_KEY_B64)
check("existType field", envelope["existType"], "NONE")
check("ch field", envelope["ch"], False)
check("incr field", envelope["incr"], False)
check("zSetEntries element", envelope["zSetEntries"][0]["element"], README_ELEMENT_B64)
check("zSetEntries score", envelope["zSetEntries"][0]["score"], 0.0)
check("redundant zsetEntries spelling mirrored",
      envelope["zsetEntries"], envelope["zSetEntries"])

print("ZADD option flags are skipped")
_, flag_args = parse_monitor_line(
    '1.0 [0 1.2.3.4:1] "ZADD" "Customer" "NX" "CH" "0" "'
    + CUSTOMER_JSON.replace('"', '\\"') + '"'
)
flagged = list(build_messages("ZADD", flag_args))
check("NX/CH flags do not corrupt the score/member pairing",
      flagged[0]["zSetEntries"][0]["element"], README_ELEMENT_B64)

print("non-sorted-set writes still carry a value field")
set_msgs = list(build_messages("SET", ["someKey", "someValue"]))
check("SET produces a value field", set_msgs[0]["value"], b64("someValue"))
check("SET has no zSetEntries", "zSetEntries" in set_msgs[0], False)

print()
if failures:
    print("FAILED: %d check(s): %s" % (len(failures), ", ".join(failures)))
    raise SystemExit(1)
print("All contract checks passed - payload matches the documented connector.")
