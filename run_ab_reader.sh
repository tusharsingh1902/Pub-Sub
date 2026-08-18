#!/bin/bash
# run_ab_reader.sh -- matched A/B comparison of the buffered vs unbuffered
# broker read path.
#
# Both binaries are built from the same source except for readLine, and both
# are benchmarked with identical settings, warmup, and run length. That makes
# the resulting ratio defensible; comparing runs from different binary
# generations or thread counts does not.
#
# Usage:  ./run_ab_reader.sh
#
# Requires: broker_server, broker_server_unbuffered, and bench built.

set -e

THREADS=8
PER_THREAD=100000
PAYLOAD=100
PARTS=8
SYNC_MODE="${SYNC_MODE:-everyn}"
SYNC_N="${SYNC_N:-100}"
RESULTS="ab_reader.txt"

cleanup() {
    if [ -n "$BROKER_PID" ] && kill -0 "$BROKER_PID" 2>/dev/null; then
        kill "$BROKER_PID" 2>/dev/null || true
        wait "$BROKER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "buffered vs unbuffered read path" | tee "$RESULTS"
echo "date: $(date)" | tee -a "$RESULTS"
echo "$THREADS threads x $PER_THREAD msgs, $PARTS partitions, ${PAYLOAD}B payload" \
    | tee -a "$RESULTS"
echo "sync policy: $SYNC_MODE ($SYNC_N)" | tee -a "$RESULTS"

PORT=9700
for BINARY in ./broker_server_unbuffered ./broker_server; do
    DATA="abdata"
    rm -rf "$DATA"; mkdir -p "$DATA"

    PORT=$((PORT + 1))
    "$BINARY" "$PORT" "$DATA" "$SYNC_MODE" "$SYNC_N" > /dev/null 2>&1 &
    BROKER_PID=$!

    for _ in $(seq 1 50); do
        if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then break; fi
        sleep 0.1
    done

    printf 'CREATE bench %d\nQUIT\n' "$PARTS" | nc -w 1 127.0.0.1 "$PORT" > /dev/null

    echo "" | tee -a "$RESULTS"
    echo "--- $BINARY ---" | tee -a "$RESULTS"
    BROKER_PORT=$PORT ./bench bench "$THREADS" "$PER_THREAD" "$PAYLOAD" \
        | grep -E "successful|errors|elapsed|throughput|latency" | tee -a "$RESULTS"

    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
    BROKER_PID=""
    rm -rf "$DATA"
done

echo "" | tee -a "$RESULTS"
echo "results written to $RESULTS"