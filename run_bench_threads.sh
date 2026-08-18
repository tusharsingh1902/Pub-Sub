#!/bin/bash
# run_bench_threads.sh -- sweeps client thread count at a fixed partition count.
#
# Purpose: the partition sweep showed flat throughput. The hypothesis is that
# the limit is request-response round trips, not partition lock contention:
# each client thread waits for a reply before sending the next message, so
# at most <threads> requests are ever in flight.
#
# If that hypothesis is right, throughput should scale with thread count.
# Where it stops scaling is the real bottleneck.
#
# Usage:  ./run_bench_threads.sh

set -e

PARTS=8
TOTAL_MESSAGES=800000
PAYLOAD=100
SYNC_MODE="${SYNC_MODE:-everyn}"   # none | everyn | always
SYNC_N="${SYNC_N:-100}"
BASE_PORT=9800
RESULTS="bench_threads.txt"

cleanup() {
    if [ -n "$BROKER_PID" ] && kill -0 "$BROKER_PID" 2>/dev/null; then
        kill "$BROKER_PID" 2>/dev/null || true
        wait "$BROKER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "thread count sweep, $PARTS partitions" | tee "$RESULTS"
echo "date: $(date)" | tee -a "$RESULTS"
echo "sync policy: $SYNC_MODE ($SYNC_N)" | tee -a "$RESULTS"
echo "total messages per run: $TOTAL_MESSAGES, payload: ${PAYLOAD}B" | tee -a "$RESULTS"
echo "" | tee -a "$RESULTS"

PORT=$BASE_PORT
for THREADS in 1 2 4 8 16 32 64 128; do
    PER_THREAD=$((TOTAL_MESSAGES / THREADS))

    DATA="tdata_${THREADS}"
    rm -rf "$DATA"; mkdir -p "$DATA"

    PORT=$((PORT + 1))
    ./broker_server "$PORT" "$DATA" "$SYNC_MODE" "$SYNC_N" > /dev/null 2>&1 &
    BROKER_PID=$!

    # Wait for the port to actually accept connections rather than guessing.
    for _ in $(seq 1 50); do
        if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then break; fi
        sleep 0.1
    done

    printf 'CREATE bench %d\nQUIT\n' "$PARTS" | nc -w 1 127.0.0.1 "$PORT" > /dev/null

    echo "" | tee -a "$RESULTS"
    echo "--- $THREADS threads x $PER_THREAD messages ---" | tee -a "$RESULTS"
    BROKER_PORT=$PORT ./bench bench "$THREADS" "$PER_THREAD" "$PAYLOAD" \
        | grep -E "successful|errors|elapsed|throughput|latency" | tee -a "$RESULTS"

    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
    BROKER_PID=""
    rm -rf "$DATA"
done

echo "" | tee -a "$RESULTS"
echo "results written to $RESULTS"