#!/bin/bash
# run_bench.sh -- sweeps the benchmark across partition counts and fsync modes.
#
# Produces the numbers that go in the README and on the resume. Run it with
# nothing else heavy running on the machine.
#
# Usage:  ./run_bench.sh
#
# Requires: broker_server and bench already built.
# Override policy with: SYNC_MODE=none ./run_bench.sh

set -e

PORT=9300
THREADS=8
PER_THREAD=100000
PAYLOAD=100
SYNC_MODE="${SYNC_MODE:-everyn}"   # none | everyn | always
SYNC_N="${SYNC_N:-100}"
RESULTS="bench_results.txt"

cleanup() {
    if [ -n "$BROKER_PID" ] && kill -0 "$BROKER_PID" 2>/dev/null; then
        kill "$BROKER_PID" 2>/dev/null || true
        wait "$BROKER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "pub-sub broker benchmark" | tee "$RESULTS"
echo "date: $(date)" | tee -a "$RESULTS"
echo "sync policy: $SYNC_MODE ($SYNC_N)" | tee -a "$RESULTS"
echo "threads: $THREADS, messages/thread: $PER_THREAD, payload: ${PAYLOAD}B" \
    | tee -a "$RESULTS"
echo "" | tee -a "$RESULTS"

# --- partition count sweep -------------------------------------------------
echo "=== throughput vs partition count ===" | tee -a "$RESULTS"
for PARTS in 1 2 4 8 16; do
    DATA="bench_data_p${PARTS}"
    rm -rf "$DATA"; mkdir -p "$DATA"

    PORT=$((PORT + 1))
    ./broker_server "$PORT" "$DATA" "$SYNC_MODE" "$SYNC_N" > /dev/null 2>&1 &
    BROKER_PID=$!
    sleep 1

    printf 'CREATE bench %d\nQUIT\n' "$PARTS" | nc -w 1 127.0.0.1 "$PORT" > /dev/null

    echo "" | tee -a "$RESULTS"
    echo "--- $PARTS partitions ---" | tee -a "$RESULTS"
    BROKER_PORT=$PORT ./bench bench "$THREADS" "$PER_THREAD" "$PAYLOAD" \
        | tee -a "$RESULTS"

    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
    BROKER_PID=""
    rm -rf "$DATA"
done

echo "" | tee -a "$RESULTS"
echo "results written to $RESULTS"
echo ""
echo "For the durability comparison, rerun with:"
echo "  SYNC_MODE=none ./run_bench.sh"
echo "  SYNC_MODE=always ./run_bench.sh"