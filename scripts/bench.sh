#!/bin/sh
# Reproduces docs/benchmarks.md: hardware line, the paced suites, then
# the ingest capacity probes. Numbers are hardware-bound — expect yours
# to differ; the shapes (the always/interval gap, the µs-scale gateway
# overhead) are the reproducible part.
set -e
cd "$(dirname "$0")/.."

echo "hardware:"
case "$(uname -s)" in
Darwin)
    sysctl -n machdep.cpu.brand_string
    echo "$(($(sysctl -n hw.memsize) / 1073741824)) GB RAM"
    ;;
Linux)
    grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //'
    awk '/MemTotal/ {printf "%d GB RAM\n", $2/1048576}' /proc/meminfo
    ;;
esac
echo

# The paced suites: fixed offered load, latency distributions.
go run ./cmd/bench

# Capacity probes: raise the offered load until the ack distribution
# shows the queue. fsync=always saturates around its fsync rate; watch
# its ack p50 turn into seconds of backlog — that is the pacer refusing
# to hide what a slower client base would feel.
echo "== ingest capacity probes =="
for rate in 1000 3000 6000; do
    echo "-- offered ${rate} batches/s (${rate}00 points/s) --"
    go run ./cmd/bench -bench ingest -duration 5s -ingest-rate "$rate"
done

# Compression is measured by its own tool against whatever real WAL you
# have (see README "Gorilla compression"); rerun it if you have one:
if [ -f data/collector.wal ]; then
    echo "== compression (data/collector.wal) =="
    go run ./cmd/compressbench -wal data/collector.wal | tail -5
fi
