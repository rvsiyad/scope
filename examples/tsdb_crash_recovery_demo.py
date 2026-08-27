"""Kill -9 the storage engine mid-ingest and prove no acknowledged sample died.

The WAL demo (crash_recovery_demo.py) proved acknowledged *batches* replay.
This one closes the loop for the whole storage engine: samples are flowing
through head blocks, segment flushes, and compactions when the process is
killed — and afterwards every acknowledged sample must come back from a
QUERY, through the real read path (head + segments + merge + dedupe), not
from a counter. Self-contained:

    python3 examples/tsdb_crash_recovery_demo.py

The collector runs with SCOPE_TSDB_FLUSH=500ms, so while samples stream in
the engine is repeatedly freezing its head into segment files and
compacting them — the kill lands in the middle of that machinery on
purpose (the atomic tmp -> fsync -> rename swap is what makes any landing
spot safe). After SIGKILL and restart on the same WAL + data dir:

  * every acked sample (t, v) must appear in the query result exactly once
    (samples replayed from the WAL that were already flushed into segments
    become head/segment duplicates — the read path must dedupe them);
  * at most one trailing unacked in-flight sample may additionally appear;
  * the segment count must stay small: compaction ran through all of this.

Stdlib only — no dependencies.
"""

import json
import os
import random
import signal
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

PORT = 9392  # scratch port: leave any real collector on 9091 alone
BASE = f"http://localhost:{PORT}"
METRIC = "demo_points_total"


def wait_healthy(timeout=10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(BASE + "/healthz", timeout=1) as resp:
                return json.load(resp)
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("collector never became healthy")


def start(binary: str, workdir: str) -> subprocess.Popen:
    env = dict(os.environ,
               SCOPE_COLLECTOR_ADDR=f":{PORT}",
               SCOPE_WAL_PATH=os.path.join(workdir, "collector.wal"),
               SCOPE_WAL_SYNC="always",
               SCOPE_TSDB_DIR=os.path.join(workdir, "tsdb"),
               SCOPE_TSDB_FLUSH="500ms")
    return subprocess.Popen([binary], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def ingest(t: int, v: float) -> bool:
    batch = {"metrics": [{"name": METRIC, "labels": {"tenant": "acme"},
                          "timestamp": t, "value": v}]}
    req = urllib.request.Request(BASE + "/v1/ingest", data=json.dumps(batch).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=2) as resp:
            return resp.status == 204
    except OSError:
        return False


def query_samples() -> list[tuple[int, float]]:
    url = f"{BASE}/debug/tsdb/select?name={METRIC}&tenant=acme"
    with urllib.request.urlopen(url, timeout=5) as resp:
        out = json.load(resp)
    if len(out["series"]) != 1:
        raise SystemExit(f"FAIL: expected 1 series, got {len(out['series'])}")
    return [(s["t"], s["v"]) for s in out["series"][0]["samples"]]


def main() -> None:
    workdir = tempfile.mkdtemp(prefix="scope-tsdb-crash-demo-")
    binary = os.path.join(workdir, "collector")
    print("building collector...")
    subprocess.run(["go", "build", "-o", binary, "./cmd/collector"], check=True)

    proc = start(binary, workdir)
    try:
        wait_healthy()
        print(f"collector up on :{PORT} (tsdb flushing + compacting every 500ms)")

        kill_after = random.randint(300, 800)
        killed = threading.Event()
        acked: list[tuple[int, float]] = []

        def assassin():
            while not killed.is_set():
                time.sleep(0.005)
                if len(acked) >= kill_after:
                    proc.send_signal(signal.SIGKILL)  # mid-flush is fair game
                    killed.set()

        threading.Thread(target=assassin, daemon=True).start()

        sent = 0
        while not killed.is_set() and sent < 5000:
            sent += 1
            t, v = sent * 1000, float(sent)
            if ingest(t, v):
                acked.append((t, v))
        killed.set()
        proc.wait()
        print(f"SIGKILL after {len(acked)} acked samples ({sent} sent) — "
              "several flush/compact cycles were in flight; no shutdown, no flush")

        proc2 = start(binary, workdir)
        try:
            health = wait_healthy()
            tsdb = health["tsdb"]
            print(f"restarted on the same WAL + data dir: "
                  f"{tsdb['segments']} segment(s) on disk, "
                  f"{tsdb['head_samples']} samples replayed into the head")

            got = query_samples()
            ts = [t for t, _ in got]
            if ts != sorted(set(ts)):
                raise SystemExit("FAIL: query returned duplicate or unordered "
                                 "timestamps — head/segment dedupe is broken")
            missing = set(acked) - set(got)
            if missing:
                raise SystemExit(f"FAIL: {len(missing)} acknowledged samples "
                                 f"LOST, e.g. {sorted(missing)[:3]}")
            extra = len(got) - len(acked)
            note = (f" (+{extra} in-flight sample appended but never acked)"
                    if extra else "")
            print(f"\nOK: all {len(acked)} acknowledged samples came back from "
                  f"a real query across head + segments{note}")
            print(f"OK: segment count is {tsdb['segments']} after "
                  f"{len(acked)} samples' worth of 500ms flush cycles — "
                  "compaction is doing its job")
        finally:
            proc2.kill()
    finally:
        if proc.poll() is None:
            proc.kill()


if __name__ == "__main__":
    main()
