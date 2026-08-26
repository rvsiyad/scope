"""Kill -9 the collector mid-ingest and prove no acknowledged batch died.

Self-contained: builds the collector, runs it on a scratch port with a
scratch WAL, and manages the whole crash from here — no setup needed:

    python3 examples/crash_recovery_demo.py

The script blasts batches at /v1/ingest and counts the 204 acks. Partway
through, a background thread SIGKILLs the collector — no shutdown hook, no
final flush, the process just stops existing (sends fail until the restart).
Then it starts a fresh collector on the same WAL and compares:

    batches acked before the kill  <=  batches_received after restart

That inequality is the ack contract. The collector runs SCOPE_WAL_SYNC=always
(fsync before every 204), so an ack means "on disk" — the write-ahead log
replays every acknowledged batch into the restarted process. At most one
*unacked* in-flight batch may also appear (appended, killed before the 204
left the socket); an ack can never disappear.

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

PORT = 9391  # scratch port: leave any real collector on 9091 alone
BASE = f"http://localhost:{PORT}"


def wait_healthy(timeout=10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(BASE + "/healthz", timeout=1) as resp:
                return json.load(resp)
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("collector never became healthy")


def start(binary: str, wal_path: str) -> subprocess.Popen:
    env = dict(os.environ,
               SCOPE_COLLECTOR_ADDR=f":{PORT}",
               SCOPE_WAL_PATH=wal_path,
               SCOPE_WAL_SYNC="always")
    return subprocess.Popen([binary], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def ingest(i: int) -> bool:
    batch = {"spans": [{"trace_id": f"trace-{i}", "span_id": "s", "name": "request",
                        "start": 1, "end": 2}]}
    req = urllib.request.Request(BASE + "/v1/ingest", data=json.dumps(batch).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=2) as resp:
            return resp.status == 204
    except OSError:
        return False


def main() -> None:
    workdir = tempfile.mkdtemp(prefix="scope-crash-demo-")
    binary = os.path.join(workdir, "collector")
    wal_path = os.path.join(workdir, "collector.wal")
    print("building collector...")
    subprocess.run(["go", "build", "-o", binary, "./cmd/collector"], check=True)

    proc = start(binary, wal_path)
    try:
        wait_healthy()
        print(f"collector up on :{PORT} (wal={wal_path}, sync=always)")

        kill_after = random.randint(40, 120)
        killed = threading.Event()

        def assassin():
            while not killed.is_set():
                time.sleep(0.005)
                if acked[0] >= kill_after:
                    proc.send_signal(signal.SIGKILL)  # no goodbyes
                    killed.set()

        acked = [0]
        threading.Thread(target=assassin, daemon=True).start()

        sent = 0
        while not killed.is_set() and sent < 5000:
            sent += 1
            if ingest(sent):
                acked[0] += 1
        killed.set()
        proc.wait()
        print(f"SIGKILL after {acked[0]} acked batches ({sent} attempted) — "
              "no shutdown, no flush, the process just stopped existing")

        proc2 = start(binary, wal_path)
        try:
            health = wait_healthy()
            received = health["batches_received"]
            print(f"restarted on the same WAL: {health['wal']['records']} records "
                  f"replayed, batches_received={received}")

            if received >= acked[0]:
                slack = received - acked[0]
                extra = (f" (+{slack} in-flight batch appended but never acked)"
                         if slack else "")
                print(f"\nOK: every one of the {acked[0]} acknowledged batches "
                      f"survived the kill{extra}")
            else:
                print(f"\nFAIL: {acked[0] - received} acknowledged batches were LOST")
                raise SystemExit(1)
        finally:
            proc2.kill()
    finally:
        if proc.poll() is None:
            proc.kill()


if __name__ == "__main__":
    main()
