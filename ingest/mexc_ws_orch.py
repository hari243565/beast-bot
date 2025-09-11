#!/usr/bin/env python3
"""
Orchestrator + Prometheus exporter.

- Starts hot_ingest binary (file or ws mode)
- Tails stdout looking for JET_ACK / PUB lines and extracts ack_time_us / flush_time_us
- Exposes /metrics on port 8000 for Prometheus to scrape
"""

import subprocess, time, os, signal, yaml, sys, re, threading
from pathlib import Path
from prometheus_client import start_http_server, Histogram, Counter

REPO_ROOT = Path(__file__).resolve().parents[1]
BIN = REPO_ROOT / "ingest" / "hot_ingest" / "target" / "release" / "hot_ingest"

# Prometheus metrics
ACK_LATENCY = Histogram("hot_ingest_jet_ack_us", "JetStream ack time (microseconds)", buckets=(50,100,250,500,1000,2000,5000,10000))
PUB_FLUSH_LATENCY = Histogram("hot_ingest_pub_flush_us", "Publish+flush time (microseconds)", buckets=(50,100,250,500,1000,2000,5000,10000))
PUBLISH_COUNT = Counter("hot_ingest_publishes_total", "Total publish attempts")
PUBLISH_ERRORS = Counter("hot_ingest_publish_errors_total", "Total publish errors")

# regex patterns
JET_ACK_RE = re.compile(r"JET_ACK\s+seq_local=(\d+)\s+subj=([^ ]+)\s+ack=.*ack_time_us=(\d+)")
PUB_RE = re.compile(r"PUB\s+seq_local=(\d+)\s+subj=([^ ]+)\s+bytes=\d+\s+flush_time_us=(\d+)")

def load_config():
    cfg_path = REPO_ROOT / "config" / "mexc.yaml"
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg

def start_hot_ingest(mode="file", extra_args=None):
    if not BIN.exists():
        print("Binary not found. Build it: cd ingest/hot_ingest && cargo build --release")
        sys.exit(2)
    args = [str(BIN), "--mode", mode]
    if mode == "file":
        args += ["--file", str(REPO_ROOT / "ingest" / "sample_feed.jsonl")]
    else:
        cfg = load_config()
        args += ["--ws-url", cfg.get("mexc", {}).get("ws_base", "wss://example.invalid")]
    cfg = load_config()
    args += ["--nats-url", f"nats://{cfg.get('nats', {}).get('host','127.0.0.1')}:{cfg.get('nats', {}).get('port',4222)}"]
    args += ["--subj-prefix", cfg.get("nats", {}).get("subjects", {}).get("raw", "mexc.raw")]
    # enable JetStream by default (we want durable acks)
    args += ["--jetstream"]
    if extra_args:
        args += extra_args
    print("Starting hot_ingest:", " ".join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, preexec_fn=os.setsid)
    return p

def tail_and_metrics(p):
    """
    Read lines from the process stdout, print them, and update metrics when we find patterns.
    """
    try:
        while True:
            line = p.stdout.readline()
            if line == "" and p.poll() is not None:
                break
            if not line:
                time.sleep(0.01)
                continue
            line = line.strip()
            print("[hot_ingest]", line)

            # match JET_ACK
            m = JET_ACK_RE.search(line)
            if m:
                try:
                    seq = int(m.group(1))
                    subj = m.group(2)
                    ack_time_us = int(m.group(3))
                    ACK_LATENCY.observe(ack_time_us)
                    PUBLISH_COUNT.inc()
                except Exception as e:
                    print("metric parse err (JET_ACK):", e)
                    PUBLISH_ERRORS.inc()
                continue

            # match PUB flush
            m2 = PUB_RE.search(line)
            if m2:
                try:
                    seq = int(m2.group(1))
                    subj = m2.group(2)
                    flush_time_us = int(m2.group(3))
                    PUB_FLUSH_LATENCY.observe(flush_time_us)
                    PUBLISH_COUNT.inc()
                except Exception as e:
                    print("metric parse err (PUB):", e)
                    PUBLISH_ERRORS.inc()
                continue
    except Exception as e:
        print("tail error:", e)

def supervise(mode="file", max_restarts=5):
    # start Prometheus HTTP server on 8000
    start_http_server(8000)
    print("Prometheus metrics available on http://127.0.0.1:8000/metrics")

    restarts = 0
    while True:
        p = start_hot_ingest(mode=mode)
        # use thread to tail so we can supervise loop concurrently if needed
        t = threading.Thread(target=tail_and_metrics, args=(p,), daemon=True)
        t.start()
        # wait for process to exit
        ret = p.wait()
        print("hot_ingest exited with", ret)
        if ret == 0:
            # Instead of exiting when child exits cleanly,
            # keep metrics server alive and restart the hot_ingest after a short delay.
            print("clean exit — keeping metrics server running; restarting hot_ingest in 3s")
            time.sleep(3)
            # continue the loop and restart hot_ingest
            continue
        restarts += 1
        if restarts > max_restarts:
            print("Too many restarts, giving up.")
            break
        print("Restarting hot_ingest in 3s...")
        time.sleep(3)

if __name__ == "__main__":
    mode = "file"
    if len(sys.argv) > 1 and sys.argv[1] == "ws":
        mode = "ws"
    supervise(mode=mode)

