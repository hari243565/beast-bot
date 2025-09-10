#!/usr/bin/env python3
"""
Simple orchestrator:
- reads config/mexc.yaml for ws_url, nats_url, subject prefix
- starts the hot_ingest binary (file or ws mode)
- if binary dies, restarts up to N times
- provides a simple REST-resync example using ccxt (optional)
"""
import subprocess, time, os, signal, yaml, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BIN = REPO_ROOT / "ingest" / "hot_ingest" / "target" / "release" / "hot_ingest"

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
    print("Starting hot_ingest:", " ".join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, preexec_fn=os.setsid)
    return p

def tail_process(p):
    try:
        while True:
            line = p.stdout.readline()
            if not line:
                break
            print("[hot_ingest]", line.strip())
    except Exception as e:
        print("Tail error:", e)

def supervise(mode="file", max_restarts=5):
    restarts = 0
    while True:
        p = start_hot_ingest(mode=mode)
        tail_process(p)
        ret = p.poll()
        print("hot_ingest exited with", ret)
        if ret == 0:
            print("clean exit")
            break
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
