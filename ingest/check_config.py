# ingest/check_config.py
import yaml
import os
import sys

# find config relative to this script
cfg_path = os.path.join(os.path.dirname(__file__), "../config/mexc.yaml")
cfg_path = os.path.abspath(cfg_path)

if not os.path.exists(cfg_path):
    print("CONFIG MISSING:", cfg_path)
    sys.exit(2)

with open(cfg_path, "r") as f:
    cfg = yaml.safe_load(f)

print("Loaded config:", cfg_path)
print("MEXC REST base:", cfg.get("mexc", {}).get("rest_base"))
print("MEXC WS base:", cfg.get("mexc", {}).get("ws_base"))
print("NATS host:", cfg.get("nats", {}).get("host"))
print("ClickHouse host:", cfg.get("clickhouse", {}).get("host"))
print("Redis host:", cfg.get("redis", {}).get("host"))
print("Raw parquet path:", cfg.get("mexc", {}).get("raw_parquet_path"))
