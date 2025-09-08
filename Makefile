.PHONY: bench smoke

bench:
	python bench/json_vs_orjson.py

smoke:
	python scripts/smoke_uvloop.py && python scripts/smoke_orjson.py
