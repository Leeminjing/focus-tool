"""Trusted deterministic short tools; each reads a real local fixture."""
import json
from pathlib import Path


def read_file():
    return {"kind": "read", "value": len(Path("corpus.txt").read_bytes())}


def search_text():
    return {"kind": "search", "value": Path("corpus.txt").read_text(encoding="utf-8").count("Patrol")}


def parse_config():
    config = json.loads(Path("config.json").read_text(encoding="utf-8"))
    return {"kind": "parse", "value": sum(item["x"] for item in config["items"])}
