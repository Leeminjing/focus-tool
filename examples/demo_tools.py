"""Ordinary local Python tools; both sync and async callables are supported."""
import asyncio
import json
from pathlib import Path


def read_config():
    return json.loads(Path("config.json").read_text())


def repair_config():
    Path("repaired.json").write_text('{"valid":true}')
    return json.loads(Path("repaired.json").read_text())


async def echo(value):
    await asyncio.sleep(0)
    return {"value": value}


def increment(value):
    return {"answer": value + 1}
