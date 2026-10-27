"""Fixed-work local scan for both full-runtime comparison arms."""
import hashlib
import json
import sys
from pathlib import Path

data = Path("corpus.bin").read_bytes()
passes = int(sys.argv[1])
for _ in range(passes):
    digest = hashlib.sha256(data).hexdigest()
print(json.dumps({"passes": passes, "digest": digest}), flush=True)
