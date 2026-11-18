"""Extract members from a truncated .tar.gz until the stream ends. Args: part outdir."""

import sys
import tarfile
from pathlib import Path

src, out = sys.argv[1], Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
n = 0
try:
    with tarfile.open(src, "r|gz") as tf:
        for m in tf:
            if not m.isfile():
                continue
            name = m.name.lstrip("./")
            if ".." in name:
                continue
            f = tf.extractfile(m)
            data = f.read() if f else b""
            p = out / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
            n += 1
except Exception as e:  # truncated stream
    print("stopped:", type(e).__name__, e)
print("files", n)
