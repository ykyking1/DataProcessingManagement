"""Saf CPU-bound mikro-benchmark -- disk I/O yok, paralellik/throttling
testinde CPU tarafını izole etmek için."""
import sys
import time

iters = int(sys.argv[1]) if len(sys.argv) > 1 else 450_000_000
t0 = time.time()
s = 0
for i in range(iters):
    s += i * i
elapsed = time.time() - t0
print(f"{iters} iter, {elapsed:.2f}s, checksum={s}")
