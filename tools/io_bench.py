"""Saf disk-yazma mikro-benchmark -- CPU işi yok (aynı buffer tekrar tekrar
yazılıyor), paralellik/EDR-tarama testinde disk I/O tarafını izole etmek
için."""
import sys
import time

output_path = sys.argv[1]
target_mb = int(sys.argv[2]) if len(sys.argv) > 2 else 500

buf = bytes(64 * 1024 * 1024)  # 64MB sıfır buffer, tekrar tekrar yazılacak
n_writes = max(1, target_mb // 64)

t0 = time.time()
with open(output_path, "wb") as f:
    for _ in range(n_writes):
        f.write(buf)
    f.flush()
elapsed = time.time() - t0
mb = n_writes * 64
print(f"{mb} MB, {elapsed:.2f}s, {mb/elapsed:.1f} MB/s")
