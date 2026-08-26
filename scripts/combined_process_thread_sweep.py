# -*- coding: utf-8 -*-
"""
Bolum 42.1'in AYNI 10-dosyalik/22GB is yukunde (5.000 ve 50.000 satirlik
tier'ler, 5 sutun-tier'i), N=6 process/thread=1'in kaydiyla (631,1sn)
karsilastirmali olarak N process x K thread kombinasyonlarini test eder.
Amac: process-bazli paralellik (farkli dosyalar) ile ZSTD'nin kendi
dahili thread'ini (TEK dosya icinde) BIRLIKTE kullanmanin, sadece
process-paralelligi kullanmaktan daha iyi olup olmadigini bulmak.
Toplam ~20 cekirdek sinirini asmayacak N x K kombinasyonlari denenir.
"""
import sys
import time
import os
from concurrent.futures import ProcessPoolExecutor
import zstandard as zstd

GRID_DIR = "/work/synthetic_grid"
CHUNK = 64 * 1024 * 1024

FILES = [
    f"{GRID_DIR}/synthetic_10k_5000.tab",
    f"{GRID_DIR}/synthetic_20k_5000.tab",
    f"{GRID_DIR}/synthetic_30k_5000.tab",
    f"{GRID_DIR}/synthetic_40k_5000.tab",
    f"{GRID_DIR}/synthetic_50k_5000.tab",
    f"{GRID_DIR}/synthetic_10k_50000.tab",
    f"{GRID_DIR}/synthetic_20k_50000.tab",
    f"{GRID_DIR}/synthetic_30k_50000.tab",
    f"{GRID_DIR}/synthetic_40k_50000.tab",
    f"{GRID_DIR}/synthetic_50k_50000.tab",
]

n_processes = int(sys.argv[1])
k_threads = int(sys.argv[2])

total_src_size = sum(os.path.getsize(f) for f in FILES)


def compress_one(src_path, threads):
    out_path = src_path + f".ptcombo.zst"
    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=12, threads=threads)
    with open(src_path, "rb") as fin, open(out_path, "wb") as fout:
        compressor = cctx.stream_writer(fout)
        while True:
            chunk = fin.read(CHUNK)
            if not chunk:
                break
            compressor.write(chunk)
        compressor.flush(zstd.FLUSH_FRAME)
    elapsed = time.time() - t0
    os.remove(out_path)
    return os.path.basename(src_path), elapsed


print(f"N={n_processes} process x K={k_threads} thread (toplam ~{n_processes*k_threads} cekirdek talebi)", flush=True)
t0 = time.time()
with ProcessPoolExecutor(max_workers=n_processes) as pool:
    futures = [pool.submit(compress_one, f, k_threads) for f in FILES]
    file_results = [fut.result() for fut in futures]
wall_time = time.time() - t0
throughput = total_src_size / wall_time / (1024 ** 2)
print(f"SONUC N={n_processes} K={k_threads}: toplam_sure={wall_time:.1f}sn throughput={throughput:.1f}MB/sn", flush=True)
for name, elapsed in sorted(file_results, key=lambda x: -x[1]):
    print(f"  {name}: {elapsed:.1f}sn", flush=True)
