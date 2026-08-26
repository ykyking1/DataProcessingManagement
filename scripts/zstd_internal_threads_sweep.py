# -*- coding: utf-8 -*-
"""
Su ana kadar hep "N ayri tek-thread'li process" (ProcessPoolExecutor)
denendi. Bu script ZSTD'nin KENDI dahili coklu-thread destegini
(`ZstdCompressor(threads=N)`) test ediyor -- TEK BUYUK dosyayi birden
fazla cekirdekle sikistirmak, N ayri dosyayi paralel sikistirmaktan
FARKLI bir mekanizma. Kullanicinin mentor hatirlamasindan esinlenildi
("5 belgeyi 300 satirlik parcalara bolup her parcaya 4 worker" ->
muhtemelen "5 dosya, her biri 4 cekirdek kullanan tek bir sikistirma
cagrisi" anlamina geliyordu).
"""
import time
import os
import zstandard as zstd

GRID_DIR = "/work/synthetic_grid"
CHUNK = 64 * 1024 * 1024
SRC = f"{GRID_DIR}/synthetic_30k_50000.tab"  # ~4,3GB, orta-buyuk temsili dosya

THREAD_VALUES = [1, 2, 4, 8, 16, 20]

src_size_mb = os.path.getsize(SRC) / 1024**2
print(f"Test dosyasi: 30k_50000.tab ({src_size_mb:.1f}MB)", flush=True)
print(flush=True)

results = {}
for n_threads in THREAD_VALUES:
    out = f"{GRID_DIR}/zstd_threads_test_{n_threads}.tab.zst"
    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=12, threads=n_threads)
    with open(SRC, "rb") as fin, open(out, "wb") as fout:
        compressor = cctx.stream_writer(fout)
        while True:
            chunk = fin.read(CHUNK)
            if not chunk:
                break
            compressor.write(chunk)
        compressor.flush(zstd.FLUSH_FRAME)
    elapsed = time.time() - t0
    out_size_mb = os.path.getsize(out) / 1024**2
    os.remove(out)
    mbps = src_size_mb / elapsed
    results[n_threads] = elapsed
    print(f"threads={n_threads:2d}: {elapsed:6.1f}sn  {mbps:6.1f}MB/s  cikti={out_size_mb:.1f}MB", flush=True)

print(flush=True)
print("=== OZET ===", flush=True)
baseline = results[1]
for n_threads, elapsed in results.items():
    speedup = baseline / elapsed
    print(f"threads={n_threads:2d}: {elapsed:6.1f}sn  hizlanma={speedup:.2f}x", flush=True)
