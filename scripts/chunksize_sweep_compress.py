import time
import os
import zstandard as zstd

GRID_DIR = "/work/synthetic_grid"
ZSTD_LEVEL = 12
SRC = f"{GRID_DIR}/synthetic_30k_50000.tab"  # temsili orta-buyuk dosya (~4GB)

src_size = os.path.getsize(SRC)
print(f"Kaynak: {SRC} ({src_size/(1024**3):.2f}GB)", flush=True)
print(flush=True)

CHUNK_SIZES = [
    ("4MB", 4 * 1024 * 1024),
    ("16MB", 16 * 1024 * 1024),
    ("32MB (mevcut varsayilan)", 32 * 1024 * 1024),
    ("64MB", 64 * 1024 * 1024),
    ("128MB", 128 * 1024 * 1024),
]

results = []
for label, chunk_size in CHUNK_SIZES:
    out_path = SRC + ".zst"
    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
    with open(SRC, "rb") as fin, open(out_path, "wb") as fout:
        compressor = cctx.stream_writer(fout)
        while True:
            chunk = fin.read(chunk_size)
            if not chunk:
                break
            compressor.write(chunk)
        compressor.flush(zstd.FLUSH_FRAME)
    elapsed = time.time() - t0
    out_size = os.path.getsize(out_path)
    throughput = src_size / elapsed / (1024 ** 2)
    print(f"{label:>28}: {elapsed:7.1f}sn  {throughput:7.1f}MB/sn  cikti={out_size/(1024**2):.1f}MB", flush=True)
    results.append((label, elapsed, throughput))
    os.remove(out_path)

print(flush=True)
print("=== OZET ===", flush=True)
fastest = min(results, key=lambda r: r[1])
for label, elapsed, throughput in results:
    print(f"{label:>28}: {elapsed:7.1f}sn ({'EN HIZLI' if label == fastest[0] else f'{(elapsed/fastest[1]-1)*100:+.1f}%'})", flush=True)
