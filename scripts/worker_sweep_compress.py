import time
import os
import zstandard as zstd
from concurrent.futures import ProcessPoolExecutor, as_completed

GRID_DIR = "/work/synthetic_grid"
ZSTD_LEVEL = 12
CHUNK = 32 * 1024 * 1024  # sabit -- bu testte sadece worker sayisi degisiyor

# 5.000 VE 50.000 satirlik tier'ler -- 5 sutun-tier'ini kapsayan, boyut
# cesitliligi olan, 10 BAGIMSIZ DOSYALIK sabit is yuku (N=10'a kadar
# anlamli test edebilmek icin en az 10 gorev gerekiyor -- 5 dosyayla
# N=6+ zaten en fazla 5 paralel calisir, anlamsiz olurdu)
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

total_src_size = sum(os.path.getsize(f) for f in FILES)
print(f"Is yuku: {len(FILES)} dosya, toplam {total_src_size/(1024**3):.2f}GB", flush=True)
print(flush=True)


def compress_one(src_path):
    out_path = src_path + ".zst"
    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
    with open(src_path, "rb") as fin, open(out_path, "wb") as fout:
        compressor = cctx.stream_writer(fout)
        while True:
            chunk = fin.read(CHUNK)
            if not chunk:
                break
            compressor.write(chunk)
        compressor.flush(zstd.FLUSH_FRAME)
    elapsed = time.time() - t0
    out_size = os.path.getsize(out_path)
    os.remove(out_path)  # bir sonraki denemeye temiz baslamak icin
    return os.path.basename(src_path), elapsed, out_size


# N=1 ve N=2 daha once olculmustu (kesinti oncesi): 1250.9sn, 825.5sn.
# Kaldigimiz yerden devam -- sadece kalanlari test ediyoruz.
WORKER_COUNTS = [4, 6, 10, 16]
results = {1: (1250.9, total_src_size / 1250.9 / (1024 ** 2)),
           2: (825.5, total_src_size / 825.5 / (1024 ** 2))}
print(f"N= 1: toplam_sure=  1250.9sn  (onceki oturumdan)", flush=True)
print(f"N= 2: toplam_sure=   825.5sn  (onceki oturumdan)", flush=True)

for n_workers in WORKER_COUNTS:
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        futures = [pool.submit(compress_one, f) for f in FILES]
        file_results = [fut.result() for fut in futures]
    wall_time = time.time() - t0
    total_out_size = sum(r[2] for r in file_results)
    throughput = total_src_size / wall_time / (1024 ** 2)  # MB/sn
    print(
        f"N={n_workers:2d}: toplam_sure={wall_time:7.1f}sn  "
        f"throughput={throughput:7.1f}MB/sn  cikti_toplam={total_out_size/(1024**3):.3f}GB",
        flush=True,
    )
    results[n_workers] = (wall_time, throughput)

print(flush=True)
print("=== OZET ===", flush=True)
baseline = results[1][0]
for n_workers, (wall_time, throughput) in results.items():
    speedup = baseline / wall_time
    print(f"N={n_workers:2d}: {wall_time:7.1f}sn  hizlanma={speedup:.2f}x  "
          f"verimlilik={speedup/n_workers*100:.0f}%", flush=True)
