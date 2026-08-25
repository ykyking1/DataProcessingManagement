# -*- coding: utf-8 -*-
"""
Bolum 26.3'un 'max_download_threads 4->8/20 net kazanc sagliyor' bulgusu
~1.000 sutunluk eski (parquet donemi) semada olculmustu, hic 10.000+
sutunluk genis semada test edilmemisti (plan Bolum 32'nin acik takip
maddesi). Bu script o bosluğu kapatiyor: TEK bir zaten-MinIO'da-var
olan orta-buyuklukte grid dosyasi (30k sutun x 50k satir) uzerinde,
DIGER HER SEY SABIT (max_threads=2, max_insert_threads=1, vs. --
pipeline_grid_to_clickhouse.py'deki SETTINGS ile ayni) tutularak
SADECE max_download_threads taraniyor.

Her deger icin TAMAMEN AYRI/gecici bir tabloya yuklenip hemen silinir
(Bolum 41.1'in "ayni tabloya art arda yukleme bellek baskisi biriktiriyor"
bulgusuna uygun -- adil bir karsilastirma icin de gerekli, kirli/isinmis
tablo etkisini sifirla).

Kullanim: docker exec t2p-cmp3 python3 /work/../scripts/... (repo /work
altina mount'lu degilse, dosyayi container'a kopyalayip calistirin).
"""
import time
import json
from clickhouse_driver import Client

GRID_DIR = "/work/synthetic_grid"
BUCKET = "telemetry"
TEST_TAG = "30k_50000"  # orta-buyuklukte, temsili dosya (30k sutun x 50k satir)
TABLE_NAME = "dl_threads_sweep_test"
THREAD_VALUES = [4, 8, 12, 16, 20]  # 4 = ClickHouse varsayilani, 20 = container'daki mantiksal cekirdek sayisi

BASE_SETTINGS = {
    "max_query_size": 300_000_000,
    "max_ast_elements": 10_000_000,
    "max_expanded_ast_elements": 10_000_000,
    "input_format_parallel_parsing": 0,
    "max_threads": 2,
    "max_insert_threads": 1,
    "max_block_size": 8192,
    "max_insert_block_size": 8192,
}

ch = Client(host="clickhouse", user="default", password="ch123", database="default", settings=BASE_SETTINGS)

manifest_path = f"{GRID_DIR}/synthetic_{TEST_TAG}_columns.json"
with open(manifest_path, "r", encoding="utf-8") as f:
    gen_manifest = json.load(f)
cols = gen_manifest["column_order"]
row_count_expected = gen_manifest["n_rows"]

col_defs = []
for c in cols:
    if c == "aircraft_type":
        col_defs.append(f"`{c}` LowCardinality(String) CODEC(ZSTD)")
    elif c == "timestamp" or c.startswith("f"):
        col_defs.append(f"`{c}` Float64 CODEC(ZSTD)")
    else:
        col_defs.append(f"`{c}` UInt8 CODEC(T64, ZSTD)")
ddl = (
    f"CREATE TABLE {TABLE_NAME} (\n  "
    + ",\n  ".join(col_defs)
    + "\n) ENGINE = MergeTree() ORDER BY tuple()\n"
    "SETTINGS min_bytes_for_wide_part = 10737418240000, min_rows_for_wide_part = 1000000000"
)

object_key = f"grid/synthetic_{TEST_TAG}.tab.zst"
s3_url = f"http://minio:9000/{BUCKET}/{object_key}"
insert_sql = (
    f"INSERT INTO {TABLE_NAME} SELECT * FROM s3("
    f"'{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')"
)

print(f"Test dosyasi: {TEST_TAG} ({len(cols)} sutun, {row_count_expected} satir bekleniyor)", flush=True)
print(f"Sabit ayarlar: max_threads=2, max_insert_threads=1 (pipeline ile ayni)", flush=True)
print(flush=True)

results = []
for n_threads in THREAD_VALUES:
    settings = dict(BASE_SETTINGS)
    settings["max_download_threads"] = n_threads

    ch.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    ch.execute(ddl, settings=BASE_SETTINGS)

    t0 = time.time()
    try:
        ch.execute(insert_sql, settings=settings)
        load_time = time.time() - t0
        row_count = ch.execute(f"SELECT count() FROM {TABLE_NAME}", settings=BASE_SETTINGS)[0][0]
        ok = (row_count == row_count_expected)
        print(f"max_download_threads={n_threads:2d}: {load_time:6.1f}sn  satir={row_count}  "
              f"{'OK' if ok else 'SATIR SAYISI UYUSMUYOR!'}", flush=True)
        results.append((n_threads, load_time, ok))
    except Exception as e:
        elapsed = time.time() - t0
        print(f"max_download_threads={n_threads:2d}: HATA ({elapsed:.1f}sn sonra) -> {type(e).__name__}: {e}", flush=True)
        results.append((n_threads, None, False))
    finally:
        ch.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

print(flush=True)
print("=== OZET ===", flush=True)
baseline = next((t for n, t, ok in results if n == 4 and ok), None)
for n_threads, load_time, ok in results:
    if load_time is None:
        print(f"max_download_threads={n_threads:2d}: BASARISIZ", flush=True)
        continue
    speedup = f"{baseline/load_time:.2f}x" if baseline else "n/a"
    print(f"max_download_threads={n_threads:2d}: {load_time:6.1f}sn  hizlanma={speedup}", flush=True)
