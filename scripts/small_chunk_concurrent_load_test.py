# -*- coding: utf-8 -*-
"""
Kullanicinin mentorunun MinIO->ClickHouse GECISI icin anlattigi yontem:
5 belgeyi 300'er SATIRLIK parcalara bolup her parcaya (chunk grubuna)
4 worker atamak. Bu, Bolum 43.3'te test ettigimiz N=2 esazamanli
yuklemeden IKI noktada farkli: (1) COK DAHA KUCUK parca boyutu (300
satir, bizim ~16-20k satirlik parcalarimiza karsi), (2) DAHA FAZLA
worker (4, bizim N=2'mize karsi).

Bu script synthetic_10k_5000.tab'i (5.000 satir, 10.002 sutun) 300'er
satirlik ~17 parcaya boler, her parcayi ayri sikistirip MinIO'ya yukler,
sonra N=4 esazamanli (ThreadPoolExecutor) ClickHouse'a yukler -- TOPLAM
sureyi, ayni veriyi TEK parca halinde sirali yuklemenin suresiyle
karsilastirir.
"""
import time
import json
import os
import zstandard as zstd
from clickhouse_driver import Client
from minio import Minio
from concurrent.futures import ThreadPoolExecutor

GRID_DIR = "/work/synthetic_grid"
BUCKET = "telemetry"
TAG = "10k_5000"
SRC_TAB = f"{GRID_DIR}/synthetic_{TAG}.tab"
ROWS_PER_CHUNK = 300
N_WORKERS = 4
ZSTD_LEVEL = 12
CHUNK_BYTES = 64 * 1024 * 1024

SETTINGS = {
    "max_query_size": 300_000_000,
    "max_ast_elements": 10_000_000,
    "max_expanded_ast_elements": 10_000_000,
    "input_format_parallel_parsing": 0,
    "max_threads": 2,
    "max_insert_threads": 1,
    "max_block_size": 8192,
    "max_insert_block_size": 8192,
}

mc = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin123", secure=False)

manifest_path = f"{GRID_DIR}/synthetic_{TAG}_columns.json"
with open(manifest_path, "r", encoding="utf-8") as f:
    gen_manifest = json.load(f)
cols = gen_manifest["column_order"]
row_count_tab = gen_manifest["n_rows"]

col_defs = []
for c in cols:
    if c == "aircraft_type":
        col_defs.append(f"`{c}` LowCardinality(String) CODEC(ZSTD)")
    elif c == "timestamp" or c.startswith("f"):
        col_defs.append(f"`{c}` Float64 CODEC(ZSTD)")
    else:
        col_defs.append(f"`{c}` UInt8 CODEC(T64, ZSTD)")


def build_ddl(table_name):
    return (
        f"CREATE TABLE {table_name} (\n  "
        + ",\n  ".join(col_defs)
        + "\n) ENGINE = MergeTree() ORDER BY tuple()\n"
        "SETTINGS min_bytes_for_wide_part = 10737418240000, min_rows_for_wide_part = 1000000000"
    )


def make_ch():
    return Client(host="clickhouse", user="default", password="ch123", database="default", settings=SETTINGS)


# ---- 1) BASELINE: TEK parca, SIRALI yukleme (mevcut pipeline deseni) ----
print("=== BASELINE: tek parca, sirali ===", flush=True)
ch = make_ch()
ch.execute("DROP TABLE IF EXISTS baseline_test")
ch.execute(build_ddl("baseline_test"), settings=SETTINGS)
s3_url_full = f"http://minio:9000/{BUCKET}/grid/synthetic_{TAG}.tab.zst"
t0 = time.time()
ch.execute(
    f"INSERT INTO baseline_test SELECT * FROM s3('{s3_url_full}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')",
    settings=SETTINGS,
)
baseline_time = time.time() - t0
row_count = ch.execute("SELECT count() FROM baseline_test", settings=SETTINGS)[0][0]
ch.execute("DROP TABLE IF EXISTS baseline_test", settings=SETTINGS)
print(f"Baseline: {baseline_time:.1f}sn, {row_count} satir", flush=True)
print(flush=True)

# ---- 2) Kaynak dosyayi 300'er satirlik parcalara bol, sikistir, yukle ----
print(f"=== KUCUK PARCA: {ROWS_PER_CHUNK} satir/parca, N={N_WORKERS} esazamanli yukleme ===", flush=True)
print("Bolme + sikistirma + MinIO yukleme (sirali, tek seferlik hazirlik)...", flush=True)

t_prep0 = time.time()
chunk_object_keys = []
with open(SRC_TAB, "r", encoding="utf-8", newline="") as fin:
    header = fin.readline()
    idx = 0
    while True:
        idx += 1
        part_path = f"{GRID_DIR}/small_chunk_{idx}.tab"
        rows_this = 0
        with open(part_path, "w", encoding="utf-8", newline="") as fp:
            fp.write(header)
            for _ in range(ROWS_PER_CHUNK):
                line = fin.readline()
                if not line:
                    break
                fp.write(line)
                rows_this += 1
        if rows_this == 0:
            os.remove(part_path)
            break
        out_zst = part_path + ".zst"
        cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
        with open(part_path, "rb") as pf, open(out_zst, "wb") as of:
            c = cctx.stream_writer(of)
            while True:
                chunk = pf.read(CHUNK_BYTES)
                if not chunk:
                    break
                c.write(chunk)
            c.flush(zstd.FLUSH_FRAME)
        object_key = f"grid/small_chunk_{idx}.tab.zst"
        mc.fput_object(BUCKET, object_key, out_zst)
        chunk_object_keys.append(object_key)
        os.remove(out_zst)
        os.remove(part_path)
prep_time = time.time() - t_prep0
print(f"Hazirlik tamam: {len(chunk_object_keys)} parca, {prep_time:.1f}sn", flush=True)


def load_one_chunk(idx_key):
    idx, object_key = idx_key
    ch_local = make_ch()
    table_name = f"small_chunk_test_{idx}"
    ch_local.execute(f"DROP TABLE IF EXISTS {table_name}")
    ch_local.execute(build_ddl(table_name), settings=SETTINGS)
    s3_url = f"http://minio:9000/{BUCKET}/{object_key}"
    ch_local.execute(
        f"INSERT INTO {table_name} SELECT * FROM s3('{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')",
        settings=SETTINGS,
    )
    row_count = ch_local.execute(f"SELECT count() FROM {table_name}", settings=SETTINGS)[0][0]
    ch_local.execute(f"DROP TABLE IF EXISTS {table_name}", settings=SETTINGS)
    return row_count


t0 = time.time()
with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
    row_counts = list(pool.map(load_one_chunk, enumerate(chunk_object_keys, start=1)))
load_time = time.time() - t0
total_rows = sum(row_counts)

print(f"Yukleme (N={N_WORKERS} esazamanli): {load_time:.1f}sn, toplam {total_rows} satir", flush=True)
print(f"Hazirlik+yukleme toplam: {prep_time+load_time:.1f}sn", flush=True)
print(flush=True)

print("=== OZET ===", flush=True)
print(f"Baseline (tek parca, sirali): {baseline_time:.1f}sn", flush=True)
print(f"Kucuk parca (N={N_WORKERS}, {ROWS_PER_CHUNK} satir/parca) SADECE yukleme: {load_time:.1f}sn", flush=True)
print(f"Kucuk parca hazirlik+yukleme TOPLAM: {prep_time+load_time:.1f}sn", flush=True)
if total_rows != row_count_tab:
    print(f"UYARI: satir sayisi uyusmuyor! beklenen={row_count_tab} bulunan={total_rows}", flush=True)

# temizlik
for key in chunk_object_keys:
    mc.remove_object(BUCKET, key)
