# -*- coding: utf-8 -*-
"""
Kesfedici/tanisal script -- 50.002 sutunluk (en riskli tier) dosyada
KAC SATIRA kadar TEK INSERT ile guvenle yuklenebildigini bulmak icin.
Postgres'e HICBIR SEY YAZMAZ (manifest'i kirletmemek icin) -- sadece
konsola sonuc basar. `synthetic_50k_100000.tab`'in ILK N_ROWS satirini
fiziksel olarak ayirip sikistirir, MinIO'ya ayri bir obje olarak
yukler, ClickHouse'a tek INSERT ile alir, basari/hata + sureyi raporlar.

Kullanim: python3 probe_max_rows_at_50k_cols.py <N_ROWS>
Her cagridan once ClickHouse'un TEMIZ/restart edilmis olmasi onerilir
(host bellek durumu olcumu etkiliyor, bkz. plan Bolum 45.1/46).
"""
import sys
import time
import json
import os
import zstandard as zstd
from clickhouse_driver import Client
from minio import Minio

GRID_DIR = "/work/synthetic_grid"
BUCKET = "telemetry"
TAG = "50k_100000"
SRC_TAB = f"{GRID_DIR}/synthetic_{TAG}.tab"
ZSTD_LEVEL = 12
CHUNK = 64 * 1024 * 1024

N_ROWS = int(sys.argv[1])

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

ch = Client(host="clickhouse", user="default", password="ch123", database="default", settings=SETTINGS)
mc = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin123", secure=False)

manifest_path = f"{GRID_DIR}/synthetic_{TAG}_columns.json"
with open(manifest_path, "r", encoding="utf-8") as f:
    gen_manifest = json.load(f)
cols = gen_manifest["column_order"]
n_total_cols = len(cols)

col_defs = []
for c in cols:
    if c == "aircraft_type":
        col_defs.append(f"`{c}` LowCardinality(String) CODEC(ZSTD)")
    elif c == "timestamp" or c.startswith("f"):
        col_defs.append(f"`{c}` Float64 CODEC(ZSTD)")
    else:
        col_defs.append(f"`{c}` UInt8 CODEC(T64, ZSTD)")
ddl = (
    f"CREATE TABLE probe_table (\n  "
    + ",\n  ".join(col_defs)
    + "\n) ENGINE = MergeTree() ORDER BY tuple()\n"
    "SETTINGS min_bytes_for_wide_part = 10737418240000, min_rows_for_wide_part = 1000000000"
)

print(f"Prob: {n_total_cols} sutun x {N_ROWS} satir ({n_total_cols*N_ROWS/1e6:.1f}M hucre)", flush=True)

part_tab = f"{GRID_DIR}/probe_{N_ROWS}.tab"
t0 = time.time()
with open(SRC_TAB, "r", encoding="utf-8", newline="") as fin:
    header = fin.readline()
    with open(part_tab, "w", encoding="utf-8", newline="") as fp:
        fp.write(header)
        for _ in range(N_ROWS):
            line = fin.readline()
            if not line:
                break
            fp.write(line)
split_time = time.time() - t0
part_size = os.path.getsize(part_tab)
print(f"  bolme: {split_time:.1f}sn, {part_size/(1024**2):.1f}MB", flush=True)

out_zst = part_tab + ".zst"
object_key = f"grid/probe_{N_ROWS}.tab.zst"
t0 = time.time()
cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
with open(part_tab, "rb") as fin, open(out_zst, "wb") as fout:
    compressor = cctx.stream_writer(fout)
    while True:
        chunk = fin.read(CHUNK)
        if not chunk:
            break
        compressor.write(chunk)
    compressor.flush(zstd.FLUSH_FRAME)
compress_time = time.time() - t0
zst_size = os.path.getsize(out_zst)
print(f"  sikistirma: {compress_time:.1f}sn, {zst_size/(1024**2):.1f}MB", flush=True)

t0 = time.time()
mc.fput_object(BUCKET, object_key, out_zst)
upload_time = time.time() - t0
print(f"  MinIO yukleme: {upload_time:.1f}sn", flush=True)
os.remove(out_zst)
os.remove(part_tab)

s3_url = f"http://minio:9000/{BUCKET}/{object_key}"
ch.execute("DROP TABLE IF EXISTS probe_table")
ch.execute(ddl, settings=SETTINGS)
insert_sql = (
    f"INSERT INTO probe_table SELECT * FROM s3("
    f"'{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')"
)
t0 = time.time()
try:
    ch.execute(insert_sql, settings=SETTINGS)
    load_time = time.time() - t0
    row_count = ch.execute("SELECT count() FROM probe_table", settings=SETTINGS)[0][0]
    ok = (row_count == N_ROWS)
    print(f"SONUC: BASARILI -- yukle={load_time:.1f}sn, satir={row_count} {'OK' if ok else 'SATIR SAYISI UYUSMUYOR'}", flush=True)
except Exception as e:
    load_time = time.time() - t0
    print(f"SONUC: COKTU -- {load_time:.1f}sn sonra -- {type(e).__name__}: {str(e)[:300]}", flush=True)
finally:
    ch.execute("DROP TABLE IF EXISTS probe_table", settings=SETTINGS)
    mc.remove_object(BUCKET, object_key)
