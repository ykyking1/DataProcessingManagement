# -*- coding: utf-8 -*-
"""
`max_threads` ve `max_insert_threads` -- pipeline'da Bolum 37/41.1'den
beri sadece "bellek guvenligi icin dusuk tut" varsayimiyla 2/1'de sabit
tutulmustu, HICBIR ZAMAN izole test edilmemisti (max_download_threads
Bolum 43.2'de test edildi, kazanc yoktu; max_insert_threads sadece
~1.000 sutun olcekte test edilmisti -- Bolum 28.1, artirmak kotulestirmisti).

Artik proaktif bolme (Bolum 46) sayesinde her parca guvenli boyutta
(~833M hucre) -- bu guvenli marj icinde thread sayisini artirip hiz
kazanci var mi test ediliyor. Zaten MinIO'da duran bir parca objesi
(grid/synthetic_50k_100000_p1.tab.zst, 50.002 sutun x ~16.667 satir)
kullaniliyor, her denemeden once ClickHouse'un fresh restart edilmis
olmasi onerilir (host disinda calistiriliyor, restart'lar ayri
komutlarla yapiliyor).
"""
import sys
import json
import time
from clickhouse_driver import Client

GRID_DIR = "/work/synthetic_grid"
BUCKET = "telemetry"
OBJECT_KEY = "grid/synthetic_50k_100000_p1.tab.zst"
TAG_FOR_COLUMNS = "50k_100000"  # sutun semasi ayni dosyadan
TABLE_NAME = "thread_sweep_test"

BASE_SETTINGS = {
    "max_query_size": 300_000_000,
    "max_ast_elements": 10_000_000,
    "max_expanded_ast_elements": 10_000_000,
    "input_format_parallel_parsing": 0,
}

# sys.argv[1] = "threads" ya da "insert_threads", sys.argv[2] = deger
mode = sys.argv[1]
value = int(sys.argv[2])

ch = Client(host="clickhouse", user="default", password="ch123", database="default", settings=BASE_SETTINGS)

manifest_path = f"{GRID_DIR}/synthetic_{TAG_FOR_COLUMNS}_columns.json"
with open(manifest_path, "r", encoding="utf-8") as f:
    gen_manifest = json.load(f)
cols = gen_manifest["column_order"]

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

settings = dict(BASE_SETTINGS)
if mode == "threads":
    settings["max_threads"] = value
    settings["max_insert_threads"] = 1
elif mode == "insert_threads":
    settings["max_threads"] = 2
    settings["max_insert_threads"] = value
else:
    raise SystemExit("mode 'threads' ya da 'insert_threads' olmali")

s3_url = f"http://minio:9000/{BUCKET}/{OBJECT_KEY}"
insert_sql = (
    f"INSERT INTO {TABLE_NAME} SELECT * FROM s3("
    f"'{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')"
)

ch.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
ch.execute(ddl, settings=BASE_SETTINGS)

t0 = time.time()
try:
    ch.execute(insert_sql, settings=settings)
    elapsed = time.time() - t0
    row_count = ch.execute(f"SELECT count() FROM {TABLE_NAME}", settings=BASE_SETTINGS)[0][0]
    print(f"SONUC mode={mode} deger={value}: {elapsed:.1f}sn, {row_count} satir, OK", flush=True)
except Exception as e:
    elapsed = time.time() - t0
    print(f"SONUC mode={mode} deger={value}: COKTU {elapsed:.1f}sn sonra -- {type(e).__name__}: {str(e)[:200]}", flush=True)
finally:
    ch.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}", settings=BASE_SETTINGS)
