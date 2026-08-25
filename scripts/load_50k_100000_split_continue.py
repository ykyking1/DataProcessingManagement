# -*- coding: utf-8 -*-
"""
load_50k_100000_split.py bir onceki denemede (yanlislikla ClickHouse
restart edilmeden calistirildigi icin, birikmis bellek yuzunden) parcaA
yuklemesinde coktu. parcaA'nin sikistirilmis hali zaten MinIO'da
(grid/synthetic_50k_100000_partA.tab.zst), parcaB'nin yerel .tab hali
hala diskte -- bu script SIFIRDAN BASLAMADAN (bolme/sikistirma/yukleme
tekrarlanmadan) kaldigi yerden devam eder: once parcaA'yi MEVCUT MinIO
objesinden ClickHouse'a yukler, sonra parcaB'yi sikistirip yukler.
"""
import time
import json
import os
import zstandard as zstd
from clickhouse_driver import Client
from minio import Minio
import psycopg2

GRID_DIR = "/work/synthetic_grid"
BUCKET = "telemetry"
TAG = "50k_100000"
TAB_FILE_NAME = f"synthetic_{TAG}.tab"
ZSTD_LEVEL = 12
CHUNK = 64 * 1024 * 1024

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

pg = psycopg2.connect(host="postgres", dbname="telemetry_meta", user="postgres", password="pg123")
pg.autocommit = True
ch = Client(host="clickhouse", user="default", password="ch123", database="default", settings=SETTINGS)
mc = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin123", secure=False)

manifest_path = f"{GRID_DIR}/synthetic_{TAG}_columns.json"
with open(manifest_path, "r", encoding="utf-8") as f:
    gen_manifest = json.load(f)
cols = gen_manifest["column_order"]
aircraft_type = gen_manifest["aircraft_type"]
row_count_tab = gen_manifest["n_rows"]
n_total_cols = len(cols)
src_size = os.path.getsize(f"{GRID_DIR}/{TAB_FILE_NAME}")

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


cur = pg.cursor()
cur.execute(
    """
    UPDATE conversion_manifest SET status='processing', attempt_count = attempt_count + 1, updated_at = now()
    WHERE tab_file_name = %s
    RETURNING attempt_count
    """,
    (TAB_FILE_NAME,),
)
attempt_no = cur.fetchone()[0]
cur.execute(
    "INSERT INTO conversion_manifest_history (tab_file_name, attempt_no, aircraft_type, status) VALUES (%s, %s, %s, 'processing')",
    (TAB_FILE_NAME, attempt_no, aircraft_type),
)
cur.close()
print(f"attempt_no={attempt_no}", flush=True)


def mark_this_error(error_detail):
    c = pg.cursor()
    c.execute(
        "UPDATE conversion_manifest SET status='error', error_detail=%s, updated_at=now() WHERE tab_file_name=%s",
        (error_detail[:4000], TAB_FILE_NAME),
    )
    c.execute(
        "UPDATE conversion_manifest_history SET status='error', error_detail=%s, finished_at=now() "
        "WHERE tab_file_name=%s AND attempt_no=%s",
        (error_detail[:4000], TAB_FILE_NAME, attempt_no),
    )
    c.close()


total_row_count = 0
total_disk_bytes = 0
total_load_time = 0.0
total_compress_time = 0.0
total_upload_time = 0.0
total_zst_size = 0
object_keys = []

try:
    # --- parcaA: MinIO'da zaten var, direkt ClickHouse'a yukle ---
    print("--- Yari A (zaten MinIO'da, direkt yukleniyor) ---", flush=True)
    object_key_a = f"grid/synthetic_{TAG}_partA.tab.zst"
    stat_a = mc.stat_object(BUCKET, object_key_a)
    total_zst_size += stat_a.size
    object_keys.append(object_key_a)
    table_a = f"synthetic_{TAG}_partA"
    s3_url = f"http://minio:9000/{BUCKET}/{object_key_a}"
    ch.execute(f"DROP TABLE IF EXISTS {table_a}")
    ch.execute(build_ddl(table_a), settings=SETTINGS)
    t0 = time.time()
    ch.execute(
        f"INSERT INTO {table_a} SELECT * FROM s3('{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')",
        settings=SETTINGS,
    )
    load_time = time.time() - t0
    total_load_time += load_time
    row_count = ch.execute(f"SELECT count() FROM {table_a}", settings=SETTINGS)[0][0]
    disk_bytes = ch.execute(
        f"SELECT sum(bytes_on_disk) FROM system.parts WHERE table='{table_a}' AND active", settings=SETTINGS
    )[0][0]
    print(f"  ClickHouse yukleme: {load_time:.1f}sn, {row_count} satir, {disk_bytes/(1024**2):.1f}MB", flush=True)
    total_row_count += row_count
    total_disk_bytes += disk_bytes
    ch.execute(f"DROP TABLE IF EXISTS {table_a}", settings=SETTINGS)

    # --- parcaB: yerel .tab hala mevcut, sikistir+yukle+ClickHouse'a al ---
    print("--- Yari B (sikistir + yukle) ---", flush=True)
    part_b_tab = f"{GRID_DIR}/synthetic_{TAG}_partB.tab"
    out_zst = part_b_tab + ".zst"
    object_key_b = f"grid/synthetic_{TAG}_partB.tab.zst"
    table_b = f"synthetic_{TAG}_partB"

    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
    with open(part_b_tab, "rb") as fin, open(out_zst, "wb") as fout:
        compressor = cctx.stream_writer(fout)
        while True:
            chunk = fin.read(CHUNK)
            if not chunk:
                break
            compressor.write(chunk)
        compressor.flush(zstd.FLUSH_FRAME)
    compress_time = time.time() - t0
    zst_size = os.path.getsize(out_zst)
    total_compress_time += compress_time
    total_zst_size += zst_size
    print(f"  sikistirma: {compress_time:.1f}sn, {zst_size/(1024**2):.1f}MB", flush=True)

    t0 = time.time()
    mc.fput_object(BUCKET, object_key_b, out_zst)
    upload_time = time.time() - t0
    total_upload_time += upload_time
    object_keys.append(object_key_b)
    print(f"  MinIO yukleme: {upload_time:.1f}sn", flush=True)
    os.remove(out_zst)
    os.remove(part_b_tab)

    s3_url = f"http://minio:9000/{BUCKET}/{object_key_b}"
    ch.execute(f"DROP TABLE IF EXISTS {table_b}")
    ch.execute(build_ddl(table_b), settings=SETTINGS)
    t0 = time.time()
    ch.execute(
        f"INSERT INTO {table_b} SELECT * FROM s3('{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')",
        settings=SETTINGS,
    )
    load_time = time.time() - t0
    total_load_time += load_time
    row_count = ch.execute(f"SELECT count() FROM {table_b}", settings=SETTINGS)[0][0]
    disk_bytes = ch.execute(
        f"SELECT sum(bytes_on_disk) FROM system.parts WHERE table='{table_b}' AND active", settings=SETTINGS
    )[0][0]
    print(f"  ClickHouse yukleme: {load_time:.1f}sn, {row_count} satir, {disk_bytes/(1024**2):.1f}MB", flush=True)
    total_row_count += row_count
    total_disk_bytes += disk_bytes
    ch.execute(f"DROP TABLE IF EXISTS {table_b}", settings=SETTINGS)
except Exception as e:
    mark_this_error(f"{type(e).__name__}: {e}")
    pg.close()
    raise

match = (total_row_count == row_count_tab)
print(flush=True)
print(f"TOPLAM: {total_row_count} satir (beklenen {row_count_tab}) -> {'OK' if match else 'HATA!'}", flush=True)

error_note = (
    "Ozel islem: 50.002 sutun x 100.000 satir (5B hucre) tek objede/INSERT'te "
    "ClickHouse'un bellek tavanina her zaman carpiyor -- KAYNAK DOSYA FIZIKSEL "
    "olarak ikiye bolunup, her yari AYRI sikistirilip AYRI MinIO objesi olarak "
    "yuklenip AYRI/gecici ClickHouse tablosuna yuklendi (Bolum 41.2/44 yontemi). "
    "Ayrica: ClickHouse'un ONCEKI basarisiz denemeden BIRIKMIS bellegi restart "
    "edilmeden bu islem baslatilinca yine coktu -- restart SONRASI basarili oldu, "
    "cok buyuk tek dosyalarda restart adimi atlanmamali."
    if match else "HATA: yari toplamlari beklenen satir sayisini tutmuyor!"
)

cur = pg.cursor()
cur.execute(
    """
    INSERT INTO conversion_manifest
        (tab_file_name, aircraft_type, is_subset, subset_row_count,
         row_count_tab, row_count_clickhouse,
         column_count, had_trailing_tab_issue,
         tab_zst_object_key, tab_zst_size_bytes, original_size_bytes,
         compression_algorithm, compression_level,
         compress_duration_seconds, minio_upload_duration_seconds,
         clickhouse_load_duration_seconds,
         clickhouse_table_name, clickhouse_disk_bytes,
         clickhouse_loaded_at, status, error_detail)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s)
    ON CONFLICT (tab_file_name) DO UPDATE SET
        aircraft_type = EXCLUDED.aircraft_type,
        row_count_tab = EXCLUDED.row_count_tab,
        row_count_clickhouse = EXCLUDED.row_count_clickhouse,
        column_count = EXCLUDED.column_count,
        tab_zst_object_key = EXCLUDED.tab_zst_object_key,
        tab_zst_size_bytes = EXCLUDED.tab_zst_size_bytes,
        original_size_bytes = EXCLUDED.original_size_bytes,
        compression_algorithm = EXCLUDED.compression_algorithm,
        compression_level = EXCLUDED.compression_level,
        compress_duration_seconds = EXCLUDED.compress_duration_seconds,
        minio_upload_duration_seconds = EXCLUDED.minio_upload_duration_seconds,
        clickhouse_load_duration_seconds = EXCLUDED.clickhouse_load_duration_seconds,
        clickhouse_table_name = EXCLUDED.clickhouse_table_name,
        clickhouse_disk_bytes = EXCLUDED.clickhouse_disk_bytes,
        clickhouse_loaded_at = EXCLUDED.clickhouse_loaded_at,
        status = EXCLUDED.status,
        error_detail = EXCLUDED.error_detail,
        updated_at = now()
    """,
    (
        TAB_FILE_NAME, aircraft_type, False, None,
        row_count_tab, total_row_count,
        n_total_cols, False,
        " + ".join(object_keys), total_zst_size, src_size,
        "zstd", ZSTD_LEVEL,
        total_compress_time, total_upload_time,
        total_load_time,
        f"synthetic_{TAG}_partA + synthetic_{TAG}_partB", total_disk_bytes,
        "done" if match else "verification_failed", error_note,
    ),
)
cur.execute(
    """
    UPDATE conversion_manifest_history
    SET aircraft_type = %s, row_count_tab = %s, row_count_clickhouse = %s,
        column_count = %s, tab_zst_object_key = %s, tab_zst_size_bytes = %s,
        original_size_bytes = %s, compression_algorithm = %s, compression_level = %s,
        compress_duration_seconds = %s, minio_upload_duration_seconds = %s,
        clickhouse_load_duration_seconds = %s, clickhouse_table_name = %s,
        clickhouse_disk_bytes = %s, clickhouse_loaded_at = now(),
        status = %s, error_detail = %s, finished_at = now()
    WHERE tab_file_name = %s AND attempt_no = %s
    """,
    (
        aircraft_type, row_count_tab, total_row_count,
        n_total_cols, " + ".join(object_keys), total_zst_size,
        src_size, "zstd", ZSTD_LEVEL,
        total_compress_time, total_upload_time,
        total_load_time, f"synthetic_{TAG}_partA + synthetic_{TAG}_partB",
        total_disk_bytes,
        "done" if match else "verification_failed", error_note,
        TAB_FILE_NAME, attempt_no,
    ),
)
cur.close()
pg.close()
print("TAMAMLANDI", flush=True)
