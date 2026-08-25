# -*- coding: utf-8 -*-
"""
Uctan uca pipeline: ham .tab -> temizle -> ZSTD ile sikistir -> MinIO'ya
yaz -> ClickHouse'a yukle -> Postgres manifest'e (conversion_manifest)
metadata kaydet.

Su an dataset_01.tab'in ilk 2000 satirlik alt kumesiyle DOGRULAMA
amacli calisacak sekilde sabit degerlerle yazildi (bkz. plan Bolum
40). Tam dosyayla (10,9GB) calistirmadan once parametrize edilmesi
gerekiyor -- SRC_TAB, TABLE_NAME, AIRCRAFT_TYPE, is_subset/
subset_row_count gibi degerler yeniden ele alinmali.

Onemli: yeni bir kaynak dosya/ucak tipiyle calisirken AIRCRAFT_TYPE
mutlaka doldurulmali (farkli ucak tiplerinde sutun sayisi degisebiliyor).

DURUM TAKIBI (plan Bolum 41.3, 2026-08-23 eklendi): calisma baslamadan
once Postgres'e 'processing' durumunda bir satir yazilir, attempt_count
artirilir. Basarisiz olursa satir SILINMEZ, 'error' durumuna ve
error_detail'e hata mesaji yazilarak guncellenir -- oncesinde
basarisiz denemeler Postgres'e hic yansimiyordu, bu duzeltiliyor.
"""
import time
import os
import zstandard as zstd
from minio import Minio
from clickhouse_driver import Client
import psycopg2

SRC_TAB = "/work/dataset_01_small.tab"
OUT_ZST = "/work/dataset_01_small.tab.zst"
BUCKET = "telemetry"
OBJECT_KEY = "raw/dataset_01_small.tab.zst"
TABLE_NAME = "dataset_01_raw"
SOURCE_TAB_FILE_NAME = "dataset_01.tab"  # manifest'te tuttugumuz orijinal (temiz) dosya adi
SUBSET_ROW_COUNT = 2000
AIRCRAFT_TYPE = None  # bu sentetik dosya icin bilinmiyor -- gercek veride DOLDURULMALI
HAD_TRAILING_TAB_ISSUE = True  # bu dosyada bilinen/onceden tespit edilmis sorun
ZSTD_LEVEL = 12

CHUNK = 64 * 1024 * 1024  # Bolum 42.2: 32MB->64MB ~%6,5 hizlanma, 128MB'a kiyasla daha az bellek riski

pg = psycopg2.connect(host="postgres", dbname="telemetry_meta", user="postgres", password="pg123")
pg.autocommit = True


def mark_processing(tab_file_name, aircraft_type):
    cur = pg.cursor()
    cur.execute(
        """
        INSERT INTO conversion_manifest (tab_file_name, aircraft_type, status, attempt_count)
        VALUES (%s, %s, 'processing', 1)
        ON CONFLICT (tab_file_name) DO UPDATE SET
            aircraft_type = EXCLUDED.aircraft_type,
            status = 'processing',
            attempt_count = conversion_manifest.attempt_count + 1,
            updated_at = now()
        """,
        (tab_file_name, aircraft_type),
    )
    cur.close()


def mark_error(tab_file_name, error_detail):
    cur = pg.cursor()
    cur.execute(
        """
        UPDATE conversion_manifest
        SET status = 'error', error_detail = %s, updated_at = now()
        WHERE tab_file_name = %s
        """,
        (error_detail[:4000], tab_file_name),
    )
    cur.close()


mark_processing(SOURCE_TAB_FILE_NAME, AIRCRAFT_TYPE)

try:
    print("=== 0) Kaynak dogrulama ===", flush=True)
    with open(SRC_TAB, "rb") as f:
        header = f.readline().rstrip(b"\n")
    cols = header.decode().split("\t")
    print(f"Sutun sayisi: {len(cols)} (ilk 3: {cols[:3]}, son 3: {cols[-3:]})", flush=True)

    with open(SRC_TAB, "rb") as f:
        row_count_tab = sum(1 for _ in f) - 1  # header haric
    print(f"Satir sayisi (header haric): {row_count_tab}", flush=True)

    print(flush=True)
    print("=== 1) ZSTD ile sikistirma ===", flush=True)
    src_size = os.path.getsize(SRC_TAB)
    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
    with open(SRC_TAB, "rb") as fin, open(OUT_ZST, "wb") as fout:
        compressor = cctx.stream_writer(fout)
        while True:
            chunk = fin.read(CHUNK)
            if not chunk:
                break
            compressor.write(chunk)
        compressor.flush(zstd.FLUSH_FRAME)
    compress_time = time.time() - t0
    zst_size = os.path.getsize(OUT_ZST)
    print(f"Kaynak: {src_size/(1024**2):.2f}MB -> ZSTD({ZSTD_LEVEL}): {zst_size/(1024**2):.2f}MB "
          f"sure {compress_time:.2f}sn oran {src_size/zst_size:.2f}x", flush=True)

    print(flush=True)
    print("=== 2) MinIO'ya yukleme ===", flush=True)
    mc = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin123", secure=False)
    if not mc.bucket_exists(BUCKET):
        mc.make_bucket(BUCKET)
    t0 = time.time()
    mc.fput_object(BUCKET, OBJECT_KEY, OUT_ZST)
    upload_time = time.time() - t0
    print(f"MinIO'ya yuklendi: {BUCKET}/{OBJECT_KEY} sure {upload_time:.2f}sn", flush=True)

    print(flush=True)
    print("=== 3) ClickHouse hedef tablosu ===", flush=True)
    # Guvenlik-once yaklasimi: b0-b699'un gercek deger araligi henuz tek
    # tek dogrulanmadigi icin simdilik hepsi Float64 (bkz. plan Bolum 31.4
    # guvenlik uyarisi) -- optimizasyon (UInt8+T64) sonraki detayli
    # gecistirmede ele alinacak.
    col_defs = [f"`{c}` Float64 CODEC(ZSTD)" for c in cols]
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n  "
        + ",\n  ".join(col_defs)
        + "\n) ENGINE = MergeTree() ORDER BY tuple()"
    )
    ch = Client(host="clickhouse", user="default", password="ch123", database="default")
    ch.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    ch.execute(ddl)
    print(f"Tablo olusturuldu: {TABLE_NAME} ({len(cols)} sutun)", flush=True)

    print(flush=True)
    print("=== 4) s3() ile ClickHouse'a yukleme ===", flush=True)
    s3_url = f"http://minio:9000/{BUCKET}/{OBJECT_KEY}"
    insert_sql = (
        f"INSERT INTO {TABLE_NAME} SELECT * FROM s3("
        f"'{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')"
    )
    t0 = time.time()
    ch.execute(insert_sql)
    load_time = time.time() - t0
    row_count_ch = ch.execute(f"SELECT count() FROM {TABLE_NAME}")[0][0]
    print(f"Yukleme tamamlandi: {row_count_ch} satir, sure {load_time:.2f}sn", flush=True)

    table_size = ch.execute(
        f"SELECT sum(bytes_on_disk) FROM system.parts WHERE table='{TABLE_NAME}' AND active"
    )[0][0]
    print(f"ClickHouse disk boyutu: {table_size/(1024**2):.2f}MB", flush=True)

    print(flush=True)
    print("=== 5) Uc yonlu dogrulama ===", flush=True)
    match = (row_count_tab == row_count_ch)
    print(f"tab satir sayisi = {row_count_tab}, ClickHouse satir sayisi = {row_count_ch} -> "
          f"{'ESLESIYOR' if match else 'ESLESMIYOR!'}", flush=True)

    # basit bir icerik kontrolu: ilk float sutununun toplami
    first_float_col = cols[1]  # timestamp'ten sonraki ilk f-sutunu
    ch_sum = ch.execute(f"SELECT sum(`{first_float_col}`) FROM {TABLE_NAME}")[0][0]
    print(f"Ornek kontrol: sum({first_float_col}) ClickHouse'ta = {ch_sum}", flush=True)
except Exception as e:
    mark_error(SOURCE_TAB_FILE_NAME, f"{type(e).__name__}: {e}")
    pg.close()
    raise

print(flush=True)
print("=== 6) Postgres manifest kaydi ===", flush=True)
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
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s, NULL)
    ON CONFLICT (tab_file_name) DO UPDATE SET
        aircraft_type = EXCLUDED.aircraft_type,
        is_subset = EXCLUDED.is_subset,
        subset_row_count = EXCLUDED.subset_row_count,
        row_count_tab = EXCLUDED.row_count_tab,
        row_count_clickhouse = EXCLUDED.row_count_clickhouse,
        column_count = EXCLUDED.column_count,
        had_trailing_tab_issue = EXCLUDED.had_trailing_tab_issue,
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
        error_detail = NULL,
        updated_at = now()
    """,
    (
        SOURCE_TAB_FILE_NAME,
        AIRCRAFT_TYPE,
        True,
        SUBSET_ROW_COUNT,
        row_count_tab,
        row_count_ch,
        len(cols),
        HAD_TRAILING_TAB_ISSUE,
        OBJECT_KEY,
        zst_size,
        src_size,
        "zstd",
        ZSTD_LEVEL,
        compress_time,
        upload_time,
        load_time,
        TABLE_NAME,
        table_size,
        "done" if match else "verification_failed",
    ),
)
cur.execute("""
    SELECT id, tab_file_name, aircraft_type, is_subset, subset_row_count, status,
           row_count_tab, row_count_clickhouse, column_count, had_trailing_tab_issue,
           compression_algorithm, compression_level, original_size_bytes,
           compress_duration_seconds, minio_upload_duration_seconds,
           clickhouse_load_duration_seconds, clickhouse_table_name, clickhouse_disk_bytes
    FROM conversion_manifest
""")
cols_desc = [d[0] for d in cur.description]
print("Postgres'teki kayitlar:", flush=True)
for row in cur.fetchall():
    for k, v in zip(cols_desc, row):
        print(f"    {k}: {v}", flush=True)
    print(flush=True)
cur.close()
pg.close()

print(flush=True)
print("=== OZET ===", flush=True)
print(f"Sikistirma: {compress_time:.2f}sn, oran {src_size/zst_size:.2f}x", flush=True)
print(f"MinIO yukleme: {upload_time:.2f}sn", flush=True)
print(f"ClickHouse yukleme: {load_time:.2f}sn", flush=True)
print(f"Dogrulama: {'BASARILI' if match else 'BASARISIZ'}", flush=True)
