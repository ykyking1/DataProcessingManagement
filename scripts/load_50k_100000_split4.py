# -*- coding: utf-8 -*-
"""
50k_100000 icin 2'ye bolme bile host'un su anki ciddi bellek darligi
(reboot sonrasi Windows'ta 16,48GB'nin sadece ~1,8GB'i bos) yuzunden
yetmedi -- kullanici karariyla 4'e bolunuyor (her parca 25.000 satir,
1,25 milyar hucre yerine 2,5 milyar). Kaynak dosya TEK GECISTE 4
parcaya bolunur, her parca SIRAYLA (esazamanli DEGIL -- host zaten
sikisik) sikistirilip yuklenir, ClickHouse tarafinda AYRI/gecici
tabloya alinir, dogrulanir, silinir.
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
SRC_TAB = f"{GRID_DIR}/synthetic_{TAG}.tab"
QUARTER = 25000  # 100.000 / 4
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
src_size = os.path.getsize(SRC_TAB)

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


# ---- 1) KAYNAK DOSYAYI TEK GECISTE 4 PARCAYA BOL ----
print("Kaynak dosya 4 parcaya bolunuyor...", flush=True)
part_paths = [f"{GRID_DIR}/synthetic_{TAG}_q{i+1}.tab" for i in range(4)]
t0 = time.time()
with open(SRC_TAB, "r", encoding="utf-8", newline="") as fin:
    header = fin.readline()
    for part_path in part_paths:
        with open(part_path, "w", encoding="utf-8", newline="") as fp:
            fp.write(header)
            for _ in range(QUARTER):
                line = fin.readline()
                if not line:
                    break
                fp.write(line)
split_time = time.time() - t0
sizes = [os.path.getsize(p) / (1024**2) for p in part_paths]
print(f"Bolme tamamlandi: {split_time:.1f}sn, parca boyutlari(MB)={sizes}", flush=True)

total_row_count = 0
total_disk_bytes = 0
total_load_time = 0.0
total_compress_time = 0.0
total_upload_time = 0.0
total_zst_size = 0
object_keys = []

try:
    for qi, part_path in enumerate(part_paths, start=1):
        print(f"--- Parca {qi}/4 ---", flush=True)
        out_zst = part_path + ".zst"
        object_key = f"grid/synthetic_{TAG}_q{qi}.tab.zst"
        table_name = f"synthetic_{TAG}_q{qi}"

        t0 = time.time()
        cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
        with open(part_path, "rb") as fin, open(out_zst, "wb") as fout:
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
        mc.fput_object(BUCKET, object_key, out_zst)
        upload_time = time.time() - t0
        total_upload_time += upload_time
        object_keys.append(object_key)
        print(f"  MinIO yukleme: {upload_time:.1f}sn", flush=True)
        os.remove(out_zst)
        os.remove(part_path)

        s3_url = f"http://minio:9000/{BUCKET}/{object_key}"
        ch.execute(f"DROP TABLE IF EXISTS {table_name}")
        ch.execute(build_ddl(table_name), settings=SETTINGS)
        t0 = time.time()
        ch.execute(
            f"INSERT INTO {table_name} SELECT * FROM s3('{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')",
            settings=SETTINGS,
        )
        load_time = time.time() - t0
        total_load_time += load_time
        row_count = ch.execute(f"SELECT count() FROM {table_name}", settings=SETTINGS)[0][0]
        disk_bytes = ch.execute(
            f"SELECT sum(bytes_on_disk) FROM system.parts WHERE table='{table_name}' AND active", settings=SETTINGS
        )[0][0]
        print(f"  ClickHouse yukleme: {load_time:.1f}sn, {row_count} satir, {disk_bytes/(1024**2):.1f}MB", flush=True)
        total_row_count += row_count
        total_disk_bytes += disk_bytes
        ch.execute(f"DROP TABLE IF EXISTS {table_name}", settings=SETTINGS)
except Exception as e:
    mark_this_error(f"{type(e).__name__}: {e}")
    pg.close()
    raise

match = (total_row_count == row_count_tab)
print(flush=True)
print(f"TOPLAM: {total_row_count} satir (beklenen {row_count_tab}) -> {'OK' if match else 'HATA!'}", flush=True)

error_note = (
    "Ozel islem: 50.002 sutun x 100.000 satir (5B hucre). Host makine bu "
    "oturumda ciddi bellek darligi yasadi (reboot sonrasi Windows'ta "
    "16,48GB'nin ~1,8GB'i bos) -- 2'ye bolme (Bolum 41.2'nin tarihsel "
    "yontemi) fresh ClickHouse restart'i ile bile yetmedi, 4'e bolunerek "
    "(25.000 satir/parca) basariyla yuklendi (Bolum 44 devami)."
    if match else "HATA: parca toplamlari beklenen satir sayisini tutmuyor!"
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
        " + ".join(f"synthetic_{TAG}_q{i}" for i in range(1, 5)), total_disk_bytes,
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
        total_load_time, " + ".join(f"synthetic_{TAG}_q{i}" for i in range(1, 5)),
        total_disk_bytes,
        "done" if match else "verification_failed", error_note,
        TAB_FILE_NAME, attempt_no,
    ),
)
cur.close()
pg.close()
print("TAMAMLANDI", flush=True)
