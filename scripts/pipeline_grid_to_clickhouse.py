# -*- coding: utf-8 -*-
"""
scripts/gen_synthetic_grid.py ile uretilen 20 dosyalik grid'i (bkz.
plan Bolum 41) tek tek: temizle (bu dosyalarda trailing-tab sorunu
YOK) -> ZSTD ile sikistir -> MinIO'ya yaz -> ClickHouse'a s3() ile
yukle -> Postgres conversion_manifest'e kaydet.

Kullanim:
  python3 pipeline_grid_to_clickhouse.py            -> tum 20 dosyayi
                                              sirayla isler (Postgres'te
                                              zaten 'done' olanlari atlar)
  python3 pipeline_grid_to_clickhouse.py 20 50000   -> SADECE o dosyayi isler

IKI KRITIK BULGU (plan Bolum 41.1-41.2, 2026-08-22/23):
1. Binary-turu (mixed/zero/one) sutunlari Float64 olarak parse etmek
   ClickHouse'ta ciddi bir bellek/parse maliyeti getiriyor
   (SerializationNumber<double>::deserializeText her "0"/"1" degeri
   icin bile tam double parse yapiyor). Cozum: bu sutunlar UInt8
   olarak tiplenmeli (Bolum 37'nin basarili 45k-sutun deneyiyle ayni
   yaklasim) -- bu senaryoda guvenli, cunku veri bizim urettigimiz
   sentetik veri (garantili 0/1).
2. ClickHouse'un "(total) memory limit exceeded" hatasi, AYNI (buyuyen)
   tabloya art arda INSERT yapildiginda -- restart sonrasi bile --
   tekrarlanabiliyor (mevcut tabloya yeniden baglanmak/veri eklemek
   bellek baskisi tasiyor). En buyuk dosya (50k sutun x 100k satir,
   5 milyar hucre) icin cozum: dosyayi TAMAMEN AYRI/gecici tablolara
   bolerek yuklemek -- her yari kendi tablosunda dogrulanip hemen
   silinir, TEK birlesik buyuk tablo hic olusturulmaz (bkz. plan
   Bolum 41.2, orada elle uygulanan cozumun anlatimi var; bu script
   otomatik bolme icermiyor, gerekirse elle tekrarlanmali).
"""
import sys
import time
import os
import json
import zstandard as zstd
from minio import Minio
from clickhouse_driver import Client
import psycopg2

GRID_DIR = "/work/synthetic_grid"
BUCKET = "telemetry"
ZSTD_LEVEL = 12
CHUNK = 32 * 1024 * 1024

COLUMN_TIERS = [10, 20, 30, 40, 50]  # bin
ROW_COUNTS = [1000, 5000, 50000, 100000]

# Bolum 37'de ogrenilen guvenlik ayarlari -- gecikmeden BASTAN uygulaniyor
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

ch = Client(host="clickhouse", user="default", password="ch123", database="default",
            settings=SETTINGS)
mc = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin123", secure=False)
if not mc.bucket_exists(BUCKET):
    mc.make_bucket(BUCKET)
pg = psycopg2.connect(host="postgres", dbname="telemetry_meta", user="postgres", password="pg123")
pg.autocommit = True


def process_file(n_cols_k, n_rows):
    tag = f"{n_cols_k}k_{n_rows}"
    src_tab = f"{GRID_DIR}/synthetic_{tag}.tab"
    manifest_path = f"{GRID_DIR}/synthetic_{tag}_columns.json"
    out_zst = f"{GRID_DIR}/synthetic_{tag}.tab.zst"
    object_key = f"grid/synthetic_{tag}.tab.zst"
    table_name = f"synthetic_{tag}"

    with open(manifest_path, "r", encoding="utf-8") as f:
        gen_manifest = json.load(f)
    cols = gen_manifest["column_order"]
    aircraft_type = gen_manifest["aircraft_type"]
    n_total_cols = len(cols)

    row_count_tab = gen_manifest["n_rows"]
    src_size = os.path.getsize(src_tab)

    # 1) sikistirma
    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
    with open(src_tab, "rb") as fin, open(out_zst, "wb") as fout:
        compressor = cctx.stream_writer(fout)
        while True:
            chunk = fin.read(CHUNK)
            if not chunk:
                break
            compressor.write(chunk)
        compressor.flush(zstd.FLUSH_FRAME)
    compress_time = time.time() - t0
    zst_size = os.path.getsize(out_zst)

    # 2) MinIO'ya yukleme
    t0 = time.time()
    mc.fput_object(BUCKET, object_key, out_zst)
    upload_time = time.time() - t0

    # 3) ClickHouse hedef tablosu (Compact format zorlanmis)
    # DUZELTME: binary-turu (m/z/o) sutunlari Float64 olarak parse etmek
    # (SerializationNumber<double>::deserializeText) 20k_50000'de tekrar
    # eden "(total) memory limit exceeded" hatasinin KOK NEDENIYDI --
    # temiz ClickHouse restart sonrasi bile AYNI hata cikiyordu, yani
    # onceki tablo/cache birikimi degil, dogrudan bu parse maliyetiydi.
    # Bolum 37'de basarili olan 45k-sutun testinde de binary sutunlar
    # UInt8 idi -- bu sentetik veride guvenli (garantili 0/1), o yuzden
    # simdi ayni yola donuluyor.
    col_defs = []
    for c in cols:
        if c == "aircraft_type":
            col_defs.append(f"`{c}` LowCardinality(String) CODEC(ZSTD)")
        elif c == "timestamp" or c.startswith("f"):
            col_defs.append(f"`{c}` Float64 CODEC(ZSTD)")
        else:
            # m*/z*/o* -- mixed/zero/one, hepsi garantili 0/1
            col_defs.append(f"`{c}` UInt8 CODEC(T64, ZSTD)")
    ddl = (
        f"CREATE TABLE {table_name} (\n  "
        + ",\n  ".join(col_defs)
        + "\n) ENGINE = MergeTree() ORDER BY tuple()\n"
        "SETTINGS min_bytes_for_wide_part = 10737418240000, min_rows_for_wide_part = 1000000000"
    )
    ch.execute(f"DROP TABLE IF EXISTS {table_name}")
    ch.execute(ddl, settings=SETTINGS)

    # 4) s3() ile yukleme
    s3_url = f"http://minio:9000/{BUCKET}/{object_key}"
    insert_sql = (
        f"INSERT INTO {table_name} SELECT * FROM s3("
        f"'{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')"
    )
    t0 = time.time()
    ch.execute(insert_sql, settings=SETTINGS)
    load_time = time.time() - t0
    row_count_ch = ch.execute(f"SELECT count() FROM {table_name}", settings=SETTINGS)[0][0]

    table_size = ch.execute(
        f"SELECT sum(bytes_on_disk) FROM system.parts WHERE table='{table_name}' AND active",
        settings=SETTINGS,
    )[0][0]

    match = (row_count_tab == row_count_ch)

    # 5) Postgres manifest kaydi
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
             clickhouse_loaded_at, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
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
            updated_at = now()
        """,
        (
            f"synthetic_{tag}.tab",
            aircraft_type,
            False,
            None,
            row_count_tab,
            row_count_ch,
            n_total_cols,
            False,
            object_key,
            zst_size,
            src_size,
            "zstd",
            ZSTD_LEVEL,
            compress_time,
            upload_time,
            load_time,
            table_name,
            table_size,
            "done" if match else "verification_failed",
        ),
    )
    cur.close()

    # ClickHouse tablosunu SIL -- metrikler Postgres'e kaydedildi, arsiv zaten
    # MinIO'da .tab.zst olarak kalici duruyor. Bircok genis tablonun AYNI ANDA
    # var olmasi ClickHouse'un bellek basincini artiriyor (20k_50000'de
    # "(total) memory limit exceeded" hatasi buradan kaynaklandi), o yuzden
    # her dosyadan sonra temizliyoruz.
    ch.execute(f"DROP TABLE IF EXISTS {table_name}", settings=SETTINGS)

    # diskten temizle (zst MinIO'da kalici, .tab ise zaten kalici kaynakta duruyor -- sadece uretilen .zst kopyasini sil)
    os.remove(out_zst)

    print(
        f"[{aircraft_type}] {tag}: kaynak={src_size/(1024**2):.1f}MB zst={zst_size/(1024**2):.1f}MB "
        f"({src_size/zst_size:.2f}x) sikistir={compress_time:.1f}sn yukle_minio={upload_time:.2f}sn "
        f"yukle_ch={load_time:.1f}sn satir={row_count_ch} ch_disk={table_size/(1024**2):.1f}MB "
        f"{'OK' if match else 'HATA!'}",
        flush=True,
    )
    return {
        "compress_time": compress_time,
        "upload_time": upload_time,
        "load_time": load_time,
        "src_size": src_size,
        "zst_size": zst_size,
        "match": match,
    }


# Tek-dosya modu: "python3 pipeline_grid_to_clickhouse.py 20 50000" seklinde
# cagirilirsa SADECE o dosyayi isler (host'tan ClickHouse'u her dosya
# oncesi temiz baslatip cagirmak icin -- Bolum 41.1'deki bellek
# birikimi sorununu onlemek amaciyla).
if len(sys.argv) == 3:
    n_cols_k = int(sys.argv[1])
    n_rows = int(sys.argv[2])
    r = process_file(n_cols_k, n_rows)
    print("TEK_DOSYA_TAMAMLANDI", flush=True)
    pg.close()
    sys.exit(0)

# Zaten basariyla islenmis dosyalari atla (kaldigi yerden devam)
cur0 = pg.cursor()
cur0.execute("SELECT tab_file_name FROM conversion_manifest WHERE status='done'")
already_done = {row[0] for row in cur0.fetchall()}
cur0.close()

print(f"Toplam {len(COLUMN_TIERS)*len(ROW_COUNTS)} dosya islenecek.", flush=True)
print(f"Zaten tamamlanmis: {len(already_done)}", flush=True)
grand_t0 = time.time()
results = []
for n_cols_k in COLUMN_TIERS:
    for n_rows in ROW_COUNTS:
        tag = f"{n_cols_k}k_{n_rows}"
        if f"synthetic_{tag}.tab" in already_done:
            print(f"[atlanildi, zaten tamamlanmis] {tag}", flush=True)
            continue
        try:
            r = process_file(n_cols_k, n_rows)
            results.append(r)
        except Exception as e:
            print(f"HATA: {n_cols_k}k_{n_rows} -> {type(e).__name__}: {e}", flush=True)
            raise

grand_elapsed = time.time() - grand_t0
print(flush=True)
print("=== TÜMÜ TAMAMLANDI ===", flush=True)
print(f"Toplam süre: {grand_elapsed/60:.1f}dk", flush=True)
print(f"Toplam sıkıştırma süresi: {sum(r['compress_time'] for r in results)/60:.1f}dk", flush=True)
print(f"Toplam MinIO yükleme süresi: {sum(r['upload_time'] for r in results):.1f}sn", flush=True)
print(f"Toplam ClickHouse yükleme süresi: {sum(r['load_time'] for r in results)/60:.1f}dk", flush=True)
print(f"Başarısız dosya sayısı: {sum(1 for r in results if not r['match'])}", flush=True)

cur = pg.cursor()
cur.execute("SELECT count(*) FROM conversion_manifest WHERE status='done'")
print(f"Postgres'te 'done' durumunda kayıt: {cur.fetchone()[0]}", flush=True)
cur.close()
pg.close()
