# -*- coding: utf-8 -*-
"""
scripts/gen_synthetic_grid.py ile uretilen 20 dosyalik grid'i (bkz.
plan Bolum 41) iki fazda isler:

  FAZ 1 (PARALEL, N=6 worker): her dosya icin temizle (bu dosyalarda
    trailing-tab sorunu YOK) -> ZSTD ile sikistir -> MinIO'ya yaz.
    Bolum 42.1'de N=6'nin tatlı nokta oldugu (N=10/16 fayda saglamiyor,
    en buyuk tek dosyanin suresi alt sinir olusturuyor) bulundu.
  FAZ 2 (SIRALI): her dosya icin ClickHouse'a s3() ile yukle ->
    Postgres conversion_manifest'e kaydet -> tabloyu sil.
    N=2 esazamanli yukleme (2 AYRI dosya, 2 AYRI/gecici tabloya)
    denendi ve olculdu (Bolum 43.3, ~1,3x) ama gercek 20 dosyalik
    grid'in tamaminda test edilince (Bolum 45) toplam kazanc sadece
    ~1,06x cikti -- cogu buyuk dosya zaten izole kaliyordu, kucuk
    dosyalarda da kazanc gurultu seviyesindeydi. Getirdigi ek
    karmasiklik/risge (thread-safety, hucre-esigi mantigi, es zamanli
    buyuk tablo yuklemesi) degmedigi icin KULLANICI KARARIYLA sirali
    yapiya geri donuldu (Bolum 45.2).

Kullanim:
  python3 pipeline_grid_to_clickhouse.py            -> tum 20 dosyayi
                                              isler (Postgres'te zaten
                                              'done' olanlari atlar)
  python3 pipeline_grid_to_clickhouse.py 20 50000   -> SADECE o dosyayi isler

UC KRITIK BULGU (plan Bolum 41.1-41.2, 42.1-42.2, 2026-08-22/23):
1. Binary-turu (mixed/zero/one) sutunlari Float64 olarak parse etmek
   ClickHouse'ta ciddi bir bellek/parse maliyeti getiriyor
   (SerializationNumber<double>::deserializeText her "0"/"1" degeri
   icin bile tam double parse yapiyor). Cozum: bu sutunlar UInt8
   olarak tiplenmeli (Bolum 37'nin basarili 45k-sutun deneyiyle ayni
   yaklasim) -- bu senaryoda guvenli, cunku veri bizim urettigimiz
   sentetik veri (garantili 0/1).
2. ClickHouse'un "(total) memory limit exceeded" hatasi, AYNI (buyuyen)
   tabloya art arda INSERT yapildiginda -- restart sonrasi bile --
   tekrarlanabiliyor. En buyuk dosya (50k sutun x 100k satir, 5 milyar
   hucre) icin cozum: dosyayi TAMAMEN AYRI/gecici tablolara bolerek
   yuklemek -- her yari kendi tablosunda dogrulanip hemen silinir,
   TEK birlesik buyuk tablo hic olusturulmaz (bkz. plan Bolum 41.2,
   orada elle uygulanan cozumun anlatimi var; bu script otomatik
   bolme icermiyor, gerekirse elle tekrarlanmali).
3. Sikistirma adimi icin N=6 paralel worker + 64MB okuma parcasi
   optimal (Bolum 42.1-42.2) -- N=10/16 fayda saglamiyor, N=6'nin
   uzerine cikmak sadece verimliligi dusuruyor.

NOT (Bolum 45.2): Faz 2 icin N=2 esazamanli yukleme de denenmisti,
kucuk-orta olcekli dosyalarda ~1,3x kazanc olcculmustu (Bolum 43.3)
ama gercek 20 dosyalik grid'in tamaminda toplam kazanc sadece ~1,06x
cikinca (cogu buyuk dosya zaten guvenlik geregi izole kaliyordu),
karmasikligi/riski gerekcelendirmedigi icin kaldirildi -- Faz 2 sirali.

PROAKTIF BOLME (Bolum 46, 2026-08-25): 50k_100000 (50.002 sutun x
100.000 satir) tekrar tekrar ClickHouse bellek tavanina carpinca,
kullanicinin sorusuyla ("kacta bolmemiz lazim, sutun mu satir mi")
KONTROLLU bisection yapildi -- her adimda ClickHouse fresh restart,
tek dosya, izole. Sonuc: 50.002 sutunda ~20.000-25.000 satir (~1
milyar hucre) civari GUVENLI/RISKLI sinir. Kesin bulgu: SUTUN sayisi
belirleyici, hucre sayisi degil -- 40.002 sutun 4 milyar hucreyi
sorunsuz tasirken 50.002 sutun 1,25 milyar hucrede coker (40k->50k
sutun arasi duzgun degil KESKIN bir ucurum, ClickHouse'un TSV
parser'inin sutun basina sabit tampon maliyeti super-dogrusal
buyuyor). Bu makinede tavan `.wslconfig`'deki SABIT `memory=12GB`
WSL butcesinden kaynaklaniyor (host toplami 16,48GB) -- gecici bir
sizinti degil, yapisal bir kisit.

Kullanici karariyla: CH_LOAD_SAFE_CELL_LIMIT esigini asan dosyalar
ARTIK Faz 1'de OTOMATIK olarak fiziksel parcalara bolunuyor (satir
bazinda, her parca ayri sikistirilip AYRI MinIO objesi olarak
yukleniyor -- LIMIT/OFFSET ile TEK buyuk objeden "yari okumak" ise
YARAMIYOR, cunku ClickHouse'un paralel S3 okuyucusu tampon boyutunu
ALTTAKI OBJENIN TAM boyutuna gore ayarliyor, satir siniristina gore
degil). Faz 2'de her parca kendi AYRI/gecici tablosuna yuklenip
dogrulanir, sonuclar TOPLANIP TEK bir Postgres kaydi yazilir.

DURUM TAKIBI (plan Bolum 41.3): her dosya denemesi FAZ 1 baslamadan
once Postgres'e 'processing' durumunda bir satir yazilir ve
attempt_count artirilir; basarisiz olursa satir SILINMEZ, 'error'
durumuna ve error_detail'e hata mesaji yazilarak GUNCELLENIR (hangi
fazda basarisiz oldugu error_detail'e not edilir).
"""
import sys
import time
import os
import json
import math
import zstandard as zstd
from minio import Minio
from clickhouse_driver import Client
from concurrent.futures import ProcessPoolExecutor, as_completed
import psycopg2

GRID_DIR = "/work/synthetic_grid"
BUCKET = "telemetry"
ZSTD_LEVEL = 12
CHUNK = 64 * 1024 * 1024  # Bolum 42.2: 32MB->64MB ~%6,5 hizlanma, 128MB'a kiyasla daha az bellek riski
COMPRESS_WORKERS = 6  # Bolum 42.1: N=6 tatli nokta, N=10/16 fayda saglamiyor
CPU_CORES = os.cpu_count() or 20
ZSTD_MAX_INTERNAL_THREADS = 8  # Bolum 46.5: tek-dosya izole testte tatli nokta, 16/20 gerilemeye basliyor
CH_LOAD_SAFE_CELL_LIMIT = 1_000_000_000  # Bolum 46: 50k sutunda ~20-25k satirda (1-1,25 milyar hucre) tavan bulundu; guvenli marj icin 1 milyar

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


def aircraft_label(n_cols_k):
    return f"AIRCRAFT_{n_cols_k}K"


def mark_processing(pg_conn, tab_file_name, aircraft_type):
    """Faz 1 baslamadan once 'processing' durumunda bir satir yaz/guncelle,
    attempt_count'u ATOMIK artirir (Postgres'in UPDATE...RETURNING'i satir
    kilidi kullanir -- ayni dosya icin iki cagri hicbir zaman ayni
    attempt_count'u goremez, bkz. plan Bolum 44). Boylece yarida kesilen/
    patlayan denemeler bile Postgres'te en azindan 'bu dosya denendi'
    izini birakir.

    Ayrica conversion_manifest_history'de bu denemeye (tab_file_name,
    attempt_no) ait YENI bir satir acar -- conversion_manifest'in aksine
    bu tablo HICBIR ZAMAN uzerine yazilmaz, her deneme kendi satirinda
    kalir (Bolum 44). Donen attempt_no, ayni denemenin sonunda
    mark_error()/load_and_record()'a hangi history satirinin
    guncellenecegini soylemek icin cagirana geri veriliyor."""
    cur = pg_conn.cursor()
    cur.execute(
        """
        INSERT INTO conversion_manifest (tab_file_name, aircraft_type, status, attempt_count)
        VALUES (%s, %s, 'processing', 1)
        ON CONFLICT (tab_file_name) DO UPDATE SET
            aircraft_type = EXCLUDED.aircraft_type,
            status = 'processing',
            attempt_count = conversion_manifest.attempt_count + 1,
            updated_at = now()
        RETURNING attempt_count
        """,
        (tab_file_name, aircraft_type),
    )
    attempt_no = cur.fetchone()[0]
    cur.execute(
        """
        INSERT INTO conversion_manifest_history (tab_file_name, attempt_no, aircraft_type, status)
        VALUES (%s, %s, %s, 'processing')
        """,
        (tab_file_name, attempt_no, aircraft_type),
    )
    cur.close()
    return attempt_no


def mark_error(pg_conn, tab_file_name, attempt_no, error_detail):
    """Deneme basarisiz olunca conversion_manifest'teki satiri SILMEDEN
    'error' durumuna guncelle, hata mesajini kaydet. Ayrica
    conversion_manifest_history'deki AYNI denemenin (tab_file_name,
    attempt_no) satirini de 'error' + finished_at ile kapatir."""
    cur = pg_conn.cursor()
    cur.execute(
        """
        UPDATE conversion_manifest
        SET status = 'error', error_detail = %s, updated_at = now()
        WHERE tab_file_name = %s
        """,
        (error_detail[:4000], tab_file_name),  # asiri uzun stack trace'leri kes
    )
    cur.execute(
        """
        UPDATE conversion_manifest_history
        SET status = 'error', error_detail = %s, finished_at = now()
        WHERE tab_file_name = %s AND attempt_no = %s
        """,
        (error_detail[:4000], tab_file_name, attempt_no),
    )
    cur.close()


def _compress_one_file(src_path, out_zst, zstd_threads=1):
    """Tek bir dosyayi ZSTD ile sikistirir (64MB okuma parcasi, Bolum 42.2),
    (sure, cikti_boyutu) dondurur.

    Bolum 46.5: `zstd_threads` ZSTD'nin KENDI dahili coklu-thread
    destegini kullanir (process-bazli paralellikten FARKLI bir mekanizma
    -- TEK dosyayi birden fazla cekirdekle sikistirir). Tek basina
    (rekabetsiz) test edildiginde threads=8 ~2,5x kazanc verdi, ama
    N=6 process havuzuyla BIRLIKTE (her worker de threads>1 kullanirsa)
    karisik cok-dosyali is yukunde kazanci YOK EDIYOR (cekirdek
    asiri-abonelik) -- o yuzden SADECE havuzda kuyruklama olmayacagi
    GARANTI oldugunda (bkz. cagiran taraftaki hesaplama) kullanilmali."""
    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL, threads=zstd_threads)
    with open(src_path, "rb") as fin, open(out_zst, "wb") as fout:
        compressor = cctx.stream_writer(fout)
        while True:
            chunk = fin.read(CHUNK)
            if not chunk:
                break
            compressor.write(chunk)
        compressor.flush(zstd.FLUSH_FRAME)
    return time.time() - t0, os.path.getsize(out_zst)


def _upload_one_file(local_path, object_key):
    """Tek bir dosyayi MinIO'ya yukler, sureyi dondurur. Kendi Minio
    client'ini kurar (process havuzunda paylasilan baglanti guvenli degil)."""
    mc = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin123", secure=False)
    t0 = time.time()
    mc.fput_object(BUCKET, object_key, local_path)
    return time.time() - t0


def _split_file_by_rows(src_tab, tag, rows_per_part, total_rows):
    """Kaynak .tab dosyasini rows_per_part'lik parcalara boler, header her
    parcada tekrarlanir. Bolum 46: FIZIKSEL bolme SART -- ClickHouse'un
    paralel S3 okuyucusu tampon boyutunu ALTTAKI OBJENIN TAM boyutuna gore
    ayarladigi icin tek buyuk objede LIMIT/OFFSET ile "yari okumak" ise
    yaramiyor, gercekten AYRI/kucuk objeler gerekiyor. (parca_yolu, parca_satir_sayisi)
    listesi dondurur."""
    parts = []
    with open(src_tab, "r", encoding="utf-8", newline="") as fin:
        header = fin.readline()
        part_idx = 0
        rows_written_total = 0
        while rows_written_total < total_rows:
            part_idx += 1
            part_path = f"{GRID_DIR}/synthetic_{tag}_p{part_idx}.tab"
            rows_this_part = 0
            with open(part_path, "w", encoding="utf-8", newline="") as fp:
                fp.write(header)
                for _ in range(rows_per_part):
                    line = fin.readline()
                    if not line:
                        break
                    fp.write(line)
                    rows_this_part += 1
            if rows_this_part == 0:
                os.remove(part_path)
                break
            parts.append((part_path, rows_this_part))
            rows_written_total += rows_this_part
    return parts


def compress_and_upload(n_cols_k, n_rows, zstd_threads=1):
    """FAZ 1 -- ProcessPoolExecutor worker'i icinde calisir. DB baglantisi
    kullanmaz (process havuzunda paylasilan soket guvenli degil), sadece
    dosya IO + MinIO.

    Bolum 46: dosyanin toplam hucre sayisi (sutun x satir)
    CH_LOAD_SAFE_CELL_LIMIT'i asarsa, kaynak dosya PROAKTIF olarak
    fiziksel parcalara bolunur -- her parca ayri sikistirilip AYRI MinIO
    objesi olarak yuklenir. Donen `parts` listesi 1 (normal, bolunmemis)
    ya da N eleman (bolunmus) icerebilir; load_and_record() bu iki durumu
    da isliyor.

    `zstd_threads` (Bolum 46.5): ZSTD'nin dahili coklu-thread destegi --
    SADECE havuzda kuyruklama olmayacagi (bekleyen dosya sayisi <=
    worker sayisi) garanti oldugunda >1 verilmeli, cagiran taraf bunu
    hesaplar (bkz. asagida coklu-dosya modu)."""
    tag = f"{n_cols_k}k_{n_rows}"
    src_tab = f"{GRID_DIR}/synthetic_{tag}.tab"
    manifest_path = f"{GRID_DIR}/synthetic_{tag}_columns.json"

    with open(manifest_path, "r", encoding="utf-8") as f:
        gen_manifest = json.load(f)
    cols = gen_manifest["column_order"]
    aircraft_type = gen_manifest["aircraft_type"]
    row_count_tab = gen_manifest["n_rows"]
    n_total_cols = len(cols)
    src_size = os.path.getsize(src_tab)

    total_cells = n_total_cols * row_count_tab
    n_parts = max(1, math.ceil(total_cells / CH_LOAD_SAFE_CELL_LIMIT))

    parts = []
    if n_parts == 1:
        out_zst = f"{GRID_DIR}/synthetic_{tag}.tab.zst"
        object_key = f"grid/synthetic_{tag}.tab.zst"
        compress_time, zst_size = _compress_one_file(src_tab, out_zst, zstd_threads)
        upload_time = _upload_one_file(out_zst, object_key)
        os.remove(out_zst)
        parts.append({
            "object_key": object_key, "zst_size": zst_size,
            "compress_time": compress_time, "upload_time": upload_time,
            "row_count": row_count_tab,
        })
    else:
        rows_per_part = math.ceil(row_count_tab / n_parts)
        print(f"  [FAZ1 {tag}] {total_cells/1e9:.2f} milyar hucre > "
              f"{CH_LOAD_SAFE_CELL_LIMIT/1e9:.1f} milyar sinir -- {n_parts} parcaya "
              f"bolunuyor (Bolum 46)", flush=True)
        for i, (part_path, part_rows) in enumerate(
            _split_file_by_rows(src_tab, tag, rows_per_part, row_count_tab), start=1
        ):
            out_zst = part_path + ".zst"
            object_key = f"grid/synthetic_{tag}_p{i}.tab.zst"
            compress_time, zst_size = _compress_one_file(part_path, out_zst, zstd_threads)
            upload_time = _upload_one_file(out_zst, object_key)
            os.remove(out_zst)
            os.remove(part_path)
            parts.append({
                "object_key": object_key, "zst_size": zst_size,
                "compress_time": compress_time, "upload_time": upload_time,
                "row_count": part_rows,
            })
            print(f"  [FAZ1 {tag}] parca {i}/{n_parts} tamam: {part_rows} satir, "
                  f"sikistir={compress_time:.1f}sn yukle_minio={upload_time:.2f}sn", flush=True)

    return {
        "tag": tag,
        "n_cols_k": n_cols_k,
        "n_rows": n_rows,
        "tab_file_name": f"synthetic_{tag}.tab",
        "cols": cols,
        "n_total_cols": n_total_cols,
        "aircraft_type": aircraft_type,
        "row_count_tab": row_count_tab,
        "src_size": src_size,
        "parts": parts,
        "compress_time": sum(p["compress_time"] for p in parts),
        "upload_time": sum(p["upload_time"] for p in parts),
        "zst_size": sum(p["zst_size"] for p in parts),
    }


def load_and_record(ch, pg_conn, cr):
    """FAZ 2 -- tek dosyalik is birimi, SIRALI cagrilir (Bolum 45.2).
    ClickHouse'a yukler, Postgres'e kaydeder, tabloyu siler.

    Bolum 46: `cr["parts"]` 1'den fazla elemanli olabilir (Faz 1'de
    proaktif bolunmus dosyalar) -- her parca kendi AYRI/gecici tablosuna
    yuklenip dogrulanir, TEK birlesik buyuk tablo hic olusturulmaz (Bolum
    41.2'nin elle uygulanan cozumunun otomatik hali); sonuclar toplanip
    TEK bir Postgres kaydi yazilir."""
    tag = cr["tag"]
    cols = cr["cols"]
    n_total_cols = cr["n_total_cols"]
    parts = cr["parts"]
    multi = len(parts) > 1

    # DUZELTME: binary-turu (m/z/o) sutunlari Float64 olarak parse etmek
    # (SerializationNumber<double>::deserializeText) "(total) memory limit
    # exceeded" hatasinin KOK NEDENIYDI -- UInt8 olarak tiplenmeli (Bolum
    # 37/41.1), bu sentetik veride guvenli (garantili 0/1).
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

    row_count_ch = 0
    table_size = 0
    load_time = 0.0
    table_names = []
    for i, part in enumerate(parts, start=1):
        table_name = f"synthetic_{tag}" if not multi else f"synthetic_{tag}_p{i}"
        table_names.append(table_name)
        ch.execute(f"DROP TABLE IF EXISTS {table_name}")
        ch.execute(build_ddl(table_name), settings=SETTINGS)

        s3_url = f"http://minio:9000/{BUCKET}/{part['object_key']}"
        insert_sql = (
            f"INSERT INTO {table_name} SELECT * FROM s3("
            f"'{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')"
        )
        t0 = time.time()
        ch.execute(insert_sql, settings=SETTINGS)
        load_time += time.time() - t0
        part_row_count = ch.execute(f"SELECT count() FROM {table_name}", settings=SETTINGS)[0][0]
        part_disk_bytes = ch.execute(
            f"SELECT sum(bytes_on_disk) FROM system.parts WHERE table='{table_name}' AND active",
            settings=SETTINGS,
        )[0][0] or 0
        row_count_ch += part_row_count
        table_size += part_disk_bytes
        # ClickHouse tablosunu SIL -- her parcadan sonra hemen, birden fazla
        # buyuk tablonun AYNI ANDA var olmasi bellek basincini artiriyor (Bolum 41.1)
        ch.execute(f"DROP TABLE IF EXISTS {table_name}", settings=SETTINGS)
        if multi:
            print(f"  [FAZ2 {tag}] parca {i}/{len(parts)} yuklendi: {part_row_count} satir, "
                  f"yukle_ch={time.time()-t0:.1f}sn", flush=True)

    object_key_combined = " + ".join(p["object_key"] for p in parts)
    table_name_combined = " + ".join(table_names)
    match = (cr["row_count_tab"] == row_count_ch)

    cur = pg_conn.cursor()
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
            cr["tab_file_name"],
            cr["aircraft_type"],
            False,
            None,
            cr["row_count_tab"],
            row_count_ch,
            n_total_cols,
            False,
            object_key_combined,
            cr["zst_size"],
            cr["src_size"],
            "zstd",
            ZSTD_LEVEL,
            cr["compress_time"],
            cr["upload_time"],
            load_time,
            table_name_combined,
            table_size,
            "done" if match else "verification_failed",
        ),
        )

    # conversion_manifest_history -- AYNI denemenin (tab_file_name,
    # attempt_no) satirini 'done'/'verification_failed' ile kapatir.
    # conversion_manifest'in aksine bu satir bir daha ASLA
    # guncellenmeyecek -- bir sonraki deneme YENI bir attempt_no ile
    # kendi satirini acacak (Bolum 44).
    cur.execute(
        """
        UPDATE conversion_manifest_history
        SET aircraft_type = %s, is_subset = %s, subset_row_count = %s,
            row_count_tab = %s, row_count_clickhouse = %s,
            column_count = %s, had_trailing_tab_issue = %s,
            tab_zst_object_key = %s, tab_zst_size_bytes = %s, original_size_bytes = %s,
            compression_algorithm = %s, compression_level = %s,
            compress_duration_seconds = %s, minio_upload_duration_seconds = %s,
            clickhouse_load_duration_seconds = %s,
            clickhouse_table_name = %s, clickhouse_disk_bytes = %s,
            clickhouse_loaded_at = now(), status = %s, finished_at = now()
        WHERE tab_file_name = %s AND attempt_no = %s
        """,
        (
            cr["aircraft_type"], False, None,
            cr["row_count_tab"], row_count_ch,
            n_total_cols, False,
            object_key_combined, cr["zst_size"], cr["src_size"],
            "zstd", ZSTD_LEVEL,
            cr["compress_time"], cr["upload_time"], load_time,
            table_name_combined, table_size,
            "done" if match else "verification_failed",
            cr["tab_file_name"], cr["attempt_no"],
        ),
    )
    cur.close()

    # NOT: ClickHouse tablolari yukaridaki dongude her parcadan sonra ZATEN
    # silindi -- burada tekrar silinecek bir sey yok (Bolum 46).

    print(
        f"[{cr['aircraft_type']}] {tag}: kaynak={cr['src_size']/(1024**2):.1f}MB "
        f"zst={cr['zst_size']/(1024**2):.1f}MB ({cr['src_size']/cr['zst_size']:.2f}x) "
        f"sikistir={cr['compress_time']:.1f}sn yukle_minio={cr['upload_time']:.2f}sn "
        f"yukle_ch={load_time:.1f}sn satir={row_count_ch} ch_disk={table_size/(1024**2):.1f}MB "
        f"{f'({len(parts)} parca) ' if multi else ''}"
        f"{'OK' if match else 'HATA!'}",
        flush=True,
    )
    return {
        "compress_time": cr["compress_time"],
        "upload_time": cr["upload_time"],
        "load_time": load_time,
        "src_size": cr["src_size"],
        "zst_size": cr["zst_size"],
        "match": match,
    }


def make_pg_conn():
    conn = psycopg2.connect(host="postgres", dbname="telemetry_meta", user="postgres", password="pg123")
    conn.autocommit = True
    return conn


def make_ch_client():
    return Client(host="clickhouse", user="default", password="ch123", database="default", settings=SETTINGS)


# Tek-dosya modu: "python3 pipeline_grid_to_clickhouse.py 20 50000" seklinde
# cagirilirsa SADECE o dosyayi isler (host'tan ClickHouse'u her dosya
# oncesi temiz baslatip cagirmak icin -- Bolum 41.1'deki bellek
# birikimi sorununu onlemek amaciyla). Tek dosyada paralellik anlamsiz,
# faz 1 + faz 2 sirayla, ayni process'te calisir.
if len(sys.argv) == 3:
    n_cols_k = int(sys.argv[1])
    n_rows = int(sys.argv[2])
    tag = f"{n_cols_k}k_{n_rows}"
    tab_file_name = f"synthetic_{tag}.tab"

    pg = make_pg_conn()
    ch = make_ch_client()
    attempt_no = mark_processing(pg, tab_file_name, aircraft_label(n_cols_k))
    try:
        # Bolum 46.5: tek-dosya modunda BASKA hicbir dosya rekabet etmiyor --
        # ZSTD'nin dahili coklu-thread'i (tek dosyada ~2,5x kazanc) guvenle
        # kullanilabilir.
        cr = compress_and_upload(n_cols_k, n_rows, zstd_threads=ZSTD_MAX_INTERNAL_THREADS)
        cr["attempt_no"] = attempt_no
        load_and_record(ch, pg, cr)
    except Exception as e:
        mark_error(pg, tab_file_name, attempt_no, f"{type(e).__name__}: {e}")
        pg.close()
        raise
    print("TEK_DOSYA_TAMAMLANDI", flush=True)
    pg.close()
    sys.exit(0)

# Coklu-dosya modu -- FAZ 1 (paralel) + FAZ 2 (sirali, Bolum 45.2)
pg = make_pg_conn()
ch = make_ch_client()

cur0 = pg.cursor()
cur0.execute("SELECT tab_file_name FROM conversion_manifest WHERE status='done'")
already_done = {row[0] for row in cur0.fetchall()}
cur0.close()

pending = []
for n_cols_k in COLUMN_TIERS:
    for n_rows in ROW_COUNTS:
        tag = f"{n_cols_k}k_{n_rows}"
        if f"synthetic_{tag}.tab" in already_done:
            print(f"[atlanildi, zaten tamamlanmis] {tag}", flush=True)
        else:
            pending.append((n_cols_k, n_rows))

print(f"Toplam {len(COLUMN_TIERS)*len(ROW_COUNTS)} dosya, {len(pending)} tanesi islenecek "
      f"(Faz 1: N={COMPRESS_WORKERS} paralel sikistirma).", flush=True)
grand_t0 = time.time()

# Bolum 46.5: bekleyen dosya sayisi worker sayisindan AZ/ESITSE, havuzda
# HICBIR kuyruklama olmayacagi GARANTI -- her worker kendi dosyasini alir,
# digerlerini beklemez, boylece bos kalacak fazladan cekirdekleri ZSTD'nin
# dahili thread'ine ayirmak guvenli. Bekleyen > worker sayisiysa (normal/
# dolu havuz durumu) Bolum 46.5'in ana bulgusu geciyor: N=6/thread=1 karisik
# is yukunde daha iyi, thread=1 kaliyor.
if len(pending) <= COMPRESS_WORKERS and len(pending) > 0:
    zstd_threads_per_worker = max(1, min(ZSTD_MAX_INTERNAL_THREADS, CPU_CORES // len(pending)))
else:
    zstd_threads_per_worker = 1

print(flush=True)
print(f"=== FAZ 1: sikistirma + MinIO yukleme (N={COMPRESS_WORKERS} paralel, "
      f"dosya basina zstd_threads={zstd_threads_per_worker}) ===", flush=True)
compress_results = {}
attempt_nos = {}  # tag -> bu calistirmadaki attempt_no (conversion_manifest_history icin, Bolum 44)
with ProcessPoolExecutor(max_workers=COMPRESS_WORKERS) as pool:
    futures = {}
    for n_cols_k, n_rows in pending:
        tag = f"{n_cols_k}k_{n_rows}"
        attempt_nos[tag] = mark_processing(pg, f"synthetic_{tag}.tab", aircraft_label(n_cols_k))
        futures[pool.submit(compress_and_upload, n_cols_k, n_rows, zstd_threads_per_worker)] = tag
    for future in as_completed(futures):
        tag = futures[future]
        try:
            cr = future.result()
            cr["attempt_no"] = attempt_nos[tag]
            compress_results[tag] = cr
            print(f"  [FAZ1 tamam] {tag}: sikistir={cr['compress_time']:.1f}sn "
                  f"yukle_minio={cr['upload_time']:.2f}sn", flush=True)
        except Exception as e:
            print(f"  [FAZ1 HATA] {tag}: {type(e).__name__}: {e}", flush=True)
            mark_error(pg, f"synthetic_{tag}.tab", attempt_nos[tag], f"[Faz1-sikistirma] {type(e).__name__}: {e}")

phase1_elapsed = time.time() - grand_t0
print(f"Faz 1 tamamlandi: {phase1_elapsed/60:.1f}dk, {len(compress_results)}/{len(pending)} basarili", flush=True)

print(flush=True)
print("=== FAZ 2: ClickHouse'a sirali yukleme (Bolum 45.2) ===", flush=True)
phase2_t0 = time.time()
results = []
for n_cols_k, n_rows in pending:
    tag = f"{n_cols_k}k_{n_rows}"
    if tag not in compress_results:
        continue  # Faz 1'de basarisiz oldu, atla
    try:
        r = load_and_record(ch, pg, compress_results[tag])
        results.append(r)
    except Exception as e:
        print(f"HATA: {tag} -> {type(e).__name__}: {e}", flush=True)
        mark_error(pg, f"synthetic_{tag}.tab", attempt_nos[tag], f"[Faz2-ClickHouse] {type(e).__name__}: {e}")
        raise
phase2_elapsed = time.time() - phase2_t0

grand_elapsed = time.time() - grand_t0
print(flush=True)
print("=== TÜMÜ TAMAMLANDI ===", flush=True)
print(f"Toplam süre: {grand_elapsed/60:.1f}dk (Faz 1: {phase1_elapsed/60:.1f}dk, Faz 2: {phase2_elapsed/60:.1f}dk)", flush=True)
print(f"Toplam sıkıştırma süresi (CPU-toplam, paralel): {sum(r['compress_time'] for r in results)/60:.1f}dk", flush=True)
print(f"Toplam MinIO yükleme süresi: {sum(r['upload_time'] for r in results):.1f}sn", flush=True)
print(f"Toplam ClickHouse yükleme süresi: {sum(r['load_time'] for r in results)/60:.1f}dk", flush=True)
print(f"Başarısız dosya sayısı: {sum(1 for r in results if not r['match'])}", flush=True)

cur = pg.cursor()
cur.execute("SELECT count(*) FROM conversion_manifest WHERE status='done'")
print(f"Postgres'te 'done' durumunda kayıt: {cur.fetchone()[0]}", flush=True)
cur.close()
pg.close()
