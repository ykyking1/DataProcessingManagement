# -*- coding: utf-8 -*-
"""
Bolum 26.2'nin "N=2 esazamanli worker (2 AYRI dosya/tablo esazamanli
yuklenirse) en iyi nokta" bulgusu ~1.000 sutunluk eski semada
olculmustu, 10.000+ sutun olcekte hic test edilmemisti (Bolum 32/43'un
acik maddesi -- 43'te SADECE max_download_threads test edildi, N=2
esazamanli worker kismi atlanmisti, kullanici bunu fark etti).

Bu test Faz 2'nin (ClickHouse yukleme) GERCEKTEN paralellestirilip
paralellestirilemeyecegini olcuyor -- Bolum 41.1'deki "AYNI/buyuyen
TEK tabloya art arda yukleme bellek baskisi biriktiriyor" bulgusuyla
KARISTIRILMAMALI: o senaryo tek tabloya sirali INSERT'lerdi, bu test
ise 2 FARKLI dosyayi 2 FARKLI/bagimsiz tabloya AYNI ANDA yuklemeyi
test ediyor -- kavramsal olarak farkli bir soru.

Iki orta-buyuklukte, zaten MinIO'da hazir grid dosyasi kullanilir
(10k_50000, 20k_50000) -- en buyuk dosyalar (50k x 100k, bilinen
bellek siniri) DEGIL, riski kontrollu tutmak icin.

Adim 1: N=1 sirali taban -- once dosya A, sonra dosya B (mevcut Faz 2
deseni).
Adim 2: N=2 esazamanli -- iki dosya AYNI ANDA, ayri thread + ayri
clickhouse_driver Client baglantisiyla, ayri hedef tablolara.
"""
import time
import json
import threading
from clickhouse_driver import Client

GRID_DIR = "/work/synthetic_grid"
BUCKET = "telemetry"
import sys
TAGS = sys.argv[1:] if len(sys.argv) > 1 else ["10k_50000", "20k_50000"]

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


def make_ch():
    return Client(host="clickhouse", user="default", password="ch123", database="default", settings=SETTINGS)


def build_ddl(table_name, cols):
    col_defs = []
    for c in cols:
        if c == "aircraft_type":
            col_defs.append(f"`{c}` LowCardinality(String) CODEC(ZSTD)")
        elif c == "timestamp" or c.startswith("f"):
            col_defs.append(f"`{c}` Float64 CODEC(ZSTD)")
        else:
            col_defs.append(f"`{c}` UInt8 CODEC(T64, ZSTD)")
    return (
        f"CREATE TABLE {table_name} (\n  "
        + ",\n  ".join(col_defs)
        + "\n) ENGINE = MergeTree() ORDER BY tuple()\n"
        "SETTINGS min_bytes_for_wide_part = 10737418240000, min_rows_for_wide_part = 1000000000"
    )


def load_one(tag, table_suffix, results, key):
    """Kendi Client'ini kurar (thread-safety icin ayri baglanti sart),
    tabloyu olusturur, yukler, suresini results[key]'e yazar."""
    ch = make_ch()
    manifest_path = f"{GRID_DIR}/synthetic_{tag}_columns.json"
    with open(manifest_path, "r", encoding="utf-8") as f:
        gen_manifest = json.load(f)
    cols = gen_manifest["column_order"]
    row_count_expected = gen_manifest["n_rows"]

    table_name = f"concur_test_{table_suffix}"
    ch.execute(f"DROP TABLE IF EXISTS {table_name}")
    ch.execute(build_ddl(table_name, cols), settings=SETTINGS)

    object_key = f"grid/synthetic_{tag}.tab.zst"
    s3_url = f"http://minio:9000/{BUCKET}/{object_key}"
    insert_sql = (
        f"INSERT INTO {table_name} SELECT * FROM s3("
        f"'{s3_url}', 'minioadmin', 'minioadmin123', 'TabSeparatedWithNames')"
    )
    t0 = time.time()
    try:
        ch.execute(insert_sql, settings=SETTINGS)
        elapsed = time.time() - t0
        row_count = ch.execute(f"SELECT count() FROM {table_name}", settings=SETTINGS)[0][0]
        ok = (row_count == row_count_expected)
        results[key] = (elapsed, ok, None)
    except Exception as e:
        elapsed = time.time() - t0
        results[key] = (elapsed, False, f"{type(e).__name__}: {e}")
    finally:
        ch.execute(f"DROP TABLE IF EXISTS {table_name}")


n = len(TAGS)
print(f"Test dosyalari (N={n}): {TAGS}", flush=True)
print(flush=True)

# --- Adim 1: N=1 sirali (mevcut Faz 2 deseni) ---
print(f"=== N=1 (sirali, {n} dosya) ===", flush=True)
seq_results = {}
t0 = time.time()
for i, tag in enumerate(TAGS):
    load_one(tag, f"seq_{i}", seq_results, i)
    r = seq_results[i]
    print(f"  {tag}: {r[0]:.1f}sn {'OK' if r[1] else 'HATA: ' + str(r[2])}", flush=True)
seq_wall = time.time() - t0
print(f"N=1 toplam duvar-saati: {seq_wall:.1f}sn", flush=True)
print(flush=True)

# --- Adim 2: N esazamanli ---
print(f"=== N={n} (esazamanli) ===", flush=True)
conc_results = {}
t0 = time.time()
threads = [threading.Thread(target=load_one, args=(tag, f"conc_{i}", conc_results, i)) for i, tag in enumerate(TAGS)]
for th in threads:
    th.start()
for th in threads:
    th.join()
conc_wall = time.time() - t0
for i, tag in enumerate(TAGS):
    r = conc_results[i]
    print(f"  {tag}: {r[0]:.1f}sn {'OK' if r[1] else 'HATA: ' + str(r[2])}", flush=True)
print(f"N={n} toplam duvar-saati: {conc_wall:.1f}sn", flush=True)
print(flush=True)

print("=== OZET ===", flush=True)
print(f"N=1 sirali:      {seq_wall:.1f}sn", flush=True)
print(f"N={n} esazamanli: {conc_wall:.1f}sn", flush=True)
if conc_wall > 0:
    print(f"Hizlanma: {seq_wall/conc_wall:.2f}x", flush=True)
