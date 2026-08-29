"""
Geniş şemalı (binlerce sütunlu) telemetri dosyalarını ClickHouse'a yükler.

AU-AIR'in standart 17 sütunluk şeması (bkz. ingest_telemetry.py /
load_clickhouse.py) dışında, çok daha geniş şemalı kaynak dosyalar da
(örn. "auair_2Mx10K_merged.tab.gz" -- 2015 sütun, kanal başına birden
fazla sensör metriği) işlenebilmesi gerekiyor; bu bizim için istisna
değil, normal bir veri şekli. Mevcut ingest_telemetry.py/process_
telemetry.py/load_clickhouse.py üçlüsü AU-AIR'in sabit 17 sütununa
(velocity_x, roll, box_x, ...) sıkı sıkıya bağlı olduğu için bu geniş
dosyalar o hattan geçemiyor -- bu script bunun yerine, dosyanın GERÇEK
sütunlarından şemayı kendisi çıkarıp ayrı bir ClickHouse tablosuna
(varsayılan: telemetry_extended) yazan, AU-AIR şemasından bağımsız bir
alternatif sağlıyor. Mevcut `telemetry` tablosuna hiç dokunulmaz.

CLI argümanları ve --metadata-out sözleşmesi (dagster/assets.py::
extended_telemetry_load'ın okuduğu alanlar) ile TAM UYUMLU -- yalnızca
ClickHouse'a YÜKLEME MEKANİZMASI değişti:

  ESKİ: --chunk-rows'luk parçalar halinde okunup HER PARÇA ayrı bir
  clickhouse_driver INSERT'i ile (satır satır) gönderiliyordu.

  YENİ (2026-08): parçalar YİNE --chunk-rows ile bellek sınırlı okunup
  temizleniyor (bu kısım DEĞİŞMEDİ -- karışık gerçek dosyalarda gerekli:
  Türkçe ';' ayraçlı CSV, .gz, bozuk sayısal değerler vb.), ama
  ClickHouse'a satır satır INSERT yerine önce yerel bir CSV+zstd
  dosyasına yazılıyor, MinIO'ya yükleniyor, sonra ClickHouse'un s3()
  tablo fonksiyonuyla TEK SEFERDE toplu okunuyor. Bu, ayrı bir
  pipeline'da (bkz. scripts/pipeline_grid_to_clickhouse.py, ~28k
  satır/sn'lik sentetik grid yüklemeleri) ölçülmüş olan ve satır-satır
  INSERT'e göre ~17,5x hızlı olan yöntemin aynısı -- bkz. proje
  belleğindeki "MinIO->ClickHouse bulk load" notu.

  Bellek prensibi hâlâ geçerli: dosya hiçbir noktada tek seferde
  belleğe alınmıyor -- --chunk-rows'luk parçalar okunup temizlenip
  sıkıştırılmış çıktı akışına yazılıyor, sadece ClickHouse'a giden SON
  adım (ağ INSERT'leri) toplulaştırıldı.

Kullanım:
    python load_extended_telemetry.py --file-path dataset.tab.gz \
        --metadata-out meta.json
"""

import argparse
import gzip
import io
import json
import threading
import time
import uuid
from pathlib import Path

import pandas as pd
import zstandard as zstd
from clickhouse_driver import Client
from minio import Minio
from minio.error import S3Error

from load_clickhouse import (
    _get_clickhouse_database,
    _get_clickhouse_native_host,
    _get_clickhouse_native_port,
    _get_clickhouse_password,
    _get_clickhouse_user,
)


# "time" doğrudan yoksa bu adaylardan ilk bulunanı zaman sütunu olarak
# kabul edilip "time" adına yeniden adlandırılır -- dashboard'daki
# filtreler (bkz. dashboard/app.py::build_clickhouse_where) "time"
# sütun adını sabit kodlanmış olarak bekliyor.
TIME_COLUMN_CANDIDATES = ["time", "timestamp_utc", "timestamp", "date"]

# Bir sütundaki dolu (null olmayan) değerlerin en az bu oranı sayısala
# çevrilebiliyorsa Float64 kabul edilir; aksi halde String -- bu geniş
# dosyalardaki sütunlar seyrek (çoğu satırda boş) olabildiği için katı
# "hepsi sayısal olmalı" kuralı yerine bir eşik kullanılıyor.
NUMERIC_FRACTION_THRESHOLD = 0.9

ZSTD_LEVEL = 12

# _split_compress_upload_by_rows: satır-parçaları ZSTD_LEVEL=12 yerine
# bu daha düşük (daha hızlı) seviyeyle sıkıştırır. Yüzlerce küçük parça
# tek tek sıkıştırıldığından seviye 12'nin oran kazancı önemsiz kalırken
# CPU maliyeti toplam süreye ciddi ekleniyor; seviye 6, zstd'nin hız/
# oran dengesinde makul bir nokta (MinIO/ağ yerel olduğu için burada
# asıl maliyet disk/ağ değil CPU süresi).
ROW_CHUNK_ZSTD_LEVEL = 6

# _fast_template_load_long_sql: ARRAY JOIN pivotunu tek seferde TÜM
# sensör sütunlarıyla değil, gruplar halinde çalıştırır -- tek seferde
# TÜM veriyi pivotlamak (ara tablo olsa da olmasa da) host'u kritik
# belleğe düşürebiliyor. Parça boyutu SABİT değil, dosyanın kaynak
# satır sayısına göre _compute_row_chunk_size() tarafından hesaplanır:
# tepe bellek kullanımını belirleyen şey parça başına sütun sayısı değil,
# parça başına ÜRETİLEN ÇIKTI satırı (kaynak_satır x parça_sütun).
#
# Ayrıca TOPLAM parça/INSERT sayısı da ayrı bir etken: her INSERT yeni
# bir MergeTree parçası yaratır, ClickHouse'un arka plan birleştirmesi
# bunu erittikçe bellek kullanır -- çok sayıda küçük INSERT'te bu
# kümülatif bir baskıya dönüşüyor. Bu yüzden hedef, parça başına ÇOK
# küçük değil, makul büyüklükte (10M çıktı satırı) tutuluyor: daha az
# ama daha büyük parça, toplam INSERT sayısını azaltır.
PIVOT_TARGET_OUTPUT_ROWS_PER_CHUNK = 10_000_000

# Bir INSERT/mutation başlatmadan önce bu değerin altında (container'ın
# /proc/meminfo::MemAvailable'ı -- Docker Desktop/WSL2'de paylaşılan
# VM'nin geneli, sadece bu container değil) host'u kritik belleğe
# düşürme riski yüksek kabul edilir. Gerçek host çökmelerinde bu değer
# hep ~0,4-1,7GB aralığındaydı ve düşüş genelde saniyeler içinde oluyordu.
MEMORY_SAFE_FLOOR_GB = 2.5


def _get_available_memory_gb() -> float | None:
    """
    Container'ın (Docker Desktop/WSL2'de paylaşılan VM'nin) şu anki
    MemAvailable değerini GB olarak döner. /proc/meminfo yoksa (Linux
    değilse) None döner -- çağıran taraf bu durumda kontrolü atlar.
    """
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    kb = int(line.split()[1])
                    return kb / (1024 * 1024)
    except (OSError, ValueError, IndexError):
        return None
    return None


def _ensure_memory_headroom(stage_label: str) -> None:
    """
    Bellek MEMORY_SAFE_FLOOR_GB'nin altındaysa, ClickHouse'a/host'a hiç
    dokunmadan (hiçbir ağır işlem başlatmadan) AÇIK bir hatayla durur --
    bkz. proje belleği: host'un öngörülemez şekilde donmasının/WSL'in
    kapanmasının önüne geçmek için, "sessizce host'u çökertmek" yerine
    "gürültülü ama zararsız bir Dagster hatası" tercih ediliyor. Bu
    hata Dagster run'ını FAILED yapar (görünür, açıklayıcı) -- host'u
    dondurmaz. Kullanıcı 'wsl --shutdown' + tekrar deneyerek düzeltebilir.
    """
    available_gb = _get_available_memory_gb()
    if available_gb is not None and available_gb < MEMORY_SAFE_FLOOR_GB:
        raise MemoryError(
            f"Bellek güvenlik eşiğinin altında ({available_gb:.2f}GB < "
            f"{MEMORY_SAFE_FLOOR_GB}GB) -- '{stage_label}' adımı GÜVENLİ "
            "OLMADIĞI için başlatılmadı (host'u kritik belleğe düşürüp "
            "donmasını önlemek için). Host'ta 'wsl --shutdown' çalıştırıp "
            "birkaç saniye bekledikten sonra bu adımı tekrar deneyin -- "
            "zaten yüklenmiş kısmi satırlar bir sonraki denemede otomatik "
            "temizlenir."
        )


# ClickHouse dinlenme hâlindeyken (arka plan birleştirmesi toparlanmışken)
# tipik olarak yalnızca birkaç düzine aktif parça ve düşük bellek
# kullanımı görülür -- sorun toplam veri hacmi değil, ard arda ÇOK HIZLI
# gelen INSERT'lerin arka plan birleştirmesinin yetişebileceğinden daha
# hızlı yeni parça biriktirmesi. Bu yüzden parça boyutunu/sayısını sabit
# ayarlamak yerine, bu baskı doğrudan ölçülüp gerektiğinde YAVAŞLATILIR.
MAX_ACTIVE_PARTS_BEFORE_CHUNK = 30
PARTS_SETTLE_POLL_SECONDS = 5
PARTS_SETTLE_MAX_WAIT_SECONDS = 300


def _wait_for_merge_pressure_to_settle(
    client: "Client", table_fqn: str, stage_label: str
) -> None:
    """
    Bir sonraki INSERT'i göndermeden önce, hedef tablodaki AKTİF parça
    sayısı MAX_ACTIVE_PARTS_BEFORE_CHUNK'ın üzerindeyse, ClickHouse'un
    arka plan birleştirmesinin bunu azaltması için bekler (kısa
    aralıklarla tekrar kontrol ederek). PARTS_SETTLE_MAX_WAIT_SECONDS
    kadar beklendiği hâlde hâlâ yüksekse, host'u riske atmak yerine
    -- _ensure_memory_headroom ile AYNI felsefe -- AÇIK bir hatayla
    durur (host donmaz, bir sonraki denemede idempotent temizlik zaten
    devrede).
    """
    database, table_name = table_fqn.split(".", 1)
    waited = 0.0
    while True:
        active_parts = client.execute(
            "SELECT count() FROM system.parts WHERE database = %(db)s "
            "AND table = %(table)s AND active",
            {"db": database, "table": table_name},
        )[0][0]
        if active_parts <= MAX_ACTIVE_PARTS_BEFORE_CHUNK:
            return
        if waited >= PARTS_SETTLE_MAX_WAIT_SECONDS:
            raise RuntimeError(
                f"'{stage_label}' adımından önce {table_fqn} tablosundaki "
                f"aktif parça sayısı ({active_parts}) {PARTS_SETTLE_MAX_WAIT_SECONDS}sn "
                f"beklenmesine rağmen {MAX_ACTIVE_PARTS_BEFORE_CHUNK}'ın altına "
                "inmedi -- arka plan birleştirmesi yetişemiyor olabilir. Host'u "
                "riske atmamak için burada durduruldu; bir sonraki denemede "
                "idempotent temizlik zaten devrede."
            )
        print(
            f"  (aktif parça sayısı {active_parts} > {MAX_ACTIVE_PARTS_BEFORE_CHUNK} -- "
            f"birleştirmenin yetişmesi için {PARTS_SETTLE_POLL_SECONDS}sn bekleniyor...)",
            flush=True,
        )
        time.sleep(PARTS_SETTLE_POLL_SECONDS)
        waited += PARTS_SETTLE_POLL_SECONDS


# Parça-baskısı hız kesicisi TEK BAŞINA yetersiz kalabiliyor: bir INSERT
# başlamadan önceki kontrol geçse bile, ClickHouse'un kendi bellek
# ayırıcısı (jemalloc) art arda gelen sorgularda kullandığı belleği
# işletim sistemine hemen geri vermediği için, TEK BİR sorgunun ÇALIŞMASI
# SIRASINDA bile bellek hızla kritiğe düşebiliyor. Bu yüzden sorgu
# çalışırken de AYRI BİR İŞ PARÇACIĞINDA bellek izlenir; kritik eşiğe
# değince sorgu KILL QUERY ile ANINDA iptal edilir -- host'u dondurmak
# yerine temiz bir hata vermenin tek güvenilir yolu budur.
MEMORY_WATCHDOG_CRITICAL_GB = 2.0
MEMORY_WATCHDOG_POLL_SECONDS = 1.0


def _execute_with_oom_watchdog(
    client: "Client", query: str, stage_label: str, client_factory
) -> None:
    """
    query'yi client üzerinde ÇALIŞTIRIRKEN, AYRI bir iş parçacığında
    /proc/meminfo'yu MEMORY_WATCHDOG_POLL_SECONDS aralıklarla izler.
    Bellek MEMORY_WATCHDOG_CRITICAL_GB'nin altına düşerse, AYRI bir
    ClickHouse bağlantısından (aynı bağlantı meşgul olduğu için)
    'KILL QUERY' gönderip sorguyu ANINDA iptal eder -- host'u kritik
    belleğe düşmeye devam etmesine izin vermek yerine, ClickHouse'un
    kendisi belleği bırakır. client_factory: gerektiğinde YENİ bir
    Client açan, argümansız çağrılabilir bir fonksiyon.
    """
    query_id = uuid.uuid4().hex
    killed = threading.Event()
    stop_watching = threading.Event()

    def _watch() -> None:
        while not stop_watching.is_set():
            available_gb = _get_available_memory_gb()
            if available_gb is not None and available_gb < MEMORY_WATCHDOG_CRITICAL_GB:
                print(
                    f"  [BEKÇİ] Bellek kritik eşiğe düştü ({available_gb:.2f}GB < "
                    f"{MEMORY_WATCHDOG_CRITICAL_GB}GB) -- '{stage_label}' sorgusu "
                    "ANINDA iptal ediliyor (KILL QUERY)...",
                    flush=True,
                )
                try:
                    kill_client = client_factory()
                    try:
                        kill_client.execute(
                            f"KILL QUERY WHERE query_id = '{query_id}' SYNC"
                        )
                    finally:
                        kill_client.disconnect()
                except Exception as exc:
                    print(f"  [BEKÇİ] KILL QUERY gönderilemedi: {exc}", flush=True)
                killed.set()
                return
            stop_watching.wait(MEMORY_WATCHDOG_POLL_SECONDS)

    watcher = threading.Thread(target=_watch, daemon=True)
    watcher.start()
    try:
        client.execute(query, settings=CH_SETTINGS, query_id=query_id)
    except Exception as exc:
        if killed.is_set():
            raise MemoryError(
                f"'{stage_label}' adımı çalışırken bellek kritik eşiğe "
                f"düştüğü için ({MEMORY_WATCHDOG_CRITICAL_GB}GB altı) sorgu "
                "bekçi tarafından otomatik iptal edildi -- host donmadan "
                "güvenle durduruldu. Host'ta 'wsl --shutdown' çalıştırıp "
                "tekrar deneyin; bir sonraki denemede idempotent temizlik "
                "kısmi satırları otomatik siler."
            ) from exc
        raise
    finally:
        stop_watching.set()
        watcher.join(timeout=2)


# jemalloc, gerçek boşta kalma süresi verilirse kullandığı belleği
# işletim sistemine geri veriyor -- art arda hiç durmadan gelen
# INSERT'ler bu fırsatı hiç tanımadığı için bellek birikip gidiyor. Bu
# yüzden her CHECKPOINT_INTERVAL_CHUNKS parçada bir DURULUR ve
# ClickHouse'un belleği gerçekten CHECKPOINT_TARGET_GB'a inene kadar
# beklenir -- büyük dosya tek nefeste değil, aralarında gerçek soğuma
# molaları olan gruplar hâlinde yüklenir.
CHECKPOINT_INTERVAL_CHUNKS = 2
CHECKPOINT_TARGET_GB = 1.2
CHECKPOINT_POLL_SECONDS = 5
CHECKPOINT_MAX_WAIT_SECONDS = 180


def _get_clickhouse_resident_gb(client: "Client") -> float | None:
    try:
        rows = client.execute(
            "SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.resident'"
        )
        return rows[0][0] / (1024**3) if rows else None
    except Exception:
        return None


def _checkpoint_cooldown(client: "Client", stage_label: str) -> None:
    """
    ClickHouse'un kendi belleğinin (jemalloc.resident) CHECKPOINT_
    TARGET_GB'a inmesini bekler -- jemalloc'a gerçek boşta kalma süresi
    tanıyıp belleği geri vermesine fırsat verir. CHECKPOINT_MAX_WAIT_
    SECONDS'a rağmen inmezse, host'u riske atmak yerine devam edilir
    (bu bir "en iyi çaba" adımı -- asıl güvenlik ağı hâlâ _ensure_
    memory_headroom + host'taki watch_host_memory.ps1).
    """
    resident_gb = _get_clickhouse_resident_gb(client)
    if resident_gb is None or resident_gb <= CHECKPOINT_TARGET_GB:
        return
    print(
        f"  [KONTROL NOKTASI] '{stage_label}' -- ClickHouse belleği "
        f"{resident_gb:.2f}GB, {CHECKPOINT_TARGET_GB}GB'a inmesi için "
        "soğuma bekleniyor...",
        flush=True,
    )
    waited = 0.0
    while waited < CHECKPOINT_MAX_WAIT_SECONDS:
        time.sleep(CHECKPOINT_POLL_SECONDS)
        waited += CHECKPOINT_POLL_SECONDS
        resident_gb = _get_clickhouse_resident_gb(client)
        if resident_gb is None or resident_gb <= CHECKPOINT_TARGET_GB:
            print(
                f"  [KONTROL NOKTASI] Soğudu ({resident_gb:.2f}GB, "
                f"{waited:.0f}sn) -- devam ediliyor.",
                flush=True,
            )
            return
    print(
        f"  [KONTROL NOKTASI] {CHECKPOINT_MAX_WAIT_SECONDS}sn beklendi, "
        f"hâlâ {resident_gb:.2f}GB -- yine de devam ediliyor (host "
        "gözetmeni gerekirse müdahale eder).",
        flush=True,
    )


def _compute_row_chunk_size(n_sensor_columns: int) -> int:
    """
    Kaynak dosya SATIR bazında bu boyuttaki küçük parçalara bölünür;
    her parça AYRI bir MinIO nesnesi olarak yüklenir ve pivot adımında
    TAM OLARAK BİR KEZ okunur (sütun bazlı parçalamada aynı tam dosya,
    kaç parçaya bölünmüşse o kadar kez yeniden indirilip açılıyordu --
    geniş dosyalarda gereksiz tekrar iş). Hedef: parça başına ~
    PIVOT_TARGET_OUTPUT_ROWS_PER_CHUNK çıktı satırı (satır_sayısı x
    TÜM sensör sütunu sayısı).
    """
    if n_sensor_columns <= 0:
        n_sensor_columns = 1
    row_chunk_size = PIVOT_TARGET_OUTPUT_ROWS_PER_CHUNK // n_sensor_columns
    # Çok dar dosyalarda (az sütun) parça aşırı büyümesin diye bir tavan
    # -- MinIO nesnesi/parça başına bellekte tutulan ham veri miktarını
    # da makul tutar.
    return max(1, min(row_chunk_size, 200_000))


def _split_compress_upload_by_rows(
    path: Path,
    mc: "Minio",
    bucket: str,
    object_prefix: str,
    row_chunk_size: int,
) -> list[tuple[str, int]]:
    """
    Ham dosyayı (başlık satırı hariç) row_chunk_size'lık gruplara böler;
    her grubu AYRI AYRI zstd'ye sıkıştırıp kendi MinIO nesnesi olarak
    yükler. Format bilerek 'TabSeparated' (başlıksız) -- şema zaten
    s3() çağrısına structure parametresiyle açıkça veriliyor, tekrar
    başlık satırına gerek yok.

    Bellek: her an bellekte sadece TEK bir parçanın ham satırları
    tutulur (row_chunk_size ile sınırlı) -- tüm dosya asla belleğe
    alınmıyor.

    Döner: [(object_key, parçadaki_satır_sayısı), ...]
    """
    opener = gzip.open if path.name.lower().endswith(".gz") else open
    cctx = zstd.ZstdCompressor(level=ROW_CHUNK_ZSTD_LEVEL)
    chunks_info: list[tuple[str, int]] = []
    chunk_idx = 0
    buf: list[bytes] = []

    def _flush() -> None:
        nonlocal chunk_idx
        if not buf:
            return
        chunk_idx += 1
        raw = b"".join(buf)
        compressed = cctx.compress(raw)
        object_key = f"{object_prefix}/chunk_{chunk_idx:05d}.tsv.zst"
        mc.put_object(
            bucket, object_key, io.BytesIO(compressed), length=len(compressed)
        )
        chunks_info.append((object_key, len(buf)))
        buf.clear()

    with opener(path, "rb") as fin:
        fin.readline()  # başlık satırını atla -- şema zaten biliniyor
        for line in fin:
            buf.append(line)
            if len(buf) >= row_chunk_size:
                _flush()
        _flush()

    return chunks_info

# ---------------------------------------------------------------------------
# Bilinen "uçak türü" şablonları -- scripts/gen_synthetic_grid.py'nin
# ürettiği sentetik grid'in isimlendirme kuralı (bkz. proje belleği).
# Manifest dosyası (_columns.json) OLMASA BİLE, sadece dosyanın BAŞLIK
# SATIRINA bakarak (veri içeriğine hiç bakmadan) bu 5 türden birine
# denk geldiğini anlayabiliyoruz -- toplam sütun sayısı + isim deseni
# (timestamp, aircraft_type, f*/m*/z*/o*) eşleşiyorsa, tipleri veriden
# ÇIKARMAK yerine DOĞRUDAN BİLİYORUZ; bu da pandas'lı örnekleme/temizlik
# adımlarını tamamen atlayıp scripts/pipeline_grid_to_clickhouse.py ile
# AYNI hızda (ham byte akışı, ~saniyeler) yüklemeyi mümkün kılıyor.
#
# EŞLEŞMEZSE (bilinmeyen sütun sayısı/isim deseni, sondaki bozuk ayraç
# sütunu vb.) sessizce mevcut GÜVENLİ pandas yoluna (aşağıdaki
# _infer_schema/_clean_and_compress_to_csv_zst) düşülür -- bu yol hiç
# değişmedi.
KNOWN_AIRCRAFT_COLUMN_COUNTS = {
    10_002: "AIRCRAFT_10K",
    20_002: "AIRCRAFT_20K",
    30_002: "AIRCRAFT_30K",
    40_002: "AIRCRAFT_40K",
    50_002: "AIRCRAFT_50K",
}

# Binlerce sütunlu bir CREATE TABLE/s3() sorgusunun ham SQL metni,
# ClickHouse'un varsayılan max_query_size'ını (256KiB) kolayca aşıyor
# ("Max query size exceeded" hatası) -- bkz. scripts/pipeline_grid_to_
# clickhouse.py::SETTINGS, aynı sınır orada da geniş şemalı (10K-50K
# sütun) tablolarda görülüp bu değerlere çıkarılmıştı. Aynı ayarlar
# burada da kullanılıyor.
CH_SETTINGS = {
    "max_query_size": 300_000_000,
    "max_ast_elements": 10_000_000,
    "max_expanded_ast_elements": 10_000_000,
    # Geniş tablolarda (binlerce sütun) paralel insert/parse'ın peak
    # belleği katlaması -- pipeline_grid_to_clickhouse.py::SETTINGS'te
    # aynı sebeple düşürülmüştü ("(total) memory limit exceeded" hatası,
    # Bölüm 37/41). Burada da INSERT SELECT FROM s3() sırasında aynı
    # patlama görüldü (10.002 sütun, 6,25GiB sunucu limitine çarptı).
    "input_format_parallel_parsing": 0,
    "max_threads": 2,
    "max_insert_threads": 1,
    # 8192'de bile 40K+ sütunlu tablolarda "(total) memory limit
    # exceeded" görüldü (2026-08-26, paylaşılan 11,68GB'lık VM
    # havuzunda) -- peak blok belleği ~sütun_sayısı x blok_boyutu x
    # ortalama_hücre_boyutu ile orantılı, bu yüzden geniş şemalarda
    # daha küçük bir blok boyutu gerekiyor.
    "max_block_size": 2048,
    "max_insert_block_size": 2048,
}

# ClickHouse'un varsayılan MergeTree "wide" part formatı, sütun başına
# AYRI dosya akışı açıyor -- binlerce sütunlu bir tabloda (INSERT
# sırasında hepsi aynı anda açık) tek başına ciddi bellek/dosya-tanıtıcı
# baskısı yaratıyor. Bu eşikleri devasa büyütmek ClickHouse'u "compact"
# part formatına (tüm sütunlar TEK dosyada) zorluyor -- aynı çözüm
# pipeline_grid_to_clickhouse.py::build_ddl'de de kullanılıyor.
WIDE_PART_GUARD_SETTINGS = (
    "min_bytes_for_wide_part = 10737418240000, "
    "min_rows_for_wide_part = 1000000000"
)


# ---------------------------------------------------------------------------
# MinIO bağlantı ayarları
#
# load_clickhouse.py'deki CLICKHOUSE_* deseninin aynısı: env değişkeni
# yoksa makul bir varsayılana düşer. Dagster (dolayısıyla bu script)
# host üzerinde çalıştığı için varsayılan host "localhost" -- t2p-cmp3
# gibi Docker container'ları içinde çalışan script'lerin kullandığı
# "minio" DNS adı burada GEÇERLİ DEĞİL.
# ---------------------------------------------------------------------------

import os


def _get_minio_endpoint() -> str:
    return os.environ.get("MINIO_ENDPOINT", "localhost:9000")


def _get_minio_access_key() -> str:
    return os.environ.get("MINIO_ACCESS_KEY", "minioadmin")


def _get_minio_secret_key() -> str:
    return os.environ.get("MINIO_SECRET_KEY", "minioadmin123")


def _get_minio_secure() -> bool:
    return os.environ.get("MINIO_SECURE", "false").strip().lower() in (
        "1", "true", "yes",
    )


def _get_minio_bucket() -> str:
    return os.environ.get("MINIO_BUCKET", "telemetry")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Geniş şemalı (AU-AIR'in sabit 17 sütununa uymayan) bir "
            "telemetri dosyasını, şemasını dosyadan çıkararak ayrı bir "
            "ClickHouse tablosuna yükler."
        )
    )
    parser.add_argument("--file-path", required=True, type=Path)
    parser.add_argument("--table-name", default="telemetry_extended")
    parser.add_argument(
        "--output-format",
        choices=["wide", "long_sql"],
        default="long_sql",
        help=(
            "'wide': dosyanın kendi geniş (sütun-başına-sensör) şemasıyla "
            "AYRI bir tablo (bkz. _fast_template_load) -- çok sayıda geniş "
            "tablo ClickHouse'un katalog belleğini zorluyor, büyük ölçekte "
            "önerilmez. 'long_sql' (varsayılan): sabit 5 sütunlu uzun/tidy "
            "format; dönüşüm ClickHouse'un kendi SQL'inde (ARRAY JOIN) "
            "yapılır, Python tarafında hiç ara dosya üretilmez."
        ),
    )
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=50_000,
        help="Temizleme sırasında bir seferde bellekte tutulacak satır sayısı.",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=50_000,
        help="Sütun tiplerini (Float64/String) çıkarmak için okunacak örnek satır sayısı.",
    )
    parser.add_argument("--metadata-out", required=True, type=Path)
    return parser.parse_args()


def _open_text(path: Path):
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def _detect_separator(path: Path) -> str:
    with _open_text(path) as f:
        header = f.readline()

    if "\t" in header:
        return "\t"

    if ";" in header:
        return ";"

    return ","


def _read_header_columns(path: Path, sep: str) -> list[str]:
    """Dosyanın SADECE ilk (başlık) satırını okuyup sütun adlarına
    ayırır -- pandas'a hiç girmeden, tek satırlık ucuz bir okuma."""

    with _open_text(path) as f:
        header_line = f.readline()

    return [name.strip() for name in header_line.rstrip("\r\n").split(sep)]


def _match_known_template(header_columns: list[str], sep: str) -> dict | None:
    """
    Başlık satırı bilinen 5 uçak türü şablonundan (bkz.
    KNOWN_AIRCRAFT_COLUMN_COUNTS) birine uyuyor mu diye bakar --
    VERİYE HİÇ BAKMADAN, sadece sütun SAYISI ve İSİM DESENİNE göre.

    Uyuyorsa (aircraft_type, ordered_columns, column_types) içeren bir
    dict döner -- column_types, scripts/pipeline_grid_to_clickhouse.py
    ::load_and_record ile AYNI kuralı kullanır (aircraft_type ->
    LowCardinality(String), timestamp/f* -> Float64, m*/z*/o* ->
    UInt8) -- iki pipeline'ın tip kararı tutarlı kalsın diye.

    Uymuyorsa (sütun sayısı bilinmiyor, sondaki bozuk ayraç sütunu,
    beklenmeyen isim deseni vb.) None döner -- çağıran taraf bunu
    mevcut GÜVENLİ pandas yoluna düşme sinyali olarak kullanır.
    """

    if sep != "\t":
        # Şablon, gen_synthetic_grid.py'nin ürettiği TAB-ayraçlı
        # dosyalara özel -- başka bir ayraç, tanımadığımız bir kaynak
        # demektir.
        return None

    if len(header_columns) < 3:
        return None

    # Sondaki bozuk ayraç sütunu (isimsiz/"Unnamed") ya da tekrarlanan
    # bir isim varsa -- güvenli olmayan bir durum, pandas yoluna düş.
    if any(not name or name.startswith("Unnamed") for name in header_columns):
        return None
    if len(set(header_columns)) != len(header_columns):
        return None

    aircraft_type = KNOWN_AIRCRAFT_COLUMN_COUNTS.get(len(header_columns))
    if aircraft_type is None:
        return None

    if header_columns[0] != "timestamp" or header_columns[1] != "aircraft_type":
        return None

    column_types: dict[str, str] = {}
    for column in header_columns[2:]:
        if column.startswith("f"):
            column_types[column] = "Float64"
        elif column.startswith(("m", "z", "o")):
            column_types[column] = "UInt8"
        else:
            # Beklenmeyen bir önek -- şablonun varsaydığı isimlendirme
            # kuralına uymuyor, güvenli tarafta kal.
            return None

    return {
        "aircraft_type": aircraft_type,
        "ordered_columns": header_columns,
        "column_types": column_types,
    }


def _drop_trailing_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bazı .tab dosyalarının her satırının SONUNDA fazladan bir ayraç
    karakteri olabiliyor (bkz. scripts/clean_tab_trailing_tab.py); bu
    pandas'ta isimsiz/boş başlıklı hayali bir sondaki sütun olarak
    görünür. Böyle bir sütun yoksa bu fonksiyon no-op'tur.
    """

    empty_columns = [
        column
        for column in df.columns
        if column == "" or str(column).startswith("Unnamed:")
    ]

    if empty_columns:
        df = df.drop(columns=empty_columns)

    return df


def _infer_schema(path: Path, sep: str, sample_rows: int):
    """
    Dosyanın ilk sample_rows satırından şemayı çıkarır.

    Döner: (time_column_or_None, {sütun_adı: "UInt8"|"Int64"|"Float64"|"String"}, tüm_sütunlar)
    time_column, ClickHouse tablosunda "time" adıyla yazılacak kaynak
    sütunun adıdır (TIME_COLUMN_CANDIDATES'teki ilk eşleşen).
    """

    sample = pd.read_csv(path, sep=sep, nrows=sample_rows, low_memory=False)
    sample.columns = sample.columns.str.strip()
    sample = _drop_trailing_empty_columns(sample)

    time_column = None
    for candidate in TIME_COLUMN_CANDIDATES:
        if candidate in sample.columns:
            time_column = candidate
            break

    column_types = {}

    for column in sample.columns:

        if column == time_column:
            continue

        non_null = sample[column].dropna()

        if non_null.empty:
            column_types[column] = "String"
            continue

        numeric = pd.to_numeric(non_null, errors="coerce")
        numeric_fraction = numeric.notna().mean()

        column_types[column] = (
            _numeric_column_type(numeric.dropna())
            if numeric_fraction >= NUMERIC_FRACTION_THRESHOLD
            else "String"
        )

    return time_column, column_types, list(sample.columns)


# ClickHouse'un UInt8 aralığı -- ikili (0/1) bayrak sütunları burada
# yoğunlukla görülüyor (geniş-şema sensör dosyalarında yaygın desen).
_UINT8_MIN, _UINT8_MAX = 0, 255


def _numeric_column_type(numeric_values: pd.Series) -> str:
    """
    Sayısal olduğu zaten belirlenmiş (NUMERIC_FRACTION_THRESHOLD'u geçmiş)
    bir sütunun DEĞERLERİNE bakarak Float64 yerine daha dar/doğru bir tip
    seçer:

      - Tüm değerler tam sayıysa (kesirli kısım yok) VE 0-255 aralığındaysa
        -> UInt8 (örn. ikili 0/1 bayrak sütunları -- 8 byte yerine 1 byte,
        hem ClickHouse diskinde hem pandas temizlik aşamasında ~8x daha az
        yer/bellek).
      - Tüm değerler tam sayıysa ama aralık dışındaysa -> Int64.
      - Kesirli değer varsa -> Float64 (eski davranış, değişmedi).

    NOT: Bu karar yalnızca --sample-rows'luk ÖRNEKTEN çıkarılıyor (aynı
    Float64/String kararı gibi) -- dosyanın geri kalanında örnekte
    görülmeyen bir değer (ör. UInt8 için 300, ya da beklenmedik bir
    ondalık) çıkarsa, ClickHouse'un s3() yüklemesi bunu SESSİZCE
    bozmak yerine YÜKSEK SESLE reddeder (tip/aralık hatası) -- bu,
    projedeki "sessizce bozma, gürültülü başarısız ol" ilkesiyle
    tutarlı bir davranış.
    """

    if numeric_values.empty:
        return "Float64"

    is_all_integer = (numeric_values % 1 == 0).all()
    if not is_all_integer:
        return "Float64"

    if (numeric_values >= _UINT8_MIN).all() and (numeric_values <= _UINT8_MAX).all():
        return "UInt8"

    return "Int64"


def _ensure_bucket(mc: Minio, bucket: str) -> None:
    try:
        if not mc.bucket_exists(bucket):
            mc.make_bucket(bucket)
    except S3Error as exc:
        raise RuntimeError(f"MinIO bucket kontrol/oluşturma hatası: {exc}") from exc


_RAW_COPY_CHUNK_SIZE = 64 * 1024 * 1024  # 64MB, pipeline_grid_to_clickhouse.py ile aynı


def _compress_raw_file_to_zst(
    path: Path, out_zst_path: Path, count_rows: bool = False
) -> tuple[float, int, int | None]:
    """
    Dosyayı HİÇ AYRIŞTIRMADAN (pandas'sız) -- ham byte akışı olarak
    doğrudan zstd'ye sıkıştırır. scripts/pipeline_grid_to_clickhouse.py
    ::_compress_one_file ile AYNI desen. .gz girişini şeffafça çözüp
    zstd'ye yeniden sıkıştırır; ikisi arasında hiçbir satır/hücre
    ayrıştırması yapılmaz -- sadece SADECE bilinen şablonla eşleşen
    (dolayısıyla biçiminden emin olduğumuz) dosyalar için güvenlidir.

    count_rows=True ise, zaten okunmakta olan byte akışındaki '\n'
    sayısını da (EKSTRA BİR DOSYA GEÇİŞİ OLMADAN) sayıp döner -- veri
    satırı sayısı = bu - 1 (başlık satırı). _fast_template_load_long_sql
    bunu, pivotu kaç parçaya böleceğine (bkz. _compute_pivot_chunk_size)
    karar vermek için kullanıyor.

    Döner: (sıkıştırma_süresi_sn, çıktı_boyutu_byte, toplam_satır_sayısı_veya_None)
    """

    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
    opener = gzip.open if path.name.lower().endswith(".gz") else open

    line_count = 0
    with opener(path, "rb") as fin, open(out_zst_path, "wb") as fout:
        compressor = cctx.stream_writer(fout)
        while True:
            chunk = fin.read(_RAW_COPY_CHUNK_SIZE)
            if not chunk:
                break
            if count_rows:
                line_count += chunk.count(b"\n")
            compressor.write(chunk)
        compressor.flush(zstd.FLUSH_FRAME)

    return time.time() - t0, out_zst_path.stat().st_size, (line_count if count_rows else None)


def _fast_template_load(args: argparse.Namespace, path: Path, template: dict) -> dict:
    """
    Bilinen bir uçak türü şablonuyla eşleşen dosyalar için hızlı yol:
    pandas'a HİÇ girmeden ham byte akışını zstd'ye sıkıştırıp MinIO'ya
    yükler, tabloyu şablonun (veriye bakmadan, sadece isim deseninden
    çıkarılan) tiplerine göre kurup ClickHouse'un s3() fonksiyonuyla
    TEK SEFERDE yükler -- scripts/pipeline_grid_to_clickhouse.py ile
    AYNI mekanizma (saniyeler mertebesinde, _clean_and_compress_to_csv_
    zst'nin dakikalar sürebilen pandas yoluna karşı).
    """

    t_start = time.time()
    aircraft_type = template["aircraft_type"]
    ordered_columns = template["ordered_columns"]
    column_types = template["column_types"]

    print(
        f"Bilinen şablon eşleşti: {aircraft_type} ({len(ordered_columns)} "
        "sütun) -- pandas atlanıyor, ham byte akışıyla yükleniyor.",
        flush=True,
    )

    local_zst_path = path.with_suffix(path.suffix + ".raw.tsv.zst")
    compress_elapsed, zst_size, _ = _compress_raw_file_to_zst(path, local_zst_path)
    print(
        f"Sıkıştırma tamam: {zst_size / (1024**2):.1f}MB, {compress_elapsed:.1f}sn.",
        flush=True,
    )

    mc = Minio(
        _get_minio_endpoint(),
        access_key=_get_minio_access_key(),
        secret_key=_get_minio_secret_key(),
        secure=_get_minio_secure(),
    )
    bucket = _get_minio_bucket()
    _ensure_bucket(mc, bucket)

    object_key = f"extended_telemetry/{args.table_name}/{path.stem}.raw.tsv.zst"
    t_upload = time.time()
    mc.fput_object(bucket, object_key, str(local_zst_path))
    upload_elapsed = time.time() - t_upload
    print(f"MinIO'ya yüklendi: {bucket}/{object_key} ({upload_elapsed:.1f}sn).", flush=True)

    local_zst_path.unlink(missing_ok=True)

    client = Client(
        host=_get_clickhouse_native_host(),
        port=_get_clickhouse_native_port(),
        user=_get_clickhouse_user(),
        password=_get_clickhouse_password(),
        database=_get_clickhouse_database(),
    )
    database = _get_clickhouse_database()
    table_fqn = f"{database}.{args.table_name}"

    # scripts/pipeline_grid_to_clickhouse.py::load_and_record ile AYNI
    # tip kuralı -- iki pipeline'ın kararları tutarlı kalsın diye.
    col_defs = ["`flight_tag` LowCardinality(String)"]
    for column in ordered_columns:
        if column == "aircraft_type":
            col_defs.append(f"`{column}` LowCardinality(String)")
        elif column == "timestamp" or column_types.get(column) == "Float64":
            col_defs.append(f"`{column}` Float64")
        else:
            col_defs.append(f"`{column}` UInt8")

    # CREATE TABLE IF NOT EXISTS: tablo bir UÇAK TÜRÜ başına TEK SEFER
    # kurulur (table_name genelde bu türe özel, ör. "..._40k") -- aynı
    # türün FARKLI dosyaları (farklı satır sayıları/"uçuşlar") aynı
    # tabloya SATIR olarak eklenir, her biri kendi CREATE TABLE'ını
    # tetiklemez. Bu, 20 ayrı geniş tablo yerine 5 tablo tutarak
    # ClickHouse'un katalog belleği yükünü ~4x azaltır (bkz. proje
    # belleğindeki tartışma, 2026-08-27).
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn}
        (
            {", ".join(col_defs)}
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        SETTINGS {WIDE_PART_GUARD_SETTINGS}
        """,
        settings=CH_SETTINGS,
    )

    protocol = "https" if _get_minio_secure() else "http"
    s3_url = f"{protocol}://{_get_minio_endpoint()}/{bucket}/{object_key}"

    # ÖNEMLİ: Farklı dosyaların (aynı uçak türü olsa bile) sütun SIRASI
    # farklı olabilir (bkz. proje belleği -- gen_synthetic_grid.py'nin
    # ikili sütunları her (n_cols,n_rows) için ayrı karıştırması). Bu
    # yüzden ham "SELECT *" (pozisyonel eşleşme) GÜVENLİ DEĞİL --
    # tablonun (ilk dosyadan kurulmuş) sabit sırasına göre, İSİM
    # bazlı eşleştirme için INSERT'in hedef sütun listesi BU dosyanın
    # KENDİ sırasıyla (ordered_columns) veriliyor; ClickHouse geri
    # kalanı (s3() çıktısının aynı sırada olması) buna göre eşler.
    flight_tag = path.stem
    insert_target_columns = ", ".join(
        f"`{c}`" for c in ["flight_tag", *ordered_columns]
    )

    t_load = time.time()
    client.execute(
        f"""
        INSERT INTO {table_fqn} ({insert_target_columns})
        SELECT '{flight_tag}' AS flight_tag, * FROM s3(
            '{s3_url}', '{_get_minio_access_key()}', '{_get_minio_secret_key()}',
            'TabSeparatedWithNames'
        )
        """,
        settings=CH_SETTINGS,
    )
    load_elapsed = time.time() - t_load

    row_count = client.execute(
        f"SELECT count() FROM {table_fqn} WHERE flight_tag = %(flight_tag)s",
        {"flight_tag": flight_tag},
        settings=CH_SETTINGS,
    )[0][0]

    elapsed_total = time.time() - t_start
    print(
        f"Tamamlandı (hızlı yol): {row_count:,} satır, {len(ordered_columns)} "
        f"sütun, sikistir={compress_elapsed:.1f}sn yukle_minio="
        f"{upload_elapsed:.1f}sn yukle_ch={load_elapsed:.1f}sn "
        f"toplam={elapsed_total:.1f}sn ({table_fqn}).",
        flush=True,
    )

    schema_rows = client.execute(f"DESCRIBE TABLE {table_fqn}", settings=CH_SETTINGS)
    schema = {row[0]: row[1] for row in schema_rows}

    return {
        "source_file": str(path),
        "table": table_fqn,
        "database": database,
        "row_count": row_count,
        "column_count": len(ordered_columns) + 1,  # +flight_tag
        "chunk_count": 1,
        "elapsed_seconds": round(elapsed_total, 1),
        "time_column_source": "timestamp",
        "schema": schema,
        "minio_bucket": bucket,
        "minio_object_key": object_key,
        "compress_duration_seconds": round(compress_elapsed, 1),
        "minio_upload_duration_seconds": round(upload_elapsed, 1),
        "clickhouse_load_duration_seconds": round(load_elapsed, 1),
        "load_method": "known_template_bypass",
        "detected_aircraft_type": aircraft_type,
        "flight_tag": flight_tag,
    }


def _fast_template_load_long_sql(args: argparse.Namespace, path: Path, template: dict) -> dict:
    """
    Uzun (long/tidy) formata SUNUCU TARAFINDA (ClickHouse SQL, ARRAY
    JOIN) çevirir; pandas melt + PyArrow (Python tarafında) yerine.

    NEDEN Python'da değil: Python tarafında dönüştürmek dev ara dosyalar
    (bir dosya için GB'larca yerel .zst) üretip WSL2'nin Linux sayfa
    önbelleğini şişiriyor, host'un bütün belleğini tüketebiliyordu.

    NEDEN ara tablo yok: ilk sürüm dosyayı önce GEÇİCİ bir MergeTree
    tablosuna yükleyip ardından ARRAY JOIN ile pivotluyordu. Bu geçici
    tablo tek başına ciddi belleğe mal oluyor, üstelik INSERT bittikten
    SONRA bile arka plan parça birleştirmesi (background merge) hiçbir
    yeni sorgu çalışmadan belleği artırmaya devam ediyordu. ARRAY JOIN
    artık doğrudan MinIO'daki dosyadan (s3() tablo fonksiyonundan)
    okunuyor -- hiç ara MergeTree tablosu oluşturulmuyor, dolayısıyla
    arka plan birleştirme maliyeti de oluşmuyor. Yüklü uzun formatın
    kendisi zaten çok az bellek kullanıyor -- darboğaz hiçbir zaman
    format değildi, ara adım olarak kullanılan geçici geniş tabloydu.
    """

    t_start = time.time()
    aircraft_type = template["aircraft_type"]
    ordered_columns = template["ordered_columns"]
    column_types = template["column_types"]
    sensor_columns = ordered_columns[2:]
    flight_tag = path.stem

    print(
        f"Bilinen şablon eşleşti: {aircraft_type} ({len(ordered_columns)} "
        "sütun) -- sunucu-taraflı (SQL) uzun format dönüşümü kullanılıyor.",
        flush=True,
    )

    # 2026-08-28: HERHANGİ bir ağır işlem (sıkıştırma dahil) başlamadan
    # önce bellek ön-kontrolü -- bkz. MEMORY_SAFE_FLOOR_GB. Bellek zaten
    # düşükse burada AÇIKÇA durur (Dagster run'ı FAILED olur, host'a
    # dokunulmaz) -- host'un öngörülemez şekilde donmasını önler.
    _ensure_memory_headroom("dosya sıkıştırma/yükleme öncesi")

    # -----------------------------------------------------------------
    # 1) Dosyayı SATIR BAZINDA küçük parçalara bölüp her parçayı AYRI
    #    sıkıştırıp AYRI bir MinIO nesnesi olarak yükle. Sütun bazlı
    #    parçalamada tek bir tam (tüm sütunlu) dosya yükleniyor, pivot
    #    adımı bunu N kez yeniden okuyup açıyordu (N = sütun parçası
    #    sayısı) -- geniş dosyalarda gereksiz, tekrar eden indirme/açma
    #    maliyeti. Şimdi her MinIO nesnesi (satır parçası) pivot'ta TAM
    #    OLARAK BİR KEZ okunuyor.
    # -----------------------------------------------------------------

    row_chunk_size = _compute_row_chunk_size(len(sensor_columns))

    mc = Minio(
        _get_minio_endpoint(),
        access_key=_get_minio_access_key(),
        secret_key=_get_minio_secret_key(),
        secure=_get_minio_secure(),
    )
    bucket = _get_minio_bucket()
    _ensure_bucket(mc, bucket)

    object_prefix = f"extended_telemetry_long/{args.table_name}/{flight_tag}"
    t_split = time.time()
    row_chunks = _split_compress_upload_by_rows(
        path, mc, bucket, object_prefix, row_chunk_size
    )
    split_elapsed = time.time() - t_split
    source_row_count = sum(count for _, count in row_chunks)
    print(
        f"Sıkıştırma+yükleme tamam: {len(row_chunks)} parça, "
        f"parça başına en fazla {row_chunk_size:,} satır, toplam "
        f"{source_row_count:,} veri satırı, {split_elapsed:.1f}sn.",
        flush=True,
    )

    client = Client(
        host=_get_clickhouse_native_host(),
        port=_get_clickhouse_native_port(),
        user=_get_clickhouse_user(),
        password=_get_clickhouse_password(),
        database=_get_clickhouse_database(),
    )
    database = _get_clickhouse_database()
    long_table_fqn = f"{database}.{args.table_name}"

    # s3() tablo fonksiyonuna okuma şemasını AÇIKÇA veriyoruz -- otomatik
    # tip algılamaya güvenmek yerine (ve arada hiç MergeTree tablosu
    # oluşturmadan) doğrudan bu şemayla okunacak.
    structure_defs = []
    for column in ordered_columns:
        if column == "aircraft_type":
            structure_defs.append(f"{column} String")
        elif column == "timestamp" or column_types.get(column) == "Float64":
            structure_defs.append(f"{column} Float64")
        else:
            structure_defs.append(f"{column} UInt8")
    structure_sql = ", ".join(structure_defs)

    # ARTIK TÜM sensör sütunları TEK seferde -- sütun-bazlı parçalamaya
    # gerek yok, çünkü her satır-parçası zaten satır ekseninde küçük
    # (bkz. _compute_row_chunk_size); tüm sütunları aynı anda ARRAY
    # JOIN'lemek bu parça boyutunda güvenli.
    pairs_sql = ", ".join(
        f"('{column}', toFloat64(`{column}`))" for column in sensor_columns
    )
    protocol = "https" if _get_minio_secure() else "http"

    # -----------------------------------------------------------------
    # 2) SUNUCU TARAFINDA (ClickHouse SQL) uzun formata çevir -- HİÇ ara
    #    MergeTree tablosu yok. ARRAY JOIN, doğrudan s3() tablo
    #    fonksiyonundan okurken her satırı (sensör_adı, değer)
    #    çiftlerinden oluşan bir diziye açıp N alt satıra böler. Python'da
    #    hiçbir satır/hücre işlenmiyor, hiç yerel/geçici ara tablo
    #    üretilmiyor -- dolayısıyla arka plan merge maliyeti de oluşmuyor.
    # -----------------------------------------------------------------

    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {long_table_fqn}
        (
            `flight_tag` LowCardinality(String),
            `time` Float64,
            `aircraft_type` LowCardinality(String),
            `sensor_name` LowCardinality(String),
            `value` Nullable(Float64)
        )
        ENGINE = MergeTree
        ORDER BY (aircraft_type, sensor_name, time)
        """,
        settings=CH_SETTINGS,
    )

    # 2026-08-28: OTOMATİK/İDEMPOTENT temizlik -- bir önceki deneme
    # bellek yetersizliği (ya da başka bir sebeple) yarıda kesildiyse,
    # bu flight_tag için KISMİ satırlar tabloda kalmış olabilir. Elle
    # temizlik gerektirmeden her çalıştırma (ilk deneme ya da yeniden
    # deneme fark etmeksizin) temiz bir başlangıç yapsın diye, pivot'a
    # başlamadan önce bu flight_tag'e ait ne varsa silinir.
    existing_rows = client.execute(
        f"SELECT count() FROM {long_table_fqn} WHERE flight_tag = %(flight_tag)s",
        {"flight_tag": flight_tag},
        settings=CH_SETTINGS,
    )[0][0]
    if existing_rows:
        print(
            f"Önceki (muhtemelen yarım kalmış) {existing_rows:,} satır "
            f"bulundu, temizleniyor...",
            flush=True,
        )
        client.execute(
            f"ALTER TABLE {long_table_fqn} DELETE WHERE flight_tag = %(flight_tag)s",
            {"flight_tag": flight_tag},
            settings={**CH_SETTINGS, "mutations_sync": 1},
        )
        print("Temizlik tamam.", flush=True)

    # Tek seferde TÜM veriyi ARRAY JOIN ile açmak (ara tablo olsa da
    # olmasa da) büyük satır x sütun çarpımlarında host'u kritik belleğe
    # düşürebiliyor. Bu yüzden her satır-parçası (yukarıda ayrı ayrı
    # yüklenen MinIO nesneleri) için AYRI bir INSERT çalıştırılır: her
    # sorgu kendi (küçük) nesnesini tam olarak bir kez okur, ARRAY
    # JOIN'in ürettiği satır sayısı (parçadaki satır x tüm sensör sayısı)
    # yaklaşık sabit kalır ve parçalar arasında ClickHouse'a belleğini
    # toparlama fırsatı verilir.
    t_pivot = time.time()
    for chunk_idx, (object_key, chunk_row_count) in enumerate(row_chunks, start=1):
        # Her parçadan önce bellek kontrolü -- bkz. MEMORY_SAFE_FLOOR_GB.
        # Mid-run'da bellek kritiğe düşerse, host'u riske atmak yerine
        # burada AÇIKÇA durur (bir sonraki denemede yukarıdaki idempotent
        # temizlik bu flight_tag'in kısmi satırlarını otomatik siler).
        stage_label = f"pivot parçası {chunk_idx}/{len(row_chunks)}"
        _ensure_memory_headroom(stage_label)
        # ...ve arka plan birleştirmesinin yetişmesi için parça-sayısı
        # kontrolü -- bkz. MAX_ACTIVE_PARTS_BEFORE_CHUNK. Çok büyük
        # dosyalarda (binlerce parça üretebilecek) INSERT hızını
        # ClickHouse'un gerçekten kaldırabildiği hıza sabitler.
        _wait_for_merge_pressure_to_settle(client, long_table_fqn, stage_label)
        # ...ve her CHECKPOINT_INTERVAL_CHUNKS parçada bir gerçek bir
        # soğuma molası -- bkz. CHECKPOINT_TARGET_GB. Sadece 1. parçada
        # DEĞİL (henüz birikecek bir şey yok).
        if chunk_idx > 1 and (chunk_idx - 1) % CHECKPOINT_INTERVAL_CHUNKS == 0:
            _checkpoint_cooldown(client, stage_label)
        chunk_s3_url = f"{protocol}://{_get_minio_endpoint()}/{bucket}/{object_key}"
        t_chunk = time.time()
        # Sorgu ÇALIŞIRKEN de bellek izlenir -- bkz. MEMORY_WATCHDOG_
        # CRITICAL_GB. Başlamadan önceki kontrol (_ensure_memory_headroom)
        # tek başına yetersiz kaldığı için (kanıt: proje belleği,
        # synthetic_20k_50000 5. parçada) eklendi.
        _execute_with_oom_watchdog(
            client,
            f"""
            INSERT INTO {long_table_fqn} (flight_tag, time, aircraft_type, sensor_name, value)
            SELECT '{flight_tag}', `timestamp`, aircraft_type, pair.1, pair.2
            FROM s3(
                '{chunk_s3_url}', '{_get_minio_access_key()}', '{_get_minio_secret_key()}',
                'TabSeparated', '{structure_sql}'
            )
            ARRAY JOIN [{pairs_sql}] AS pair
            """,
            stage_label,
            client_factory=lambda: Client(
                host=_get_clickhouse_native_host(),
                port=_get_clickhouse_native_port(),
                user=_get_clickhouse_user(),
                password=_get_clickhouse_password(),
                database=_get_clickhouse_database(),
            ),
        )
        print(
            f"  parça {chunk_idx}/{len(row_chunks)} tamam "
            f"({chunk_row_count:,} satır, {time.time() - t_chunk:.1f}sn).",
            flush=True,
        )
    pivot_elapsed = time.time() - t_pivot
    print(f"Sunucu-taraflı uzun format dönüşümü tamam (ara tablosuz): {pivot_elapsed:.1f}sn.", flush=True)

    row_count = client.execute(
        f"SELECT count() FROM {long_table_fqn} WHERE flight_tag = %(flight_tag)s",
        {"flight_tag": flight_tag},
        settings=CH_SETTINGS,
    )[0][0]

    elapsed_total = time.time() - t_start
    print(
        f"Tamamlandı (sunucu-taraflı uzun format): {row_count:,} satır "
        f"(kaynak: {len(sensor_columns):,} sensör), "
        f"sikistir_yukle={split_elapsed:.1f}sn pivot={pivot_elapsed:.1f}sn "
        f"toplam={elapsed_total:.1f}sn ({long_table_fqn}).",
        flush=True,
    )

    schema_rows = client.execute(f"DESCRIBE TABLE {long_table_fqn}", settings=CH_SETTINGS)
    schema = {row[0]: row[1] for row in schema_rows}

    return {
        "source_file": str(path),
        "table": long_table_fqn,
        "database": database,
        "row_count": row_count,
        "column_count": 5,
        "chunk_count": len(row_chunks),
        "row_chunk_size": row_chunk_size,
        "source_row_count": source_row_count,
        "elapsed_seconds": round(elapsed_total, 1),
        "time_column_source": "timestamp",
        "schema": schema,
        "minio_bucket": bucket,
        "minio_object_prefix": object_prefix,
        "compress_upload_duration_seconds": round(split_elapsed, 1),
        "clickhouse_load_duration_seconds": round(pivot_elapsed, 1),
        "load_method": "known_template_long_format_sql",
        "detected_aircraft_type": aircraft_type,
        "flight_tag": flight_tag,
        "sensor_count": len(sensor_columns),
    }


def _clean_and_compress_to_csv_zst(
    path: Path,
    sep: str,
    time_column: str,
    column_types: dict,
    ordered_columns: list,
    chunk_rows: int,
    local_zst_path: Path,
) -> tuple[int, int]:
    """
    Kaynak dosyayı --chunk-rows'luk parçalar halinde okuyup temizler
    (zaman/sayısal tip zorlama, sondaki hayalet sütunu atma) ve
    CSVWithNames formatında, zstd ile sıkıştırılmış TEK bir yerel
    dosyaya YAZAR (ClickHouse'a hiç bağlanmadan). Belleğe hiçbir anda
    dosyanın tamamı alınmaz -- yalnızca o anki parça.

    Döner: (toplam_satir, parca_sayisi)
    """

    compressor = zstd.ZstdCompressor(level=ZSTD_LEVEL)
    total_rows = 0
    chunk_index = 0

    # Sütun-sütun (Python döngüsünde tek tek `chunk[col] = ...`) yerine
    # sütun GRUBU bazlı toplu atama: binlerce sütunlu dosyalarda tek tek
    # atama, DataFrame'in her seferinde parçalanmasına (fragmentation --
    # bkz. pandas'ın kendi "highly fragmented" uyarısı) ve ciddi Python
    # döngü overhead'ine yol açıyordu (10.002 sütunlu bir dosyada tek bir
    # 50.000 satırlık dosyanın temizliği DAKİKALAR sürebiliyordu). Aynı
    # tipteki sütunları tek seferde `chunk[cols] = chunk[cols].apply(...)`
    # ile toplu dönüştürüp TEK bir blok ataması yapmak bunu büyük ölçüde
    # azaltıyor.
    float_cols = [c for c, t in column_types.items() if t == "Float64"]
    int_cols = [c for c, t in column_types.items() if t in ("UInt8", "Int64")]

    with open(local_zst_path, "wb") as raw_out, compressor.stream_writer(raw_out) as zf:

        for chunk in pd.read_csv(
            path, sep=sep, chunksize=chunk_rows, low_memory=False
        ):
            chunk.columns = chunk.columns.str.strip()
            chunk = _drop_trailing_empty_columns(chunk)

            chunk["time"] = pd.to_datetime(chunk[time_column], errors="coerce")

            if float_cols:
                chunk[float_cols] = chunk[float_cols].apply(
                    pd.to_numeric, errors="coerce"
                )

            if int_cols:
                # pandas'ın NA destekleyen tam sayı tipi ("Int64", büyük I)
                # kullanılıyor -- düz float64 kullansaydık NaN'lar yüzünden
                # değerler "1.0" gibi ondalıklı yazılırdı; ClickHouse'un
                # UInt8/Int64 CSV ayrıştırıcısı bunu KABUL ETMEZ. "Int64"
                # ile "1", "" (NA) şeklinde temiz yazılıyor.
                chunk[int_cols] = chunk[int_cols].apply(
                    pd.to_numeric, errors="coerce"
                ).astype("Int64")

            chunk = chunk[ordered_columns]

            buffer = io.StringIO()
            chunk.to_csv(
                buffer,
                index=False,
                header=(chunk_index == 0),
                na_rep="",
            )
            zf.write(buffer.getvalue().encode("utf-8"))

            total_rows += len(chunk)
            chunk_index += 1

            print(
                f"  Parça {chunk_index}: {len(chunk):,} satır temizlendi "
                f"(toplam {total_rows:,})",
                flush=True,
            )

    return total_rows, chunk_index


def main() -> None:
    args = parse_args()

    path = args.file_path

    if not path.exists():
        raise FileNotFoundError(f"Telemetri dosyası bulunamadı: {path}")

    t_start = time.time()

    sep = _detect_separator(path)
    print(f"Ayraç algılandı: {sep!r}")

    # Önce ucuz bir kontrol: başlık satırı bilinen bir uçak türü
    # şablonuyla eşleşiyor mu? Eşleşirse pandas'a hiç girmeden hızlı
    # yoldan devam edilir (bkz. KNOWN_AIRCRAFT_COLUMN_COUNTS'ın
    # yanındaki not). Eşleşmezse (bilinmeyen dosya) aşağıdaki mevcut
    # güvenli pandas yoluna sessizce düşülür.
    header_columns = _read_header_columns(path, sep)
    template = _match_known_template(header_columns, sep)

    if template is not None:
        if args.output_format == "long_sql":
            metadata = _fast_template_load_long_sql(args, path, template)
        else:
            metadata = _fast_template_load(args, path, template)
        args.metadata_out.parent.mkdir(parents=True, exist_ok=True)
        args.metadata_out.write_text(
            json.dumps(metadata, ensure_ascii=False),
            encoding="utf-8",
        )
        return

    print(
        "Bilinen şablonla eşleşmedi -- genel amaçlı pandas yoluna "
        "düşülüyor (veriden şema çıkarımı).",
        flush=True,
    )

    time_column, column_types, all_columns = _infer_schema(
        path, sep, args.sample_rows
    )

    if time_column is None:
        raise ValueError(
            f"'{path}' dosyasında zaman sütunu bulunamadı. Aranan "
            f"adaylar: {TIME_COLUMN_CANDIDATES}. Bulunan sütunlar: "
            f"{all_columns}"
        )

    ordered_columns = ["time"] + list(column_types.keys())

    print(
        f"Şema çıkarıldı: {len(ordered_columns)} sütun "
        f"(zaman sütunu kaynağı: '{time_column}')."
    )

    # -----------------------------------------------------------------
    # 1) Temizle + zstd'ye sıkıştırarak yerel bir CSV akışına yaz.
    # -----------------------------------------------------------------

    local_zst_path = path.with_suffix(path.suffix + ".clean.csv.zst")
    total_rows, chunk_count = _clean_and_compress_to_csv_zst(
        path, sep, time_column, column_types, ordered_columns,
        args.chunk_rows, local_zst_path,
    )
    compress_elapsed = time.time() - t_start
    print(
        f"Temizlik+sıkıştırma tamam: {total_rows:,} satır, "
        f"{local_zst_path.stat().st_size / (1024**2):.1f}MB, "
        f"{compress_elapsed:.1f}sn.",
        flush=True,
    )

    # -----------------------------------------------------------------
    # 2) MinIO'ya yükle.
    # -----------------------------------------------------------------

    mc = Minio(
        _get_minio_endpoint(),
        access_key=_get_minio_access_key(),
        secret_key=_get_minio_secret_key(),
        secure=_get_minio_secure(),
    )
    bucket = _get_minio_bucket()
    _ensure_bucket(mc, bucket)

    object_key = f"extended_telemetry/{args.table_name}/{path.stem}.clean.csv.zst"
    t_upload = time.time()
    mc.fput_object(bucket, object_key, str(local_zst_path))
    upload_elapsed = time.time() - t_upload
    print(f"MinIO'ya yüklendi: {bucket}/{object_key} ({upload_elapsed:.1f}sn).", flush=True)

    local_zst_path.unlink(missing_ok=True)

    # -----------------------------------------------------------------
    # 3) ClickHouse: tabloyu oluştur, s3() ile TEK SEFERDE toplu yükle.
    # -----------------------------------------------------------------

    client = Client(
        host=_get_clickhouse_native_host(),
        port=_get_clickhouse_native_port(),
        user=_get_clickhouse_user(),
        password=_get_clickhouse_password(),
        database=_get_clickhouse_database(),
    )

    database = _get_clickhouse_database()
    table_fqn = f"{database}.{args.table_name}"

    # Her sütun Nullable: bu geniş dosyalardaki kanallar seyrek olabiliyor
    # (çoğu satırda boş) -- bkz. NUMERIC_FRACTION_THRESHOLD notu.
    col_defs = ["`time` DateTime64(3)"] + [
        f"`{column}` Nullable({column_type})"
        for column, column_type in column_types.items()
    ]

    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn}
        (
            {", ".join(col_defs)}
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(time)
        ORDER BY time
        SETTINGS {WIDE_PART_GUARD_SETTINGS}
        """,
        settings=CH_SETTINGS,
    )

    protocol = "https" if _get_minio_secure() else "http"
    s3_url = f"{protocol}://{_get_minio_endpoint()}/{bucket}/{object_key}"
    insert_columns_sql = ", ".join(f"`{column}`" for column in ordered_columns)

    t_load = time.time()
    client.execute(
        f"""
        INSERT INTO {table_fqn} ({insert_columns_sql})
        SELECT * FROM s3(
            '{s3_url}', '{_get_minio_access_key()}', '{_get_minio_secret_key()}',
            'CSVWithNames'
        )
        """,
        settings=CH_SETTINGS,
    )
    load_elapsed = time.time() - t_load

    elapsed_total = time.time() - t_start

    print(
        f"Tamamlandı: {total_rows:,} satır, {len(ordered_columns)} "
        f"sütun, sikistir={compress_elapsed:.1f}sn yukle_minio="
        f"{upload_elapsed:.1f}sn yukle_ch={load_elapsed:.1f}sn "
        f"toplam={elapsed_total:.1f}sn ({table_fqn})."
    )

    schema_rows = client.execute(f"DESCRIBE TABLE {table_fqn}", settings=CH_SETTINGS)
    schema = {row[0]: row[1] for row in schema_rows}

    metadata = {
        "source_file": str(path),
        "table": table_fqn,
        "database": database,
        "row_count": total_rows,
        "column_count": len(ordered_columns),
        "chunk_count": chunk_count,
        "elapsed_seconds": round(elapsed_total, 1),
        "time_column_source": time_column,
        "schema": schema,
        # Aşağıdaki alanlar assets.py::extended_telemetry_load tarafından
        # OKUNMUYOR ama conversion_manifest'teki kardeş pipeline ile
        # tutarlı bir denetim izi bırakmak için ekstra olarak yazılıyor.
        "minio_bucket": bucket,
        "minio_object_key": object_key,
        "compress_duration_seconds": round(compress_elapsed, 1),
        "minio_upload_duration_seconds": round(upload_elapsed, 1),
        "clickhouse_load_duration_seconds": round(load_elapsed, 1),
        "load_method": "minio_s3_bulk",
    }

    args.metadata_out.parent.mkdir(parents=True, exist_ok=True)
    args.metadata_out.write_text(
        json.dumps(metadata, ensure_ascii=False),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
