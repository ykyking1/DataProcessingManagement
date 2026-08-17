# DataProcessingManagement

İHA telemetri verisini (`.ham` → `.tab` → `.parquet`) MinIO + ClickHouse +
Postgres pipeline'ına aktaran sistem. Tam mimari, kararlar ve gerekçeleri
için [docs/plan_dokumani.md](docs/plan_dokumani.md) dosyasına bakın.

## Kapsam

Bu repo, dış bir exe'nin ürettiği `.ham → .tab` çıktısından başlayarak
`.tab → .parquet` (zstd, Float64) dönüşümünü ve bunun MinIO/ClickHouse/
Postgres'e dağıtılmasını kapsar. `.ham → .tab` dönüşümü bu reponun dışında.

## Yapı

| Yol | Ne |
|---|---|
| [docs/plan_dokumani.md](docs/plan_dokumani.md) | Mimari, alınan kararlar, açık sorular |
| [docs/postgres_manifest_schema.sql](docs/postgres_manifest_schema.sql) | Metadata katalog şeması |
| [tab-to-parquet/](tab-to-parquet/) | **Üretim implementasyonu** (Rust) — streaming `.tab → .parquet` dönüştürücü |
| [prototypes/tab_to_parquet.py](prototypes/tab_to_parquet.py) | Python prototipi — artık sadece referans/karşılaştırma amaçlı |

## Rust implementasyonunu derlemek

```sh
cd tab-to-parquet
cargo build --release
./target/release/tab_to_parquet --input sample.tab --output sample.parquet
```

Bu makinede admin/UAC onayı olmadan çalışan kurulum: `winget install
Rustlang.Rustup` (MSVC linker yerine) `rustup toolchain install
stable-x86_64-pc-windows-gnu` + `rustup default stable-x86_64-pc-windows-gnu`,
ve C derleyicisi için `winget install BrechtSanders.WinLibs.POSIX.UCRT`
(taşınabilir/zip MinGW-w64 GCC, UAC tetiklemiyor). VS Build Tools (MSVC)
UAC gerektirdiği için bu makinede kurulamadı; GNU toolchain + portable GCC
tam bir alternatif.

Crate sentetik verilerle (25.000 satırdan 10GB'a kadar) uçtan uca test
edildi: satır sayısı ve tüm sütun toplamları, kaynak dosyadan bağımsız
hesaplanan değerlerle tam eşleşti. Gerçek `.tab` örneğiyle doğrulama hâlâ
yapılmadı (bkz. plan Bölüm 5, madde 1).

## CLI parametreleri

```
--input <PATH>              Girdi .tab dosyası
--output <PATH>              Çıktı .parquet dosyası
--chunk-rows <N>              Okuma/flush granülaritesi (varsayılan 10000)
--compression <zstd|snappy|gzip|none>
--max-row-group-rows <N>      Parquet row-group başına azami satır (varsayılan 100000)
```

**Önemli**: Peak belleği kontrol eden `--max-row-group-rows`'tur,
`--chunk-rows` değil -- parquet-rs'nin varsayılan row-group boyutu
(1.048.576 satır) çoğu gerçekçi dosyadan büyük olduğu için, bu ayarlanmazsa
writer tüm dosyayı `close()`'a kadar bellekte tutabilir. Çoklu-worker
paralel çalıştırırken (worker sayısı × per-worker peak bellek) mevcut RAM
bütçesiyle çapraz kontrol edilmeli -- bkz. plan dokümanının "kök neden
bulundu ve düzeltildi" notu (2026-08-14), gerçek ölçümlerle.
