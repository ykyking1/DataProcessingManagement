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

## AU-AIR sentetik raw veri üretimi

AU-AIR benzeri 10.000-50.000 sütunlu sentetik `.tab` verisini üretip doğrudan
MinIO raw bucket'ına yüklemek için çalışan Dagster container'ında:

```sh
docker compose exec dagster python /workspace/scripts/auair_generator.py --rows 100000 --cols 10000 --chunk-size 500
```

50.000 sütun hedefinde generator belleğini sınırlamak için daha küçük üretim
chunk'ı kullanılabilir: `--cols 50000 --chunk-size 100`.

Varsayılan hedef `s3://data-raw/auair-tab/inbox/` yoludur. Yüklenen dosyanın
boyutu MinIO üzerinden doğrulanır; doğrulama başarılıysa yerel geçici dosya
silinir. Yerel kopyayı korumak için `--keep-local`, aynı isimli nesneyi
değiştirmek için `--overwrite` kullanılabilir. Tüm seçenekler için `--help`
çalıştırılabilir.

Dagster'ın AU-AIR sensorleri bu yolu otomatik olarak izler ve veriyi şu akıştan
geçirir:

```text
data-raw/auair-tab/inbox
  → data-staged/auair-tab (.tab.zst)
  → Spark preprocess (dinamik kolonlar korunur)
  → Great Expectations validation raporu
  → ClickHouse s3() bulk load (auair_telemetry, compact wide parts)
  → DVC add + MinIO DVC remote push
```

Son iki adım sıralıdır: ClickHouse yazımı başarıyla tamamlanmadan DVC publish
asset'i başlamaz. ClickHouse loader, 10K-50K kolonlu veriyi Yusuf'un yüksek
kolon stratejisiyle yaklaşık 1 milyar hücrelik fiziksel satır parçalarına
ayırır. Parçalar yalnız taşıma amacıyla geçici MinIO objeleri olarak oluşturulur,
ClickHouse tarafından `s3()` ile bulk okunur ve yükleme denemesinin sonunda
silinir; kalıcı MinIO dataset yayını sonraki DVC asset'ine aittir.

İlk validation profili bilinçli olarak basittir: dosyada en az 17 kolon,
beklenen satır/kolon sayısı, zorunlu `flight_id`/`time` ve temel AU-AIR
kolonlarının doluluğu, flight ID biçimi, koordinat/irtifa ve görüntü boyutu
aralıkları kontrol edilir. Profil adı `auair-placeholder-v1`'dir.

## PostgreSQL'den commit mesajı önerisi

Başarıyla tamamlanmış ve DVC'ye publish edilmiş bir Dagster run'ı için
commit mesajı PostgreSQL kataloğundan üretilebilir. Araç Git commit veya push
komutu çalıştırmaz.

Dagster container'ından:

```sh
docker compose exec dagster python /workspace/scripts/get_commit_message.py --run-id <RUN_ID>
```

Başlıksız, otomasyon dostu çıktı için `--raw` kullanılabilir. Python içinden
de `scripts.get_commit_message.get_commit_message(run_id)` fonksiyonu
çağrılabilir.

Pipeline kimliği monorepo içinde path-aware çözülür. Son `pipeline-v*`
tag'inden sonra yalnızca DVC/data dosyaları değiştiyse pipeline sürümü aynı
kalır; pipeline kapsamındaki bir dosya değiştiyse `unreleased-<sha>` kullanılır.
Commit önerisi hem pipeline revision'ını (`Pipeline-Git-SHA`) hem de run
anındaki gerçek repository HEAD'ini (`Repository-Git-SHA`) içerir.

**Önemli**: Peak belleği kontrol eden `--max-row-group-rows`'tur,
`--chunk-rows` değil -- parquet-rs'nin varsayılan row-group boyutu
(1.048.576 satır) çoğu gerçekçi dosyadan büyük olduğu için, bu ayarlanmazsa
writer tüm dosyayı `close()`'a kadar bellekte tutabilir. Çoklu-worker
paralel çalıştırırken (worker sayısı × per-worker peak bellek) mevcut RAM
bütçesiyle çapraz kontrol edilmeli -- bkz. plan dokümanının "kök neden
bulundu ve düzeltildi" notu (2026-08-14), gerçek ölçümlerle.
