# Mimari Kararlar

## 2026-09-01 — ClickHouse workflow commit ve rollback sınırı

- AU-AIR ClickHouse satırları `dagster_run_id` ile fiziksel storage tablosuna
  yazılacak, fakat Dagster workflow'u `SUCCESS` durumuna ulaşana kadar
  dashboard sorgularına açılmayacaktır.
- Görünürlük, geniş compact partlar üzerinde pahalı bir `UPDATE` mutation
  yerine küçük `auair_telemetry_workflow_commits` registry tablosu ve
  `auair_telemetry_committed` view'ı ile yönetilecektir.
- Başarılı run-status sensorü batch/run çiftini commit registry'ye ekleyecek;
  dashboard yalnız committed view'ı okuyacaktır.
- `FAILURE` veya `CANCELED` terminal durumunda yalnız ilgili Dagster run'ına
  ait ClickHouse satırları senkron mutation ile silinecek ve silme sonucu
  doğrulanacaktır.
- MinIO çıktıları, DVC objeleri ve PostgreSQL katalog/materialization
  metadata'sı rollback kapsamına alınmayacaktır. PostgreSQL terminal run
  durumunu audit kaydı olarak koruyacaktır.
- Aynı batch'in daha eski başarılı sürümü, yeni workflow commit edilene kadar
  görünür kalacaktır. Yeni commit sonrası eski fiziksel sürüm asenkron olarak
  temizlenecektir.

## 2026-08-26 — MinIO kalıcı depolaması

- MinIO, `dpm-minio` adlı Docker container'ında çalıştırılacaktır.
- MinIO'nun container içindeki veri yolu `/data` olacaktır.
- Kalıcı depolama için Docker tarafından yönetilen `dpm_minio_data` named volume'u kullanılacaktır.
- Volume, container'a `dpm_minio_data:/data` şeklinde bağlanacaktır.
- Bu `/data` yolu repodaki `data/` klasörü değildir.
- Container durdurulsa, silinse veya yeniden oluşturulsa bile named volume silinmediği sürece MinIO verileri korunacaktır.
- Dagster ve DVC, volume'a doğrudan dosya sistemi üzerinden değil MinIO API'si üzerinden erişecektir.

## 2026-08-27 — PostgreSQL pipeline kataloğu

- PostgreSQL, Dagster loglarını veya sensor cursor'larını kopyalamak için kullanılmayacaktır.
- PostgreSQL'in uygulama şeması `pipeline_catalog` olacaktır.
- `pipeline_job_runs`, her Dagster run'ının durumunu, MX batch kimliğini ve run başında sabitlenen pipeline version/Git tag/Git SHA/container kimliğini tutacaktır.
- Pipeline sürümü monorepo HEAD'inden bağımsız, `releaserc-pipeline.toml` ile aynı pipeline path kapsamına göre çözülecektir. Son pipeline tag'inden sonra yalnızca data pointer'ları değiştiyse tag geçerliliğini korur; pipeline path'i değiştiyse son pipeline commit'i `unreleased-<sha>` olarak kaydedilir.
- `pipeline_git_sha` pipeline bileşeninin revision'ını, `repository_git_sha` ise run anındaki gerçek repository HEAD'ini tutacaktır. Pipeline ve tüm repository dirty durumları ayrı alanlarda kaydedilecektir.
- Git kimliği çözülemezse katalogda `unknown` lineage ile devam edilmeyecek; run açık bir hata ile duracaktır. Dagster image'ı bu nedenle GitPython'a ek olarak sistem `git` executable'ını da içerecektir.
- `pipeline_asset_materializations`, aynı run içinde başarıyla üretilen asset özetlerini ve MinIO, validation ve DVC çıktı metadata'sını tutacaktır.
- Büyük veri dosyaları PostgreSQL'e yazılmayacaktır; MinIO/DVC üzerinde kalacaktır.
- Run ve materialization yazımları `dagster_run_id` tabanlı idempotent upsert ile yapılacaktır.
- Başarılı, başarısız ve iptal edilmiş terminal run durumları Dagster run-status sensor'larıyla PostgreSQL'e aktarılacaktır.
- DVC hash'i yeniden hesaplanmayacak; `dvc add` tarafından oluşturulan küçük `.dvc` pointer dosyasından okunacaktır.
- Commit mesajı üretimi Dagster asset'lerinin sorumluluğu değildir. `scripts/get_commit_message.py`, tam Dagster `run_id` ile yalnızca PostgreSQL kataloğunu okuyan ayrı bir developer aracıdır.
- Araç sadece `SUCCESS` durumundaki ve aktif yayın asset'i (`published_auair_dataset`) materialization kaydı bulunan run'lar için öneri üretir; Git commit veya push komutu çalıştırmaz.
- Dirty pipeline run'ları engellenmez. Önerilen commit mesajında `Pipeline-Git-Dirty: true` olarak açıkça belirtilir.
- Dagster container'ı Windows host repository'sini `/workspace` altında bind mount ettiği için container Git'inde `core.autocrlf=true` kullanılacaktır. Böylece Windows CRLF checkout'u sahte repository/pipeline dirty durumu üretmez; gerçek DVC pointer ve kod değişiklikleri dirty olarak kalmaya devam eder.

## 2026-08-27 — Semantic release bağımlılık sabitlemesi

- `python-semantic-release==10.6.1`, GitPython'ın kaldırılmış `Actor.name_email_regex` alanını kullandığı için GitPython 3.1.60 ile çalışmamaktadır.
- Resmî GitHub Action alt bağımlılığı dışarıdan sabitlemeye izin vermediğinden release workflow'u Python CLI kurulumuna geçirilmiştir.
- Upstream python-semantic-release düzeltmesi yayınlanana kadar `GitPython==3.1.59` açıkça sabitlenecektir; düzeltme yayınlandığında bu geçici pin kaldırılacaktır.
- Pipeline release kapsamı aktif `scripts/**`, MinIO, Docker Compose, PostgreSQL katalog şeması ve release workflow/config dosyalarını içerecektir.

## 2026-08-28 — Dashboard uyumlu flight telemetry veri sözleşmesi

- Aktif E2E dataset kimliği `flightdemo` olacaktır; MX geniş-kolon generator'ı yalnız bağımsız benchmark aracı olarak kalacaktır.
- Kaynak ve işlenmiş TAB verisi dashboard'un beklediği 17 kolonlu AU-AIR uyumlu geniş şemayı kullanacaktır: `time`, konum, irtifa, hız, açı, görüntü/bounding-box, `class` ve `flight_id`.
- Spark preprocessing bu kolonları typed hale getirecek; Great Expectations tam kolon sırası, satır sayısı, null, coğrafi sınır, bounding-box ve class kurallarını doğrulayacaktır.
- Validation başarılı olduğunda veri ClickHouse `default.telemetry` MergeTree tablosuna geniş formatta ve `ZSTD(3)` codec ile yazılacaktır; dashboard sorguları aynı tabloyu doğrudan okuyacaktır.
- ClickHouse retry idempotency'si teknik `source_batch_id` kolonu üzerinden sağlanacak; yalnız yeniden yüklenen batch silinip tekrar yazılacaktır.
- DVC, işlenmiş flight verisini `data/processed/flightdemo.dvc` adlı tek ve kararlı dataset pointer'ı üzerinden versiyonlayacaktır.

## 2026-08-31 — Yüksek kolonlu sentetik AU-AIR aktif veri hattı

- 2026-08-28 tarihli sabit 17 kolonlu `flightdemo` veri hattı aktif mimari
  olmaktan çıkarılmıştır.
- Aktif dataset kimliği `auair`, raw prefix `auair-tab/inbox`, staged prefix
  `auair-tab` olacaktır.
- Generatorün dinamik kolonları Spark ve validation boyunca korunacaktır.
- Validation sonrasında veri önce ClickHouse `auair_telemetry` tablosuna
  yazılacak, ClickHouse asset'i başarıyla tamamlandıktan sonra DVC publish
  başlayacaktır. ClickHouse ve DVC paralel çalışmayacaktır.
- Aktif üretim ölçeği 10.000-50.000 kolondur. ClickHouse yazımı Yusuf'un
  `working_pipeline_yusuf` branchinde doğrulanan wide-grid yaklaşımını kullanır:
  yaklaşık 1 milyar hücrelik satır parçalama, geçici MinIO objelerinden `s3()`
  bulk insert, dar AU-AIR tipleri/codec'leri, compact MergeTree partları,
  merge-pressure throttling ve OOM query watchdog. Geçici ingest objeleri
  ClickHouse denemesinden sonra silinir; asset sırası değişmez.
- DVC, dataset'i `data/processed/auair.dvc` kararlı pointer'ı üzerinden MinIO
  DVC remote'una gönderecektir.
- `pipeline_catalog.pipeline_asset_materializations` kanonik materialization
  geçmişi olarak kalacaktır. Dashboard uyumluluğu için
  `public.asset_metadata_history` gerçek tablosu trigger ile bu katalogdan
  beslenecek ve tablo yokken yazılmış eski materialization kayıtları şema
  migrasyonunda geriye dönük doldurulacaktır.
- Dashboard container'ına hem `POSTGRES_DATABASE` hem de PostgreSQL image
  uyumluluğu için `POSTGRES_DB` verilecektir.
- Dagster hook'larının yazdığı `alerts.json`, hosttaki
  `dagster/data/alerts` klasörü üzerinden Dagster'a yazılabilir ve dashboard'a
  salt okunur bind mount edilecektir; iki servis aynı `ALERT_FILE` yolunu
  kullanacaktır.
