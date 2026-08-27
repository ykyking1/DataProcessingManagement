# Mimari Kararlar

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
- `pipeline_asset_materializations`, aynı run içinde başarıyla üretilen asset özetlerini ve MinIO, validation ve DVC çıktı metadata'sını tutacaktır.
- Büyük veri dosyaları PostgreSQL'e yazılmayacaktır; MinIO/DVC üzerinde kalacaktır.
- Run ve materialization yazımları `dagster_run_id` tabanlı idempotent upsert ile yapılacaktır.
- Başarılı, başarısız ve iptal edilmiş terminal run durumları Dagster run-status sensor'larıyla PostgreSQL'e aktarılacaktır.
- DVC hash'i yeniden hesaplanmayacak; `dvc add` tarafından oluşturulan küçük `.dvc` pointer dosyasından okunacaktır.
- Commit mesajı üretimi Dagster asset'lerinin sorumluluğu değildir. `scripts_new/get_commit_message.py`, tam Dagster `run_id` ile yalnızca PostgreSQL kataloğunu okuyan ayrı bir developer aracıdır.
- Araç sadece `SUCCESS` durumundaki ve `published_mx_dataset` materialization kaydı bulunan run'lar için öneri üretir; Git commit veya push komutu çalıştırmaz.
- Dirty pipeline run'ları engellenmez. Önerilen commit mesajında `Pipeline-Git-Dirty: true` olarak açıkça belirtilir.

## 2026-08-27 — Semantic release bağımlılık sabitlemesi

- `python-semantic-release==10.6.1`, GitPython'ın kaldırılmış `Actor.name_email_regex` alanını kullandığı için GitPython 3.1.60 ile çalışmamaktadır.
- Resmî GitHub Action alt bağımlılığı dışarıdan sabitlemeye izin vermediğinden release workflow'u Python CLI kurulumuna geçirilmiştir.
- Upstream python-semantic-release düzeltmesi yayınlanana kadar `GitPython==3.1.59` açıkça sabitlenecektir; düzeltme yayınlandığında bu geçici pin kaldırılacaktır.
- Pipeline release kapsamı aktif `scripts_new/**`, MinIO, Docker Compose, PostgreSQL katalog şeması ve release workflow/config dosyalarını içerecektir.
