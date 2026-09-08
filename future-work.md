# Gelecek Çalışmalar

## Kullanıcıya dönük semantik veri sürümü

### Problem

Dashboard şu anda yalnızca ClickHouse'taki güncel committed veriyi gösteriyor.
DVC tarafından üretilen içerik hash'i değişmez veri kimliği ve bütünlük kontrolü
için yararlı olsa da kullanıcı açısından anlamlı bir sürüm değildir.

Kullanıcıya gösterilecek veri sürümü GitHub Actions içindeki semantic release
akışının ürettiği `test-data-vX.Y.Z` etiketi olmalıdır. Pipeline kodunun sürümü
ise ayrı olarak `pipeline-vX.Y.Z` şeklinde tutulmalıdır.

### Zamanlama sorunu

Dagster çalışması sırasında gelecekte üretilecek semantic data sürümü henüz
bilinmez. Sürüm ancak aşağıdaki işlemlerden sonra oluşur:

1. Dagster veriyi işler ve ClickHouse'a `dagster_run_id` ile yazar.
2. `dvc add` ve `dvc push` çalışır; DVC hash'i oluşur.
3. Güncellenen `.dvc` pointer'ı Git'e commit edilip push edilir.
4. GitHub Actions semantic release çalıştırır ve `test-data-vX.Y.Z` etiketini
   üretir.

Bu nedenle semantic data sürümü Dagster run başında veya ClickHouse yazımı
sırasında güvenilir biçimde belirlenemez.

### Hedef mimari

GitHub Actions sürümü oluşturduktan sonra bir callback veya reconciliation işi
PostgreSQL kataloğunda şu ilişkiyi kurmalıdır:

```text
data semantic version
  -> Git tag / Git commit
  -> DVC pointer / DVC hash
  -> Dagster run_id
  -> source batch'ler
```

Kimliklerin sorumlulukları ayrı kalmalıdır:

- `data_version`: Kullanıcıya gösterilen veri sürümü (`test-data-vX.Y.Z`).
- `pipeline_version`: Veriyi üreten kodun sürümü (`pipeline-vX.Y.Z`).
- `dvc_hash`: İçeriğin değişmez teknik kimliği; normal kullanıcıdan gizlenir.
- `dagster_run_id`: Çalıştırma, transaction ve audit kimliği.

Semantic sürümü ClickHouse'taki bütün veri satırlarına sonradan yazmak yerine,
küçük bir PostgreSQL katalog/registry tablosunda `dagster_run_id` ile eşlemek
yeterlidir.

### Dashboard davranışı

Dashboard latest-only çalışma biçimini koruyabilir. Güncel veri için en az şu
bilgiler gösterilmelidir:

```text
Veri: test-data-v0.0.4 · Pipeline: pipeline-v0.1.0
```

GitHub Actions henüz veri sürümünü üretmediyse dashboard yanlış bir sürüm
göstermemeli; `Sürüm oluşturuluyor` veya `unreleased` durumu göstermelidir.
DVC hash'i yalnızca teknik detaylar bölümünde yer alabilir.

Offline çalışmada GitHub Actions çalışamayacağı için mevcut run geçici olarak
`unreleased` kalır. Semantic sürüm, repository yeniden GitHub'a gönderildiğinde
oluşturulup katalogla eşleştirilir.

### Yapılacaklar

- PostgreSQL kataloğuna semantic data release eşleme tablosu eklemek.
- GitHub Actions sonrasında çalışan callback/reconciliation mekanizmasını
  oluşturmak.
- Git tag ve commit üzerinden doğru DVC pointer, DVC hash ve Dagster run'ını
  eşlemek.
- Dashboard'da data ve pipeline semantic sürümlerini göstermek.
- Sürüm oluşana kadar `unreleased/pending` durumunu desteklemek.
- Son kullanıcı adı olarak `test-data-v` yerine `data-v` kullanılmasını
  değerlendirmek.

### Kabul kriteri

Dashboard'da görünen her yayımlanmış güncel dataset için kullanıcıya anlamlı bir
semantic data sürümü ve onu üreten pipeline sürümü gösterilebilmeli; teknik
inceleme gerektiğinde bu kayıt DVC hash'ine ve Dagster run'ına kadar izlenebilmelidir.
