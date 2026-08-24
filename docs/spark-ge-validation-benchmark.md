# Native Spark ve GE-on-Spark Validation Karşılaştırması

## Amaç

Bu çalışma, 10 GiB sentetik V1 Parquet verisini önce ortak bir Spark processing
adımından geçirir. Üretilen aynı processed Parquet üzerinde native Spark
kontrolleri ile Great Expectations'ın Spark backend'i karşılaştırılır.

GE bir processing motoru değildir. Bu nedenle processing iki alternatif için de
aynıdır; karşılaştırılan bölüm yalnızca validation katmanıdır.

## Ortam ve veri

- Spark: 4.1.1
- Great Expectations: 1.20.0
- Python: 3.12.10
- Java: 21
- Spark master: `local[8]`
- Driver memory: 4 GiB
- Makine: 28 logical core, yaklaşık 16 GiB RAM
- Girdi: 16 adet `double` feature, 83.886.080 satır, 10,002 GiB

Windows'taki Hadoop local filesystem katmanı, Parquet yazarken POSIX izinleri
için `winutils.exe` çağırır. Üçüncü taraf binary indirmemek için bu benchmark'ta
`tools/spark/WindowsLocalFileSystem.java` kullanılır. Adaptör yalnız Windows
permission ve file-status çağrılarını standart Java filesystem davranışına
çevirir; Spark transformation veya Parquet içeriğini değiştirmez.

## Ortak processing

Processing aşağıdaki işlemleri uygular:

1. Beklenen 16 feature kolonunun varlığını kontrol eder.
2. Feature kolonlarında null bulunan satırları temizler.
3. `feature_mean` kolonunu üretir.
4. `feature_spread` kolonunu üretir.
5. İlk üç feature'dan ağırlıklı `risk_score` hesaplar.
6. Skoru `low`, `medium`, `high` değerlerinden oluşan `risk_band` kolonuna çevirir.
7. Sonucu kaynak V1 dosyasının yanında Spark part dosyalarından oluşan bir
   Parquet çıktı dizinine yazar. Bu testte semantik `partitionBy` uygulanmamıştır.

### Processing sonucu

| Ölçüm | Sonuç |
|---|---:|
| Girdi boyutu | 10,002 GiB |
| Çıktı boyutu | 11,902 GiB |
| Girdi satırı | 83.886.080 |
| Çıktı satırı | 83.886.080 |
| Read + transform + write | 627,49 sn |
| Spark başlangıcı dahil toplam | 640,97 sn |

Satır sayısı korunmuş, dört derived kolon eklenmiştir. Çıktı tek büyük dosya
değil, Spark paralelliğini koruyan part Parquet dosyalarından oluşur.

## Ortak validation kuralları

Her iki yöntemde aynı 16 mantıksal kural çalıştırılır:

- Kolonlar beklenen sırayla eşleşmelidir.
- Satır sayısı 83.886.080 olmalıdır.
- `feature_01`, `feature_08`, `feature_16`, `feature_mean`, `feature_spread` ve
  `risk_score` null olmamalıdır.
- Aynı altı sayısal kolon `[0, 1]` aralığında olmalıdır.
- `risk_band` null olmamalıdır.
- `risk_band` yalnızca `low`, `medium`, `high` değerlerini içermelidir.

Native Spark bu kontrolleri tek aggregation planında birleştirir. GE aynı
kuralları declarative Expectation nesneleri ve standart validation sonucu olarak
çalıştırır. Büyük veride GE'nin varsayılan `persist=True` ayarı 4 GiB heap'e
sığmayıp disk spill ürettiği için karşılaştırmada `persist=False` kullanılmıştır.

## Validation sonuçları

İki sıra çalıştırılmıştır; çünkü ilk validator soğuk dosya I/O maliyetini taşır,
ikinci validator işletim sistemi dosya cache'inden yararlanabilir.

| Koşu | Validator | Sıra | Süre | Spark job | Stage | Sonuç |
|---|---|---:|---:|---:|---:|---|
| Native-first | Native Spark | 1 | 144,24 sn | 2 | 3 | 16/16 geçti |
| Native-first | GE-on-Spark | 2 | 12,51 sn | 64 | 68 | 16/16 geçti |
| GE-first, sıcak tekrar | GE-on-Spark | 1 | 13,02 sn | 64 | 68 | 16/16 geçti |
| GE-first, sıcak tekrar | Native Spark | 2 | 3,59 sn | 2 | 3 | 16/16 geçti |

İlk native süresi framework overhead'i değil, büyük ölçüde cold I/O süresidir.
Sıcak tekrar daha karşılaştırılabilir sonucu verir: native Spark yaklaşık 3,59
saniye, GE-on-Spark yaklaşık 13,02 saniyedir. Bu koşulda GE yaklaşık 3,62 kat
yavaştır. Buna karşılık GE, native yaklaşımın elle üretmesi gereken standart
expectation sonuçlarını ve ayrıntılı JSON raporunu hazır sağlar.

Bu benchmark yalnızca mevcut sentetik veriyle yapılan başarılı validation
koşularını kapsar. Negatif veri veya hata enjeksiyonu testi çalıştırılmamıştır.

## Trade-off

### Native Spark

Artıları:

- Kurallar tek aggregation planında birleştirilebilir.
- Sıcak veride daha düşük süre ve çok daha az Spark job/stage üretir.
- Spark planı, partitioning ve kaynak kullanımı üzerinde tam kontrol sağlar.
- Ek bir validation framework dependency'si gerektirmez.

Eksileri:

- Kural tanımları, hata ayrıntıları ve rapor şeması elle geliştirilmelidir.
- Kural sayısı arttıkça validation kodu ve bakım yükü büyür.
- Data Docs, expectation suite ve standart validation result gibi GE özellikleri
  hazır gelmez.

### GE-on-Spark

Artıları:

- Kurallar okunabilir ve declarative Expectation nesneleridir.
- Standart JSON validation sonucu, başarı istatistikleri ve unexpected count
  bilgileri hazırdır.
- Suite'ler tekrar kullanılabilir ve kalite kuralları ortak bir dil kazanır.
- Raporlama, checkpoint ve orchestration entegrasyonu native çözüme göre daha
  az özel kod gerektirir.

Eksileri:

- Aynı 16 kural için 64 job ve 68 stage üretmiştir.
- Sıcak koşuda native Spark'tan yaklaşık 3,62 kat yavaştır.
- Büyük veride varsayılan persist davranışı bellek baskısı ve disk spill
  oluşturabilir; Spark datasource ayarı bilinçli yapılmalıdır.
- GE ve Spark sürüm uyumluluğu ayrıca yönetilmelidir.

## Karar

Projede otomatik, standart ve denetlenebilir veri kalite raporuna ihtiyaç
duyulduğu için validation katmanında **GE-on-Spark** kullanılmasına karar
verilmiştir. Veri dönüştürme adımları GE tarafından değil, doğrudan native Spark
API'leriyle gerçekleştirilecektir. Böylece hedef mimari şu şekilde ayrılır:

- Processing: native Spark dönüşümleri
- Validation: Spark execution engine üzerinde Great Expectations
- Kalite çıktısı: GE validation result ve ileride Data Docs/checkpoint raporları

GE'nin sıcak koşuda gözlenen yaklaşık 9,4 saniyelik ek süresi; expectation suite,
standart sonuç şeması ve otomatik raporlama kazanımları karşılığında kabul edilen
bir trade-off'tur. Native Spark'ın daha az job/stage üretmesi performans açısından
avantajlıdır; ancak aynı raporlama ve kural yönetimi özellikleri özel kodla
geliştirilmek zorunda kalacaktır.

Bu sonuç lokal ve sentetik bir benchmark'tır; cluster network'ü, object storage,
executor sayısı, gerçek veri dağılımı ve başarısız kayıt oranı sonuçları
değiştirebilir. Production kararı aynı kuralların hedef Spark cluster'ında tekrar
ölçülmesiyle verilmelidir.

## Çalıştırma

```powershell
python scripts/benchmark_spark_processing.py
python scripts/benchmark_spark_validation.py
python scripts/benchmark_spark_validation.py --order ge-first
```

Detaylı runtime sonuçları ignore edilen `reports/benchmarks/` dizinine JSON ve
CSV olarak yazılır.
