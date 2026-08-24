# İHA Telemetri Veri İşleme Pipeline'ı — Plan Dokümanı

**Amaç**: 1.5 milyon `.ham` (İHA telemetri, ~500MB ort., saniyeden kısa periyot,
yüzlerce sütun) dosyasını ClickHouse'da sorgulanabilir hale getirmek.

---

## 1. Mimari Akış

```
[Dış exe: ham → tab]  (Windows, CLI, dosya tabanlı, erişimimiz/değiştirme
                        yetkimiz yok — sabit arayüz olarak kabul ediyoruz)
        |
        v  (.tab dosyaları — GEÇİCİ, kalıcı depolanmayacak)
        |
[BİZİM KAPSAMIMIZ: tab → parquet dönüştürücü]
   - Float64 sütunlar (String DEĞİL — bkz. Bölüm 3)
   - zstd sıkıştırma
   - Satır bazlı, streaming/chunked işleme (dosya asla tam RAM'e alınmaz)
        |
        v
   ┌─────┴─────┬───────────────┐
   v         v              v
 MinIO   ClickHouse      Postgres
(parquet (sorgu       (metadata katalog:
 arşivi)  katmanı)     satır sayısı, checksum,
                        durum, MinIO konumu)
```

**Altyapı**: Docker Compose (tek host — bkz. Bölüm 5, açık risk).

---

## 2. Kapsam Netleştirmeleri

- `.ham → .tab` dönüşümü **bizim sorumluluğumuzda değil** — mevcut bir exe
  bunu yapıyor, değiştirme yetkimiz yok.
- Exe sadece dosyaya yazabiliyor (stdout/pipe desteği yok) — `.tab` fiziksel
  olarak diske düşecek.
- Exe'nin çalıştırılması gerçek zamanlı orkestre edilmeyecek — `.tab`
  dosyalarının **önceden üretilmiş** olduğunu varsayıyoruz (dosya keşfi /
  "landing zone izleme" modeliyle ilerleniyor, exe'yi tetiklemiyoruz).
- `.tab` dosyaları **kalıcı olarak saklanmayacak** — sadece parquet'e
  dönüşene kadar var olan geçici bir ara katman.
- Bizim asıl sorumluluğumuz: **`.tab` → `.parquet` (zstd) dönüşümü** ve bu
  çıktının MinIO + ClickHouse + Postgres'e dağıtılması.

---

## 3. Alınan Teknik Kararlar (gerekçeli)

### 3.1 Sayı tipi: Float64, String değil
- Test: gerçek ClickHouse motorunda (MergeTree) String sütun, Float64'ten
  **2.2–2.5x daha büyük** çıktı.
- Daha kritik: String sütunda `ORDER BY` / `WHERE v > X` gibi sorgular
  **leksikografik karşılaştırma** yaptığı için **sessizce yanlış sonuç**
  veriyor (test: en küçük değerler yerine "1" ile başlayan değerler üste
  çıktı). Bu davranış boyuttan bağımsız, tek başına String'i eleyen neden.
- String'in doğru olduğu **tek** yer: `.tab` (insan-okunur ara format) —
  orada da round-trip garantili formatlama (`repr()`, sabit ondalık basamak
  DEĞİL) kullanılmalı.

### 3.2 Float32 vs Float64
- Kaynağın (`.ham`) gerçek genişliğini (float32/float64) formatı çözen
  kişiden teyit etmek gerekiyor — özellikle GPS enlem/boylam gibi hassasiyet
  kritik alanlarda.
- Test: bir alan gerçekten float32 hassasiyetindeyse, float64'e
  yükseltmenin zstd ile sıkıştırılmış disk maliyeti **neredeyse sıfıra
  yakın** (bazen daha küçük bile çıktı — üst mantissa bitleri öngörülebilir
  sıfır deseni oluşturuyor, çok iyi sıkışıyor).
- Ama gerçekten yüksek hassasiyet gerektiren alanlarda (GPS gibi) bu
  "bedava sıkışma" yok — gerçek entropi var, maliyet gerçek.
- **Şimdilik varsayım**: format tam netleşene kadar her şeyi Float64
  yapmak güvenli bir varsayılan (yanlışlıkla kritik bir alanı float32'de
  bırakma riskini ortadan kaldırıyor). İleride, hangi alanların gerçekten
  float32 yeterli olduğu netleşince, sorgu-anı bellek/CPU tasarrufu için
  daraltılabilir.

### 3.3 Streaming / chunked mimari
- Hiçbir dosya (ne `.ham` ne `.tab`) tam olarak RAM'e alınmıyor — sabit
  boyutlu satır grupları (chunk) halinde okunup işleniyor.
- Kesim **satır bazlı**, sütun bazlı değil (bir satırın tüm sütunları
  birlikte taşınıyor). Parquet'in kendi iç columnar depolaması ayrı,
  otomatik bir katman — bizim iş dağıtım kararımız değil.
- Chunk boyutu belirleme yöntemi: satır byte boyutu × hedef chunk MB'ı →
  satır sayısı; RAM bütçesiyle çapraz kontrol; sonra ölçerek ince ayar.
  Tek "doğru" sayı yok.
- Worker'lar arası **paylaşılan kuyruk** (pull-based) — dosyalar önceden
  worker'lara dağıtılmıyor, bu sayede çok kısa/çok uzun uçuş karışımı
  otomatik dengeleniyor.
- Aşamalar arası **sınırlı kanal (bounded channel)** ile backpressure —
  bellek kullanımı öngörülebilir kalıyor, bir aşama yavaşsa öncekiler
  otomatik yavaşlıyor (birikme/OOM riski yok).
- Farklı aşamalar (parse/encode = CPU-bound, ClickHouse yükleme = I/O ve
  merge-kapasitesi sınırlı) **ayrı worker havuzlarında**, farklı
  eşzamanlılık seviyelerinde çalışmalı.

### 3.4 ClickHouse'a yükleme
- **1.5M dosya için 1.5M ayrı INSERT atılmamalı** — ClickHouse'un
  background merge süreci yetişemez, "too many parts" hatası riski var.
- Çözüm: ya uygulama tarafında birden fazla dosyayı biriktirip az sayıda
  büyük INSERT yapmak, ya da ClickHouse'un kendi `S3()`/`file()` table
  fonksiyonlarıyla birden fazla parquet dosyasını tek sorguda toplu
  yüklemek (muhtemelen daha basit).
- Partition key kararı **henüz verilmedi** (tarih mi, uçuş ID mi, ikisi
  birden mi) — bu hem part sayısı riskini hem sorgu performansını etkiliyor.

### 3.5 Veri kaybı doğrulama stratejisi
- **Byte-level checksum farklı formatlar arası kullanılamaz** (binary →
  text → binary geçişlerinde byte'lar zaten değişiyor).
- Bunun yerine **içerik bazlı** doğrulama: satır sayısı + sütun bazlı
  istatistikler (sum/min/max), kaynak ve çıktıdan bağımsız hesaplanıp
  toleranslı karşılaştırılıyor.
- Test edildi ve kanıtlandı: hem kaba hata (satır eksikliği) hem sinsi hata
  (15 milyon değer içinde tek bir hücrenin değişmesi) bu yöntemle
  yakalanıyor.
- SHA-256 byte checksum'ı ayrı bir amaç için saklanıyor: **bit rot / uzun
  vadeli depolama bozulması** tespiti (dönüşüm doğruluğu değil).
- Üç katman arası mutabakat: `row_count_tab == row_count_parquet ==
  row_count_clickhouse` — periyodik bir reconciliation job ile kontrol
  edilmeli.
- Hata durumunda: sınırlı sayıda otomatik retry (örn. 3), sonra "manuel
  inceleme" kuyruğuna düşürme — sonsuz otomatik retry veya otomatik silme
  YOK.

### 3.6 `.tab` satır formatı — netleşen detay
- Her satırın sonunda **fazladan bir tab karakteri** var (yani satır
  `değer1\tdeğer2\t...\tdeğerN\t\n` şeklinde bitiyor, son alan boş).
- Parse mantığı buna göre: satırı `\t` ile bölmeden önce hem `\n` hem
  sondaki fazladan `\t` temizlenmeli — örn.
  `line.rstrip("\n").rstrip("\t").split("\t")`. Bu temizlik yapılmazsa,
  split sonucunda beklenen sütun sayısından bir fazla (boş) eleman çıkar
  ve sütun hizalaması kayar.
- Bu, konuştuğumuz "içerik bazlı doğrulama" (Bölüm 3.5) ile de örtüşüyor:
  parser bu detayı yanlış ele alırsa, satır sayısı doğru çıkabilir ama
  sütun hizalaması kayacağı için istatistik karşılaştırması bunu yakalar
  — yine de doğru parse etmek, hataya güvenmekten daha iyi.

### 3.7 Postgres metadata kataloğu
- Taslak şema hazır (`postgres_manifest_schema.sql`) — dosya durumu, üç
  katmandaki satır sayıları, içerik parmak izi, MinIO konumu, hata detayı.
- Worker'ların işi almasında `SELECT ... FOR UPDATE SKIP LOCKED` deseni
  öneriliyor (Postgres seviyesinde güvenli, çakışmasız iş dağıtımı).

---

## 4. Üretilen Dosyalar

| Dosya | Amaç | Durum |
|---|---|---|
| `prototypes/generate_synthetic_ham.py` | Sentetik `.ham` üretici | Artık kapsam dışı (ham→tab bizim işimiz değil), ama format mantığını anlamak için referans |
| `prototypes/ham_to_tab_converter.py` | Streaming ham→tab, round-trip hassasiyetli | Artık kapsam dışı, ama formatlama dersleri (round-trip precision) geçerli |
| `prototypes/verify_conversion.py` | İçerik bazlı doğrulama (satır sayısı + sütun istatistiği) | Mantık geçerli, tab→parquet doğrulamasına uyarlanabilir |
| `prototypes/tab_to_parquet.py` | Python prototipi: streaming tab→parquet, Float64+zstd | Test edildi, ham→tab→parquet zinciri uçtan uca doğrulandı (50.000 satır, 300 sütun, satır sayısı ve sütun toplamları tam eşleşti). **Üretim için `tab-to-parquet/` (Rust) ile değiştirildi — bkz. not aşağıda.** |
| `tab-to-parquet/` | **Üretim implementasyonu**: Rust, streaming tab→parquet, Float64+zstd | Python prototipinin birebir çevirisi, henüz gerçek `.tab` verisiyle doğrulanmadı |
| `postgres_manifest_schema.sql` | Metadata katalog şeması | Taslak, kullanılmaya hazır |

**Not (2026-08-14 güncelleme)**: Üretim ölçeğinde (256 çekirdek, PB'lerce veri)
performans-kritik parse/encode kısmının GC'siz bir dilde yazılması kararı
netleşti — **Rust** seçildi (bkz. `tab-to-parquet/`). Python script'leri artık
sadece referans/prototip niteliğinde tutuluyor. Bu karar yine de kesin
değil ("değişebilir" notu düşüldü) — ölçüm ve gerçek veriyle yeniden
değerlendirilebilir.

**Not (2026-08-14, ilk ölçek testi)**: `tab-to-parquet` (Rust) sentetik
(rastgele, gerçek telemetri değil) ~10GB'lık bir `.tab` dosyasıyla (300
sütun, 3.200.000 satır, `tools/generate_test_tab.py` ile üretildi --
üretici Rust'ta da yazıldı ama bu makinedeki kurumsal EDR/Trellix taze
derlenmiş+hızlı-yazan bir exe'yi engelledi, bkz. aşağıdaki not) uçtan uca
test edildi:
- **~26-30bin satır/sn**, toplam ~2 dakikada tamamlandı (i7-12700, laptop).
- Bellek sabit/sınırlı kaldı (10GB'lık dosya asla RAM'e alınmadı) --
  gözlemlenen tepe ~2.6GB, parquet writer'ın row-group tamponlamasından
  kaynaklanan testere-dişi (sawtooth) bir profil izliyor, dosya boyutuyla
  birlikte büyümüyor.
- Satır sayısı ve sütun toplamları, kaynaktan bağımsız hesaplanan
  değerlerle tam eşleşti (doğrulama scripti ile).
- Sıkıştırma oranı bu testte sadece **~1.41x** çıktı -- ama bu **rastgele/
  korelasyonsuz sentetik veri zstd için en kötü senaryo**, gerçek telemetri
  (yavaş değişen, ardışık örnekler arası yüksek korelasyon) çok daha iyi
  sıkışması beklenir; bu sayı üretim tahmini için kullanılmamalı.
- **Kurumsal EDR notu**: Bu geliştirme makinesinde Trellix Endpoint
  Security (HX), taze derlenmiş + hızlı/çok veri yazan native bir exe'yi
  (`generate_test_tab.exe`, test verisi üreticisi) şüpheli bulup birkaç
  saniye içinde sildi -- admin/UAC olmadığı için istisna eklenemedi, test
  verisi üretimi Python/pandas'a kaydırıldı. Ama asıl dönüştürücü
  (`tab_to_parquet.exe`, ~10GB girdi okuyup ~7.25GB çıktı yazdı) engellenmedi
  -- yani sorun genel olarak "taze Rust exe" değil, muhtemelen üretim
  dönüştürücüsünün yazma paterni (chunk'lar halinde, parquet encode/sıkıştırma
  arayla) ham/hızlı toplu yazmadan daha az "şüpheli" görünüyor. Yine de
  üretim ortamı zaten Docker/Linux hedeflediği için (bkz. Bölüm 1) bu
  büyük olasılıkla sadece yerel Windows geliştirmeye özgü bir kısıt.

**Not (2026-08-14, çoklu-worker paralellik testi -- ÖNEMLİ, beklenmedik
sonuç)**: Aynı ~10GB'lık test verisi 12 eşit parçaya bölünüp
(`tools/split_tab.py`), 12 `tab_to_parquet` süreci **aynı anda** (12
çekirdekli i7-12700'de) çalıştırıldı:
- Tek worker (tüm dosya, tek süreç): ~134.5 sn, ~23.800 satır/sn.
- 12 paralel worker (aynı toplam veri, 12 parçaya bölünmüş): **110 sn**
  duvar saati, ~29.100 satır/sn toplam.
- **Hızlanma: sadece ~1.2x -- 12x işçiye karşılık ~%10 verimlilik.**
  Beklenen (doğrusal ölçekleme) 12x olurdu. Tüm 12 çıktının satır sayısı
  toplamı doğrulandı (3.200.000, tam eşleşti) -- doğruluk sorunu yok,
  sadece **ölçekleme** sorunu.
- Kesin kök neden teşhis edilemedi (admin/performans sayaçlarına tam
  erişim yok) ama üç aday var: (1) Trellix EDR'ın eşzamanlı dosya
  taramayı sıraya alması (yukarıdaki nottaki Trellix bulgusuyla tutarlı),
  (2) laptop'ın sürdürülebilir all-core termal/güç sınırlaması, (3) tek
  NVMe disk üzerinde I/O çakışması (daha az olası, disk NVMe -- WD SN740).
- **Sonuç**: Bu makinede/ortamda ölçülen "tek dosya throughput'u"ndan
  "N worker ile Nx hızlanma" varsayımı yapılamaz. Bölüm 5, madde 7'deki
  "donanım büyüklüğü gerçek ortamda ölçülmeden belirlenmemeli" uyarısını
  doğrudan doğruluyor -- gerçek üretim ortamında (Docker/Linux, Trellix
  yok) bu test tekrarlanmalı, worker sayısı/donanım kararları o ölçüme
  göre verilmeli.

**Not (2026-08-14, kök neden analizi -- ÖNEMLİ)**: Yukarıdaki ~1.2x
hızlanma anomalisinin nedenini izole etmek için iki bağımsız
mikro-benchmark çalıştırıldı (`tools/cpu_bench.py`, `tools/io_bench.py`):

1. **Saf CPU-bound test** (disk I/O yok, sadece aritmetik döngü): tek
   worker 62.5sn, 12 paralel worker'ın her biri ~110-133sn (ort. ~119sn)
   sürdü -- yani tüm çekirdekler dolunca **her çekirdek tek başınayken
   olduğundan ~1.9x yavaşlıyor** (muhtemelen termal/güç sınırlaması,
   12. nesil Intel'in sürdürülebilir all-core saat hızı düşüşü). Buna
   rağmen toplam agregat throughput **~5.6x** hızlandı -- yani CPU
   tarafı kısmen ölçekleniyor.
2. **Saf disk-yazma testi** (CPU işi yok, sadece büyük buffer'ları arka
   arkaya yazma): tek worker **1143 MB/s**, ama 12 paralel worker'ın
   *her biri* sadece **~12.7 MB/s**'e düştü -- toplam agregat **~148
   MB/s**, yani **tek worker'ın tek başına yaptığının ~%13'ü**. 12
   worker birlikte, 1 worker'dan daha az iş çıkarıyor.

**Sonuç**: Gerçek dünya `tab_to_parquet` testindeki zayıf ölçekleme
(~1.2x) esas olarak **disk I/O'nun eşzamanlı süreçlerde boğulmasından**
kaynaklanıyor (CPU throttling da katkıda bulunuyor ama disk etkisi çok
daha büyük). 12 worker'ın hepsinin ~12.7 MB/s'e (agregat ~150 MB/s sabit
bir tavana) sıkışması rastgele değil -- sabit bir havuzun adil
paylaşıldığını düşündürüyor; bu, eşzamanlı/toplu yazma davranışını
kısıtlayan bir mekanizmanın (muhtemelen Trellix'in çekirdek-modu filtre
sürücüsü -- `xagt` kullanıcı-modu sürecinin CPU'su her iki testte de
sıfır kaldı, ama bu kernel-mode taramayı dışlamaz) imzası, ve daha önce
`generate_test_tab.exe`'yi karantinaya alan "toplu/hızlı yazma şüpheli"
davranış kuralıyla aynı aile. **Kesin/nihai teşhis admin erişimi
olmadan yapılamadı** -- ama pratik sonuç aynı: bu makinede disk I/O'yu
çoklu-sürece bölmek fayda değil zarar veriyor, gerçek üretim ortamında
(Docker/Linux, Trellix yok) bu davranış tekrar test edilmeli.

**Not (2026-08-14, Docker/WSL2'de tekrar test -- İKİ ÖNEMLİ SONUÇ)**: Bu
makinede zaten kurulu olan Docker Desktop (WSL2 backend) içinde aynı testler
tekrarlandı -- hem üretim hedefine (Docker/Linux) çok daha yakın hem de
Trellix'in NTFS filtresini bypass ediyor (container'ın kendi iç
diski/volume'u kullanıldı, Windows bind-mount değil).

1. **Disk I/O çöküşü Docker'da yok -- Windows/Trellix'e özgüymüş, teyit
   edildi.** Saf disk-yazma testi: tek worker 1.2GB/s, 12 paralel worker
   *her biri* ~142-145 MB/s (agregat **~1755 MB/s**, tek worker'ın
   **~1.46x**'i) -- native Windows'taki agregat ~148 MB/s'lik çöküşün
   *tam tersi*. Aynı fiziksel disk, aynı makine -- tek fark container'ın
   Windows NTFS/Trellix katmanının dışında kendi diskini kullanması.
   Ayrıca tek-worker gerçek dönüşüm bile Docker'da native Windows'tan
   **~1.7x daha hızlı** çıktı (78sn vs 134.5sn, 3.2M satır) -- muhtemelen
   Trellix'in tek akışta bile eklediği tarama ek yükünden kurtulmak.

2. **AMA yeni bir risk ortaya çıktı: 12 paralel worker'da OOM (bellek
   yetersizliği) ile SESSİZ veri kaybı.** 12 paralel `tab_to_parquet`
   Docker'da çalıştırıldığında bir worker (part_01) veri okumayı bitirip
   son chunk'ı yazdıktan hemen sonra kesildi -- parquet dosyası sadece
   5.36MB (beklenen ~657.7MB yerine), "Tamamlandı" satırı hiç
   yazdırılmadı, **ama genel komut yine de exit code 0 (başarılı)
   döndü** -- hata sessizce yutuldu. Toplam satır sayısı 2.933.333 çıktı
   (beklenen 3.200.000'den tam 266.667 -- kaybolan worker'ın satır
   sayısı kadar -- eksik). Kök neden: bu makinede 15.7GB fiziksel RAM
   var ama Docker Desktop'ın WSL2 sanal makinesi (`.wslconfig`
   ayarlanmamış, varsayılan) sadece **~7.6GB** (host'un ~yarısı)
   kullanıyor; native Windows testinde tek bir worker'ın row-group
   tamponlarken ~2.6GB'a kadar çıktığı gözlemlenmişti (bkz. yukarıdaki
   ilk 10GB testi notu) -- 12 worker × ~2-2.6GB tepe potansiyel olarak
   24-31GB eder, 7.6GB sınırını fazlasıyla aşıyor, Linux OOM killer
   devreye giriyor.
   - **Bu, Postgres manifest / üç-yönlü mutabakat tasarımının (Bölüm
     3.5, 3.7) neden gerekli olduğunun somut kanıtı**: satır sayısı
     karşılaştırması olmasaydı bu sessiz veri kaybı fark edilmezdi.
   - **Aksiyon önerisi**: worker sayısı × per-worker bellek tepe değeri,
     mevcut RAM bütçesiyle çapraz kontrol edilmeli (plan Bölüm 3.3'te
     zaten önerilen ama şimdi somut kanıtla desteklenen bir kural).
     Pratik çözümler: (a) `--chunk-rows` düşürülerek per-worker tepe
     bellek küçültülebilir, (b) worker sayısı RAM bütçesine göre
     sınırlanabilir, (c) Docker/WSL2 kullanılacaksa `.wslconfig` ile VM
     belleği host'a yakın bir değere çıkarılabilir (admin gerektirir).
     Hiçbiri test edilmedi, sıradaki adım olabilir.

**Genel sonuç**: Bu makinede gerçekçi bir paralellik testi için Docker,
native Windows'tan kesinlikle daha iyi bir ortam (disk I/O sorunu yok,
tek-worker bile daha hızlı) -- ama worker sayısını RAM bütçesiyle
çapraz kontrol etmeden büyütmek, ortamdan bağımsız olarak (Windows'ta da
Linux'ta da) gerçek bir risk. Üretim ortamında (gerçek sunucu, muhtemelen
çok daha fazla RAM) bu tepe noktası farklı olacaktır -- kesin worker
sayısı/chunk boyutu kararı yine gerçek donanımla ölçülmeli.

**Not (2026-08-14, WSL2 belleğini arttırma denemesi -- sonuç: çözmedi,
farklı bir soruna dönüştürdü)**: `.wslconfig` ile WSL2 VM belleği ~7.6GB'dan
**12GB**'a çıkarıldı (`memory=12GB`, admin gerekmedi -- kullanıcı profilinde
dosya + `wsl --shutdown`). 12-paralel test tekrarlandı:
- OOM çökmesi düzeldi -- tüm 12 parça tamamlandı, satır sayısı tam
  doğru (3.200.000).
- AMA süre **148sn'den 363sn'ye çıktı** -- yani 12 worker artık
  tek-worker'ın (78sn) **~4.7x yavaşı**. Çalışma boyunca izlenen bellek
  kullanımı sürekli ~11.8/11.96GB'da (yani hâlâ tavana çok yakın) --
  muhtemelen swap'a düşüp ciddi şekilde yavaşladı.
- **Sonuç**: bu makinede (16GB fiziksel RAM) 12 worker × mevcut
  `--chunk-rows 50000` kombinasyonu, WSL2'ye ne kadar bellek verilirse
  verilsin sağlıklı çalışmıyor -- bellek limitini büyütmek çökmeyi
  geçici olarak bastırıyor ama asıl darboğazı (toplam bellek talebinin
  mevcut RAM'e göre fazla olması) çözmüyor, sadece "sessiz veri kaybı"
  riskini "ciddi yavaşlama" riskine çeviriyor. Asıl çözüm muhtemelen
  **per-worker bellek ayak izini küçültmek** (`--chunk-rows` düşürmek)
  ve/veya **worker sayısını gerçek RAM bütçesine göre sınırlamak** --
  ikisi de henüz test edilmedi, sıradaki mantıklı adım bu.

**Not (2026-08-14, kök neden bulundu ve DÜZELTİLDİ -- `--max-row-group-rows`)**:
Kullanıcının "chunk boyutunu azaltmak fayda sağlar" hipotezi test edildi --
**ampirik olarak yanlış çıktı**: `--chunk-rows`'u 50000'den 5000'e, 1000'e
düşürmenin peak belleğe ölçülebilir bir faydası yoktu (hepsi ~1-1.3GB
aralığında, fark gürültü düzeyinde). Kod incelenince neden bulundu:
parquet-rs'nin varsayılan row-group boyutu **1.048.576 satır** --
bizim test dosyalarımız (~266.667 satır/parça) bunun altında kaldığı için
`ArrowWriter`, `--chunk-rows` ile kaç kere `write()` çağrılırsa çağrılsın,
**tüm dosyayı `close()`'a kadar tek row-group olarak bellekte tutuyordu**.
`--chunk-rows` sadece okuma/flush granülaritesini kontrolü ediyor, peak
belleği DEĞİL.

**Düzeltme**: `tab-to-parquet`'e yeni bir `--max-row-group-rows` parametresi
eklendi (`WriterProperties::set_max_row_group_size()`), varsayılan 100.000.
Doğrulama (tek parça, 266.667 satır, peak RSS):
- `--max-row-group-rows 100000`: 869MB
- `--max-row-group-rows 20000`: 293MB
- `--max-row-group-rows 5000`: 138MB

12-paralel test bu düzeltmeyle tekrarlandı (12GB WSL2, ama artık gerek yok):
- `--max-row-group-rows 20000`: 196sn, doğru (3.2M satır), peak ~6GB --
  ama tek worker'ın (78sn) ~2.5x yavaşı (küçük row-group = daha sık
  encode/sıkıştırma çağrısı = CPU ek yükü, bir trade-off var).
- **`--max-row-group-rows 50000`: 108sn, doğru (3.2M satır), peak ~8.6GB
  -- tek worker'ın sadece ~1.4x yavaşı.** En iyi denge noktası burada
  bulundu (bu spesifik veri şekli/donanım için -- gerçek `.ham` verisiyle
  yeniden ayarlanmalı).

**Sonuç**: Bellek limitini büyütmek (`.wslconfig`) yanlış çözümdü --
asıl sorun kod düzeyindeydi ve düzeltildi. Şimdi 12 worker, WSL2'nin
varsayılan (~7.6GB) belleğine bile muhtemelen sığar (8.6GB biraz üstünde
kalıyor ama `--max-row-group-rows` ile daha da ince ayarlanabilir).
**Üretim/gerçek veri ile mutlaka `--max-row-group-rows` sütun
sayısı/RAM bütçesine göre yeniden ölçülmeli** -- burada bulunan 50000
değeri bu sentetik 300-sütunlu veriye özgü, evrensel bir sabit değil.

**Not (2026-08-14, ölçekleme eğrisi -- "12 worker neden 1 worker'dan
yavaş?" sorusuna cevap)**: N=1,2,4,6,8,12 worker için (hepsi aynı
`--max-row-group-rows 50000` ile, Docker'da) tam ölçekleme eğrisi
çıkarıldı:

| N | Süre | Agregat throughput | Hızlanma | Verimlilik |
|---|---|---|---|---|
| 1 | 21.8sn | 12.232 satır/sn | 1.00x | %100 |
| 2 | 30.6sn | 17.430 satır/sn | 1.42x | %71 |
| 4 | 47.8sn | 22.317 satır/sn | 1.82x | %46 |
| 6 | 59.7sn | 26.801 satır/sn | 2.19x | %37 |
| 8 | 74.3sn | 28.713 satır/sn | 2.35x | %29 |
| 12 | 108sn | 29.630 satır/sn | 2.42x | %20 |

Verimlilik **ani bir eşikte çökmüyor, sürekli ve düzgün azalıyor** --
CPU throttling'in kademeli doğasıyla tutarlı (bkz. yukarıdaki saf
CPU-bound test notu). Asıl kazanç N=1→6 arasında (1.0x→2.19x); N=6→12
arası neredeyse boşuna (worker sayısı 2 katına çıkıyor, hızlanma sadece
2.19x→2.42x, yani %100 fazla kaynak için %10 ek kazanç).

**Sonuç**: Bu makinede (i7-12700 laptop, throttling'e eğilimli) 12
worker (tüm mantıksal çekirdek) kullanmak israf -- **~6 worker civarı
gözlemlenen tatlı nokta**. Bu, evrensel bir kural değil, bu donanıma
özgü bir gözlem -- üretim sunucusunda (muhtemelen daha fazla çekirdek,
sürdürülebilir soğutma, throttling yok) eğri tamamen farklı olabilir;
worker sayısı kararı gerçek üretim donanımıyla yeniden ölçülmeli.

---

## 5. Açık Sorular / Netleştirilmesi Gerekenler

Öncelik sırasına göre:

1. **Gerçek `.tab` formatı (kalan kısım)**: ayraç karakterinin tab olduğu
   ve her satır sonunda fazladan tab olduğu netleşti (bkz. 3.6); ama
   ondalık gösterim biçimi, header satırı var mı, encoding hâlâ bizim
   varsayımımız. Gerçek bir örnek dosya (ya da ilk birkaç satırı)
   paylaşılırsa kesinleşir.
2. **Docker host topolojisi**: şimdilik tek host (Docker Compose) kabul
   edildi, ama bu prototip için mi yoksa production için mi net değil.
   Tek host = tek arıza noktası riski hâlâ geçerli, "sonrasına bakarız"
   denildi.
3. **Dosya transfer yolu**: `.tab` dosyaları Windows'taki exe'den, Docker
   Compose stack'inin çalıştığı yere (Linux muhtemelen, ya da WSL2) nasıl
   taşınacak? Henüz tasarlanmadı.
4. **ClickHouse partition key** stratejisi henüz seçilmedi.
5. **MinIO erasure coding / disk sayısı** konfigürasyonu henüz
   belirlenmedi.
6. **Kaynak (`.ham`) sütunlarının gerçek genişliği** (float32/float64,
   özellikle GPS alanları) — formatı çözen kişiden teyit gerekiyor.
7. **Nihai donanım büyüklüğü** (çekirdek, RAM, disk) — gerçek `.tab`
   verisiyle küçük ölçekli benchmark yapılmadan belirlenmemeli.

---

## 6. Önerilen Sıradaki Adımlar

1. Madde 1'i (gerçek `.tab` formatının kalan detayları — ondalık gösterim,
   header, encoding) netleştirin — mümkünse gerçek bir örnek `.tab`
   dosyası paylaşın.
2. Madde 2'yi (Docker host topolojisi: prototip mi production mu)
   netleştirin, üretim mimarisi kararını donanım satın alımından önce netleştirin.
3. `tab-to-parquet/` (Rust) implementasyonunu gerçek `.tab` örnekleriyle
   uçtan uca doğrulayın (Python prototipiyle üretilen çıktılarla
   karşılaştırarak).
4. Gerçek `.tab` örnekleri elinize geçtiğinde, laptop'ınızda (i7-12700,
   16GB) küçük ölçekli benchmark yapıp chunk boyutu / worker sayısı gibi
   parametreleri gerçek verilerle netleştirin.

---

## 7. Rust vs Python -- doğrudan karşılaştırma (2026-08-15)

**Kapsam değişikliği**: Test verisi 300 tümü-float sütundan, gerçekçi bir
karışıma geçti -- 1000 sütun (300 float64 + 700 binary 0/1), her biri
~10GB, 6 dosya (toplam ~61GB, `tools/generate_test_tab.py` güncellendi).
Amaç: aynı işi hem Rust (`tab-to-parquet/`) hem Python
(`tab-to-parquet-py/tab_to_parquet.py` -- Rust ile aynı CLI sözleşmesine
sahip, trailing-tab düzeltmesi ve eşdeğer row-group kontrolü eklenmiş yeni
bir implementasyon, `prototypes/tab_to_parquet.py` ile karıştırılmasın)
yaptırıp **hız** ve **veri kaybı** açısından karşılaştırmak.

Ortam: Docker (bind-mount ile salt-okunur kaynak okuma, container'ın kendi
iç diskine yazma -- Trellix'in NTFS katmanından kaçınmak için, bkz. Bölüm
6). Parametreler: `--chunk-rows 50000 --max-row-group-rows 50000`, 6
worker paralel (dosya başına bir worker).

| | Rust | Python |
|---|---|---|
| Tamamlanan dosya | **6/6** | **5/6** -- 1 dosya sessizce kayboldu |
| Toplam süre | 1377sn (~23dk) | 2118sn (~35dk, sadece 5 dosya) |
| Agregat throughput | ~10.022 satır/sn | ~5.430 satır/sn (5 dosya üzerinden) |
| Sıkıştırma oranı | 1.61x | 1.61x (aynı) |

**Veri kaybı bulgusu (en önemli sonuç)**: 6 worker aynı anda çalışırken
Python tarafında bir süreç (dataset_06) sessizce öldü -- log dosyası
tamamen boş (0 byte, traceback bile yok -- klasik SIGKILL/OOM imzası),
üretilen `.parquet` sadece 4 byte (sadece parquet magic header, hiç veri
yok). Rust tarafında hiçbir dosya kaybolmadı.

Kaybolan dosyayı **tek başına** (kaynak rekabeti olmadan, bol bellekle)
tekrar çalıştırınca 641,6sn'de sorunsuz tamamlandı, ve içerik parmak izi
Rust'ın aynı dosya için ürettiğiyle **birebir eşleşti**
(`25cb430de14d977f...`). Sonuç: **hata dosyada/mantıkta değil, Python/
pyarrow'un aynı `--chunk-rows`/`--max-row-group-rows` ayarına rağmen daha
fazla bellek/kaynak kullanmasında** -- 6 worker aynı anda çalışınca bu
fazla ayak izi container'ın bellek bütçesini Rust'tan önce taşırıyor.

**Doğruluk (başarılı olan durumlarda)**: 6 dosyanın hepsinde -- Rust ve
Python'un içerik parmak izleri (satır sayısı + sütun toplamları + SHA-256)
**birebir eşleşti**. Yani hiçbir sessiz veri bozulması yok, sadece
Python'da (kaynak baskısı altında) tam dosya kaybı riski var.

**Sonuç**: Aynı koşullarda Rust hem daha hızlı (~1,85x, kaybolan dosyanın
tekrar üretilmesi hesaba katılırsa ~2x) hem daha güvenli (veri kaybı yok).
Bu, daha önce Rust'ı seçme kararının (Bölüm 4) somut bir doğrulaması.

**Yan not -- disk alanı yönetimi**: Bu test sırasında host diskinin
(~476GB, dinamik WSL2 vhdx paylaşıyor) %99 dolup 8GB'a kadar düştüğü
görüldü. Container içinde `.parquet` çıktılarını silmek **host'ta hiçbir
alan geri kazandırmadı** (WSL2'nin sanal diski thin-provisioned, silme
sonrası otomatik küçülmüyor -- Bölüm 6'daki bulguyla tutarlı). Gerçek
geri kazanım için Docker Desktop GUI'sinden "Clean / Purge data"
gerekiyor (admin gerektirmeyebilir ama GUI etkileşimi gerektiriyor).
Ayrıca: `wsl --shutdown` komutu sadece bizim container'ı değil, makinedeki
**diğer projelerin de tüm Docker container'larını durdurdu** (veri kaybı
yok, sadece durduruldu) -- ileride bu komuta dikkatli yaklaşılmalı.

## 8. Rust'ta bulunan gereksiz kopya + tekrar test (2026-08-17)

**Kod incelemesi**: Kullanıcının "iki kod da gereksiz iş yapmıyor mu"
sorusu üzerine `tab-to-parquet/src/main.rs` incelendi. Her flush'ta
`buf_ts.clone()` ve her sütun için `col.clone()` yapılıp hemen ardından
`.clear()` ile orijinaller atılıyordu -- `Float64Array` zaten
`Vec<f64>`'ün sahipliğini alabildiği için bu tam bir kopya boşa
gidiyordu (1000 sütunda flush başına ~400MB, dosya başına ~46 flush ~=
~18GB boşa memcpy). **Düzeltme**: `.clone()` yerine
`std::mem::replace(&mut buf, Vec::with_capacity(chunk_rows))` ile
sahiplik devri yapıldı, kopya ortadan kalktı. Python tarafında eşdeğer
bir sorun YOK -- `np.array(buf_ts, ...)` zaten kaçınılmaz tek bir
dönüşüm kopyası (liste -> array, farklı bellek yapıları).

Küçük ölçekte (50.000 satır) bağımsız doğrulamayla (satır sayısı + sütun
toplamları) düzeltmenin doğruluğu teyit edildi.

**Disk sorunu çözüldü**: Docker Desktop GUI'sinden "Clean / Purge data"
(WSL2 seçilerek) çalıştırıldı -- host disk boşluğu **8,1GB'dan
160GB'a** çıktı. Diğer projelerin (iha-video-search, anomalydetection)
Docker verileri de silindi (kullanıcı onayıyla, önemli değildi).

**Tekrar test (düzeltilmiş Rust + aynı Python, 6 worker, aynı 6 dosya,
aynı parametreler)**:

| | Rust (düzeltilmiş) | Python |
|---|---|---|
| 6 worker paralel süre | **1136sn** (6/6 başarı) | 2326sn (**5/6** -- dataset_05 kayboldu) |
| Kayıp dosyayı tamamlamak | -- | +618,5sn (tek başına, sorunsuz) |
| **6/6 için toplam** | **1136sn** | **2944,5sn** |
| Önceki testle kıyas | 1377sn -> 1136sn (**~%17,5 daha hızlı**, clone düzeltmesi sayesinde) | değişmedi (koda dokunulmadı) |

**İki önemli doğrulama**:
1. Clone düzeltmesi ölçülebilir bir fayda sağladı -- Rust ~%17,5 daha
   hızlandı, Rust/Python farkı ~2,0x'ten **~2,59x**'e çıktı.
2. **Veri kaybı tekrarlandı** -- bu kez farklı bir dosyada (dataset_05,
   öncekinde dataset_06). Yani bu tek seferlik bir talihsizlik değil,
   Python/pyarrow'un 6-worker senaryosunda **tekrarlanabilir bir
   güvenilirlik sorunu**. Kaybolan dosya yine tek başına (rekabet
   olmadan) sorunsuz tamamlandı ve Rust'la birebir aynı fingerprint'i
   verdi (`74cb439da58407ed`) -- veri/mantık hatası değil, kaynak
   rekabeti teyidi tekrar doğrulandı.

**Sonuç**: Rust kararı (Bölüm 4) iki bağımsız testle de doğrulandı --
hem daha hızlı hem güvenilir. Python/pyarrow'un 6-worker altında veri
kaybetme riski, farklı dosyalarla iki kez tekrarlandığı için tesadüf
değil, gerçek/sistematik bir davranış olarak kabul edilmeli.

## 9. "En iyi hal" karşılaştırması -- kod optimizasyonları ve DuckDB denemesi (2026-08-17)

Kullanıcının "iki koda da en iyi halini vererek adil karşılaştıralım"
isteği üzerine hem Rust hem Python'da ek incelemeler ve düzeltmeler
yapıldı.

**Rust**: `split_tab_line` her veri satırında bir `Vec<&str>` tahsis
ediyordu (1000 sütunda satır başına ~16KB, 2,3M satırda ~36GB'lık gereksiz
küçük tahsis toplamı). Ana döngü iterator'ü doğrudan tüketecek şekilde
yeniden yazıldı (`strip_trailing_tab` + `.split('\t')` üzerinde `Vec`'e
toplamadan). Doğruluğu küçük ölçekte teyit edildi. **Ölçülen etki: yok
denecek kadar az / gürültü seviyesinde** (1136sn -> 1196sn, hatta hafif
kötüleşme) -- clone düzeltmesinin aksine bu optimizasyonun gerçek dünya
faydası ölçülemedi.

**Python -- iki optimizasyon denendi, biri geri alındı**:
1. `order='F'` (Fortan/sütun-öncelikli numpy dizisi) -- teoride
   `vals_arr[:, i]` sütun çıkarmayı kopyasız yapması bekleniyordu. İzole
   ölçümde **%31 daha YAVAŞ** çıktı (numpy nested-list'ten önce C-order
   dolduruyor, sonra F-order'a ayrıca kopyalıyor) -- **GERİ ALINDI**. Ölçmeden
   varsayılan bir optimizasyonun tersine tepebileceğinin somut örneği.
2. Satır bazlı `float()` çağrılarını kaldırıp string'leri buffer'da tutup
   flush'ta numpy'a toplu (C seviyesi) string->float64 dönüşümü yaptırmak --
   izole mikro-benchmark'ta **~2,5x hızlı** ölçüldü (2,75sn vs 6,80sn,
   n=50000, cols=1000) ve doğruluğu (satır sayısı + sütun toplamları, max
   fark 0,0) teyit edildi. **KORUNDU.**

**Gerçek pipeline'da ölçülen etki -- mikro-benchmark'ın aksine küçük
çıktı**: Tam dönüşüm (I/O + zstd sıkıştırma + parquet encode dahil) için
Python'un izole süresi optimizasyon öncesi ~618,5-641,6sn, sonrası
**621,5sn** -- pratikte fark yok. Demek ki string->float dönüşüm adımı,
toplam sürenin küçük bir parçasıymış; asıl darboğaz başka yerde (muhtemelen
zstd sıkıştırma/parquet encode). **Önemli yan bulgu**: optimize kodla 6-worker
testi bu kez **6/6 dosyayı kaybetmeden tamamladı** (önceki iki denemede
her seferinde 1 dosya OOM ile kayboluyordu) -- string tabanlı buffer'ın
daha düşük bellek ayak izi, hız kazandırmasa da güvenilirliği artırmış
olabilir (kesin neden-sonuç kanıtlanmadı, gözlem düzeyinde).

**Doğru "izole vs paralel" karşılaştırması (düzeltilmiş metodoloji)**:
İlk yapılan "toplam süre / 6 = per-worker süre" hesaplaması YANLIŞTI --
6 worker paralel çalışırken `wait` en yavaş worker'ı beklediği için, HER
worker'ın gerçek süresi toplam duvar saatine yakın (ortalamaya değil).
Düzeltilmiş tablo:

| | İzole (tek dosya, rekabetsiz) | 6-worker paralel (gerçek süre) | Rekabet cezası |
|---|---|---|---|
| Rust | 294sn | ~1136-1196sn | ~3,9-4,1x |
| Python (optimize) | 621,5sn | 2078sn (6/6 başarı) | ~3,3x |
| Rust/Python oranı | **2,11x** | **~1,78x** | -- |

**Sonuç**: Rust izole halde ~2,1x daha hızlı; paralel ortamda fark daralıyor
(~1,78x) çünkü **Rust orantısal olarak Python'dan daha fazla rekabet
cezası ödüyor** (muhtemelen zaten çok hızlı olduğu için paylaşılan disk/
bellek bant genişliği tavanına daha çabuk/sert çarpıyor) -- ama mutlak
hızı o kadar yüksek ki bu daha büyük orantısal ceza bile Rust'ı öne
geçirmeye yetiyor. "İkisi benzer oranda ceza ödüyor" ilk varsayımı
YANLIŞTI, ölçümle düzeltildi.

**DuckDB denemesi (kısmi, tamamlanmadı)**: `.tab` formatındaki trailing-tab
sorunu `null_padding=true, strict_mode=false` ile aşılabiliyor (fazladan
hayalet bir sütun oluşuyor, `EXCLUDE` ile atılıyor), tüm sütunlar
`COLUMNS(*)::DOUBLE` ile zorlanabiliyor. Tek dosya, izole, DuckDB'nin
kendi dahili çoklu-thread'liğiyle: **205,7sn, sıkıştırma 2,12x** (Rust/
Python'un 1,61x'inden belirgin daha iyi). AMA bu, Rust/Python'un
"tek-thread worker" modeliyle mimari olarak farklı (DuckDB tek süreçte
kendi içinde paralel) -- 6 dosyalık adil bir karşılaştırma için mimari
netleştirilmeli (kullanıcı isteğiyle bu tur ertelendi, ileride
değerlendirilebilir).

## 10. Bind-mount I/O darboğazı ayıklanmış karşılaştırma (2026-08-17)

Kullanıcı "optimizasyon neden işe yaramadı" sorusuna cevap ararken, üç
aşamalı bir profiling (`tools/profile_python_convert.py`: oku+böl /
dönüştür / yaz) yapıldı. **Bind-mount üzerinden okurken**: oku+böl **%59,5**
(285,3sn), dönüştür %24,7 (118,2sn), yaz %15,8 (76,0sn) -- yani optimize
ettiğimiz "dönüştür" adımı zaten toplamın küçük bir dilimiydi (Amdahl
Yasası), asıl pay okuma+bölmedeydi.

Kullanıcının önerisiyle aynı dosya container'ın kendi iç diskine
kopyalanıp (bind-mount'suz) profiling tekrarlandı: oku+böl payı **%59,5 ->
%15,7**'ye düştü (285,3sn -> 63,6sn, ~4,5x azalma) -- bind-mount okumasının
gerçekten ciddi bir maliyet olduğu doğrulandı (Trellix mi, Docker'ın
virtiofs/9p çeviri katmanı mı olduğu ayırt edilemedi, ikisi de olası).

**Asıl soru**: I/O darboğazı olmadan Rust/Python farkı değişir mi? Aynı
yerel (bind-mount'suz) dosyada temiz bir karşılaştırma yapıldı:

| | Bind-mount (I/O darboğazlı) | Yerel disk (darboğazsız) |
|---|---|---|
| Rust | 294sn | 329sn |
| Python (optimize) | 621,5sn | 567sn |
| **Rust/Python oranı** | **2,11x** | **1,72x** |

Fingerprint'ler birebir eşleşti (`ae9e0e5a28669e98`), veri doğru.

**Sonuç**: Hipotez KISMEN doğrulandı -- I/O darboğazı farkı gerçekten
şişiriyordu (2,11x -> 1,72x), ama farkın büyük kısmı hâlâ duruyor. Yani
Rust/Python farkı esasen I/O'dan değil, **gerçekten CPU/dil seviyesinden**
(derlenmiş kod vs yorumlanan kod, Python'un nesne ek yükü) kaynaklanıyor.
İlginç yan not: yerel diskte Rust hafifçe yavaşladı (294->329sn), Python
hafifçe hızlandı (621,5->567sn) -- muhtemelen ölçüm varyansı + Rust'ın
zaten I/O'ya daha az bağımlı olması (okuma+işleme o kadar hızlı ki I/O'nun
payı baştan beri küçüktü).

## 11. `pyarrow.csv` ile Python'u Rust'a yetiştirme denemesi -- BAŞARILI (2026-08-17)

"Python'u daha da hızlandırmanın yolu yok mu" sorusuna cevap arandı.
Elle satır/değer ayrıştırma (mevcut `tab_to_parquet.py`) yerine, okuma +
tip dönüşümünü tamamen Arrow'un C++ CSV okuyucusuna (`pyarrow.csv.open_csv`)
devreden yeni bir implementasyon yazıldı: `tab-to-parquet-py/
tab_to_parquet_pyarrow_csv.py`. Mantık: Python artık elle `split('\t')` +
`float()` yapmıyor, sadece Arrow'un ürettiği batch'leri chunk'layıp
Float64'e cast edip parquet'e yazıyor (satır bazlı iş Python'da değil,
C++'ta).

**Format uyumu**: pyarrow.csv, DuckDB'nin aksine trailing-tab formatını
**hiçbir özel ayara gerek kalmadan** kabul etti -- otomatik olarak adı
boş (`''`) bir "hayalet" sütun oluşturuyor (DuckDB'deki `column1001` ile
aynı fikir), bu `table.select(real_names)` ile atılıyor. Binary sütunlar
`int64` olarak algılanıyor, `target_schema`'ya `.cast()` ile Float64'e
zorlanıyor.

**İlk deneme OOM'a çarptı (kendi hatam)**: `ReadOptions(block_size=
chunk_rows * 20_000)` ifadesi ~954MB'lık TEK bir okuma bloğu demekti --
ilk chunk'a bile ulaşmadan SIGKILL (log tamamen boş, parquet 4 byte --
tanıdık imza). `block_size=4MB` (makul, sabit bir değer) ile düzeltildi
-- `chunk_rows` kontrolü zaten `buf_rows >= chunk_rows` mantığıyla ayrıca
sağlanıyor, block_size'ın onunla birebir eşleşmesi gerekmiyor.

**Sonuç (izole, yerel/I/O-darboğazsız dosyada)**:

| | Süre |
|---|---|
| Rust | 329sn |
| Python (elle parse, optimize) | 567sn |
| **Python + pyarrow.csv** | **329sn -- Rust ile BİREBİR AYNI** |

Doğruluk DuckDB'nin SQL agregasyonuyla (Python'a veri çekmeden, bellek
güvenli) doğrulandı: satır sayısı tam eşleşti (2.300.000), sütun
toplamları göreceli toleransla (rtol=1e-9) tam eşleşti (max fark
2,28e-08 -- floating-point toplama sırası farkı, veri bozulması değil).
Bellek kullanımı sağlıklı kaldı (OOM yok, kullanılan bellek ~1,5-2,6GB,
artan kısım sadece Linux dosya önbelleği).

**Sonuç/çıkarım**: "Python dilinde" Rust'ı yakalayamazsınız (yorumlanan
dil + nesne ek yükü yapısal bir fark), AMA "Python'dan çağrılan C++
kütüphanesiyle" (pyarrow.csv, zaten mevcut bağımlılığımız, DuckDB gibi
yeni bir bağımlılık gerektirmiyor) yetişebilirsiniz -- çünkü bu noktada
karşılaştırma artık "Rust vs Python" değil, "elle yazılmış parser vs
optimize edilmiş C++ parser" haline geliyor. Bu, mevcut worker-per-process
mimarimize (Rust ile birebir aynı CLI sözleşmesi, --chunk-rows/--max-row-
group-rows) hiçbir mimari değişiklik olmadan oturuyor -- DuckDB'nin
aksine paralellik modeli sorunu yok.

**Sıradaki adım (henüz yapılmadı)**: Bu sonuç sadece izole (1 dosya,
rekabetsiz) ortamda ölçüldü. 6-worker paralel senaryoda (asıl üretim
senaryomuz) bu üstünlüğün korunup korunmadığı, ve bellek ayak izinin
paralel yükte OOM riski taşıyıp taşımadığı henüz test edilmedi.

## 12. `pyarrow.csv` 6-worker testi -- disk %100 dolma olayı + OOM (2026-08-17)

**Disk %100 dolma olayı**: 6-worker `pyarrow.csv` testi ilk denemede
host diskini tamamen doldurdu (476G/476G, 0 boş alan) -- önceki turlardan
kalan test dosyaları (local_dataset_03.tab kopyası, rust_local/py_local/
pyarrowcsv_local parquet'leri, hepsi container içi volume'de, hiç
silinmemiş) + yeni ~39GB'lık çıktı toplamda taştı. Sonuç: `Input/output
error`, `Bus error (core dumped)`, Docker daemon 500 hatası vermeye
başladı, container tamamen kayboldu (`docker ps -a` boş döndü). Disk
kendiliğinden (WSL2/Docker'ın kendi kurtarma mekanizmasıyla) ve
kullanıcının Docker Desktop GUI'den "Clean/Purge data" çalıştırmasıyla
161GB boşa döndü -- **host'taki `testdata/dataset_*.tab` kaynak dosyaları
tamamen sağlam kaldı** (bunlar container'ın vhdx'inde değil, doğrudan
Windows NTFS'te). Ders: uzun test turları arasında ara/geçici dosyaları
düzenli temizlemek gerekiyor, disk kullanımını sadece test başında değil
sürekli izlemek şart.

**Temiz container ile tekrar deneme**: Container sıfırdan kuruldu, disk
156GB boşla başladı (temiz durum). 6-worker `pyarrow.csv` testi 559sn'de
"bitti" ama **doğrulamada veri kaybı bulundu**:

```
OSError: [Errno 12] Error reading bytes from file.
Detail: [errno 12] Cannot allocate memory
```

`dataset_02` işlenirken (8. parçadan sonra, ~402.000 satır işlenmişken)
bu hata fırlatıldı, süreç düzgün bir Python traceback'iyle sonlandı
(sessiz SIGKILL değil -- bu, elle-parse Python/Rust'taki "boş log, 4 byte
parquet" imzasından FARKLI, daha "nazik" bir OOM davranışı, ama sonuç
aynı: dataset_02.parquet sadece ~1,19GB'da yarım kaldı, beklenen ~6,47GB
yerine). Diğer 5 dosya sorunsuz tamamlandı (2.300.000 satır her biri).

**Sonuç**: `pyarrow.csv` izole halde Rust'la eşit hız + düşük bellek
gösterdi (Bölüm 11), ama **6 paralel worker'da yine OOM riski taşıyor**
-- elle-parse Python'daki sorunun aynısı, sadece hata daha görünür/nazik.
559sn'lik süre 6/6 tamamlanmadığı için geçerli bir karşılaştırma sayısı
DEĞİL. `--max-row-group-rows`'u küçültmek (Rust'ta işe yaramış çözüm)
burada da denenmeli -- henüz yapılmadı.

## 13. `pyarrow.csv` 6-worker OOM'unun kök nedeni bulundu ve düzeltildi (2026-08-17)

**Önce `--max-row-group-rows` küçültme denendi (Rust'taki çözümle
simetri için) -- işe yaramadı.** `--chunk-rows 20000 --max-row-group-rows
20000` ile 6-worker tekrar denendi: 436sn'de "bitti" ama yine
`dataset_06`'da aynı hata (`OSError: [Errno 12] Cannot allocate memory`)
ile veri kaybı oldu. Bu, sorunun PARQUET YAZMA tarafında (row-group
boyutu, bizim zaten kontrol ettiğimiz parametre) değil, **CSV OKUMA
tarafında** olduğunu gösterdi.

**Gerçek kök neden**: `pyarrow.csv.ReadOptions` varsayılan olarak
`use_threads=True` -- yani her Python worker süreci, CSV okurken **kendi
içinde de ayrıca çoklu thread** açıyordu. Worker-per-process mimarimizde
(6 ayrı Python süreci) her sürecin İÇİNDE de fazladan thread açması,
beklenenden çok daha fazla eşzamanlı bellek/CPU rekabetine yol açıyordu
-- 6 süreç × (içeride) N thread, sadece 6 tek-thread sürecin toplamından
çok daha fazla kaynak talebi demek.

**Düzeltme**: `ReadOptions(use_threads=False)`. Container sıfırlanıp
(temiz disk, 96GB boş) `--chunk-rows 20000 --max-row-group-rows 20000
use_threads=False` ile tekrar test edildi:

| | Süre | Doğruluk |
|---|---|---|
| Rust (bind-mount, 6-worker) | 1136-1196sn | 6/6 |
| Python elle-parse (bind-mount, 6-worker) | 2078sn | 6/6 |
| Python + pyarrow.csv (düzeltilmiş, `use_threads=False`) | **497sn** | **6/6 -- veri kaybı YOK** |

Satır sayısı tam doğrulandı (13.800.000 = 13.800.000). Bellek kullanımı
da düzeldi -- önceki denemede ~9,7GB'a çıkan kullanım, bu sefer ~4,8-
5,1GB'da kaldı (container'ın 12GB sınırına çok daha rahat bir marj).

**SONUÇ (bu bölümün en önemli bulgusu)**: Doğru ayarlanmış
`pyarrow.csv` tabanlı Python implementasyonu, 6-worker paralel
senaryoda **Rust'tan ~2,3-2,4x, elle-parse Python'dan ~4,2x daha hızlı**
-- VE veri kaybı riski yok. Bu, "Python'u Rust'a yetiştirme" sorusunun
sadece izole değil, **asıl üretim senaryomuzda (çoklu worker paralel)**
da geçerli olduğunu kanıtlıyor. Kritik ayar: `use_threads=False` --
worker-per-process mimarisinde CSV okuyucunun kendi içindeki
paralelliğini kapatmak şart, aksi halde çift paralellik (süreç x thread)
OOM'a yol açıyor.

**Yan not (disk hijyeni)**: Bu turda disk her adımda kontrol edilip
gereksiz çıktılar temizlendi (Bölüm 12'deki "%100 dolma" olayından ders
alınarak) -- disk hiçbir noktada 70GB'ın altına inmedi.

## 14. DuckDB'yi de adil koşullarda (6-worker) test etme -- kullanıcı isteğiyle (2026-08-17)

Kullanıcı haklı bir noktaya değindi: amaç "hangi araç kazanır" değil,
"60GB'ı en hızlı/ucuz/doğru nasıl dönüştürürüz" -- bu yüzden DuckDB'yi de
worker-per-process mimarimize uyarlanmış şekilde (6 ayrı DuckDB süreci,
her biri `PRAGMA threads=1` ile kendi iç paralelliği kapatılmış --
pyarrow.csv'deki `use_threads=False` ile aynı mantık) test ettik. Yeni
script: `tab-to-parquet-py/tab_to_parquet_duckdb.py`.

**Format/kod notu**: `COLUMNS({...})` sözdizimi DuckDB'de çalışmadı,
her sütun için açık `"col"::DOUBLE AS "col"` SELECT listesi kullanıldı.
Ayrıca DuckDB'nin hayalet (trailing-tab) sütununa verdiği otomatik isim
sürümden sürüme değişebileceği için (`c != ""` kontrolü işe yaramadı,
DuckDB ona boş string değil "column1001" gibi bir isim vermişti), gerçek
sütun adları DuckDB'ye sormak yerine **header satırından doğrudan**
okunuyor.

**Sonuç (izole, threads=1)**: DuckDB **188sn** -- Rust'tan (294-329sn)
ve pyarrow.csv'den (329sn) bile hızlı, ÜSTELİK tek thread'le. Sıkıştırma
**2,09x** -- Rust/pyarrow.csv'nin 1,61x'inden belirgin iyi.

**Sonuç (6-worker paralel, threads=1)**: **565sn, 6/6 satır tam doğru
(2.300.000 her biri), veri kaybı YOK.**

| | Süre | Doğruluk | Sıkıştırma | Toplam çıktı (6 dosya) |
|---|---|---|---|---|
| Rust | 1136-1196sn | 6/6 | 1,61x | ~40,7GB |
| Python elle-parse | 2078sn | 6/6 | 1,61x | ~40,7GB |
| **Python + pyarrow.csv** | **497sn** | 6/6 | 1,61x | ~40,7GB |
| **DuckDB (threads=1)** | 565sn | 6/6 | **2,09x** | **~31,4GB** |

**Çıkarım -- tek kazanan yok, hedefe göre değişir**:
- **Hız önceliğiyse**: pyarrow.csv (%14 daha hızlı)
- **Depolama maliyeti önceliğiyse**: DuckDB (~%23 daha az disk/MinIO
  alanı -- 1,5M dosyalık üretim ölçeğinde ciddi bir fark yaratabilir)
- İkisi de doğru, güvenilir, aynı worker-per-process mimarisine
  (Postgres manifest'in SKIP LOCKED deseniyle) sorunsuz oturuyor
- DuckDB'nin daha iyi sıkıştırmasının nedeni araştırılmadı (muhtemelen
  farklı bir encoding stratejisi/varsayılan -- ileride merak edilirse
  incelenebilir)

**Genel oturum sonucu**: Başlangıçta "Rust vs Python" sorusuyla
başlanan bu araştırma, sonunda "elle yazılmış parser (dil fark etmez)
vs optimize edilmiş C++ motoru (pyarrow.csv/DuckDB)" sonucuna vardı --
ikisi de mevcut hand-written Rust/Python implementasyonlarından çok daha
hızlı ve güvenilir çıktı.

## 15. Sıkıştırma farkının kök nedeni -- binary sütunlarda dictionary encoding (2026-08-17)

DuckDB'nin Rust'tan (2,09x vs 1,61x) neden daha iyi sıkıştığı, spekülasyon
yerine parquet dosyalarının kendi meta verisi (`pyarrow.parquet.
ParquetFile(...).metadata`) incelenerek araştırıldı -- her iki dosya aynı
küçük kaynaktan (`small_test.tab`, 999 satır), aynı row-group ayarıyla
(tek row group) yeniden üretilip sütun bazında karşılaştırıldı.

| | Float sütunlar (300 adet) | Binary sütunlar (700 adet) |
|---|---|---|
| Rust (parquet-rs) | 1,04x | **0,93x** (sıkıştırma sonrası BÜYÜYOR) |
| DuckDB | 1,05x | **1,33x** (gerçek kazanç) |

**Fark tamamen binary (0/1) sütunlardan geliyor** -- float sütunlarda
(rastgele veri, zaten iyi sıkışmıyor) ikisi de neredeyse aynı. Encoding
meta verisi: Rust'ın binary sütun için "kullanılabilir" listesi
`(PLAIN, RLE, RLE_DICTIONARY)` gösteriyor ama net sonuç kötü (negatif
sıkıştırma); DuckDB binary sütun için açıkça **`PLAIN_DICTIONARY`**
kullanmış -- sadece 2 farklı değer (0.0/1.0) olduğu için veriyi
sözlük+referans şeklinde çok daha kompakt kodluyor.

**Sonuç**: DuckDB'nin parquet yazıcısı, düşük-kardinaliteli (az farklı
değerli) sütunlar için dictionary encoding'i parquet-rs'den (Rust) daha
agresif/etkili kullanıyor. Bizim veri setimizin şekli (1000 sütunun
700'ü binary) tam olarak bu fırsatı öne çıkaran bir senaryo -- bu yüzden
fark bu kadar belirgin (2,09x vs 1,61x). Gerçek `.ham` verisinde kaç
sütunun düşük-kardinaliteli (flag/durum/binary tipi) olduğu netleşince,
bu fark üretim ölçeğinde ne kadar önemli olacağı daha iyi tahmin
edilebilir -- düşük-kardinaliteli sütun oranı yüksekse DuckDB'nin
depolama avantajı büyür, tamamen sürekli/analog sensör verisiyse (GPS,
ivme vb.) fark küçülür.

**Kök neden parquet-rs (Rust) tarafında düzeltilebilir mi?** Muhtemelen
evet -- `WriterProperties::builder().set_dictionary_enabled(true)` gibi
bir ayar zaten var olabilir (varsayılan davranış farklı bir eşiğe/
sezgisel kurala dayanıyor olabilir), ama bu henüz araştırılmadı/
denenmedi. İleride Rust implementasyonunun sıkıştırmasını iyileştirmek
istenirse buradan başlanabilir.

**Deneme: `use_dictionary=True` pyarrow.csv'de açıkça zorlandı --
İŞE YARAMADI.** `tab-to-parquet-py/tab_to_parquet_pyarrow_csv.py`'de
`ParquetWriter`'a `use_dictionary=True` eklendi. Küçük ölçekte (999
satır) meta veri incelendi: encoding listesi Rust'la BİREBİR aynı
(`PLAIN, RLE, RLE_DICTIONARY`), binary sütunda yine negatif sıkıştırma.
Küçük ölçekte zstd'nin sabit paket ek yükünün orantısal olarak abartılı
görünebileceği düşünülüp TAM ölçekte (10GB, dataset_01) doğrulandı:
sonuç yine **1,61x** -- değişmedi. **Sonuç: bu basit bir ayar sorunu
değil, DuckDB'nin dictionary encoding algoritması gerçekten farklı/daha
etkili** (parquet-rs ve pyarrow/Arrow-C++ -- iki bağımsız implementasyon
-- ikisi de aynı, daha zayıf sonucu veriyor). Daha derin bir çözüm
(örn. dictionary page boyutu limitleri, farklı encoding versiyonu
zorlama) mümkün ama giderek azalan getiri bölgesi -- şimdilik
denenmedi, gerekirse ileride konu olabilir.

## 16. zstd maksimum sıkıştırma seviyesi denemesi -- kötü trade-off (2026-08-17)

Kullanıcının isteğiyle pyarrow.csv'ye `compression_level` parametresi
eklendi, zstd seviye **22 (maksimum)** ile tam ölçekli (10GB,
dataset_01) test edildi.

| | Süre | Sıkıştırma | Doğruluk |
|---|---|---|---|
| Varsayılan zstd seviyesi | 214sn | 1,61x | ✓ |
| **zstd seviye 22 (max)** | **717sn (~3,35x yavaş)** | **1,96x (%22 daha iyi)** | ✓ (veri bozulmadı) |

Doğruluk `tools/compare_parquets.py` ile teyit edildi -- satır sayısı
tam eşleşti, sütun toplamları göreceli toleransla (rtol=1e-9) tam
eşleşti (max fark 1,9e-6, floating-point toplama sırası farkı,
zararsız).

**Sonuç: KÖTÜ trade-off, önerilmiyor.** Süre ~3,35x artıyor ama
sıkıştırma sadece %22 iyileşiyor -- ve hâlâ DuckDB'nin varsayılan
ayarının (2,09x) gerisinde kalıyor. Üretim ölçeğinde (1,5M dosya) bu,
dönüşüm süresini üçe katlayıp disk tasarrufunu ancak kısmen artırmak
demek. **zstd max seviye kullanılmamalı.** Orta seviyeler (örn. 9-12)
denenmedi, daha makul bir denge sunabilir ama henüz test edilmedi.

## 17. DuckDB varsayılan ayarlarla test -- mevcut ayarların zaten optimal olduğu doğrulandı (2026-08-17)

Kullanıcının "DuckDB için de en iyi hali bulalım, hiç talimat vermezsek
ne olur" sorusu üzerine `tab_to_parquet_duckdb.py`'ye `threads=None` /
`row_group_size=None` desteği eklendi (PRAGMA/ROW_GROUP_SIZE hiç
verilmiyor, DuckDB'nin tam kendi varsayılanına bırakılıyor).

**İzole (tek dosya)**: varsayılan 218sn/2,12x -- `threads=1` ile
aldığımız (188sn/2,09x) sonuca göre **biraz daha yavaş, biraz daha iyi
sıkışan**. Fark küçük, yön belirsiz (ölçüm gürültüsü olabilir).

**6-worker paralel (asıl önemli test)**: varsayılan (threads sınırsız)
547sn -- ama **`dataset_03` ve `dataset_06`'da veri kaybı**:

```
_duckdb.IOException: IO Error: Could not read from file "...":
Cannot allocate memory
```

Bu, pyarrow.csv'de bulduğumuz kök nedenin (Bölüm 13) BİREBİR AYNISI --
DuckDB da varsayılan halde kendi içinde çoklu-thread açıyor, 6 paralel
süreçte çifte paralellik OOM'a yol açıyor. Bellek izlemesi de bunu
destekledi -- varsayılan halde kullanım daha değişken/yüksek (~4,3-9,5GB
dalgalı) `threads=1`'in (~4-5GB istikrarlı) aksine.

**Sonuç**: Bizim zaten kullandığımız `threads=1` ayarı tesadüf/aşırı
temkinli bir önlem DEĞİL, **gerçekten gerekli**. Varsayılan hali ~%3
daha hızlı görünüyor (547sn vs 565sn) ama güvenilmez olduğu için bu
kazanç geçersiz. `row_group_size` için de: izole testte DuckDB kendi
varsayılanıyla ~50.417 satırlık gruplar seçmişti -- bizim elle verdiğimiz
50.000 zaten DuckDB'nin kendi optimal aralığına çok yakın, ekstra bir
kazanç fırsatı yok. **Şu anki ayarlar (`threads=1`,
`row_group_size=50000`) zaten optimal/gerekli.**

## 18. DuckDB worker-sayısı ölçekleme eğrisi -- N=1..6 (2026-08-17)

Kullanıcının "45 dakikalığına AFK olacağım, gerekli görürsen farklı
sayıda worker'larla da dene" talimatı üzerine, Rust'ta çok daha önce
yapılan N=1,2,4,6,8,12 ölçekleme metodolojisinin DuckDB karşılığı
kuruldu. Her testte `threads=1`, `row_group_size=50000` (Bölüm 17'de
zorunlu olduğu doğrulanan ayarlar) kullanıldı, N ayrı OS süreci paralel
çalıştırıldı (N farklı dosya, birer worker).

Ek olarak, öncesinde tek bir DuckDB sürecinin 6 dosyayı SIRAYLA, kendi
iç paralelliğine (PRAGMA sınırı yok) bırakarak işlemesi de test edildi
("DuckDB'ye kendi karar versin" senaryosu, `duckdb_single_process_batch.py`):
toplam **1305,4sn** (dosya başına 185,4 - 247,0sn), bellek güvenli
(~1,4-2,3GB) ama 6-paralel-süreç yaklaşımından (565sn) **~2,3 kat daha
yavaş** -- yani DuckDB'nin kendi iç paralelliği, bizim manuel N-worker
paralelliğimizin yerini tutmuyor, ikisi birlikte gerekiyor.

Sonuçlar (her N için toplam duvar-saati süresi, tüm dosyalar 6/6 doğru
-- satır sayısı + sütun toplamları eşleşti):

| N | Süre | Dosya başına (min-max) | Agregat throughput (satır/sn) | Hızlanma (N=1'e göre) | Verimlilik |
|---|---|---|---|---|---|
| 1 (izole) | 188sn | -- | 12.234 | 1,00x | %100 |
| 1 (sıralı, tek süreç, tüm 6 dosya) | 1305,4sn | 185,4-247,0sn | 10.575 | -- | -- (paralellik yok) |
| 2 | 240sn | 236,9-239,1sn | 19.167 | 1,57x | %78,4 |
| 3 | 337sn | 331,6-336,7sn | 20.474 | 1,67x | %55,7 |
| 4 | 393sn | 380,1-392,1sn | 23.410 | 1,91x | %47,8 |
| 6 | 565sn | (Bölüm 14) | 24.425 | 2,00x | %33,3 |

**Yorum**:
- Verimlilik N arttıkça düzgün azalıyor -- klasik azalan getiri deseni,
  Rust'ın N=1..12 eğrisiyle aynı şekilde. Ama DuckDB genel olarak daha
  iyi verim koruyor: Rust N=6'da ~%20 verimlilikteydi, DuckDB N=6'da
  %33,3 -- disk I/O + CPU örtüşmesi (pyarrow.csv'de gördüğümüz 104-116%
  CPU kullanımına benzer şekilde) DuckDB'de de daha iyi durumda.
- **Agregat throughput hiç durmadan artıyor** (N=1: 12.234 → N=6:
  24.425 satır/sn) -- yani elimizdeki 6 dosyayı toplamda **en hızlı
  bitirme** açısından hâlâ N=6 en iyisi. Kısmi paralellik (N=2,3,4)
  denemek, kalan dosyaları beklemeye bırakır ve toplam süreyi uzatır.
- N=8 veya N=12 test edilemedi çünkü elimizde sadece 6 farklı dosya
  var; aynı dosyayı birden fazla worker'a vermek farklı bir rekabet
  senaryosu olur, gerçek üretim senaryomuzu (her worker farklı dosya)
  temsil etmez. Daha fazla test dosyası üretmek (~90dk/dosya) bu
  noktada disk alanı (33GB serbest) ve zaman açısından gerekçesiz
  görüldü.
- **Pratik sonuç -- bizim gerçek 1,5M dosyalık üretim senaryomuz
  için**: elimizdeki 6 dosyalık test seti için N=6 (hepsi paralel) en
  iyisi. Ama gerçek üretimde işlenecek dosya sayısı worker havuzu
  boyutundan çok daha fazla olacağı için (sürekli akan iş kuyruğu),
  asıl soru "havuzda kaç worker sürekli tutulmalı" sorusu -- bu eğri bu
  soruyu N=6'ya kadar yanıtlıyor: verimlilik kaybı olsa da N arttıkça
  toplam iş hep daha hızlı bitiyor, bu yüzden mevcut 6-worker/12-core
  makine sınırı içinde 6 worker'ı sürdürmek mantıklı; makine
  büyütülürse (daha fazla çekirdek) N=8-12 aralığının da test edilmesi
  gerekir.

## 19. N=20 testi -- pratik tavan bulundu, %35 veri kaybı (2026-08-17)

Kullanıcının "bütün çekirdekler çalışacağı için" sorusu üzerine, N=20'yi
gerçekten test ettik. Elimizde sadece 6 dosya olduğu için, mevcut 6
dosya (`tools/split_tab.py` ile, streaming/RAM-güvenli) toplam 20
parçaya bölündü (2 dosya x4 parça + 4 dosya x3 parça = 20, her parça
2,74GB ya da 3,65GB, header her parçada korunuyor). Split ve
dönüştürme çıktıları host'un sıkışık `C:` diskine değil, container'ın
kendi iç hacmine (`/work`, 1TB, o an 912GB boş) yazıldı -- host disk
alanı hiç etkilenmedi. 20 parça satır bazında doğrulandı (toplam
13.800.000 satır, kaynakla birebir eşleşti).

20 worker (`threads=1`, `row_group_size=50000`) paralel başlatıldı.
Sonuç:

- **7/20 worker (%35) OOM/SIGKILL (exit 137) ile öldü** -- parquet
  çıktıları temiz 0 byte kaldı (sessiz bozulma yok, kolayca tespit
  edilebilir bir hata: exit kod + boş dosya).
- **13/20 başarılı olan da ciddi yavaşladı**: dosya başına 511,9-
  662,8sn (dosyalar 2,74-3,65GB, izole halde N=1 eğrisine göre ~47-
  63sn beklenirdi -- yani başarılı worker'lar bile **~10,6-10,9x**
  yavaşladı, N=6'daki normal paralellik cezasından (izoleye göre ~3x)
  çok farklı, gerçek bir bellek-thrashing/swap rejimi).
- Toplam duvar saati: 664sn. Container belleği (`free -h`) test
  boyunca sürekli ~11GiB/11GiB dolu kaldı (sadece 85-200MB boş),
  swap'a da girdi (aynı `.wslconfig`/bellek-büyütme deneyinde
  gördüğümüz "sürekli dolu bellek -> ciddi yavaşlama" paterniyle
  birebir aynı, Bölüm 6).
- Agregat throughput (sadece başarılı satırlar): ~12.990 satır/sn --
  **N=1 izolenin (12.234) bile sadece ~%6 üstünde**, N=6'nın
  (24.425) neredeyse yarısı. Yani N=20, N=6'ya göre neredeyse hiçbir
  hız kazancı vermiyor VE verinin %35'ini kaybediyor.

**Sonuç: N=20 bu makinede kesinlikle pratik tavanın üzerinde.**
Gerçek tavan 6 ile 20 arasında bir yerde -- container RAM bütçesi
(~11-12GB) ve N=6'nın kullandığı ~4-5GB'a bakılırsa tahmini 12-14
civarı olabilir, ama bu ayrıca test edilmedi (kullanıcı isterse
N=8/12/16 ile aralık daraltılabilir). Root cause N=6 testindeki
(Bölüm 13/17) çifte paralellik OOM'unun aynı ailesi değil -- burada
`threads=1` zaten doğru ayarlanmış, sorun sadece ham worker SAYISININ
container'ın sabit ~11-12GB bellek bütçesini aşması.

## 20. Final Karşılaştırma -- Rust vs Python vs pyarrow.csv vs DuckDB (2026-08-17)

Bu bölüm, önceki 19 bölüme dağılmış tüm ölçümleri tek bir yerde
topluyor. Dört implementasyon da aynı veri setiyle (6×10GB, 1000 sütun
[300 float64 + 700 binary 0/1], 6 worker/süreç, worker-per-process
mimarisi) test edildi.

### Özet tablo

| | Rust | Python (elle parse) | Python+pyarrow.csv | DuckDB |
|---|---|---|---|---|
| **İzole (tek dosya, tek worker)** | 294-329sn | 567-621,5sn | 329sn | **188sn** |
| **6-worker paralel (toplam)** | 1136-1196sn | 2078-2944,5sn | **497sn** | 565sn |
| **6-worker'da veri kaybı** | Hiç (6/6 hep) | **Sistematik** (2 ayrı denemede, 2 farklı dosya kayboldu) | Düzeltme öncesi kayıp, düzeltme sonrası 0 | Düzeltme öncesi kayıp, düzeltme sonrası 0 |
| **Sıkıştırma oranı** | 1,61x | 1,61x | 1,61x | **2,09x** |
| **6-worker bellek (toplam)** | Düşük/istikrarlı (`--max-row-group-rows` ile) | **En yüksek** (kayıpların kök nedeni) | ~9,7GB→~4,8-5,1GB (`use_threads=False` sonrası) | ~4-5GB istikrarlı |
| **Kök neden düzeltmesi gerekli mi** | Hayır (baştan doğru) | Denendi, tam çözülemedi | Evet -- `use_threads=False` (çifte paralellik) | Evet -- `threads=1` (çifte paralellik) |
| **Worker-sayısı tavanı (bu makinede)** | ~6 (N=12'de verim %20'ye düşüyor) | test edilmedi | test edilmedi | **6 güvenli, 20 başarısız (%35 kayıp)**, gerçek tavan tahmini 12-14 |
| **Yeni bağımlılık gerekiyor mu** | Rust toolchain (zaten kurulu) | Yok (numpy/pyarrow zaten var) | Yok (pyarrow zaten parquet için gerekli) | Evet (`pip install duckdb`) |
| **Kod karmaşıklığı** | Orta (row-group/ownership yönetimi) | En yüksek (elle parse) | Düşük (CSV okuyucusuna devret) | **En düşük** (tek SQL COPY komutu) |

### Neden bu sonuçlar çıktı -- kısa nedensellik zinciri

1. **"Rust vs Python" sorusu yanlış çerçeveydi.** Elle yazılmış parser'lar
   (Rust dahil) optimize edilmiş native motorlardan (pyarrow'un C++ CSV
   okuyucusu, DuckDB'nin C++ analitik motoru) yapısal olarak daha yavaş.
   Asıl fark "derlenmiş dil vs yorumlanan dil" değil, "elle satır-satır
   parse vs vektörize/toplu okuma motoru" (Bölüm 11 sonucu).
2. **Python'un elle-parse sürümü güvenilmezliği hiç çözülemedi** -- iki
   ayrı 6-worker denemesinde iki farklı dosya sessizce kayboldu (OOM/
   SIGKILL, boş log). Kayıp dosyalar tek başına çalıştırıldığında
   Rust'la birebir aynı veri parmak izini verdi -- yani mantık hatası
   değil, saf bellek ayak izi sorunu, kod optimizasyonlarıyla (deferred
   parsing, numpy toplu dönüşüm) iyileşmedi (Bölüm 8-9).
3. **pyarrow.csv ve DuckDB'nin ikisi de aynı "çifte paralellik" tuzağına
   düştü, ikisi de aynı ilaçla düzeldi**: worker-per-process mimarisinde
   kütüphanenin kendi iç thread havuzunu (`use_threads`/`threads`)
   KAPATMAK şart -- aksi halde N süreç × M iç thread container bellek
   bütçesini aşıyor (Bölüm 13 ve 17). Bu düzeltmeden sonra ikisi de
   %100 güvenilir.
4. **DuckDB'nin sıkıştırma avantajı köküne inildi** (Bölüm 15): parquet
   yazıcısı düşük-kardinaliteli (binary 0/1) sütunlarda dictionary
   encoding'i parquet-rs'den (Rust) daha agresif kullanıyor -- veri
   setimizin 700/1000 sütunu binary olduğu için bu fark büyüyor.
5. **DuckDB'nin worker-sayısı tavanı diğerlerinden daha net ölçüldü**
   (Bölüm 18-19) çünkü sadece DuckDB için N=1..6..20 tam eğri
   çıkarıldı -- N=6 sağlam, N=20 verinin %35'ini kaybediyor. Rust'ta
   benzer bir eğri N=1..12 için çıkarılmıştı (tavan ~6, ama orada
   sorun veri kaybı değil sadece azalan verimlilikti -- Rust hiçbir
   worker sayısında veri kaybetmedi, sadece yavaşladı). Python
   (elle-parse) ve pyarrow.csv için worker-sayısı ölçekleme eğrisi hiç
   çıkarılmadı (N=6 dışında test edilmedi).

### Final karar

**Tek kazanan yok, öncelik neyse ona göre değişir:**
- **Hız önceliğiyse**: `Python + pyarrow.csv` (497sn @ 6-worker, en
  hızlısı, yeni bağımlılık gerektirmiyor).
- **Depolama maliyeti önceliğiyse**: `DuckDB` (565sn, sadece ~%14 daha
  yavaş ama ~%23 daha az disk -- 1,5M dosyalık üretim ölçeğinde bu
  MinIO/ClickHouse depolama maliyetinde belirgin fark yaratabilir).
- **Kesinlikle ELENEN**: Python elle-parse implementasyonu -- hem en
  yavaş hem tek sistematik veri kaybı riski taşıyan seçenek, hiçbir
  senaryoda tercih edilir değil.
- **Rust**: artık "varsayılan/tek seçenek" değil ama tamamen elenmiş de
  değil -- güvenilir, orta hızlı, ekstra bağımlılık gerektirmiyor (zaten
  kurulu toolchain). pyarrow.csv/DuckDB'nin ikisi de hız ve/veya
  sıkıştırmada onu geçtiği için üretim varsayılanı olarak öncelik
  taşımıyor, ama üçüncü güvenli alternatif olarak durur.
- **Worker sayısı**: 6, bu makinede hem hız hem güvenilirlik açısından
  doğrulanmış en iyi nokta (Bölüm 18-19) -- ne daha azı (verim kaybı,
  toplam iş daha yavaş biter) ne daha fazlası (N=20'de %35 veri kaybı)
  mantıklı.

  **GÜNCELLEME: bkz. Bölüm 21 -- bu sonuç `row_group_size` küçültülerek
  aşıldı, N=20 artık güvenli VE daha hızlı çıktı.**

## 21. N=20'yi kurtarma -- `row_group_size` küçültme ile %100 başarı ve 2x hız (2026-08-18)

Kullanıcının "chunk boyutlarını 20 worker aynı anda RAM'e sığacak
şekilde ayarlayıp deneyelim" talimatı üzerine, DuckDB'nin
`row_group_size` parametresi (Bölüm 20'deki N=20 başarısızlığının
olası çözümü) gerçekten ölçülerek test edildi.

**Önce izole/küçük ölçekli profil çıkarıldı** (300.000 satırlık örnek
dosya, `resource.getrusage(RUSAGE_CHILDREN).ru_maxrss` ile tek worker
tepe belleği ölçüldü -- container'da `/usr/bin/time` olmadığı için bu
yönteme geçildi):

| row_group_size | Peak bellek (örnek ölçek) | Sıkıştırma | Süre |
|---|---|---|---|
| 50.000 (mevcut) | 1235MB | 2,09x | 20,0sn |
| 20.000 | 654MB | 2,12x | 19,3sn |
| 10.000 | 513MB | 1,99x | 17,4sn |
| 5.000 | 464MB | 1,97x | 17,6sn |

20.000'e düşürmek belleği ~yarıya indirirken sıkıştırmaya zarar
vermiyor (hatta hafif iyileşiyor) -- ama en temkinli/güvenli seçenek
olarak **5.000** gerçek ölçekte test edildi (20 worker × ~464MB ≈
9,3GB, 11GB bütçenin altında marj bırakıyor).

**Gerçek ölçek sonucu (6 kaynak dosya yeniden 20 parçaya bölündü,
`threads=1`, `row_group_size=5000`, 20 worker paralel)**:

| | N=6 (rgs=50.000, Bölüm 14) | N=20 (rgs=50.000, Bölüm 19) | **N=20 (rgs=5.000)** |
|---|---|---|---|
| Toplam süre | 565sn | 664sn | **290sn** |
| Başarı | 6/6 | 13/20 (%65) | **20/20 (%100)** |
| Agregat throughput | 24.425 satır/sn | ~12.990 satır/sn | **47.586 satır/sn** |
| Sıkıştırma | 2,09x | 2,09x (sadece hayatta kalanlar) | 1,97x |
| Peak bellek (`free -h`) | ~4-5GB | 11GB/11GB (doldu, OOM) | ~5,9-6,9GB (rahat marj, hiç OOM yok) |

**`row_group_size`'ı küçültmek hem hipotezi doğruladı hem beklenenden
iyi sonuç verdi**: N=20 sadece güvenli hale gelmekle kalmadı, AYNI
toplam veriyi (60GB, 13.800.000 satır) N=6'nın **neredeyse yarı
sürede** işledi (290sn vs 565sn, ~1,95x hızlanma) -- sıkıştırma sadece
~%6 düşük (1,97x vs 2,09x), kabul edilebilir bir bedel.

**Metodolojik dürüstlük notu**: bu testte üç değişken aynı anda
değişti -- worker sayısı (6->20), `row_group_size` (50.000->5.000),
VE dosya boyutu (10GB->2,74-3,65GB parça, çünkü sadece 6 kaynak
dosyamız var). Hızlanmanın tam olarak hangi değişkenden geldiği (daha
küçük row-group'un kendisi mi, daha küçük dosyaların I/O paralelliğini
daha iyi kullanması mı, yoksa ikisinin birleşimi mi) ayrıştırılmadı --
bunu izole etmek için N=6'yı da aynı küçük parçalarla ve
`row_group_size=5000` ile tekrar test etmek gerekir (yapılmadı).
Pratik/eyleme geçirilebilir sonuç yine de net: **bu kombinasyon
(N=20, `row_group_size=5000`, ~3GB'lık parçalar) hem güvenilir hem
mevcut N=6/rgs=50000 yaklaşımından belirgin daha hızlı.**

**Sonuç -- Bölüm 20'nin "worker sayısı 6" tavsiyesi güncellendi**:
Sabit `row_group_size=50000` varsayımı altında 6 doğruydu. Ama
`row_group_size` de bir ayar değişkeni olarak ele alınınca, daha
yüksek worker sayıları (20'ye kadar test edildi, daha da yükseği
denenmedi) hem güvenli hem daha hızlı olabiliyor -- **doğru üretim
ayarı tek bir sabit worker sayısı değil, worker sayısı VE
`row_group_size`'ın birlikte, hedef makinenin RAM bütçesine göre
ayarlanması gereken bir çift.** Üretim sunucusunda (256 çekirdek,
farklı RAM bütçesi) bu ikili yeniden ölçülmeli; bu makinedeki 20/5.000
kombinasyonu doğrudan oraya taşınabilir bir "sihirli sayı" değil,
metodoloji taşınabilir.

## 22. `row_group_size=20000` denemesi -- sistem çökmesi, örnek-ölçek tahmini tutmadı (2026-08-18)

Bölüm 21'deki küçük-örnek profilinde `row_group_size=20000` en iyi
nokta gibi görünmüştü (654MB/worker, sıkıştırma 2,12x -- 50.000'den
bile iyi). Kullanıcının "6-7GB kullanmışız, hâlâ yerimiz var" gözlemi
üzerine bu değer gerçek ölçekte (766binlik tam parçalar, N=20) test
edildi.

**Sonuç: örnek-ölçek tahmini gerçek ölçekte tutmadı, tam sistem
kilitlenmesi.** İlk 50 saniyede bellek 8,5Gi->9,0Gi'ye tırmandı, hemen
ardından `docker exec`/`docker stats`/`docker top` -- yani Docker
daemon'ının container'a dokunan HER işlemi -- 40-90 saniye içinde
cevapsız kaldı. Host makine sağlıklıydı (8,7GB boş RAM, Docker Desktop
process'leri "Responding: True"), ama WSL2 VM'i içeride tamamen
tıkanmıştı. ~20 dakika bekleme + kurtarma denemesinden sonra Docker
Desktop zaten kendiliğinden çökmüştü (`npipe` soketi kayboldu) --
**manuel restart (`Stop-Process` + `wsl --shutdown` + yeniden başlatma)
gerekti.** Container ve volume sağlam kaldı (crash'ten etkilenmedi),
ama testin kendisi **20/20 worker de yarım kaldı** (250-360MB'lık
kesik parquet dosyaları, hiçbiri tamamlanmadı, tek bir exit kodu bile
yazılamadı).

**Bu, `row_group_size=50000`'in verdiği hatadan (Bölüm 19) NİTELİKSEL
OLARAK DAHA KÖTÜ bir başarısızlık modu**: rgs=50000'de OOM-killer
devreye girip başarısız worker'ları temiz şekilde öldürüyordu (anında
0-byte dosya, net exit kodu 137, üretim manifestosu bunu kolayca
yakalar). rgs=20000'de ise sistem OOM-killer'ın müdahale edemeyeceği
kadar ağır bir swap-thrashing'e girip **tamamen donuyor** -- dışarıdan
müdahale olmadan süresiz askıda kalabilirdi, üretimde bu "sessizce
saatlerce takılı kalan" bir worker havuzu anlamına gelir, tespiti çok
daha zordur.

**Neden küçük-örnek tahmini tutmadı**: 300.000 satırlık örnek dosya
gerçek parçaların (766.667 satır) ~%39'u büyüklüğünde -- DuckDB'nin
CSV okuyucusunun kendi iç tamponlaması (auto-detect/sniffing, okuma
ilerisi tamponu) muhtemelen dosya boyutuyla orantısız büyüyor, salt
`row_group_size`'a bağlı değil. Yani örnek-ölçek profilleme
`row_group_size`'ın YÖNÜNÜ (küçültmek belleği azaltır) doğru
gösterdi ama MUTLAK değerleri güvenilir şekilde tam ölçeğe
taşımadı -- bu bir daha dikkate alınmalı, tam ölçekte doğrulanmamış
bir ayarı üretime almadan önce mutlaka gerçek dosya boyutuyla test
etmek gerekir.

**Güncel durum**: `row_group_size=5000` hâlâ tek doğrulanmış güvenli
N=20 ayarı (Bölüm 21, gerçek ölçekte 20/20 başarılı, ~5,9-6,9GB peak
bellek). `20000` denenmemeli/güvenilmez olarak işaretlendi. Aradaki
değerler (10000, 15000) test edilmedi.

**GÜNCELLEME: bkz. Bölüm 23 -- `row_group_size=10000` test edildi,
5.000'den hem hızlı hem daha iyi sıkıştırılmış çıktı, yeni en iyi
nokta.**

## 23. `row_group_size=10000` -- yeni en iyi nokta, container'ın tam sıfırdan kurulumu (2026-08-18)

Bölüm 22'deki çökme sırasında, kullanıcının host diskini kontrol etmesi
üzerine **host C: diski 0,4GB'a kadar düşmüş** olduğu bulundu --
sorumlusu `docker_data.vhdx` (WSL2/Docker'ın sanal disk dosyası),
158,8GB'a şişmişti (container içinde `rm -rf` ile silinen dosyalar
vhdx'i küçültmüyor, sadece boş alan olarak içeride tutuluyor -- Bölüm
12'de belgelenen davranışın aynısı). Kullanıcı Docker Desktop'ın
"Clean/Purge data" (WSL2) özelliğini çalıştırdı: host disk 0,4GB->
155,3GB'a döndü **ama bu, `t2p-cmp3` container'ını ve `t2p-work4`
volume'unu da tamamen sildi** (imaj dahil). Kaynak `.tab` dosyaları
host'ta (vhdx'in dışında) olduğu için etkilenmedi.

**Container sıfırdan yeniden kuruldu**:
```
docker volume create t2p-work4
docker run -d --name t2p-cmp3 \
  -v "c:/Users/PC_4150_YD26/DataProcessingManagement:/host" \
  -v t2p-work4:/work \
  rust:1-bookworm sleep infinity
```
`apt-get` HTTP (port 80, `deb.debian.org`) bağlantısı zaman aşımına
uğradı (muhtemelen Docker Desktop restart sonrası ağın tam
oturmaması ya da bir filtre) ama HTTPS (pypi.org, port 443) çalıştı --
`apt`'a hiç gerek kalmadan `get-pip.py` (`https://bootstrap.pypa.io/get-pip.py`)
ile pip kuruldu (Debian'ın PEP 668 "externally managed environment"
korumasını aşmak için `--break-system-packages` gerekti -- tek
kullanımlık container'da risksiz), ardından `pip install duckdb
pyarrow numpy` ile bağımlılıklar geri geldi (duckdb 1.5.5, pyarrow
25.0.1, numpy 2.4.6).

**Yeniden kurulan container ile `row_group_size=10000` test edildi**
(6 kaynak dosya yeniden 20 parçaya bölündü, bu kez bellek her 10-15
saniyede bir sıkıca izlendi -- Bölüm 22'deki geç fark etme sorununu
önlemek için):

| | N=20 (rgs=50.000) | N=20 (rgs=20.000) | N=20 (rgs=5.000) | **N=20 (rgs=10.000)** |
|---|---|---|---|---|
| Sonuç | 13/20 (%65) | **sistem kilitlendi, 0/20** | 20/20 | **20/20** |
| Süre | 664sn | tamamlanamadı | 290sn | **235sn** |
| Sıkıştırma | 2,09x (sadece hayatta kalanlar) | -- | 1,97x | **1,99x** |
| Peak bellek | 11GB/11GB (doldu) | 9GB'da kilitlendi | ~5,9-6,9GB | ~5,0-5,6GB (en düşük!) |
| Agregat throughput | ~12.990 satır/sn | -- | 47.586 satır/sn | **58.723 satır/sn** |

**`row_group_size=10000`, `5000`'e göre de net üstün çıktı** -- hem
daha hızlı (235sn vs 290sn) hem daha iyi sıkıştırılmış (1,99x vs
1,97x) hem daha düşük bellek kullanan (~5,0-5,6GB vs ~5,9-6,9GB).
Yani belek-sıkıştırma-hız üçgeninde `5000` ile `10000` arasında bir
"trade-off" yokmuş -- `10000` üçünde de `5000`'i geçiyor. `20000` ise
hâlâ güvenilmez/tehlikeli olarak işaretli kalıyor (Bölüm 22).

**Yeni en iyi doğrulanmış DuckDB ayarı bu makinede: `threads=1`,
`row_group_size=10000`, N=20 worker.** Bu, Bölüm 14'teki orijinal
N=6/`row_group_size=50000` yaklaşımına (565sn) göre **~2,4x hızlanma**
sağlıyor (235sn), güvenilirlik kaybı olmadan (20/20 vs 6/6, ikisi de
%100). `row_group_size=15000` gibi aradaki değerler hâlâ test
edilmedi, ama `10000` zaten hem `5000`'den hem (görünüşe göre)
`20000`'in güvensizliğinden daha iyi bir nokta olduğu için ek
tarama şu an için gerekçesiz görünüyor.

## 24. MinIO -> ClickHouse yükleme darboğazı -- s3() toplu yükleme ile ~17-20x hızlanma (2026-08-18)

Kullanıcı, henüz üretimde olmayan ama tasarım aşamasında endişe
duyulan bir sonraki darboğazı sordu: MinIO'daki parquet dosyalarının
ClickHouse'a taşınması. Bu, plan dokümanının Bölüm 3.4'ünde ZATEN
öngörülmüştü ("1.5M dosya için 1.5M ayrı INSERT atılmamalı... ya
uygulama tarafında biriktirip az sayıda büyük INSERT, ya da
ClickHouse'un S3()/file() fonksiyonlarıyla toplu yükleme") ama hiç
gerçek ölçekte doğrulanmamıştı. Repo'daki tek mevcut ClickHouse yükleme
kodu (`dagster/assets/clickhouse.py`, AU-AIR prototip verisiyle) tam
olarak uyarılan anti-pattern'i kullanıyordu: `pandas` DataFrame ->
Python tuple listesi -> `clickhouse_driver.execute(INSERT...VALUES,
rows)`.

**Test ortamı**: Docker'da MinIO + ClickHouse + `t2p-cmp3` (client)
aynı network'te (`t2p-net`) ayağa kaldırıldı. Test verisi: mevcut
`.tab` kaynaklarından 100.000 satırlık bir dilim, doğrulanmış DuckDB
ayarıyla (`threads=1`, `row_group_size=10000`) parquet'e çevrildi
(1001 sütun: `timestamp` + `f0`..`f999`, hepsi Float64) ve MinIO'ya
yüklendi. ClickHouse'da eşleşen şema ile `MergeTree` tablosu
oluşturuldu (`ORDER BY timestamp`).

**Sonuçlar (100.000 satır, tek dosya)**:

| Yöntem | Süre | Throughput |
|---|---|---|
| Naive `pandas`+`clickhouse_driver` INSERT (mevcut `dagster/clickhouse.py` deseni) | 37,11sn (10,78sn pandas dönüşüm + 26,34sn INSERT) | 2.694 satır/sn |
| **ClickHouse `s3()` toplu yükleme** (`INSERT INTO ... SELECT * FROM s3(...)`) | **2,12sn** | **47.159 satır/sn** |

**~17,5x hızlanma**, Python/pandas'ı devre dışı bırakıp veriyi
ClickHouse'un kendi C++ motoruna (S3 okuma + parquet decode + insert
hepsi native) bırakarak -- bugün DuckDB'de yaptığımız "satır-satır
Python işlemeyi bypass et" prensibinin birebir aynısı.

**Wildcard çoklu-dosya testi** (planın asıl önerdiği çözüm: "1,5M ayrı
sorgu" sorununu gidermek için tek sorguda çok dosya): aynı içerikten 4
kopya daha MinIO'ya yüklenip `s3('http://minio:9000/telemetry/bench/sample_*.parquet', ...)`
ile TEK SORGUDA 5 dosya (500.000 satır) yüklendi: **9,16sn, 54.595
satır/sn** -- tekil dosya testinden bile hafif daha hızlı, yani
wildcard ile çoklu dosya yüklemenin dosya başına ek bir maliyeti YOK.
Doğruluk teyit edildi: ClickHouse'daki `sum(f0)` (-711210,13188),
kaynak parquet'in DuckDB ile hesaplanan toplamıyla (×5) birebir eşleşti.

**"Too many parts" endişesi kısmen doğrulandı**: tek wildcard INSERT
(5 dosya, 500.000 satır) ClickHouse'ta **4 aktif parça** oluşturdu (muhtemelen
ClickHouse'un kendi iç blok boyutu sınırından, dosya sayısından değil).
Bu, "dosya başına 1 parça" ya da "satır başına 1 parça" senaryosundan
çok daha iyi -- 1,5M dosyayı örneğin 100-1000'lik gruplar halinde
wildcard ile yükleseydik, toplam parça sayısı background merge
sürecinin rahatça yetişebileceği bir aralıkta kalır.

**Sonuç/tavsiye**:
- **MinIO->ClickHouse için Python/pandas tabanlı satır-satır INSERT
  KULLANILMAMALI** -- `dagster/assets/clickhouse.py`'deki mevcut
  desen, gerçek 1,5M dosyalık ölçeğe hiç taşınmamalı, sadece küçük
  ölçekli prototip/demo amaçlı kaldı.
- **Üretim deseni**: dönüştürülen parquet dosyaları MinIO'ya
  yazıldıkça, Postgres manifest tablosundaki durumu "hazır" olan
  dosyalar periyodik olarak (örn. saatlik/günlük batch, ya da N dosya
  biriktiğinde) `INSERT INTO ... SELECT * FROM s3('.../*.parquet',
  ...)` ile wildcard/glob pattern kullanılarak TOPLU yüklenmeli --
  dosya başına ayrı sorgu değil.
- **Açık kalan tasarım kararı (plan açık sorular #4)**: partition
  key/`ORDER BY` stratejisi hâlâ üretim şemasına göre netleştirilmeli
  -- bu testte basitlik için ham `timestamp` (Float64) ile `ORDER BY
  timestamp` kullanıldı, gerçek şemada uygun bir `DateTime64` sütunu +
  `PARTITION BY toYYYYMM(...)` gibi bir strateji (mevcut
  `dagster/assets/clickhouse.py`'nin zaten kullandığı desen) daha
  doğru olur; bu, hem sorgu performansını hem merge/parça davranışını
  etkiler, ayrı ele alınmalı.
- **Ölçek notu**: bu testte 100k-500k satırlık küçük örneklerle
  çalışıldı, sonuçlar oranlar (17-20x) olarak güvenilir ama gerçek
  `.ham` verisinin satır yoğunluğu netleşmeden 1,5M dosyalık tam
  senaryo için kesin süre tahmini yapılmadı.

## 25. ClickHouse partition key kararı -- İHA bazlı vs zaman bazlı vs bileşik, gerçek ölçümle karar (2026-08-18)

Kullanıcı "sorgular genelde araç bazlı olur, İHA'ya göre gruplamak
daha iyi olmaz mı" diye sordu -- makul bir itiraz, varsayımla değil
ölçerek cevaplandı. Üç ClickHouse tablosu (aynı şema: `timestamp
DateTime`, `iha_id UInt8`, `latitude/longitude/altitude Float64`,
100.000.000 satır, 6 İHA, 1 yıllık zaman aralığı, veri doğrudan
ClickHouse SQL'iyle (`numbers()`+`rand()`) üretildi -- parquet/MinIO
adımı bu testte YOK, sadece ClickHouse'un kendi sorgu motoru
ölçülüyor) oluşturuldu:

- `tel_time_part`: `PARTITION BY toYYYYMM(timestamp)`, `ORDER BY (iha_id, timestamp)` -- 52 aktif parça
- `tel_iha_part`: `PARTITION BY iha_id`, `ORDER BY (iha_id, timestamp)` -- 54 aktif parça
- `tel_composite`: `PARTITION BY (iha_id, toYYYYMM(timestamp))`, `ORDER BY (iha_id, timestamp)` -- **372 aktif parça**

**Üç sorgu senaryosu, her biri 3 kez çalıştırılıp en iyi süre alındı**:

| Senaryo | `tel_time_part` (aylık) | `tel_iha_part` (İHA) | `tel_composite` (bileşik) |
|---|---|---|---|
| Araç + dar zaman (iha_id=3, 1 hafta) | **6,7ms** | 7,6ms | 27,0ms |
| Sadece araç filtresi (iha_id=3, tüm zamanlar) | 37,3ms | **25,9ms** | 45,7ms |
| Sadece zaman filtresi (1 hafta, tüm araçlar) | **13,3ms** | 16,5ms | 21,6ms |

**Bulgular**:
1. Araç bazlı sorgu hızı **`ORDER BY (iha_id, timestamp)`'tan geliyor, `PARTITION BY`'dan değil** -- her üç şemada da `iha_id` `ORDER BY`'ın başında olduğu için araç filtreli sorgular zaten hızlı; `PARTITION BY` seçimi bunu marjinal etkiliyor.
2. Kullanıcının sezgisi kısmen doğrulandı: `PARTITION BY iha_id`, SADECE "araç filtresi, zaman filtresi yok" senaryosunda gerçekten daha hızlı (~%30, 37,3ms->25,9ms) -- çünkü tek partition'a gidiyor, 12 ayı taramıyor. Ama diğer iki senaryoda (ikisi de zaman filtresi içeriyor) aylık partition kazanıyor, farklar küçük.
3. **Bileşik partition (`iha_id`+ay) HER ÜÇ senaryoda da EN KÖTÜSÜ çıktı** -- "en iyisini birleştirelim" sezgisi burada ters tepti. Sebep: 6 İHA × 12 ay kombinasyonu 372 aktif parçaya bölünmüş (aylık'ın 52'sine, İHA'nın 54'üne karşı) -- çok daha fazla, çok daha küçük partition/parça demek, her sorgu daha fazla partition metadata'sı açıp taramak zorunda kalıyor, merge de partition sınırını aşamadığı için veri daha az konsolide kalıyor. Beklenen "iki avantajı birleştir" sonucu yerine "iki dezavantajı birleştir" oldu.

**Karar (ölçümle doğrulandı, varsayımla değil)**: `PARTITION BY
toYYYYMM(timestamp)` + `ORDER BY (iha_id, timestamp)` -- 3 senaryonun
2'sinde en hızlı, kaybettiği senaryoda fark küçük, VE zaman bazlı
arşivleme/eski-veri-silme avantajını koruyor (bkz. Bölüm 24), VE en
sağlıklı parça/merge davranışını veriyor. Bileşik partition fikri
somut olarak elendi -- düşünsel olarak makul görünse de ölçüldüğünde
her senaryoda daha kötü çıktı.

**Not**: Bu test tek seferlik bir anlık görüntü (100M satır bir kerede
yüklendi) -- gerçek üretimde sürekli/artımlı yükleme altında merge
davranışının uzun vadede nasıl evrileceği (özellikle `tel_iha_part`'ın
partition başına sınırsız büyümesi -- Bölüm 24'te tartışılan risk) bu
testte gözlemlenmedi, ayrı bir uzun-vadeli test gerektirir.

## 26. MinIO->ClickHouse yükleme sınırları -- dosya sayısı, eşzamanlı worker, ortam kirlenmesi (2026-08-18)

Kullanıcının "sınırları test edelim" isteği üzerine `s3()` toplu
yüklemenin iki ayrı boyutu ölçüldü: (a) tek sorguda kaç dosya, (b) kaç
eşzamanlı sorgu/worker. Yol boyunca önemli bir metodolojik ders de
çıktı.

### 26.1 Tek sorguda dosya sayısı -- ortam kirlenmesi tuzağı

İlk turda 5->25->100 dosya denendi (aynı bucket'a biriktirerek):
5 dosya 9,16sn (54.595 satır/sn), 25 dosya 70,91sn (35.256 satır/sn),
100 dosya 459,42sn (21.766 satır/sn) -- düzgün bir düşüş gibi
göründü. Ama `max_insert_threads`/`max_download_threads`'i 20'ye
çıkarıp "25 dosyayı" tekrar test edince sonuç **1200sn'de (20dk)
KILL QUERY ile durduruldu** -- ayarları artırmak YARDIMCI OLMADI,
kötüleştirdi. Bunu araştırırken **asıl kök nedenin dosya sayısı
DEĞİL, test ortamının kendisinin saatlerce süren birikmiş yükten
yorulmuş olması** olduğu bulundu:
- `docker stats`: ClickHouse boşta bile %99,93 CPU, 7,2GB bellek
  (idle'da olmaması gereken bir yük)
- Host disk 62,9GB'dan 19,9GB'a düşmüş (vhdx büyümesi, Bölüm 22/12'nin
  aynı paterni), host RAM 2,4GB'a düşmüş
- İzole/temiz bucket'ta bile aynı "25 dosya" testi 235-255sn sürdü
  (ilk ölçümün 70,91sn'sinden ~3,3x yavaş) -- bucket'ın kendisi temiz
  olsa bile SİSTEM genel olarak yorulmuştu

**Çözüm**: kullanıcı Docker Desktop "Clean/Purge data" çalıştırdı
(disk 19,9->155,8GB), tüm container'lar (`t2p-cmp3`, `minio`,
`clickhouse`, imajlar) sıfırdan yeniden kuruldu (`apt` yerine yine
`get-pip.py` + `--break-system-packages`, Bölüm 23'teki yöntemin
aynısı). **Temiz ortamda tekrar ölçüm**: 5 dosya 15,21sn (32.873
satır/sn), 25 dosya 84,58sn (29.559 satır/sn) -- ilk (kirlenmemiş)
ölçümlere çok daha yakın, throughput 5->25 dosyada sadece ~%10
düşüyor (önceki yanlış "felaket" düşüşü ortam kirliliğinden kaynaklıydı).

**Ders**: Uzun süren (saatler süren) benchmark oturumlarında **ortamın
kendisi bir değişken haline geliyor** -- WSL2/Docker'ın disk (vhdx
thin-provisioning büyümesi) ve bellek (WSL2'nin belleği Windows'a geç
iade etmesi, normal ama birikimli) davranışı, ölçtüğümüz asıl
parametreden (dosya sayısı) bağımsız olarak sonuçları kirletebiliyor.
Uzun test turlarında ARA SIRA "temiz ortamda tekrar ölç" kontrolü
yapmadan, tek bir uzun oturumun sonuçlarına güvenilmemeli.

**Ek bulgu -- CPU kullanımı düşük, iş I/O-bound**: temiz ortamda 25
dosyalık bir yükleme sırasında `docker stats` ile canlı CPU izlendi:
%300-432 (20 çekirdeğin ~%15-22'si) -- yani `max_threads=20` olsa da
sorgu CPU'yu doldurmuyor, ağ/disk I/O'da bekliyor. Bu, thread sayısını
artırmanın neden işe yaramadığını (üstte, 1200sn'lik çökme) açıklıyor.

**Ek bulgu -- soğuk/sıcak önbellek etkisi**: aynı 25 dosya İKİNCİ kez
okunduğunda süre 84,58sn->53,97sn'ye düştü (throughput 29.559->46.319
satır/sn) -- MinIO/OS önbelleği "ısınmış". Üretimde her dosya
muhtemelen sadece 1 kez okunacağı için, İLK okuma (soğuk) sayıları
gerçek senaryoyu temsil eder, tekrar okuma sayıları aşırı iyimserdir.

### 26.2 Eşzamanlı worker sayısı -- N=4'te bellek limiti aşıldı

Darboğazın I/O-bound olduğu bulununca, "daha fazla eşzamanlı sorgu
CPU'nun boşta kalan kısmını doldurabilir mi" test edildi. Her N için
TAMAMEN TAZE (önceden hiç okunmamış) 10'ar dosyalık izole gruplar
kullanıldı (soğuk-önbellek etkisini karıştırmamak için), hepsi aynı
hedef tabloya (`telemetry_concurrent`) paralel yazdı:

| N | Toplam süre | Başarı | Agregat throughput |
|---|---|---|---|
| 1 | 18,58sn | 1/1 (%100) | 53.821 satır/sn |
| 2 | 32sn | 2/2 (%100) | **62.500 satır/sn (en iyi)** |
| 4 | 91sn | **3/4 (%75)** | ~32.967 satır/sn (düşüş + kayıp) |

N=4'te bir worker **temiz bir hatayla** düştü (sessiz veri kaybı
değil):
```
DB::Exception: (total) memory limit exceeded: would use 9.13 GiB
(attempt to allocate chunk of 3.00 MiB), current RSS: 7.91 GiB,
maximum: 9.12 GiB.
```
Bu, ClickHouse'un `max_memory_usage=0` (sorgu başı sınırsız) olmasına
rağmen **sunucu geneli toplam bellek limitine** (container'ın 11,68GB
bütçesinin ~%78'i, muhtemelen ClickHouse'un varsayılan
`max_server_memory_usage` payı) çarptığını gösteriyor.

**Sonuç**: N=1->2 arası paralellik net kazanç veriyor (+%16 agregat
throughput, klasik "bireysel worker yavaşlıyor ama toplam iş hızlanıyor"
paterni -- N=1 18,58sn/worker'dan N=2'de ~31,5sn/worker'a çıktı ama
2 tane aynı anda).

**N=3 ayrıca test edildi (yine tamamen taze/izole 3x10 dosyalık
gruplarla) -- beklenmedik ve önemli bir sonuç verdi**: 129sn, 3/3
başarılı (%100) ama agregat throughput **23.256 satır/sn'ye çöktü** --
N=1'den (53.821) bile kötü. Bellek izlemesi boyunca sürekli 7,6Gi'den
9,1Gi'ye tırmandığı görüldü -- tam N=4'ün çöktüğü ~9,12GB sınırına
yaklaşmış ama aşmamış. Yorum: N=4'te "kırmızı bölge" (açık hata) var,
N=3'te ise sınıra yaklaşırken ClickHouse'un kendini sessizce
yavaşlattığı (muhtemelen bellek baskısı altında ek disk spill/throttle)
bir **"sarı bölge"** var -- büyüklük olarak çökmeden önce performans
zaten ciddi bozuluyor.

**GÜNCEL SONUÇ: bu makinede gerçek güvenli tavan N=2, "2 ile 4 arası"
değil.** N=3 zaten belirgin bozulma gösteriyor (N=1'den bile yavaş),
N=4 açıkça çöküyor -- tab->parquet'teki DuckDB N=20 testine (Bölüm 19)
benzer ama daha erken/keskin bir "verinin bir kısmını kaybetmeden
büyütülebilecek worker sayısı çok sınırlı" deseni, burada tavan 6
değil 2.

**Genel tavsiye (üretim tasarımı için)**: MinIO->ClickHouse yükleme
worker'ları (a) dosya başına değil, onlarca-dosyalık batch'ler
halinde (`s3()` wildcard, Bölüm 24) çalışmalı, (b) eşzamanlı **2**
worker'ı aşmamalı (bu makinede -- üretim sunucusunda ClickHouse'un
kendi bellek payı farklı olacağı için bu sayı yeniden ölçülmeli), (c)
"memory limit exceeded" gibi net hatalar Postgres manifest'te retry
tetiklemeli, sessiz kayıp riski düşük görünüyor bu senaryoda.

### 26.3 `max_download_threads` düzeltmesi -- ölçülü artış (4->8) gerçekten yardımcı oluyor

Bölüm 26.1'deki "thread artırmak kötüleştirdi (1200sn'de KILL QUERY)"
sonucu KİRLİ ortamda ölçülmüştü, güvenilmezdi -- temiz ortamda,
`max_insert_threads`+`max_download_threads` ikisini birden aşırı
değere (20/20) değil, sadece `max_download_threads`'i ölçülü bir
değere (4->8) çıkararak tekrar test edildi (iki TAMAMEN taze/izole
10'ar dosyalık grup, cold-cache karşılaştırması adil kalsın diye):

| Ayar | Süre | Throughput |
|---|---|---|
| Varsayılan (`max_download_threads=4`) | 44,27sn | 22.591 satır/sn |
| **`max_download_threads=8`** | **32,93sn** | **30.368 satır/sn (~%34 daha hızlı)** |

**Düzeltme**: Bölüm 26.1'in "thread artırmak yardımcı olmuyor" sonucu
YANLIŞTI -- o ölçüm kirli ortamda yapılmıştı. Ölçülü bir artış (4->8)
gerçek ve belirgin bir kazanç veriyor. Daha yüksek değerler (12, 16,
20) bu oturumda tekrar bellek/host baskısı nedeniyle test edilemedi --
`max_download_threads=8`, N=2 eşzamanlı worker ile birlikte
(Bölüm 26.2) kullanılabilecek, doğrulanmış bir başlangıç noktası.

**Metodolojik ders (tekrar)**: Bölüm 26.1'de zaten çıkarılan "kirli
ortamda ölçüm güvenilmez" dersi burada ikinci kez doğrulandı -- aynı
parametrenin (thread sayısı) etkisi kirli/temiz ortamda TERS yöne
çıktı (kötüleşme vs %34 iyileşme). Ortam sağlığı kontrol edilmeden
alınan "X işe yaramıyor" sonuçlarına güvenilmemeli.

**Devamı -- tam eğri 4'ten 20'ye kadar çıkarıldı, kesintisiz kazanç.**
Aradaki test sırasında `vmmemWSL` host RAM'ini yine ~9GB'a kadar
tüketti (host'ta 0,7GB boş kaldı) -- bu kez purge değil, kullanıcının
`wsl --shutdown` komutu ile çözüldü (RAM 0,7->7,9GB'a döndü). **Önemli
fark**: `wsl --shutdown`, Docker Desktop'ın "Clean/Purge data"
özelliğinden farklı olarak container/volume verisini SİLMİYOR --
sadece durduruyor, `docker start` ile hepsi (paketler, MinIO verisi
dahil) aynen geri geldi, yeniden kurulum gerekmedi. (Yan etkisi hâlâ
geçerli: bu makinedeki TÜM WSL tabanlı Docker container'larını
durdurur, sadece bizimkini değil.)

| `max_download_threads` | Süre | Throughput |
|---|---|---|
| 4 (varsayılan) | 44,27sn | 22.591 satır/sn |
| 8 | 32,93sn | 30.368 satır/sn |
| 12 | 27,86sn | 35.888 satır/sn |
| 16 | 23,12sn | 43.259 satır/sn |
| **20** | **19,49sn** | **51.316 satır/sn (4'e göre +%127)** |

Her adım (10'ar dosyalık TAMAMEN taze/izole gruplarla, cold-cache
adil kalsın diye) kesintisiz kazanç verdi, hiç çökme olmadı --
`max_download_threads=20` TEK BAŞINA (yalnızca bunu artırıp
`max_insert_threads`'e dokunmadan) sorunsuz ve en iyi sonucu verdi.
**Sonuç: önceki çökme (Bölüm 26.1, 1200sn) hem kirli ortamdan hem
`max_insert_threads`+`max_download_threads` ikisinin BİRLİKTE 20'ye
çıkarılmasından kaynaklanıyormuş -- sadece `max_download_threads`'i
artırmak (container'ın mantıksal çekirdek sayısı olan 20'ye kadar)
tamamen güvenli ve güçlü bir kazanç kaynağı.**

**GÜNCELLEME -- kombinasyon test edildi, tahmin YANLIŞ çıktı (bkz.
Bölüm 26.4).** "Muhtemelen çarpımsal fayda sağlarlar" hipotezi
ölçüldü ve reddedildi -- N=2 + `max_download_threads=20` birlikte,
ikisinden AYRI AYRI daha kötü sonuç verdi.

### 26.4 N=2 + max_download_threads=20 kombinasyonu -- birbirini beslemiyor, çakışıyor

İki taze/izole 10'ar dosyalık grup, N=2 eşzamanlı worker ile, her biri
`max_download_threads=20` ayarıyla aynı hedef tabloya yüklendi:

| Konfigürasyon | Süre | Agregat throughput |
|---|---|---|
| N=2, varsayılan thread (Bölüm 26.2) | 32sn | **62.500 satır/sn (en iyi)** |
| N=1, `max_download_threads=20` (Bölüm 26.3) | 19,49sn | 51.316 satır/sn |
| **N=2 + `max_download_threads=20` (bu test)** | 46sn | **43.478 satır/sn (İKİSİNDEN DE KÖTÜ)** |

**Yorum**: İki optimizasyon "farklı darboğazları hedefliyor, çarpımsal
fayda sağlar" varsayımı YANLIŞ. Aslında ikisi de AYNI kısıtlı kaynağı
(gerçek I/O bant genişliği -- tek MinIO instance'ı/Docker ağı)
paylaşıyor: N=2 worker × 20 download thread = aynı anda 40'a kadar
bağlantı aynı darboğaza yükleniyor, birbirine eklenmek yerine
çakışıyor/rekabet ediyor (bu oturumun çok daha önceki bir bölümünde,
native Windows'ta 12 paralel worker'ın disk I/O'sunu ~148MB/s'e
çökerttiği bulgusuyla aynı aile).

**GÜNCEL/NİHAİ ÖNERİ (üretim için)**: İkisini birleştirmeye çalışmak
yerine **TEK bir optimizasyonu seç, ikisini birden değil**:
- Tek worker/az sayıda eşzamanlı yükleme yeterliyse: `max_download_threads`'i
  yüksek tut (bu makinede 20, hedef sunucunun çekirdek sayısına göre
  ölçeklenmeli).
- Çok sayıda eşzamanlı yükleme gerekiyorsa (üretimde muhtemel senaryo
  -- sürekli akan iş kuyruğu): N=2'yi varsayılan thread ayarlarıyla
  kullan, `max_download_threads`'i artırma -- gerçek I/O bant
  genişliğini paylaşan birden fazla worker zaten var, üstüne thread de
  eklemek darboğazı büyütüyor.
- İkisi ARASINDAKİ optimal denge (örn. N=2 + threads=8, ya da N=3 +
  threads=4 gibi ara noktalar) bu oturumda test edilmedi, üretim
  donanımında gerekirse ayrıca taranmalı.

**GÜNCELLEME -- N=2 için ara thread değerleri tarandı, sonuç
GÜRÜLTÜLÜ/tutarsız çıktı, tekrar taramaya değmez (bkz. altta).**

### 26.5 N=2 için thread ince ayarı -- güvenilir bir örüntü bulunamadı

N=2 sabit tutulup `max_download_threads` 6 ve 10 değerleriyle (her
biri tamamen taze/izole 2x10 dosyalık gruplarla) test edildi:

| N=2, thread ayarı | Süre | Throughput |
|---|---|---|
| **4 (varsayılan)** | 32sn | **62.500 satır/sn** |
| 6 | 62sn | 32.258 satır/sn |
| 10 | 71sn | 28.169 satır/sn |
| 20 (Bölüm 26.4) | 46sn | 43.478 satır/sn |

**Düzgün/monoton bir eğri değil** -- 4'ten sonra sürekli kötüleşiyor,
20'de kısmen toparlanıyor ama 4'ün gerisinde kalıyor. Bu, güvenilir
bir "optimal ara nokta" olduğunu göstermiyor; büyük ihtimalle çoğunlukla
ölçüm gürültüsü (her test farklı zamanda, host/WSL2 kaynak durumu
hafifçe farklı, tekrar/ortalama alınmadı, tek seferlik ölçüm).

**Sonuç: bu makinede N=2 için elle thread ayarlamanın güvenilir bir
kazancı YOK, hatta varsayılan (4) en iyisi çıktı.** Daha fazla ince
ayar aramak bu noktada gürültüyü kovalamak olur -- durduruldu.
**Nihai, sade tavsiye**: N=2 çalıştırılacaksa thread ayarına hiç
dokunulmamalı (varsayılan kalsın); tek worker yeterliyse
`max_download_threads` yükseltilmeli (hedef makinenin çekirdek
sayısına göre).

### 26.6 Dosya boyutu etkisi izole edildi -- çok-küçük dosya az-büyük dosyadan hafif hızlı

Bölüm 26.1'de dosya SAYISI test edilirken toplam veri hacmi de birlikte
büyüyordu (5 dosya=1,14GB, 25 dosya=5,7GB) -- "dosya sayısı" ile
"toplam hacim" ayrıştırılmamıştı. Bu kez SABİT toplam hacimde (~2,3GB,
1.000.000 satır) iki yapı karşılaştırıldı:

| Yapı | Süre | Throughput |
|---|---|---|
| Az-büyük (2 dosya × 1138,5MB, 500k satır/dosya) | 82,74sn | 12.086 satır/sn |
| **Çok-küçük (40 dosya × 57,1MB, 25k satır/dosya)** | **70,11sn** | **14.264 satır/sn (~%18 daha hızlı)** |

**Sonuç -- beklenenin tersi**: Çok sayıda küçük dosya, az sayıda büyük
dosyadan biraz daha hızlı çıktı. Sezgisel beklenti "az dosya = az
per-dosya overhead, daha hızlı" yönündeydi ama tam tersi oldu --
muhtemelen ClickHouse'un `s3()` okuyucusu (varsayılan
`max_download_threads=4` ile bile) birden fazla dosya arasında bir
miktar okuma paralelliği sağlıyor; sadece 2 büyük dosyada bu
paralellik fırsatı çok sınırlı kalıyor (en fazla 2 dosya aynı anda
okunabilir, 40 dosyada çok daha fazla eşzamanlı okuma fırsatı var).
**Pratik sonuç: dosya boyutu küçük/orta kalsın diye endişelenmeye
gerek yok -- MinIO->ClickHouse yükleme mekanizması, üretimdeki
muhtemel dosya boyutu dağılımına (küçükten büyüğe) karşı esnek
görünüyor, ekstra bir "dosyaları birleştir" adımına gerek yok.**

### 26.7 Bellek tavanını büyütme denemesi -- N=3'ü yavaşlattı, N=4'ü kurtarmadı

ClickHouse'un sunucu geneli bellek limiti (`max_server_memory_usage_to_ram_ratio`,
varsayılan 0,9 -> ~9,17GB) container restart edilmeden (config.d'ye
XML override + `docker restart clickhouse`, veri kaybı yok) 0,95'e
çıkarılıp ~10,64GB'a yükseltildi. Ardından N=3 ve N=4 tamamen taze/izole
verilerle tekrar test edildi:

- **N=3 (yüksek tavan)**: 186sn, 3/3 başarılı ama Bölüm 26.2'deki
  orijinal ölçümden (129sn) DAHA YAVAŞ -- tavanı büyütmek yardımcı
  olmadı, kötüleşti.
- **N=4 (yüksek tavan)**: 155sn, **hâlâ 3/4 (%75) başarı** -- bir
  worker yine "(total) memory limit exceeded" ile düştü, bu kez
  ~9,80GB'da (yeni ~10,64GB tavanına yakın ama tam değil).

**Sonuç: bellek tavanını ölçülü büyütmek (0,9->0,95) NE N=3'ü
hızlandırdı NE N=4'ü kurtardı.** Bu, sorunun "ClickHouse'un tavanı
çok muhafazakar" olmadığını, gerçekten **4 eşzamanlı worker'ın
(1001 sütunlu geniş parquet ile) taleplerinin bu container bütçesini
aştığını** gösteriyor -- küçük bir tavan artışı yetersiz, sorunu
çözmek için ya çok daha büyük bir bellek artışı (host/WSL2 riskiyle,
bugün zaten defalarca sorun yaşadık) ya da worker başına veri/bellek
ayak izini küçültmek (tab->parquet'teki `row_group_size` küçültme
dersine benzer, MinIO->ClickHouse tarafında henüz denenmedi) gerekir.
Config değişikliği geri alındı (varsayılan 0,9'a dönüldü), veri
kaybı olmadı. **Nihai sonuç değişmedi: bu makinede güvenli tavan N=2.**

### 26.8 Sürekli/artımlı yükleme testi -- merge sağlığı 20 batch boyunca gözlemlendi, güven verici

Bölüm 25'te açık bırakılan soru ("gerçek üretimde sürekli/artımlı
yükleme altında merge davranışının uzun vadede nasıl evrileceği...
ayrı bir uzun-vadeli test gerektirir") test edildi -- kullanıcı molaya
çıkarken bu testi otonom tamamlamam istendi.

**Yöntem**: `telemetry_continuous` tablosuna, 20 ardışık batch (her
biri 3 dosya, 300.000 satır) art arda `s3()` ile yüklendi -- toplamda
6.000.000 satır. Her batch sonrası: o batch'in süresi, kümülatif satır
sayısı, aktif parça sayısı, o an çalışan merge sayısı kaydedildi.

**Sonuçlar (20 batch, tam kayıt plan dosyasında)**:
- **Süre trendi**: batch 0 = 5,81sn, batch 19 = 8,21sn -- ilk 10
  batch ortalaması ~6,12sn, son 10 batch ortalaması ~6,77sn (~%11
  yavaşlama). Hafif ama gerçek bir eğilim var, felaket değil.
- **Aktif parça sayısı**: 9'dan başlayıp genel olarak 24-31 aralığına
  yükseldi ama SINIRSIZ BÜYÜMEDİ -- düzgün monoton artış değil,
  zaman zaman düşüşler de var (örn. batch 9'da 28 -> batch 10'da 19),
  yani background merge periyodik olarak parçaları gerçekten
  konsolide ediyor.
- **Eşzamanlı çalışan merge sayısı**: sürekli 1-4 arasında, hiç
  birikip patlamadı.
- **Hiç hata/çökme olmadı** -- 6M satır tam ve doğru yüklendi, "too
  many parts" gibi bir hataya hiç rastlanmadı.

**Sonuç: bu ölçekte (20 batch, 128sn, 6M satır) ClickHouse'un
background merge süreci yükleme hızına YETİŞEBİLİYOR** -- parça
sayısı sınırlı bir aralıkta dalgalanıyor, süre hafifçe (~%11) yavaşlıyor
ama patlamıyor. **Önemli sınırlama**: bu hâlâ görece kısa/küçük
ölçekli bir test (128 saniye, 6M satır) -- gerçek üretimde saatler/
günler süren, milyonlarca dosyalık sürekli yükleme altında bu hafif
yavaşlama eğiliminin uzun vadede birikip birikmeyeceği (plato mı
yapıyor yoksa yavaşça büyümeye mi devam ediyor) bu testle kesin
kanıtlanmadı -- çok daha uzun soluklu bir test gerektirir, ama bu
kısa testin sonucu **güven verici** (patlama/çökme belirtisi yok).

## 27. KRİTİK DÜZELTME -- Bölüm 24-26.8'deki tüm testler yanlış sütun eşleşmesiyle ölçülmüş (2026-08-18)

Kullanıcının "bench_sample.parquet 1000 sütun demi" sorusu üzerine
şema kontrol edilirken ciddi bir metodoloji hatası bulundu.

**Hata**: Kaynak parquet'in gerçek sütun isimleri `timestamp` + `f0`-`f299`
(300 float) + `b0`-`b699` (700 binary) -- ama Bölüm 24'ten beri TÜM
ClickHouse hedef tabloları
`cols = ["timestamp Float64"] + [f"f{i} Float64" for i in range(1000)]`
ile, yani `f0`'dan `f999`'a kadar 1000 tane "f" ile oluşturulmuştu.
`INSERT INTO ... SELECT * FROM s3(...)` **isme göre eşleştiriyor**
(konuma göre değil) -- doğrulandı: `timestamp=0.012` satırında
kaynakta `f0=-302.816`, yanlış-isimli tabloda `f0` alanı (aslında
kaynağın `b0`'ına denk gelen pozisyonda) **0** çıktı. Yani kaynağın
`f0`-`f299`'u doğru eşleşiyordu ama `b0`-`b699`'u (700 sütun, verinin
%70'i) hedef tabloda karşılığı olmadığı için **sessizce atlanmış/0
olarak dolmuş olabilir**.

**Etkinin ölçülmesi**: Aynı kaynak dosya, iki şemayla yüklendi:

| Şema | Süre | Throughput |
|---|---|---|
| YANLIŞ (`f0`-`f999`, Bölüm 24-26.8'de kullanılan) | 1,52sn | 65.866 satır/sn |
| **DOĞRU (`f0`-`f299`+`b0`-`b699`, gerçek veri)** | **2,36sn** | **42.413 satır/sn** |

**Doğru/tam veriyle yüklemek ~%55 (1,55x) daha yavaş.** Yani Bölüm
24-26.8'deki TÜM mutlak throughput sayıları (örn. "51.316 satır/sn"),
gerçekte kaynağın sadece ~%30'unu (300/1000 sütun) doğru şekilde
işleyip geri kalan ~700 sütunu (muhtemelen S3/parquet okuma
seviyesinde bile atlayarak, kolon projeksiyonu sayesinde) hiç
taşımadan elde edilmiş -- **gerçek/tam veri yükünde bu sayılar ~1,55x
daha düşük olmalı.**

**Neden bu, tüm sonuçları çöpe atmıyor**: Hata Bölüm 24'ten beri
YAPILAN HER TESTTE aynı şekilde (tutarlı olarak) vardı -- yani
testler ARASI karşılaştırmalar (N=2'nin en iyi nokta olması,
`max_download_threads`'in 4->20 artışının fayda sağlaması, küçük
dosyanın büyük dosyadan hızlı olması, N=4'ün bellek limitine
çarpması, bileşik ayarların çakışması, sürekli yüklemenin merge
sağlığı) muhtemelen YÖN olarak hâlâ geçerli -- hepsi aynı "eksik
veri" koşulunda, birbirine göre ölçüldü. **Ama mutlak sayılara
(satır/sn, saniye) güvenilmemeli** -- gerçek değerler yaklaşık
~%35-40 daha düşük (1/1,55 ≈ 0,65 çarpanı) olarak düşünülmeli.

**How to apply**: MinIO->ClickHouse için gerçek/üretim kodu
yazılırken, ClickHouse hedef tablosunun sütun isimlerinin kaynak
parquet'in GERÇEK sütun isimleriyle birebir eşleştiğinden mutlaka
emin olunmalı (`DESCRIBE TABLE s3(...)` ile kaynağın şemasını
doğrulamak, ya da `SELECT * FROM s3(...) LIMIT 0` ile önce şema
kontrolü yapmak iyi bir alışkanlık olur) -- sessiz sütun
eşleşmemesi, hem yanlış/eksik veri yüklenmesine HEM yanıltıcı
performans ölçümlerine yol açabiliyor, ikisi de bu oturumda
somut olarak gözlemlendi.

**Düzeltilmiş nihai sayı -- gerçek en iyi konfigürasyon (N=2 +
varsayılan thread, Bölüm 26.2'nin gerçek/doğru şemayla tekrarı)**:

| | Süre | Throughput |
|---|---|---|
| Yanlış şema (Bölüm 26.2) | 32sn | 62.500 satır/sn |
| **Doğru şema (güvenilir)** | **44sn** | **45.455 satır/sn** |

~%27 daha düşük ama YÖN/KARAR değişmedi -- **N=2 eşzamanlı worker +
varsayılan `max_download_threads` bu makinede hâlâ en iyi doğrulanmış
MinIO->ClickHouse yükleme konfigürasyonu, sadece mutlak sayı
45.455 satır/sn olarak düzeltildi.** Diğer testlerin (N=3/N=4 çöküşü,
dosya boyutu, bellek tavanı, sürekli yükleme) sayıları da benzer bir
oranda (~%25-45) düşük tahmin edilmiş olabilir ama tek tek yeniden
ölçülmedi -- yön/karar geçerliliğini koruyor, sadece mutlak rakamlara
dikkatli yaklaşılmalı.

## 28. Kalan boşluklar -- max_insert_threads, gerçekçi veri, hedef tablo sıkıştırması (2026-08-18, doğru şemayla)

Bölüm 27'nin düzeltmesinden sonra, kalan üç açık soru **doğru şemayla**
(kaynakla birebir eşleşen `f0`-`f299`+`b0`-`b699`) test edildi.

### 28.1 `max_insert_threads` izole test edildi -- tam ters yönde etki

`max_download_threads`'in aksine (Bölüm 26.3, artırmak net kazanç),
`max_insert_threads` TEK BAŞINA (download_threads varsayılanda
bırakılarak) 4'ten 20'ye tarandı:

| `max_insert_threads` | Süre | Throughput |
|---|---|---|
| **4 (varsayılan)** | 18,79sn | **53.207 satır/sn (en iyi)** |
| 8 | 21,84sn | 45.792 satır/sn |
| 12 | 34,32sn | 29.136 satır/sn |
| 16 | 25,09sn | 39.853 satır/sn |
| 20 | 45,60sn | 21.930 satır/sn (en kötü) |

**Sonuç: `max_insert_threads`'i artırmak sürekli KÖTÜLEŞTİRİYOR**
(küçük bir dalgalanma dışında düzgün azalan bir eğri). Mantığı:
download thread'leri S3'ten okurken I/O-bekleme dolduruyor (boşta
CPU'yu kullanıyor, Bölüm 26.3), ama insert thread'leri AYNI hedef
tabloya yazarken birbirleriyle kilit/senkronizasyon rekabeti
yaratıyor -- artırmak yardımcı olmuyor, çakışmayı büyütüyor. **Net
tavsiye: `max_insert_threads`'e hiç dokunulmamalı, varsayılanda (4)
bırakılmalı.** Bu, Bölüm 26.4'teki "N=2+download_threads=20
kombinasyonu neden kötüleşti" bulgusunu da netleştiriyor -- muhtemelen
insert tarafında da benzer bir çakışma etkisi vardı.

### 28.2 Gerçekçi/çeşitli veri ile test -- içerik çeşitliliğinin etkisi yok

Bugüne kadarki TÜM MinIO/ClickHouse testleri `bench_sample.parquet`'in
aynı içerikli kopyalarıyla yapılmıştı -- bu, gerçek/çeşitli üretim
verisini yansıtmıyor olabilirdi (önbellekleme/sıkıştırma davranışı
farklı olabilirdi). Bunu izole etmek için 10 GERÇEKTEN FARKLI dosya
üretildi (2 farklı kaynak `.tab` dosyasından, 5'er farklı satır
aralığından, her biri benzersiz byte içerikli) ve aynı toplam
hacimdeki (10 dosya) "aynı içeriğin 10 kopyası" senaryosuyla
karşılaştırıldı:

| Senaryo | Süre | Throughput |
|---|---|---|
| repeat10 (aynı içeriğin 10 kopyası) | 54,02sn | 18.512 satır/sn |
| diverse10 (10 gerçekten farklı dosya) | 53,57sn | 18.667 satır/sn |

**Sonuç: fark %1'in altında, ölçüm gürültüsü seviyesinde -- içerik
çeşitliliğinin yükleme hızına anlamlı bir etkisi YOK.** Sebep:
bugünkü tüm testler zaten "taze prefix" (hiç önceden okunmamış
MinIO klasörü) metodolojisiyle yapıldı -- kopya içerikli dosyalar
bile ilk okunuşlarında önbellek avantajı elde edemiyordu, yani
metodoloji zaten adildi. **Bugüne kadarki tüm testlerde kopya veri
kullanmış olmamız, sonuçları yapay olarak hızlandırmamış/bozmamış.**

**Not -- mutlak sayı tutarsızlığı**: Bu testin sonucu (~18.500 satır/sn,
N=1/varsayılan/10 dosya/doğru şema) Bölüm 28.1'deki AYNI konfigürasyonla
(N=1/varsayılan/10 dosya/doğru şema, `max_insert_threads=4` testi)
ölçülen 53.207 satır/sn'den belirgin düşük çıktı -- muhtemelen bu
testten hemen önce çalışan ~50 dakikalık `sed` dilim çıkarma işleminin
(yoğun disk I/O) bıraktığı geçici ortam yükünden. Bugünkü genel
metodoloji dersini bir kez daha doğruluyor: **aynı konfigürasyonun
mutlak sayısı bile oturum içinde zamana göre değişebiliyor, göreli
karşılaştırmalara (aynı anda/arka arkaya ölçülenlere) güvenilmeli,
farklı zamanlarda ölçülmüş mutlak sayılara değil.**

### 28.3 ClickHouse hedef tablosunun kendi sıkıştırma codec'i -- ZSTD net kazanç

Şimdiye kadar hep ClickHouse'un varsayılan codec'i (LZ4) ile
yüklenmişti -- parquet tarafının sıkıştırması (Bölüm 21-23) çok
optimize edildi ama ClickHouse'un KENDİ depolama sıkıştırması hiç
ayarlanmamıştı. Aynı veri (`diverse10`, 10 dosya), iki farklı hedef
tablo şemasıyla yüklendi:

| Codec | Süre | Throughput | Disk boyutu |
|---|---|---|---|
| Varsayılan (LZ4) | 44,22sn | 22.614 satır/sn | 3.395,9MB |
| **Explicit `CODEC(ZSTD)`** | **41,94sn** | **23.845 satır/sn** | **2.488,6MB (~%27 daha az)** |

**Sonuç: `CODEC(ZSTD)` hem yükleme hızında hafif kazanç (~%5,
gürültü seviyesine yakın ama en azından yavaşlatmıyor) HEM disk
boyutunda büyük kazanç (~%27) veriyor.** Net tavsiye: ClickHouse
hedef tablosunda tüm sütunlara `CODEC(ZSTD)` uygulanmalı, varsayılan
LZ4'e güvenilmemeli -- 1,5M dosyalık üretim ölçeğinde bu, MinIO'nun
yanı sıra ClickHouse'un kendi disk maliyetinde de ciddi bir tasarruf
sağlar (tab->parquet tarafında DuckDB'nin sıkıştırma üstünlüğünü
bulduğumuz Bölüm 15'teki mantığın ClickHouse-tarafı karşılığı).

## 29. MinIO indirme süresi vs ClickHouse işleme süresi -- darboğaz kesin olarak ClickHouse tarafında (2026-08-19)

Kullanıcının "MinIO'ya sorgu atma ve ClickHouse'a sorgu atma
sürelerini kıyaslayabilir miyiz" sorusu üzerine, aynı 10 dosya
(`diverse10`) iki farklı şekilde ölçüldü: (a) `minio` Python
client'ıyla SAF indirme (ClickHouse hiç karışmadan, sadece byte'ları
çekip belleğe okuma), (b) ClickHouse'un `INSERT...SELECT * FROM
s3(...)` ile TAM süreci (indir+parquet decode+tip dönüşümü+tabloya
sıkıştırıp yazma).

*(Not: bu testten hemen önce Docker Desktop/WSL2 17 saatlik bir
boşluktan sonra (muhtemelen makine uykuya geçmişti) yeniden
başlatılması gerekti -- `docker start` ile container'lar sorunsuz
geri geldi, veri/paket kaybı olmadı, purge gerekmedi.)*

| Aşama | Süre | Toplam içindeki pay |
|---|---|---|
| Saf MinIO indirme (10 dosya, 2277,3MB, ClickHouse yok) | 6,72sn (338,9MB/sn) | **~%10** |
| ClickHouse'un tam süreci (indir+decode+yaz) | 69,56sn | %100 |
| Fark (decode+tip dönüşümü+yazma payı) | 62,84sn | **~%90** |

**Sonuç: darboğaz kesinlikle MinIO/ağ tarafında DEĞİL, ClickHouse'un
kendi işleme sürecinde (parquet decode + tip dönüşümü + sıkıştırıp
yazma).** MinIO'dan saf indirme çok hızlı (338,9MB/sn) -- toplam
sürenin sadece ~%10'u. Bu, bugünkü birçok bulguyu birbirine bağlıyor:

- `max_insert_threads`'in (yazma tarafı) sonuçları bu kadar güçlü
  etkilemesinin sebebi: zamanın zaten büyük çoğunluğu ClickHouse'un
  kendi yazma/decode işinde geçiyor.
- Bölüm 27'deki "yanlış şema (300 sütun) %55 daha hızlıydı" bulgusu
  tutarlı: daha az sütun işlemek, asıl darboğaz olan ClickHouse-tarafı
  decode/yazma süresini doğrudan kısaltıyor.
- Genel tema (Bölüm 15, 27, 29 ortak): **1000 sütunlu geniş veriyi
  decode etmek pahalı** -- bu üçüncü kez, artık doğrudan/net şekilde
  doğrulandı.

**Pratik sonuç**: MinIO/ağ tarafını daha fazla optimize etmenin
(daha hızlı disk, daha iyi network) getirisi sınırlı -- MinIO zaten
darboğaz değil. Asıl kazanç fırsatı ClickHouse'un kendi decode/yazma
verimliliğinde (`CODEC(ZSTD)`, sütun sayısını gerçek ihtiyaca göre
tutmak, `max_insert_threads`'i dokunmadan bırakmak gibi Bölüm
26-28'de bulunan optimizasyonlar zaten bu yöndeydi).

**Not**: `system.query_log`'un `ProfileEvents` haritasından
(`S3ReadMicroseconds` vb.) ayrıca bir kırılım denendi ama sorgu
boş/sıfır sonuç döndü (muhtemelen bu ClickHouse sürümünde metrik adı
farklı ya da agregasyon sözdizimi hatalı) -- yukarıdaki "saf indirme
vs tam süreç" karşılaştırması zaten net bir cevap verdiği için bu
ayrıca araştırılmadı.

**Ek soru -- "MinIO indirmesi hızlıysa, sorguları doğrudan MinIO'dan
yapmayalım mı, ClickHouse'a neden yüklüyoruz?"** Bu soru somut olarak
test edildi: aynı 10 dosya/1M satır üzerinde aynı basit aggregate
sorgusu (`count()`, `avg(f0)`) iki şekilde çalıştırıldı --

| Senaryo | En iyi süre |
|---|---|
| MinIO'yu doğrudan sorgula (`s3()`, önceden yükleme yok) | 104,2ms |
| **Önceden ClickHouse'a yüklenmiş tablo** | **15,4ms** |

**Önceden yüklenmiş tablo ~6,8x daha hızlı** -- MinIO tarafı ısındıktan
sonraki EN İYİ hali bile (ilk/soğuk deneme 802ms'ydi). Sebep: canlı
sorguda ClickHouse HER SEFERİNDE parquet'i yeniden decode etmek
zorunda (Bölüm 29'da bulunan asıl pahalı işlem) -- MinIO'dan indirme
hızlı olsa da decode maliyeti canlı sorguda tekrar tekrar ödeniyor,
önceden yüklemede bir kez ödenip bitiyor. Ayrıca bu fark FİLTRELİ
sorgularda çok daha büyür -- Bölüm 25'te önceden yüklenmiş tabloda
partition pruning + sıralı index sayesinde **6,7ms** almıştık (canlı
sorguda böyle bir index yok, eşleşen tüm dosyalar her seferinde
taranır). **Sonuç: MinIO'nun hızlı olması "önceden yükleme gereksiz"
anlamına gelmiyor -- tam tersine, pahalı olan decode adımını BİR KEZ
ödemek (yükleme), her sorguda TEKRAR TEKRAR ödemekten (canlı
sorgulama) çok daha ucuz. MinIO->ClickHouse mimarisi doğru kurgulanmış.**

**Bu fark veri hacmiyle nasıl değişiyor? -- neredeyse ORANTILI
büyüyor (kötüye doğru, canlı sorgulama için).** Aynı karşılaştırma
5x veri hacminde (50 dosya/5.000.000 satır) tekrarlandı:

| Veri hacmi | Canlı `s3()` (filtresiz) | Önceden yüklenmiş | Fark |
|---|---|---|---|
| 10 dosya (1M satır) | 104,2ms | 15,4ms | 6,8x |
| **50 dosya (5M satır, 5x)** | **416,9ms** | **11,9ms (değişmedi)** | **34,9x** |

Fark 6,8x'ten 34,9x'e çıktı -- veri 5 kat büyüyünce fark de ~5,1 kat
büyüdü, neredeyse birebir orantılı. Sebep tam beklenen: **canlı
sorgu süresi veri hacmiyle DOĞRU ORANTILI büyüyor** (104,2ms->416,9ms,
~4x -- her byte'ı yeniden decode ediyor, kısayol yok), **önceden
yüklenmiş sorgu süresi veri hacminden neredeyse BAĞIMSIZ kalıyor**
(15,4ms->11,9ms, pratikte değişmedi -- ClickHouse'un sıkıştırılmış
formatı zaten hızlı taranıyor).

Filtreli sorguda (dar zaman aralığı, `WHERE timestamp<1`, sadece
12.500/5.000.000 satırı eşleşiyor) da benzer desen: canlı 205,7ms vs
önceden-yüklenmiş 10,8ms (19x fark) -- gerçek üretimde tam
partition'lı/index'li kurulumla (Bölüm 25'teki 6,7ms gibi) bu fark
muhtemelen çok daha da büyür.

**Nihai sonuç**: "MinIO hızlı, direkt sorgulayalım" fikri küçük
ölçekte bile kötüydü (6,8x), veri büyüdükçe **çok daha savunulamaz**
hale geliyor (34,9x, sadece 5 kat veri artışında). 1,5M dosyalık
üretim ölçeğinde önceden yükleme (ETL) mimarisi bir tercih değil,
zorunluluk -- canlı sorgulamanın performans farkı ölçekle katlanarak
kötüleşiyor.

**Üçüncü nokta eklendi: 200 dosya/20M satır (2026-08-19).** Aynı
karşılaştırma bir kademe daha büyütüldü (bu turda disk %100 dolma
krizi + Docker Desktop'ın purge sırasında `wsl --unmount` hatası
vermesi -- `wsl --shutdown` ile çözüldü, kullanıcı purge'ü tekrar
denedi, container'lar sıfırdan yeniden kuruldu). Yükleme adımı
beklenenden çok yavaş çıktı (~40dk, `system.processes` ile canlı
izlendi -- takılı değildi, gerçekten 20M satırı işliyordu, ~8.344
satır/sn gibi düşük bir hızda; muhtemelen TEK sorguda 200 dosyanın
kendi ek maliyeti, Bölüm 26.1'deki "dosya sayısı arttıkça throughput
düşüyor" deseninin bir uzantısı).

| Dosya sayısı | Satır | Canlı (filtresiz) | Önceden yüklenmiş | Fark |
|---|---|---|---|---|
| 10 | 1M | 104,2ms | 15,4ms | 6,8x |
| 50 | 5M | 416,9ms | 11,9ms | 34,9x |
| **200** | **20M** | **2.637,4ms** | **14,3ms** | **184,2x** |

Filtreli sorguda (dar zaman aralığı) fark daha da uçlaşıyor:

| Dosya sayısı | Canlı (filtreli) | Önceden yüklenmiş | Fark |
|---|---|---|---|
| 50 | 205,7ms | 10,8ms | 19,0x |
| **200** | **4.974,5ms** | **11,5ms** | **433,9x** |

**En çarpıcı bulgu**: Önceden yüklenmiş tarafın süresi veri 20 kat
büyümesine rağmen (10->200 dosya) PRATİKTE DEĞİŞMEDİ (15,4ms->14,3ms).
Canlı sorgu ise 104,2ms->2.637,4ms'ye fırladı (~25x, veri artışından
(20x) bile hızlı -- SÜPER-DOĞRUSAL büyüme). Bu, canlı sorgulamanın
sadece "veri hacmiyle orantılı" değil, **hacim büyüdükçe orantılıdan
da kötü** bir şekilde yavaşladığını gösteriyor -- tek sorguda çok
dosyalı okumanın kendi ek yükü (Bölüm 26.1) buna ekleniyor. **1,5M
dosyalık gerçek üretim ölçeğinde bu fark muhtemelen binlerce/on
binlerce kat olurdu -- önceden yükleme mimarisinin zorunluluğunu
kesin olarak kanıtlıyor.**

## 30. Mimari değişiklik önerisi -- MinIO'da parquet yerine ham `.tab.zst` arşivi, ClickHouse yine parquet üzerinden yüklenir (2026-08-19)

Kullanıcı yeni bir mimari önerdi: MinIO'da parquet yerine `.tab`'ın
doğrudan ZSTD ile sıkıştırılmış hali (`.tab.zst`) tutulsun -- "boyut
olarak daha küçük oluyor sanırım" gerekçesiyle. Bu iki ayrı soruyu
gerektirdi: (a) boyut iddiası doğru mu, (b) ClickHouse bunu nasıl
yükleyecek.

### 30.1 Boyut karşılaştırması -- sezgi doğru, hatta beklenenden fazla

Aynı kaynak (`bench_sample.tab`, 454,0MB) iki şekilde sıkıştırıldı:

| Format | Boyut | Oran |
|---|---|---|
| `.tab` (ham) | 454,0MB | -- |
| `.parquet` (ZSTD, DuckDB, mevcut yöntem) | 227,7MB | 1,99x |
| **`.tab.zst`** (Python `zstandard`, seviye 3) | **187,7MB** | **2,42x -- parquet'ten %18 küçük** |
| `.tab.zst` (seviye 19, yüksek) | 149,0MB | 3,05x -- ama tek dosya için 409sn sürdü (1,5M dosyada kullanılamaz) |

**Ham `.tab`'ı doğrudan ZSTD ile sıkıştırmak, parquet'ten daha küçük
çıkıyor.** Sebep: verinin 700/1000 sütunu binary (0/1) -- metin
halinde "0\t1\t0\t0\t1..." deseni çok tekrarlı, ZSTD bunu TÜM dosya
boyunca (sütunlar arası, satırlar arası) örüntü olarak yakalıyor.
Parquet ise sıkıştırmayı sütun-sütun İZOLE yapıyor (her column chunk
ayrı sıkıştırma bağlamı), bu daha büyük ölçekli/çapraz-sütun
tekrarları kaçırıyor.

### 30.2 ClickHouse ham sıkıştırılmış TSV'yi doğrudan okuyabiliyor -- ama daha yavaş

ClickHouse'un `s3()` fonksiyonu `TabSeparatedWithNames` formatıyla,
dosya uzantısından (`.zst`) sıkıştırmayı OTOMATİK algılayarak
`.tab.zst`'i doğrudan okuyabildi -- parquet'e hiç gerek kalmadan:

| Kaynak format | Süre (100k satır, tek dosya) | Throughput |
|---|---|---|
| `.tab.zst` (ham, doğrudan) | 10,25sn | 9.753 satır/sn |
| **`.parquet`** (ZSTD, DuckDB) | **4,51sn** | **22.160 satır/sn (~2,27x daha hızlı)** |

**Parquet ~2,27x daha hızlı yükleniyor** -- sebep: parquet'te
değerler zaten binary/tipli (float64, 8 byte), ClickHouse doğrudan
kullanıyor. Ham `.tab.zst`'te değerler hâlâ METİN ("443.532506"
gibi), ClickHouse her satırda/sütunda bunu sayıya çevirmek (parse)
zorunda -- bu, oturumun en başından beri (Rust vs Python elle-parse)
gördüğümüz "metin parse etmek pahalı" temasının bir tekrarı.

### 30.3 Nihai mimari kararı -- ikisinin en iyisini birleştiren hibrit

Kullanıcının kararı: **ClickHouse yine parquet üzerinden yüklenir**
(hızlı yol korunur), **MinIO'da SADECE `.tab.zst` kalıcı olarak
tutulur** (küçük arşiv, parquet MinIO'da kalıcı değil, sadece geçici
"staging" -- ClickHouse'a yüklendikten sonra silinir).

**Akış uçtan uca test edildi ve doğrulandı**:
1. `.tab` -> parquet'e çevrildi, MinIO'ya geçici olarak yüklendi
2. ClickHouse `s3()` ile parquet'ten yükledi (100.000 satır)
3. Parquet MinIO'dan silindi
4. **ClickHouse'daki veri hiç etkilenmedi** (satır sayısı ve `sum(f0)`
   silme öncesi/sonrası birebir aynı) -- ClickHouse veriyi kendi
   formatına yazdıktan sonra kaynak parquet'e bağımlı değil
5. MinIO'da sadece `.tab.zst` (187,7MB) kaldı
6. **Üç katman mutabakatı doğrulandı**: arşivdeki `.tab.zst` açılıp
   elle satır sayısı (100.000, eşleşti) ve `sum(f0)` (-142242,02637600095
   vs ClickHouse'un -142242,02637600008 -- son birkaç ondalık basamak
   farkı sadece toplama sırası kaynaklı float gürültüsü, gerçek
   uyuşmazlık değil) hesaplandı, ClickHouse'daki değerle eşleşti.

**Sonuç**: Bu mimari hem depolama tasarrufu (MinIO'da ~%18 daha az
yer, 1,5M dosyada ciddi fark) HEM yükleme hızı (ClickHouse hâlâ
parquet'in ~2,27x hızından yararlanıyor) sağlıyor -- ödünleşimi
gerektirmeden ikisinin de avantajını alan bir tasarım. Tek ek
maliyet: pipeline artık her `.tab` dosyası için İKİ ayrı dönüşüm
üretmeli (parquet + `.tab.zst`), ve parquet'in "yükle-doğrula-sil"
döngüsünün Postgres manifest tarafında doğru yönetilmesi gerekir
(henüz üretim kodunda uygulanmadı, tasarım/doğrulama aşamasında).

### 30.4 `.tab.zst` sıkıştırma seviyesi -- DÜZELTME, seviye 3 değil seviye 12 asıl tatlı nokta

Kullanıcı "seviye 19 daha da fazla sıkıştırıyordu, seviye 3'ün en iyi
yöntem olduğuna emin misin" diye haklı bir itirazda bulundu -- 30.1'de
sadece iki uç (3 ve 19) test edilmiş, aradaki seviyeler (parquet
tarafında Bölüm 16'da da aynı boşluk bırakılmıştı) hiç taranmamıştı.
Tam tarama yapıldı:

| Seviye | Boyut | Parquet'e göre oran | Süre (tek dosya, 454MB kaynak) |
|---|---|---|---|
| 3 | 187,7MB | %18 küçük | 2,8sn |
| 6 | 174,7MB | %23 küçük | 6,3sn |
| 9 | 171,7MB | %25 küçük | 12,2sn |
| **12** | **165,9MB** | **%27 küçük** | **27,5sn** |
| 15 | 160,9MB | %29 küçük | **191,3sn (uçurum)** |
| 19 | 149,0MB | %35 küçük | 398,2sn |

**Net bir maliyet uçurumu 12->15 arasında**: süre ~7x artıyor (27,5sn
->191,3sn) ama boyut kazancı sadece ~%3 (165,9MB->160,9MB) -- ZSTD'nin
belirli bir seviyeden sonra çok daha pahalı bir arama stratejisine
geçmesinden (parquet tarafındaki max-seviye kötü trade-off'unun,
Bölüm 16, aynı ailesi).

**DÜZELTİLMİŞ TAVSİYE: seviye 3 değil, seviye 12.** Seviye 3'ten
belirgin daha iyi sıkışıyor (%27 küçük vs %18) ve hâlâ makul bir
sürede (27,5sn/dosya) -- seviye 15+'nin uçurumuna hiç yaklaşmıyor.
1,5M dosyada paralel worker havuzuyla (tab->parquet tarafında bulunan
N=6-20 aralığı gibi) makul sürede tamamlanır -- "tek worker'da 477
gün" rakamı yanıltıcı, biz zaten hiçbir yerde tek worker'la
çalışmıyoruz. Bu, Bölüm 30.1'deki seviye 3 tavsiyesini düzeltiyor.

### 30.5 `.tab.zst` sıkıştırmasının veri kaybına yol açıp açmadığı -- SHA256 ile kesin doğrulama

Kullanıcı, veri hassas olduğu için ("ondalıklar bile önemli")
sıkıştırmanın veri kaybına yol açıp açmadığının kanıtlanmasını
istedi -- varsayım değil, kesin kanıt. Orijinal `.tab` dosyasının
SHA256 hash'i alındı, üç farklı seviyede (3, 12, 19) sıkıştırılıp
açıldı, açılan dosyanın hash'i orijinalle karşılaştırıldı:

| Seviye | Açılmış boyut (orijinalle aynı mı) | SHA256 eşleşiyor mu | Byte-byte birebir aynı mı |
|---|---|---|---|
| 3 | 476.072.469 byte (evet) | ✅ | ✅ |
| 12 | 476.072.469 byte (evet) | ✅ | ✅ |
| 19 | 476.072.469 byte (evet) | ✅ | ✅ |

**Üç seviyede de SHA256 hash birebir eşleşti -- dosyanın hiçbir
byte'ı değişmedi.** Ayrıca spesifik satırlar (0, 1, 50.000, 99.999,
100.000) elle karşılaştırıldı, hepsi birebir aynı çıktı. Bu beklenen
bir sonuç -- ZSTD tanımı gereği KAYIPSIZ (lossless) bir algoritma,
sıkıştırma seviyesi hız/boyut dengesini etkiler ama doğruluğu
etkilemez (JPEG gibi kayıplı yöntemlerin aksine). **Sonuç: `.tab.zst`
(hangi seviyede olursa olsun, önerilen seviye 12 dahil) hassas
veride sıfır ondalık/veri kaybına yol açmıyor -- kriptografik hash
ile kanıtlandı, varsayım değil.**

### 30.6 Seviye 22 (ZSTD maksimum) de denendi -- seviye 19'dan bile kötü bir yatırım

Kullanıcı ZSTD'nin maksimum seviyesini (22) merak etti, tablo
tamamlandı:

| Seviye | Boyut | Oran | Süre |
|---|---|---|---|
| 3 | 187,7MB | 2,42x | 2,8sn |
| 6 | 174,7MB | 2,60x | 6,3sn |
| 9 | 171,7MB | 2,64x | 12,2sn |
| **12 (tavsiye)** | **165,9MB** | **2,74x** | **27,5sn** |
| 15 | 160,9MB | 2,82x | 191,3sn |
| 19 | 149,0MB | 3,05x | 398,2sn |
| 22 (maksimum) | 145,9MB | 3,11x | **784,4sn (13dk)** |

19->22 arası: boyut sadece ~%2 daha küçülüyor (149,0MB->145,9MB) ama
süre neredeyse 2 katına çıkıyor (398,2sn->784,4sn). 1,5M dosyada
20 paralel worker'la bile ~680 gün demek -- kullanılamaz. SHA256 hash
yine birebir eşleşti (seviye 22'de de veri kaybı yok) ama bu, boyut
kazancının süre maliyetine değmediği gerçeğini değiştirmiyor. **Seviye
12 tavsiyesi değişmedi -- bu, parquet tarafındaki (Bölüm 16) "max
seviye kötü trade-off" dersinin ham `.tab` sıkıştırması tarafında da
bir kez daha doğrulanması.**

### 30.7 Parquet (DuckDB) veri kaybına yol açıyor mu -- ilk şüpheli sonuç, ama gerçek sebep doğrulama scriptinde çıktı

`.tab.zst` için yapılan SHA256 doğrulamasının (Bölüm 30.5) aynısı bu
kez `.parquet` için denendi -- ama parquet'te ham byte kopyalama
değil bir TİP DÖNÜŞÜMÜ (metin->float64) olduğu için hash yerine
**tam sayısal eşitlik** karşılaştırması gerekti: orijinal `.tab`
(pandas ile okunup float64'e çevrilmiş) ile `.parquet` (pyarrow ile
okunmuş) arasında 100.100.000 hücrenin tamamı (100.000 satır ×
1001 sütun) tolerans sıfır şekilde karşılaştırıldı.

**İlk sonuç şüpheli görünüyordu**: 9.299/100.100.000 hücre (%0,0093)
tam eşleşmiyordu, hepsi `timestamp` sütununda, farklar 1e-17-1e-18
mertebesinde (float64'ün son biti). Kök nedeni bulmak için tek bir
örnek derinlemesine incelendi: kaynak `.tab`'taki ham metin aslında
`'0.036000000000000004'` idi (temiz "0.036" değil -- sentetik veri
kayan-nokta toplamayla üretildiği için böyle). Python'un kendi
`float()` fonksiyonu (IEEE754 doğru-yuvarlama referansı) bu metni
`0.036000000000000004` olarak veriyor -- **DuckDB/parquet'in verdiği
değerle hex seviyesinde BİREBİR aynı** (`0x1.26e978d4fdf3cp-5`).
**pandas'ın verdiği değer ise `0.036` -- 1 bit farklı, YANLIŞ
yuvarlanmış** (`0x1.26e978d4fdf3bp-5`).

**200 örnekle genelleme doğrulandı**: tüm 9.299 uyuşmazlıktan 200'ü
tek tek kontrol edildi -- **200/200'ünde pandas hatalı, DuckDB
Python'un referans değeriyle birebir doğru** (0 ters durum). Yani
ilk bulunan "uyuşmazlık", parquet'in bir kusuru DEĞİL, doğrulama
scriptinde kullanılan `pandas`'ın CSV parser'ının bu sınır
durumlarında (son bitte) küçük bir yuvarlama kusuruydu.

**Nihai sonuç: `.parquet` (DuckDB dönüşümü) da `.tab.zst` gibi sıfır
veri kaybına yol açmıyor** -- IEEE754 doğru-yuvarlama seviyesinde
doğrulandı (Python'un kendi referans dönüşümüyle birebir eşleşiyor).
Hem MinIO'daki `.tab.zst` arşivi (byte-seviyesinde, SHA256) hem
ClickHouse'a giden `.parquet` (sayısal doğruluk seviyesinde) hassas
veride güvenli.

## 31. Binary sütunların tipi -- neden Float64 kullanıldığı, ve ClickHouse'un kendi deposunda bulunan büyük kazanç

Kullanıcı önemli bir soru sordu: "biz niye 0 ve 1'leri Float64 olarak
saklıyoruz". Dürüst cevap: bu bilinçli bir karar değildi --
`tab_to_parquet_duckdb.py` TÜM sütunları (binary dahil) `::DOUBLE`
olarak dönüştürüyor, muhtemelen oturumun başında gerçek `.ham` şeması
bilgisi olmadığı için tek-tip basitleştirme yapılmıştı (plan açık
sorular listesinde hâlâ "kaynak sütunların gerçek genişliği teyit
edilmedi" maddesi duruyor). Veri seti sabit %30 float64/%70 binary
yapısında (300 float + 700 binary sütun).

### 31.1 ClickHouse'un kendi deposunda UInt8+T64 -- büyük kazanç

ClickHouse hedef tablosunda binary sütunlar (`b0`-`b699`) `Float64`
yerine `UInt8` + `T64` codec ile tanımlanıp aynı kaynak parquet'ten
yüklendi (10 dosya, 1M satır):

| | Süre | Disk boyutu |
|---|---|---|
| Float64 (mevcut, ZSTD) | 36,59sn | 970,0MB |
| **UInt8 + T64+ZSTD** | **19,53sn (~%87 daha hızlı)** | **661,8MB (~%32 daha küçük)** |

**Not**: `T64` codec'i `Float64` tipini desteklemiyor (ClickHouse
hatasıyla doğrulandı) -- doğru tip (`UInt8`) şart, sadece codec
değişikliği yetmiyor.

### 31.2 Ama parquet dosyasının KENDİSİ neredeyse hiç değişmiyor -- Bölüm 30.1'in geçerliliği korunuyor

Kaynak `.tab` dosyası, binary sütunlar `UTINYINT` (DuckDB'nin küçük
tam sayı tipi) olarak yeniden parquet'e çevrildi:

| | Boyut |
|---|---|
| Eski parquet (hepsi Float64) | 227,7MB |
| Yeni parquet (b sütunları UTINYINT) | 227,5MB (**sadece %0,1 fark**) |

**Sebep**: Parquet, düşük-kardinaliteli (binary) sütunlarda deklare
edilen tipten BAĞIMSIZ olarak zaten dictionary/RLE encoding
kullanıyor (Bölüm 15'te bulunan mekanizmanın aynısı) -- sadece 2
farklı değer (0/1) olduğu için parquet bunu Float64 bile olsa küçük
bir sözlük + referans indeksleriyle sıkıştırıyor. Tip değişikliği bu
seviyede neredeyse hiçbir şey katmıyor.

**ClickHouse'un kendi deposu ise bu akıllılığı miras almıyor** --
parquet'ten Float64 olarak okuyup düz ZSTD'ye bırakınca, ClickHouse
her değeri 8 byte olarak görüyor, parquet'in sözlük hilesinden
yararlanamıyor. `UInt8`+`T64` vererek bu akıllılığı ClickHouse
tarafında da EL İLE sağlamış olduk.

**Sonuç -- iki ayrı katman, iki ayrı karar**:
- **Bölüm 30.1'deki parquet vs `.tab.zst` karşılaştırması GEÇERLİ
  kalıyor** -- parquet zaten en iyi haliyle (dictionary encoding
  sayesinde) ölçülmüştü, tip sorunu onu neredeyse hiç etkilemiyordu.
- **Bölüm 28.3'teki ClickHouse'un kendi depolama codec testi
  EKSİKTİ** -- orada UInt8+T64 hiç denenmemişti, gerçek büyük kazanç
  (parquet'ten değil) ClickHouse'un KENDİ hedef tablo şemasında
  bekliyormuş.

**GÜNCEL TAVSİYE**: ClickHouse hedef tablosunda binary/düşük-
kardinaliteli sütunlar için `UInt8` (ya da uygun küçük tam sayı tipi)
+ `T64, ZSTD` codec zinciri kullanılmalı -- parquet üretiminde
tip değişikliğine gerek yok (kazanç yok), ama ClickHouse tablo
şemasında bu kesinlikle uygulanmalı (~%32 disk, ~%87 hız kazancı).

### 31.3 Kazancın kaynağı izole edildi -- tip mi codec mi

31.1'de aynı anda iki şey değişmişti (Float64->UInt8 TİP + ZSTD->T64
CODEC), hangisinin asıl kazancı sağladığı ayrıştırılmamıştı. Üç
konfigürasyon ayrı ayrı test edildi:

| | Süre | Disk boyutu |
|---|---|---|
| A) Float64 + ZSTD (baseline) | 32,12sn | 1.088,7MB |
| **B) UInt8 + ZSTD (sadece TİP değişti)** | **22,51sn (~%30 hızlı)** | **869,0MB (~%20 küçük)** |
| C) UInt8 + T64,ZSTD (tip+codec) | 24,19sn | 680,4MB (B'den ~%22 daha küçük) |

**Sonuç: asıl temel/kök düzeltme doğru TİP seçimi** -- sadece
Float64'ten UInt8'e geçmek (codec hâlâ düz ZSTD), T64'e hiç gerek
kalmadan tek başına ~%20 boyut/~%30 hız kazandırıyor. `T64` codec'i
bunun üzerine EK bir ~%22 boyut kazancı katıyor (hız üzerinde
belirgin fayda yok, B->C'de süre aslında hafif arttı, gürültü
seviyesinde). **İkisi birlikte en iyi sonucu veriyor ama tip
düzeltmesi tek başına bile (T64 olmadan) önemli, düşük riskli bir
kazanç -- iki ayrı, birbirini tamamlayan optimizasyon.**

### 31.4 GÜVENLİK UYARISI -- `b0`-`b699`'un GERÇEK üretimde UInt8'e sığacağı doğrulanmadı, sessiz veri bozulması riski var

Kullanıcının "bu sütunlar kesin UInt8 mi" sorusu kritik bir boşluk
açığa çıkardı. **Bizim SENTETİK test verimizde** `b0`-`b699` bilinçli
olarak hep 0/1 üretiliyor (`generate_test_tab.py`) -- ama **gerçek
`.ham` üretim verisinde bu hiç doğrulanmadı** (plan açık sorular
listesindeki "kaynak sütunların gerçek genişliği teyit edilmedi"
maddesiyle doğrudan bağlantılı).

**Risk somut olarak test edildi -- ClickHouse `toUInt8()` aralık dışı
değerlerde SESSİZCE YANLIŞ SONUÇ üretiyor, hata vermiyor**:

| Girdi | `toUInt8()` sonucu | Davranış |
|---|---|---|
| 300 | 44 | 300 mod 256 = 44 (sessiz taşma) |
| -1 | 255 | negatif -> en üst değere sarma |
| 256 | 0 | sınırın tam üstü -> sıfıra sarma |

Yani eğer gerçek üretimde bu 700 sütundan biri beklenmedik bir anda
UInt8 aralığının (0-255) dışına çıkarsa, veri **sessizce ve fark
edilmeden bozulur** -- hiçbir hata/uyarı olmadan.

**Sonuç: UInt8 optimizasyonu (Bölüm 31.1-31.3) üretime ALINMAMALI**,
gerçek `.ham` sütunlarının değer aralığı `.ham` formatını çözen
kişiden teyit alınana kadar. Teyit gelene kadar/sonrasında güvenli
uygulama yolları:
1. **Doğrulama adımı**: yükleme öncesi her sütun için
   `SELECT count() WHERE bN NOT IN (0,1)` (ya da beklenen aralık)
   kontrolü, aralık dışı değer varsa reddet/uyar
2. **`toUInt8OrNull()`** kullan (sessizce sarmak yerine aralık dışı
   değerleri NULL yapar -- anomali en azından fark edilebilir hale
   gelir)

Bu, Bölüm 31'deki performans kazancının hâlâ geçerli/değerli olduğunu
ama üretime alınmadan önce bir veri-doğrulama adımı gerektirdiğini
gösteriyor -- performans optimizasyonu ile veri güvenliği ayrı ele
alınmalı.

### 31.5 Otomatik sütun sınıflandırma -- sabit %30/%70 varsayımına gerek yok, veri kendini sınıflandırıyor

Kullanıcı haklı bir noktaya değindi: sentetik test verimizdeki sabit
"%30 float/%70 binary" oranı gerçek üretim verisinde değişken
olacaktır -- sabit bir varsayım yerine, HANGİ sütunların gerçekten
UInt8'e güvenle sığdığını **veriden otomatik** tespit eden bir yöntem
gerekiyor.

DuckDB ile, sütun İSMİNE hiç bakmadan, sadece her sütunun `MIN()`,
`MAX()` ve "tüm değerler tam sayı mı" (`değer = FLOOR(değer)`)
özelliklerini tarayan bir otomatik sınıflandırma test edildi (1001
sütunun tamamı, tek bir SQL sorgusunda):

| | Otomatik tespit edilen | Gerçek |
|---|---|---|
| UInt8'e (0-255 aralığı, tam sayı) güvenle sığan sütun | 700 | 700 (hepsi doğru) |
| Float64 kalması gereken sütun | 301 | 301 (hepsi doğru) |

**%100 isabet -- sütun ismi (`b`/`f` öneki) hiç kullanılmadan, sadece
gerçek veri değerlerine bakarak.** Tarama süresi 87,2sn (100k satır,
1001 sütun) -- ama bu **dosya başına tekrarlanması gereken bir adım
değil**, şema/sütun semantiği stabil olduğu sürece BİR KEZ (ya da
periyodik olarak, şema değişikliği ihtimaline karşı) yapılıp sonucu
bir yapılandırma/manifest'te saklanabilir, sonraki tüm yüklemelerde
bu sınıflandırma kullanılır.

**Pratik uygulama**: (1) Bu otomatik tarama ile sütun sınıflandırması
BİR KEZ yapılır ve saklanır (hangi sütunlar UInt8-güvenli). (2)
Yükleme sırasında `toUInt8OrNull()` (Bölüm 31.4'teki güvenlik notu)
kullanılarak, gerçek veri zamanla beklenenin dışına çıkarsa (örn. yeni
bir sütun aralığı genişlerse) bu NULL olarak yakalanır, sessizce
bozulmaz. Bu, sabit varsayım yerine hem esnek hem güvenli bir yöntem
-- performans kazancını (Bölüm 31.1-31.3) veri güvenliğinden ödün
vermeden, gerçek üretim verisinin değişkenliğine uyarlanabilir şekilde
uygulamayı mümkün kılıyor.

## 32. Eğitilmiş ZSTD sözlüğü denendi -- dosya boyutumuz için uygun değil, elendi

Kullanıcı T64'ü bir kenara bırakıp (sadece düz ZSTD ile devam), kalan
iyileştirme önerilerinden "eğitilmiş ZSTD sözlüğü" fikrini denemek
istedi. Kullanıcı önemli bir hatırlatma yaptı: **farklı uçak tipleri
için farklı sayıda sütun var** -- yani tek bir "evrensel" sözlük tüm
1,5M dosya için uygun olmaz (farklı tiplerin byte örüntüleri
farklıdır), uçak tipi başına ayrı sözlük gerekirdi. Bu yüzden önce
temel varsayımı (aynı tip dosyalarda sözlük fayda sağlıyor mu) test
etmek gerekti -- eğer bu bile başarısız olursa, çapraz-tip
genelleme testine hiç gerek kalmayacaktı.

**Test**: `bench_sample.tab` 49 parçaya bölündü (2000 satır/parça,
hepsi aynı 1000-sütunlu şema), ilk 48'i eğitim örneği olarak
kullanılıp 64KB'lık bir ZSTD sözlüğü eğitildi (9,7sn), son parça
(9.305,8KB, eğitimde KULLANILMAMIŞ "held-out") sözlüksüz ve sözlüklü
ZSTD(12) ile ayrı ayrı sıkıştırıldı:

| | Boyut |
|---|---|
| Sözlüksüz ZSTD(12) | 3.417,3KB |
| Sözlüklü ZSTD(12) | 3.481,7KB (**%1,9 DAHA BÜYÜK**) |

(Kayıpsızlık ayrıca doğrulandı -- sözlüklü sıkıştırma da tam
kayıpsız, ama boyut kazancı yok.)

**Sonuç -- temel varsayım YANLIŞ çıktı, elendi**: Sözlük yaklaşımı
aynı-tip dosyalarda bile fayda sağlamadı, hatta hafifçe kötüleştirdi.
Sebep: ZSTD sözlükleri ÇOK SAYIDA KÜÇÜK dosyayı sıkıştırırken işe
yarar (her dosya tek başına yeterli iç-tekrar barındırmadığı için
ortak sözlük eksikliği kapatır). Bizim dosyalarımız BÜYÜK (~500MB) --
test edilen ~9MB'lık parça bile ZSTD'nin kendi penceresi içinde
zaten yeterli tekrar örüntüsü buluyor, sözlüğün katacağı ek bir şey
kalmıyor (üstelik küçük bir sözlük-referans maliyeti bindiriyor).
**Bu fikir elendi -- çapraz-uçak-tipi genelleme testine gerek
kalmadı, temel senaryoda zaten kazanç yok.**

## 33. Binary sütun bit-paketleme -- bugüne kadarki EN İYİ sıkıştırma sonucu

Binary sütunları (700 tane, 0/1) metinde ("0\t1\t0...") değil, 8
değeri 1 byte'a paketleyerek (numpy `packbits`) saklamak test edildi
-- float sütunlar (`timestamp`+`f0`-`f299`) binary float64 (8 byte)
olarak, binary sütunlar bit-paketli (700 bit -> 87,5 byte/satır)
olarak birleştirilip üzerine ZSTD(12) uygulandı.

| Format | Boyut |
|---|---|
| Parquet (Bölüm 30.1) | 227,7MB |
| `.tab.zst` (ham metin, seviye 12, Bölüm 30.1) | 165,9MB |
| **Bit-paketli + ZSTD(12)** | **146,1MB** |

**`.tab.zst`'ten ~%12, parquet'ten ~%36 daha küçük -- bugüne kadarki
en iyi sıkıştırma sonucu.** Süre de rekabetçi: okuma+parse 10,3sn +
paketleme 2,4sn + sıkıştırma 10,4sn = toplam **23,1sn** (ham
`.tab.zst`'in tek başına 27,5sn'sinden bile hızlı). **Kayıpsızlık
doğrulandı** (byte-byte birebir aynı, decompress sonrası).

**Neden bu kadar iyi**: Sıkıştırma ÖNCESİ bile ham boyut 454,0MB'dan
238,0MB'a iniyor (float64 binary temsili metinden kompakt, bit-paketli
binary sütunlar metinden ~16x küçük) -- ZSTD zaten küçülmüş bu veriye
uygulanıyor, ek kazanç katıyor.

**Bedeli**: Bu artık STANDART bir format değil (parquet ya da düz
metin gibi evrensel araçlarla okunamaz) -- özel bir paketle/aç
(pack/unpack) kodu yazılıp bakımı yapılmalı, MinIO'daki arşivi
okumak isteyen her sistemin bu özel formatı bilmesi gerekir. Bu,
daha önce önerilen "daha invaziv mühendislik" bedelinin somut
karşılığı -- kazanç gerçek ve büyük, ama format-evrenselliğinden
ödün veriliyor. Üretime alınıp alınmayacağı bu ödünleşime bağlı bir
karar.

## 34. Farklı sıkıştırma ALGORİTMALARI test edildi -- bz2, ZSTD'yi geçti, yeni nihai tavsiye

Kullanıcı özel bit-paketleme yerine standart `.tab.zst` formatında
kalmaya karar verdi, ama farklı bir sıkıştırma ALGORİTMASI (ZSTD
dışında) denenip denenmediğini sordu. Şimdiye kadar sadece ZSTD'nin
seviyeleri tarandı (Bölüm 30.4) -- algoritmanın kendisi hiç
değiştirilmemişti. Üç alternatif test edildi (aynı `bench_sample.tab`,
454,0MB):

| Algoritma | Boyut | Süre |
|---|---|---|
| ZSTD seviye 12 (önceki tavsiye) | 165,9MB | 27,5sn |
| LZMA preset=6 | 144,5MB | 476,0sn (17x yavaş) |
| Brotli q=9 | 163,8MB | 65,4sn (2,4x yavaş, kazanç yok denecek kadar az) |
| **bz2 seviye 9** | **136,3MB** | **32,4sn** |

**`bz2`, ZSTD'den %17,8 daha küçük çıkıyor, süre farkı ihmal
edilebilir (~%18 yavaş, ~5sn fark)** -- LZMA'nın (17x yavaş) ve
Brotli'nin (kazanç neredeyse yok) aksine, gerçek ve ucuz bir kazanç.
SHA256 ile kayıpsızlık doğrulandı (byte-byte birebir aynı).

**Neden bz2 bu kadar iyi**: bz2, Burrows-Wheeler dönüşümü (BWT)
tabanlı bir algoritma -- ZSTD'nin sözlük/eşleştirme tabanlı
yaklaşımından farklı olarak, veriyi önce özel bir şekilde yeniden
sıralayıp (benzer bağlamdaki karakterleri bir araya getirerek) sonra
sıkıştırıyor. Bizim tab-ayraçlı, çok tekrarlı (700 binary sütun)
verimizin yapısı bu dönüşüme özellikle uygun görünüyor.

**GÜNCEL NİHAİ TAVSİYE: `.tab.zst` yerine `.tab.bz2` kullanılmalı**
-- aynı basitlik/evrensellik (bz2 de standart, yaygın desteklenen bir
format), daha iyi sıkıştırma (136,3MB, ~%18 daha küçük), ihmal
edilebilir hız bedeli. Bu, Bölüm 30'daki tüm `.tab.zst`
tavsiyelerinin `.tab.bz2` ile güncellenmesi gerektiği anlamına
geliyor -- mimari (MinIO'da ham arşiv, ClickHouse parquet üzerinden
yüklenir) aynı kalıyor, sadece MinIO arşivinin sıkıştırma algoritması
değişiyor.

### 34.1 bz2 vs ZSTD -- büyük veri ve farklı binary/float oranlarıyla genişletilmiş test

Karar kesinleştirmeden önce, `bz2` avantajının farklı ölçek ve veri
kompozisyonlarında tutarlı kalıp kalmadığı test edildi -- 463k
satırlık büyük bir örnek, ve iki uç kompozisyon (tüm-binary,
tüm-float) hazırlandı.

| Senaryo | ZSTD(12) boyut | bz2(9) boyut | bz2 kazancı | Açma hızı farkı (bz2/ZSTD) |
|---|---|---|---|---|
| Büyük veri (463k satır, 300f+700b) | 829,5MB | 681,6MB | %17,8 küçük | 16,9x yavaş |
| Tüm-binary (700 sütun) | 14,2MB | 10,7MB | **%25,0 küçük** | 16,2x yavaş |
| Tüm-float (300 sütun) | 141,0MB | 124,9MB | %11,4 küçük | 19,8x yavaş |

**İki net örüntü**:
1. **bz2'nin boyut avantajı binary oranı arttıkça büyüyor** -- tüm-
   float %11,4 -> karışık (gerçek veri) %17,8 -> tüm-binary %25.
   Bizim gerçek verimiz (%70 binary) bu avantajın güçlü tarafında.
2. **Açma hızı dezavantajı (16-20x) her senaryoda tutarlı** -- tek
   seferlik bir anomali değil, bz2'nin genel/kalıcı bir özelliği.
   Büyük veride (463k satır) her iki algoritma da orantılı/makul
   ölçekleniyor, ani bir kötüleşme yok.

**NİHAİ KARAR (kullanıcı onayı): `.tab.bz2` (bz2 seviye 9) kullanılacak.**
Boyut avantajı gerçek ve tutarlı (özellikle bizim binary-ağırlıklı
verimizde güçlü), açma hızı dezavantajı bilinçli olarak kabul
edildi -- MinIO arşivinin "soğuk depolama" rolü (nadiren geri
okunması) göz önünde bulundurularak. İleride toplu geri-yükleme
senaryosu gerçek bir ihtiyaç haline gelirse bu karar yeniden
gözden geçirilebilir.

## 35. Uç senaryo testi -- 45.000 sütunlu (1000 float+44.000 sabit-sıfır), 10GB dosyada bz2 çöküyor

Kullanıcı, çok daha uç bir kompozisyonda (bugüne kadarki 1000
sütunlu/%70-binary verimizden çok farklı) ZSTD ve bz2'yi karşılaştırmak
istedi: **45.000 sütun, 1000'i float64, 44.000'i HER ZAMAN sadece 0**
(sabit), toplam dosya boyutu ~10GB.

*(Not: bu test öncesi Docker Desktop/WSL2 yine ~22 saatlik bir
boşluktan sonra yeniden başlatılması gerekti -- `docker start` ile
container'lar sorunsuz geri geldi, veri/paket kaybı olmadı.)*

**Veri üretimi**: numpy+pandas ile verimli iki-aşamalı üretim (1000
float sütunu pandas.to_csv ile hızlıca üretilip, önceden hesaplanmış
44.000 sabit-sıfır sonekiyle birleştirildi) -- 179,9sn'de 113.869
satır, nihai boyut **10,46GB**. Bellek testere-dişi paterniyle (5,8-
9,5Gi arası salındı) güvenli kaldı, OOM olmadı.

**Sıkıştırma (streaming/akış yöntemiyle, 10GB'ı belleğe tek seferde
almadan)**:

| | Boyut | Sıkıştırma süresi | Oran |
|---|---|---|---|
| ZSTD(12) | 0,576GB (618,1MB) | **86,8sn** | 18,17x |
| **bz2(9)** | **0,464GB (497,9MB)** | **1.234,3sn (20,6 DAKİKA)** | 22,56x |

**Kritik bulgu: bu ölçekte/kompozisyonda bz2, ZSTD'den 14,22x daha
yavaş sıkışıyor** -- bugüne kadarki testlerimizdeki ~1,18-1,33x
farktan ÇOK daha büyük bir uçurum. bz2 hâlâ %19,4 daha küçük çıkıyor
(boyut avantajı tutarlı korunuyor) ama süre bedeli artık **orantısız
büyük**. İlginç bir yan not: ZSTD, doğrusal ölçekleme tahmininden
(~630sn) çok daha hızlı çıktı (86,8sn) -- aşırı tekrarlı veride ZSTD'nin
eşleşme bulması kolaylaşıyor gibi görünüyor, ama bz2'nin BWT
yaklaşımı aynı avantajı göstermiyor, tam tersine bu ölçekte/
kompozisyonda zorlanıyor.

**Açma hızı**: ZSTD 7,4sn, bz2 96,4sn -- oran 13,0x (bu, önceki
bulduğumuz 13-26x aralığıyla tutarlı, aşırı bir sapma yok, sadece
sıkıştırma tarafında sapma var).

**Kayıpsızlık**: her iki format için de doğrulandı (açılan boyut
kaynakla birebir aynı).

**Sonuç -- Bölüm 34'teki bz2 kararı bu uç senaryo için GEÇERLİ
DEĞİL**: `.tab.bz2` kararı, bugüne kadarki test verimize (1000
sütun, %70 binary/%30 float, dengeli bir kompozisyon) dayanıyordu --
orada bz2'nin süre bedeli ihmal edilebilirdi (~1,2-1,3x). Ama eğer
gerçek üretim verisinde bu tür AŞIRI GENİŞ (onbinlerce sütun) ve
AŞIRI SABİT-AĞIRLIKLI (neredeyse tüm sütunlar sabit/değişmeyen)
dosyalar olacaksa, bz2 kararı bu dosya TİPİ için yeniden
değerlendirilmeli -- ya ZSTD'ye geri dönülmeli ya da dosya tipine
göre algoritma seçen bir mantık (adaptif sıkıştırma) düşünülmeli.
**Bu, tek bir "her duruma uyan" sıkıştırma kararının riskli
olabileceğinin somut kanıtı -- gerçek üretim veri şekli netleşince
bu test tekrarlanmalı.**

### 35.1 Aynı uç senaryoda ZSTD'nin tüm seviyeleri (3-22) tarandı -- 19 gizli bir tatlı nokta

Kullanıcı, aynı 45.000 sütunlu/10,46GB dosyada bz2 ile karşılaştırma
yerine bu kez sadece ZSTD'yi farklı seviyelerle taramamızı istedi.
Aynı dosya yeniden üretildi (121,6sn, birebir aynı boyut: 10,46GB),
streaming yöntemiyle 7 seviye (3, 6, 9, 12, 15, 19, 22) sırayla
sıkıştırılıp açıldı, her seviyede kayıpsızlık doğrulandı.

| Seviye | Boyut (GB) | Sıkıştırma süresi | Açma süresi | Oran |
|---|---|---|---|---|
| 3 | 0,605 | 27,3sn | 6,7sn | 17,28x |
| 6 | 0,581 | 40,2sn | 6,7sn | 18,01x |
| 9 | 0,578 | 57,4sn | 6,8sn | 18,10x |
| 12 | 0,576 | 83,4sn | 6,4sn | 18,17x |
| 15 | 0,574 | 192,8sn | 7,7sn | 18,21x |
| **19** | **0,524** | **420,4sn (7,0dk)** | **8,3sn** | **19,97x** |
| 22 | 0,524 | 1.392,6sn (23,2dk) | 8,3sn | 19,96x |

Karşılaştırma için bz2(9) (Bölüm 35'ten): 0,464GB, 1.234,3sn
(20,6dk), açma 96,4sn, oran 22,56x.

**Üç net bulgu**:

1. **3->15 arası kademeli/beklenen ölçekleme** (27sn'den 193sn'e,
   boyut sadece %5 iyileşiyor) -- daha önceki (Bölüm 30.4) dar/normal
   veride gördüğümüz 12->15 cliff'i burada yok, çünkü veri zaten
   aşırı tekrarlı (44.000 sabit-sıfır sütun) ve düşük seviyeler bile
   bu deseni kolayca yakalıyor.

2. **YENİ bir cliff 15->19 arasında ortaya çıktı**: süre 2,2x artıyor
   (193sn->420sn) ama bu kez boyutta da GERÇEK bir kazanç var
   (0,574GB->0,524GB, oranı 18,21x'ten 19,97x'e çıkarıyor, yani
   %8,7 daha küçük). Bu, önceki normal-veri testlerinden farklı --
   orada üst seviyeler sadece süre yakıyordu, boyutta anlamlı kazanç
   yoktu. Bu uç/aşırı-tekrarlı veri şeklinde seviye 19'un optimal
   parse (btultra2) stratejisi gerçekten daha iyi bir sıkıştırma
   buluyor.

3. **22, 19'a göre KESİNLİKLE daha kötü** -- boyut aslında minicik
   bir miktar BÜYÜYOR (562.804.930 byte, 19'daki 562.293.181 byte'tan
   fazla) ve süre 3,3x artıyor (420sn->1.393sn). Bu, Bölüm 30.6'daki
   "seviye 22, 19'dan daha kötü bir takas" bulgusunu bu uç senaryoda
   da doğruluyor -- seviye 22 hiçbir zaman seçilmemeli.

**Sonuç -- ZSTD(19), bu uç senaryo için gerçek bir orta yol**:
bz2(9) hâlâ en küçük boyutu veriyor (0,464GB, ZSTD(19)'dan %11,4
daha küçük) ama neredeyse aynı sürede (1.234sn'e karşı 420sn --
ZSTD(19) ~2,9x DAHA HIZLI) ve açmada çok daha hızlı (8,3sn'e karşı
96,4sn -- ~11,6x daha hızlı). Yani bu dosya tipinde bz2'nin "sabit
süre bedeli kabul edilebilir" mantığı çöküyor (Bölüm 35), ama
ZSTD(19) makul bir süre karşılığında ZSTD(12)'nin sağladığından
belirgin daha iyi bir sıkıştırma sağlıyor. **Eğer üretimde bu tür
aşırı geniş/sabit-ağırlıklı dosyalar çıkarsa, ZSTD(19) mantıklı bir
uzlaşma noktası olabilir** -- ne ZSTD(12)'nin bıraktığı sıkıştırma
kazancından tamamen vazgeçmek, ne de bz2'nin dakikalarca süren
sıkıştırma/açma bedelini ödemek.

### 35.2 Aynı uç senaryoda bz2'nin de tüm seviyeleri (1-9) tarandı -- bz2'de "hafif seviye" diye bir şey yok

Kullanıcı, ZSTD'ye simetrik olarak bz2'nin de seviyelerini (1, 3, 5,
7, 9) bu aynı dosyada test etmemizi istedi. Dosya yeniden üretildi
(137,3sn, yine birebir 10,46GB/113.869 satır), streaming yöntemiyle
5 seviye sırayla sıkıştırılıp açıldı, kayıpsızlık her seviyede
doğrulandı.

| Seviye | Boyut (GB) | Sıkıştırma süresi | Açma süresi | Oran |
|---|---|---|---|---|
| 1 | 0,496 | 1.023,4sn (17,1dk) | 81,9sn | 21,08x |
| 3 | 0,479 | 1.146,3sn (19,1dk) | 87,9sn | 21,84x |
| 5 | 0,471 | 1.186,2sn (19,8dk) | 96,4sn | 22,19x |
| 7 | 0,467 | 1.195,1sn (19,9dk) | 92,1sn | 22,41x |
| 9 | 0,464 | 1.248,8sn (20,8dk) | 100,3sn | 22,56x |

**Çarpıcı bulgu: bz2'de bu senaryoda "hafif/hızlı seviye" diye bir
şey yok.** Seviye 1 (bz2'nin EN DÜŞÜK/en hızlı ayarı) bile 1.023,4sn
(17,1 dakika) sürüyor -- seviye 9'un (1.248,8sn) sadece %18 altında.
Bu, bz2'nin normal/dengeli verideki (Bölüm 34) davranışından kökten
farklı: orada seviyeler arası fark BWT blok boyutuyla (seviye
N=N×100KB) doğrusal/kademeli değişiyordu. Burada ise seviye 1->9
arası süre sadece 1,22x artarken (1.023->1.249sn), boyut kazancı da
mütevazı (%6,5, 0,496->0,464GB). **Yorum**: BWT'nin (Burrows-Wheeler
dönüşümü) maliyeti, 44.000 sabit-sıfır sütunun yarattığı aşırı
tekrarlı/uzun ortak alt-dizi yapısında blok boyutundan bağımsız
olarak zaten yüksek -- düşük blok boyutu (seviye 1=100KB) bile bu
veri yapısında pahalı bir sıralama/dönüşüm işi yapıyor. **Pratik
sonuç: bu dosya tipinde bz2'nin HİÇBİR seviyesi ZSTD(19)'un (420,4sn)
hızına yaklaşamıyor** -- en hızlı bz2 (seviye 1, 1.023,4sn) bile
ZSTD(19)'dan ~2,4x yavaş. Boyut tarafında bz2(1) yine de ZSTD(19)'dan
biraz daha iyi sıkışıyor (21,08x'e karşı 19,97x) ama süre bedeli
orantısız büyük. **Bu uç senaryoda bz2'nin "seviye düşürerek
hızlandırma" stratejisi işe yaramıyor -- eğer bz2'nin boyut avantajı
isteniyorsa süre bedeli (17+ dakika, seviyeden bağımsız) kaçınılmaz;
hız isteniyorsa ZSTD(19)'a geçilmeli.**

## 36. Kalıcı test dosyası oluşturuldu -- `mixed_wide_test.tab` (45.000 sütun, 3 tip karışık, rastgele dağılım)

Bundan sonraki testlerde tekrar tekrar kullanılmak üzere (üretip
silmek yerine) KALICI bir sentetik dosya oluşturuldu. Şema:

- **45.000 sütun toplam**: 1.000 float64 + 20.000 SABİT-sıfır +
  20.000 SABİT-bir + 4.000 KARIŞIK (hücre başına rastgele 0/1).
- **Sütun SIRASI rastgele karıştırıldı** (sabit seed=42 ile) --
  float/sabit-sıfır/sabit-bir/karışık türleri dosya boyunca
  dağılmış durumda, önceki testlerdeki gibi bloklar halinde
  gruplanmadı. Gerçek telemetri dosyalarındaki düzensiz sütun
  dizilimini taklit ediyor.
- **Satır sayısı düzgün/yuvarlak bir sayı**: **100.000** (önceki
  testlerdeki "113.869" gibi çirkin sayılar yerine).
- **Nihai boyut: 9,186GB (9.862.938.544 byte)** -- hedeflenen ~10GB'a
  yakın.

**Üretim scripti kalıcı olarak repoya kaydedildi**:
`scripts/gen_mixed_binary_test_file.py` -- sabit seed (42) ile tekrar
çalıştırıldığında birebir aynı dosyayı üretir (kayıp durumunda
kurtarma garantili). Üretim süresi: 1.927,9sn (~32,1dk) -- önceki
basit (sadece float+sabit-sıfır) generator'dan çok daha yavaş,
çünkü her chunk'ta 3 ayrı DataFrame (float64 + int8×2 + rastgele
int8) oluşturup birleştirmek ve sütun sırasını karıştırmak
(`chunk_df[shuffled_cols]`) ek yük getiriyor.

**Dosyanın kendisi ve bir "column manifest" (JSON) container'ın
`/work` klasöründe KALICI olarak bırakıldı (silinmedi)**:
- `/work/mixed_wide_test.tab` -- ana veri dosyası (9,19GB)
- `/work/mixed_wide_test_columns.json` -- her sütunun adı ve TÜRÜ
  (`float64`/`constant_zero`/`constant_one`/`mixed_binary`) ile
  birlikte tam sütun sırası -- ileride otomatik sınıflandırma
  doğrulaması (Bölüm 31.5) ve genel referans için.

**Not**: `/work` bir Docker named volume (`t2p-work4`) üzerinde --
container yeniden başlatılırsa (`docker start`) korunur, ama
Docker Desktop "purge/clean data" işlemiyle SİLİNİR (bu oturumda
birkaç kez yaşandığı gibi). Eğer purge olursa, `scripts/
gen_mixed_binary_test_file.py` ile birebir aynı dosya yeniden
üretilebilir -- veri kaybı riski YOK, sadece ~32 dakikalık yeniden
üretim maliyeti var.

## 37. `mixed_wide_test.tab` ile uçtan uca pipeline -- ZSTD(3) -> MinIO -> ClickHouse -> sorgu, 45.000 sütunda İKİ YENİ altyapı sınırı bulundu

Bölüm 36'daki kalıcı test dosyası (45.000 sütun, 100.000 satır,
9,19GB) ile tam pipeline denendi: ZSTD(3) sıkıştır -> MinIO'ya yükle
-> ClickHouse hedef tablosuna `s3()` ile toplu yükle -> birkaç sorgu
çalıştır. Yol boyunca 45.000 sütunun ClickHouse'un VARSAYILAN
limitlerini aştığı İKİ AYRI nokta bulundu -- ikisi de bugüne kadarki
testlerde (en fazla 1.000-1.700 sütunluk şemalarda) hiç görülmemişti.

### 37.1 ZSTD(3) sıkıştırma ve MinIO yükleme -- sorunsuz

| Adım | Sonuç |
|---|---|
| ZSTD(3) sıkıştırma (streaming) | 9,186GB -> 1,857GB, **46,4sn**, oran **4,95x** |
| MinIO'ya yükleme | 9,3sn (204,9MB/sn) |

**Not**: bu dosyanın sıkıştırma oranı (4,95x) Bölüm 35'teki aşırı
uç senaryodan (44.000 sütunun TAMAMI sabit-sıfır, ZSTD(3) oranı
17,28x) çok daha düşük -- beklenen bir sonuç, çünkü bu dosyada
binary sütunların sadece yarısı sabit (20.000 sıfır + 20.000 bir),
4.000'i tamamen rastgele (hiç sıkışmayan gürültü) ve sütun sırası da
karıştırılmış (aynı türden sütunlar yan yana değil, ZSTD'nin
satır-içi tekrar yakalama şansı azalıyor). Bu, **gerçekçi/dengeli bir
kompozisyonda ZSTD oranının aşırı-uç senaryolardan çok farklı
çıkabileceğinin somut kanıtı**.

### 37.2 İLK engel -- `max_query_size` / `max_ast_elements` (45.000 sütunlu CREATE TABLE)

Manifest'ten otomatik üretilen `CREATE TABLE` DDL'i (her sütun için
tip+codec tanımıyla) **1.542.643 karakter** çıktı. İki varsayılan
ClickHouse limiti sırayla aşıldı:

1. `max_query_size` (varsayılan 262.144 byte/~256KB) -- "Max query
   size exceeded" hatası.
2. Bunu düzeltince `max_ast_elements` (varsayılan 50.000 AST
   düğümü) -- "AST is too big. Maximum: 50000" hatası (45.000 sütun
   × sütun-başına birden fazla AST düğümü = 50.000'i kolayca aşıyor).

**Çözüm**: sorgu ayarlarına `max_query_size=200_000_000`,
`max_ast_elements=5_000_000`, `max_expanded_ast_elements=5_000_000`
eklendi -- DDL sorunsuz çalıştı. **Ders: 10.000+ sütunlu şemalarda bu
üç ayar rutin olarak yükseltilmeli, varsayılanlar bu ölçek için
tasarlanmamış.**

### 37.3 İKİNCİ ve daha ciddi engel -- "Wide" parça formatı, sütun başına ayrı yazma tamponu -- bellek taşması

Tablo oluşturulduktan sonra `s3()` ile yükleme (`INSERT INTO ...
SELECT * FROM s3(...)`) **"(total) memory limit exceeded... maximum:
9.21-9.37 GiB"** hatasıyla iki kez çöktü:

- 1. deneme (varsayılan ayarlar): hata `ParallelParsingBlockInputFormat`
  içinde -- paralel parse sırasında bellek taştı. `input_format_parallel_parsing=0`,
  `max_threads=2`, `max_insert_threads=1`, `max_block_size=8192` ile
  düzeltilmeye çalışıldı.
- 2. deneme (düşük-bellek parse ayarlarıyla): hata bu kez FARKLI bir
  yerde -- `MergeTreeDataPartWriterWide::addStreams` /
  `CompressedWriteBuffer` içinde, yani **YAZMA tarafında**. Kök
  neden: ClickHouse'un varsayılan **"Wide" MergeTree parça formatı,
  HER SÜTUN İÇİN AYRI bir sıkıştırılmış yazma akışı/tamponu açıyor**
  -- 45.000 sütunda bu, parse ayarlarından tamamen BAĞIMSIZ olarak
  sunucunun bellek bütçesini (~9,2-9,4GB) tek başına dolduruyor.

**Gerçek çözüm -- "Compact" parça formatını zorlamak**: tablo
`min_bytes_for_wide_part` ve `min_rows_for_wide_part` ayarları çok
yüksek bir değere (pratikte asla aşılmayacak) çekilerek yeniden
oluşturuldu:

```sql
CREATE TABLE mixed_wide_test (...)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 10737418240000,
         min_rows_for_wide_part = 1000000000
```

Bu, ClickHouse'a HER ZAMAN "Compact" format (tüm sütunlar TEK bir
dosyada, sütun başına ayrı akış YOK) kullanmasını zorluyor. Bu
değişiklikle yükleme **sorunsuz tamamlandı** (`system.parts.part_type
= 'Compact'` ile doğrulandı). **Ders: 10.000+ sütunlu tablolarda
varsayılan "Wide" format kullanılamaz -- `min_bytes_for_wide_part`/
`min_rows_for_wide_part` ile "Compact" format ZORUNLU olarak
tercih edilmeli.** (Not: Compact format normalde küçük/az-satırlı
parçalar için düşünülmüştür; çok-sütunlu/çok-satırlı senaryoda sorgu
performansına etkisi bu testte ölçülmedi, sadece yükleme başarısı
doğrulandı -- ayrı bir performans karşılaştırması gerekebilir.)

### 37.4 Nihai sonuçlar (Compact format + düşük-bellek parse ayarlarıyla)

| Aşama | Sonuç |
|---|---|
| ZSTD(3) sıkıştırma | 46,4sn, 4,95x oran, 1,857GB |
| MinIO'ya yükleme | 9,3sn |
| ClickHouse yükleme (`s3()`->tablo, Compact format) | **186,9sn, 535,2 satır/sn** |
| ClickHouse'daki disk boyutu (`UInt8+T64,ZSTD` + `Float64+ZSTD`) | 2,048GB |

**535,2 satır/sn, bugüne kadarki en düşük yükleme hızı** -- ama
karşılaştırılabilir değil, çünkü önceki tüm hız ölçümleri (Bölüm
24-28) ~1.000 sütunlu şemalardaydı. 45.000 sütun/satır başına
düşen decode+yazma maliyeti (Bölüm 29'un "decode dominant" bulgusunun
45x'e büyütülmüş hali) bu düşüşü açıklıyor.

**Sorgu testleri** (Compact formatta, tabloya önceden yüklenmiş
haliyle):

| Sorgu | Süre |
|---|---|
| `SELECT count()` | 69,7ms |
| Float sütununda `avg/min/max` | 1.063,3ms |
| Karışık binary sütunda filtre (`=1`) -- 50.108/100.000 satır döndü (~%50, beklenen) | 660,1ms |
| Sabit sütunlarda `sum()` -- (0, 100.000) döndü, doğru | 1.924,4ms |
| Küçük projeksiyon (2 sütun, 10 satır) | 427,7ms |

**Doğruluk kontrolü**: sabit-sıfır sütununun toplamı 0, sabit-bir
sütununun toplamı satır sayısına (100.000) eşit çıktı -- veri
kaybı/yanlış eşleşme yok. Karışık sütunda `=1` filtresi ~%50 satır
döndürdü, `rng.integers(0,2)` ile üretilen gerçek rastgele
dağılıma tutarlı.

**Genel değerlendirme**: 45.000 sütunlu bir tablo ClickHouse'da
ÇALIŞABİLİR ama varsayılan ayarlarla DEĞİL -- hem DDL hem yükleme
tarafında birden fazla varsayılan limit manuel olarak yükseltilmesi/
değiştirilmesi gerekti. Sorgu performansı (özellikle tek-sütun
agregasyonları saniyenin altında) makul, ama bu ölçekte HİÇBİR
optimizasyon (partition/order key, index) henüz denenmedi -- sadece
temel işlevsellik doğrulandı. **Tablo ve MinIO'daki `.tab.zst`
kalıcı bırakıldı, silinmedi -- kullanıcının "bir süre kullanacağız"
isteğine uygun olarak ileride üzerinde daha fazla test yapılabilir.**

## 38. `mixed_wide_test.tab` -> parquet -> MinIO -> ClickHouse denemesi -- ClickHouse'un Parquet okuyucusu 45.000 sütunda GERÇEK bir sınıra çarpıyor

Kullanıcı Bölüm 37'nin (ham metin, ZSTD3) karşılaştırması için aynı
işlemi parquet ile de denemeyi istedi: `.tab` -> parquet -> MinIO ->
ClickHouse -> aynı sorgular. Bu, oturumun en sorunlu testi oldu --
hem host'un Docker Desktop/WSL2 alt yapısı hem de ClickHouse'un
kendisi ciddi kararlılık sorunları gösterdi.

### 38.1 Host çöküşleri -- tab->parquet dönüşümünün kendisi bile host'u defalarca devirdi

`.tab -> parquet` dönüşümü (DuckDB ve ardından pandas tabanlı
denemeler) **host'un tamamını (Docker Desktop + WSL2) 4 KEZ
çökertti** -- her seferinde `docker ps` 500 Internal Server Error
vermeye başladı, bazen PowerShell komutları bile yanıt vermez oldu
(host CPU/kaynak baskısı altında tamamen tıkandı). Her seferinde
`wsl --shutdown` + Docker Desktop yeniden başlatma ile kurtarıldı,
veri kaybı olmadı (`/work` volume korundu).

**Kök neden bulundu -- pyarrow'un ParquetWriter'ı ÇOK SAYIDA KÜÇÜK
row group ile kullanılınca patlıyor**: ilk yaklaşım (pandas chunked
+ ardından pyarrow streaming CSV okuyucusu, ~40 satırlık mini-batch'ler
halinde her batch'i ayrı `write_batch()` ile yazmak) 45.000 sütun ×
binlerce mini-row-group çarpımıyla milyonlarca küçük meta veri
nesnesi biriktiriyordu -- container'ın 4GB cgroup limitine takılıp
"Killed" (OOM) ile öldü, 100.000 satırın sadece %4,2'sinde. **Düzeltme**:
pyarrow batch'lerini biriktirip DAHA AZ ama DAHA BÜYÜK row group'lar
halinde (`ROW_GROUP_TARGET=5000`) yazmaya geçildi -- bu, 100.000
satır için ~2.500 yerine ~20 row group anlamına geliyor, sütun-başı
meta veri çarpanını ~125x azaltıyor.

**Kullanıcı isteğiyle** ("yine iptal olursa farklı bir şey dene"),
tam 100.000 satırlık dosya yerine **ilk 20.000 satırlık bir alt küme**
(`mixed_wide_test_20k.tab`, 1,84GB) ile devam edildi -- host'u daha
az zorlayarak anlamlı bir karşılaştırma elde etmek için.

### 38.2 tab->parquet dönüşümü ve MinIO yükleme -- BAŞARILI

Düzeltilmiş (az/büyük row-group) script, container 1 CPU + 6GB'a
sınırlanmış halde, 20.000 satırlık alt kümede sorunsuz çalıştı:

| Adım | Sonuç |
|---|---|
| `.tab` (sıkıştırılmamış) kaynak boyutu | 1,837GB |
| Parquet dönüşümü | 209,2sn, **0,219GB, oran 8,40x** |
| MinIO'ya yükleme | 2,7sn (82,6MB/sn) |
| ClickHouse hedef tablosu oluşturma (Compact format zorlanmış) | başarılı |

### 38.3 ClickHouse'a `s3()` ile parquet yükleme -- 3 denemede de BAŞARISIZ, gerçek bir ClickHouse sınırı

Parquet dosyasını ClickHouse'a `INSERT ... SELECT * FROM s3(...,
'Parquet')` ile yüklemek **3 farklı denemede de** aynı hatayla
çöktü: `(total) memory limit exceeded`, `ParquetV3BlockInputFormat`
içinde, `Parquet::Reader::decodeDictionaryPage` sırasında.

1. **1. deneme** (düşük-bellek parse ayarlarıyla, `max_threads=1`
   vb.): 6,56GB sunucu tavanında çöktü.
2. **2. deneme** (`input_format_parquet_use_native_reader_v3=0`
   sorgu ayarıyla eski okuyucuya dönmeye çalışıldı -- **ayar sorgu
   seviyesinde etkisiz çıktı**, hâlâ V3 okuyucu kullanıldı): 9,67GB
   tavanında, sütun `o6550`'de çöktü (45.000 sütunun sadece
   %14,5'inde).
3. **3. deneme** (ClickHouse sunucu config'ine `max_server_memory_usage_to_ram_ratio`
   0,90'dan **0,95**'e çıkarıldı -- 9,6GB'tan **11,09GB**'a; ayrıca
   profil seviyesinde V3 okuyucuyu kapatma denendi, o da etkisiz
   kaldı): daha ileri gitti ama yine çöktü, 10,22GB tavanında, sütun
   `f156`'da.

**Kritik gözlem: hata her seferinde FARKLI bir sütunda oluyor, ama
her seferinde RSS neredeyse tam tavanda.** Bu, satır sayısıyla değil
**işlenen sütun sayısıyla kümülatif büyüyen, serbest bırakılmayan
bir bellek birikimi** olduğunu gösteriyor -- yani **satır sayısını
daha da azaltmak bu sorunu ÇÖZMEZ**, çünkü 45.000 sütunun kendisi
zaten orada; ~6.550 sütun işlendiğinde bile (sadece 20.000 satırlık,
235MB'lık bir dosyada) GB'larca bellek birikmiş oluyordu.
`input_format_parquet_use_native_reader_v3` ayarı hem sorgu hem
sunucu-profili seviyesinde denendi, **hiçbiri gerçek okuyucu seçimini
değiştirmedi** (`system.settings` sorgusunda değer değişse bile stack
trace hep `ParquetV3BlockInputFormat` gösterdi) -- bu ClickHouse
sürümünde V3 okuyucudan çıkış yolu bulunamadı.

**Sonuç -- bu ClickHouse sürümünün Parquet okuyucusu (V3), aşırı
geniş (45.000 sütunlu) dosyalarda GERÇEK bir bellek yönetimi
sorununa sahip** -- ayar değişikliğiyle, thread azaltmayla, ya da
sunucu bellek tavanını %5 artırmayla çözülemedi. **Bu, ince ayar
sorunu değil, mimari bir sınırlama gibi görünüyor.**

**Doğrudan karşılaştırma -- aynı 45.000 sütunlu veri, iki format,
iki farklı sonuç**:

| | Ham metin + ZSTD3 (Bölüm 37) | Parquet (Bölüm 38) |
|---|---|---|
| Satır sayısı | 100.000 (tam dosya) | 20.000 (alt küme, host stabilitesi için) |
| ClickHouse'a yükleme | **BAŞARILI** (186,9sn, 535,2 satır/sn) | **BAŞARISIZ** (3 denemede de bellek taşması) |
| Kaynak format boyutu | 1,857GB (ZSTD3) | 0,219GB (parquet, ~8,4x küçük) |

**Bu, oturumun en önemli mimari bulgularından biri**: parquet dosya
BOYUTU olarak ham metinden çok daha küçük çıksa bile (beklenen,
Bölüm 30'un bulgusuyla tutarlı), **45.000 sütunlu aşırı geniş
dosyalarda ClickHouse'a YÜKLENEBİLİRLİK açısından ham metin (TSV/
`TabSeparatedWithNames`) parquet'ten daha güvenilir** -- en azından
bu ClickHouse sürümünde/bu ortamda. Gerçek üretim verisi bu kadar
geniş sütunlu olacaksa (kullanıcının "her uçak tipi için farklı
sayıda sütun var" notu, bazı tiplerin çok geniş olabileceğini
düşündürüyor), **parquet'in ClickHouse'a yükleme adımında kör nokta
olabileceği ciddiye alınmalı** -- ya ClickHouse'un daha yeni/farklı
bir sürümü denenmeli, ya da bu ölçekte ham-metin yükleme yolu (Bölüm
30'daki hibrit mimarinin "parquet ClickHouse'a, `.tab.zst` MinIO'ya"
kararı) YENİDEN gözden geçirilmeli -- belki her iki hedefte de
ham-metin/sıkıştırılmış format kullanılmalı, aşırı geniş şemalar
için.

**Ortam notu**: ClickHouse sunucu config'i (`max_server_memory_usage_to_ram_ratio`)
test sonunda güvenli varsayılana (0,90) geri alındı, host riski
azaltmak için. `t2p-cmp3` container'ı hâlâ 1 CPU/6GB sınırlı --
ileride ağır iş yükleri için bu sınırlar gerekirse gevşetilmeli.
20.000 satırlık alt küme dosyaları (`mixed_wide_test_20k.tab`,
`.parquet`) `/work`'te kalıcı bırakıldı.

## 39. NİHAİ MİMARİ KARARI -- parquet tamamen çıkarıldı, hem MinIO hem ClickHouse için ham `.tab`+sıkıştırma kullanılacak

**Kritik bağlam düzeltmesi**: kullanıcı, gerçek üretimdeki "normal"
genişliğin en az **10.000 sütun** olacağını, 45.000 sütunlu
dosyaların da yaygın olacağını netleştirdi. Bu, Bölüm 24-34'teki
"parquet güvenilir çalışıyor" testlerinin (~1.000 sütun) gerçek
üretimin HİÇBİR yerini temsil etmediği anlamına geliyor -- parquet'in
güvenilir olduğu KANITLANMIŞ tek aralık, üretimde hiç
karşılaşılmayacak bir ölçek.

**Karar gerekçesi**: Bölüm 38'de parquet 45.000 sütunda (ClickHouse'un
V3 Parquet okuyucüsünde) kesin olarak çöktü -- ve hata SATIR sayısıyla
değil İŞLENEN SÜTUN sayısıyla kümülatif büyüyordu (20.000 satırlık
sadece 235MB'lık bir dosyada, sütunların sadece %14,5'i işlendiğinde
GB'larca bellek tükenmişti). Bu, 10.000 sütunda da benzer/yakın bir
sorun çıkma riskinin gerçek olduğu, ama net bir güvenli eşik
bilinmediği anlamına geliyor. **İki ayrı kod yolu (parquet + ham
metin, hangisinin ne zaman güvenli olduğu bilinmeden) inşa etmek,
tek bir kanıtlanmış-çalışan yola geçmekten daha riskli.**

**NİHAİ KARAR: parquet pipeline'dan tamamen çıkarılıyor.** Hem MinIO
arşivi hem ClickHouse'a yükleme kaynağı olarak **ham `.tab` +
sıkıştırma** (ZSTD ya da bz2, dosya kompozisyonuna göre -- bkz. Bölüm
34/35/35.1/35.2) kullanılacak. Bölüm 30'daki "parquet ClickHouse'a
geçici, `.tab.zst` MinIO'ya kalıcı" hibrit mimarisi GEÇERSİZ --
artık parquet üretimine hiç gerek yok, tek format/tek dosya iki amaca
da (arşiv + yükleme) hizmet ediyor.

**Bu kararın somut faydaları**:
1. **Basitlik**: tek format, tek dönüşüm adımı yok -- `.tab` dosyası
   sıkıştırılıp doğrudan hem MinIO'ya hem (gerektiğinde) ClickHouse'a
   `s3()` + `TabSeparatedWithNames` ile yüklenir (Bölüm 37'de 45.000
   sütun/100.000 satırda uçtan uca kanıtlandı).
2. **Güvenilirlik**: parquet'in ClickHouse okuma tarafındaki
   öngörülemeyen/ayarla-düzeltilemeyen bellek sorunundan (Bölüm 38)
   tamamen kaçınılıyor.
3. **Kırılgan bağımlılık ortadan kalkıyor**: DuckDB/pyarrow ile
   parquet üretimi kendi başına host'u 4 kez çökertmişti (Bölüm 38.1)
   -- bu adım artık pipeline'da hiç yok.

**Bilinçli olarak kabul edilen bedel**: ham metin yükleme, parquet'in
çalıştığı ölçekte (~1.000 sütun, Bölüm 30) parquet'ten ~2,27x daha
yavaştı. Ama bu bir "seçim" değil -- parquet'in gerçek üretim
ölçeğinde (10.000+ sütun) güvenle çalıştığına dair hiç kanıt yok,
o yüzden bu hız farkı zaten hiçbir zaman güvenle elde edilemeyecek
bir avantajdı.

**Değişmeyen/hâlâ geçerli kararlar**:
- ClickHouse hedef tablosunda `CODEC(ZSTD)` (Bölüm 28.3) ve binary
  sütunlar için `UInt8`+`CODEC(T64,ZSTD)` (Bölüm 31, gerçek `.ham`
  değer aralığı doğrulanana kadar üretime ALINMAMALI -- Bölüm 31.4
  güvenlik uyarısı hâlâ geçerli) -- bu, kaynak formattan (parquet/TSV)
  BAĞIMSIZ, ClickHouse'un kendi depolama şemasıyla ilgili, hâlâ
  geçerli.
- Sıkıştırma algoritması/seviyesi kararı dosya kompozisyonuna göre
  değişebilir (Bölüm 34: dengeli kompozisyonda bz2; Bölüm 35: aşırı
  sabit-ağırlıklı kompozisyonda ZSTD19 daha iyi) -- gerçek üretim
  dosya şekli netleşince bu karar 10.000+ sütun ölçeğinde YENİDEN
  test edilmeli (şu ana kadarki tüm sıkıştırma testleri ya ~1.000 ya
  da 45.000 sütunda yapıldı, 10.000 sütunluk gerçek "normal" ölçekte
  hiç test edilmedi).

**Açık takip maddesi**: Bölüm 26'daki worker/thread eşzamanlılık
ayarları (N=2 optimal, `max_download_threads` vb.) da ~1.000 sütunluk
ölçekte yapılmıştı -- gerçek 10.000+ sütunluk ölçekte bu ayarların
hâlâ geçerli olup olmadığı doğrulanmadı, ileride tekrar test
edilmeli.

## 40. İlk uçtan uca pipeline denemesi -- gerçek şekilli veriyle (küçük ölçek), Postgres manifest devreye alındı

Kullanıcı, artık sentetik "test dosyası" değil, gerçek şemaya sahip
(`testdata/dataset_01.tab`, 300 float + 700 binary + timestamp,
10,9GB) bir dosyayla TÜM pipeline'ı (temizleme -> sıkıştırma -> MinIO
-> ClickHouse -> Postgres manifest) uçtan uca denemeyi istedi. Önce
küçük bir alt kümeyle doğrulama yapıldı, tam dosya henüz denenmedi.

**Ortam notu**: Docker Desktop oturum başında tamamen kapalıydı,
yeniden başlatıldı; container'lar (`minio`, `clickhouse`, `t2p-cmp3`)
korunmuş halde geri geldi.

**Temizlik (kullanıcı isteğiyle, yer açmak için)**: önceki oturumlardan
kalan büyük test dosyaları silindi -- container'da ~14GB
(`mixed_wide_test.tab` ve türevleri, `bench_sample.*`), MinIO'da
**~49GB** (`scale200/` ve `codec_test/` klasörlerindeki 210 adet eski
parquet dosyası + eski `.tab.zst`/`.parquet` nesneleri). Eski
ClickHouse test tabloları (`mixed_wide_test`, `mixed_wide_test_parquet_20k`)
de silindi.

**Yeni bileşen -- Postgres**: proje boyunca hiç kurulmamıştı, bu
oturumda ilk kez `postgres:16` container'ı (`t2p-net` ağında, `t2p-pgdata`
volume'ünde) ayağa kaldırıldı. `docs/postgres_manifest_schema.sql`
şeması uygulandı; şemaya iki yeni alan eklendi (`tab_zst_object_key`,
`tab_zst_size_bytes`) -- eski `parquet_object_key`/`parquet_size_bytes`
alanları Bölüm 39 kararıyla artık kullanılmıyor ama şimdilik
dokunulmadı (kullanıcı isteğiyle: "birkaç şey ekle, sonra detaylıca
bakarız").

**Kaynak veride bulunan ve düzeltilen sorun**: `dataset_01.tab`'ın
HER satırının sonunda fazladan bir tab karakteri olduğu tespit edildi
(`...b699\t\n`) -- bu, gerçek 1001 sütun yerine 1002 alan (biri boş
"hayali sütun") sayılmasına yol açıyordu. Temizleme adımı bunu
düzeltiyor (`rstrip` ile satır sonundaki fazla tab/satır sonu
karakterleri siliniyor).

**Küçük ölçekte uçtan uca test (ilk 2.000 satır)**:

| Adım | Sonuç |
|---|---|
| Kaynak doğrulama | 1001 sütun (timestamp+300f+700b), 2.000 satır |
| ZSTD(12) sıkıştırma | 9,08MB -> 3,34MB, 0,67sn, 2,72x oran |
| MinIO'ya yükleme | 0,03sn |
| ClickHouse tablo oluşturma | 1001 sütun (tümü Float64, güvenlik-önce yaklaşımı) |
| ClickHouse yükleme (`s3()`) | 2.000 satır, 0,35sn |
| Üç yönlü doğrulama | tab satır sayısı = ClickHouse satır sayısı (2.000=2.000) ✓ |
| Postgres manifest kaydı | başarıyla eklendi, `status='done'` |

**Tip kararı**: `b0`-`b699` sütunları şimdilik hepsi `Float64` olarak
yüklendi (Bölüm 31.4'teki güvenlik uyarısı gereği -- gerçek değer
aralığı henüz tek tek doğrulanmadı). `UInt8`+`T64` optimizasyonu,
kullanıcının "sonra detaylıca bakarız" dediği ileriki adımda ele
alınacak.

**Sonuç: pipeline uçtan uca çalışıyor, doğrulandı.** Bir sonraki adım
kullanıcı onayıyla `dataset_01.tab`'ın TAMAMIYLA (10,9GB) aynı
pipeline'ı denemek.

### 40.1 Postgres manifest -- tarayıcıdan erişim ve şema genişletmesi

Kullanıcı Postgres'i tarayıcıdan kontrol etmek isteyince **Adminer**
(hafif, tek-sayfalık web DB yöneticisi) `adminer` imajıyla ayağa
kaldırıldı (`t2p-net` ağında, `localhost:8080`). Ayrıca Postgres
container'ı başlangıçta host'a port açmadan kurulmuştu -- host'tan
(GUI araçlarından) erişim için `-p 5432:5432` ile **volume korunarak**
yeniden oluşturuldu (veri kaybı olmadı, doğrulandı).

**Şema genişletmesi**: kullanıcı "Show structure" görünümündeki
`NULL`/varsayılan değerleri gerçek veri sanıp kafası karıştı --
bunun şema tanımı olduğu, gerçek kayıtların "Select data" sekmesinde
olduğu açıklandı. Ardından kullanıcı önerilen tüm yeni alanların
eklenmesini istedi. `conversion_manifest` tablosuna **13 yeni alan**
eklendi (`ALTER TABLE`, mevcut veri korunarak):

- **Sıkıştırma**: `compression_algorithm`, `compression_level`, `original_size_bytes`
- **Şema/sütun**: `column_count`, `aircraft_type`, `had_trailing_tab_issue`
- **Süre/performans**: `compress_duration_seconds`, `minio_upload_duration_seconds`, `clickhouse_load_duration_seconds`
- **ClickHouse hedefi**: `clickhouse_table_name`, `clickhouse_disk_bytes`
- **İzlenebilirlik düzeltmesi**: `is_subset`, `subset_row_count` (önceden `tab_file_name`
  alanına "ilk 2000 satırlık test alt kümesi" gibi açıklama sıkıştırılmıştı,
  bu artık ayrı/temiz alanlara taşındı -- `tab_file_name` artık sade dosya adı)

`docs/postgres_manifest_schema.sql` güncellendi, pipeline scripti
(`scripts/pipeline_tab_to_clickhouse.py`) tüm yeni alanları doldurup
tekrar çalıştırıldı -- tüm alanlar doğru doluyor, doğrulandı.

**ÖNEMLİ KURAL (kullanıcı talebiyle) -- bundan sonra yeni veri/pipeline
çalıştırmalarında `aircraft_type` MUTLAKA doldurulmalı** (farklı uçak
tiplerinde sütun sayısı değişebiliyor, bu alan olmadan hangi kaydın
hangi şemaya ait olduğu izlenemez). Şu anki `dataset_01.tab` sentetik
bir dosya olduğu için (gerçek uçak tipi bilgisi yok, repo'da da böyle
bir kayıt bulunamadı) `aircraft_type` bilinçli olarak `NULL` bırakıldı
-- uydurulmadı. Gerçek üretim verisiyle çalışılmaya başlanınca bu alan
zorunlu tutulmalı.

**Kalıcı script'ler repoya kaydedildi**:
- `scripts/pipeline_tab_to_clickhouse.py` -- tam pipeline (temizle
  [şu an ayrı adımda] -> sıkıştır -> MinIO -> ClickHouse -> Postgres
  manifest). Şu an `dataset_01.tab`'ın 2.000 satırlık alt kümesine göre
  sabit değerlerle yazılı -- tam dosyayla çalıştırmadan önce
  parametrize edilmesi gerekiyor.
- `scripts/clean_tab_trailing_tab.py` -- temizleme adımı (fazladan
  satır-sonu tab'ını siler), tüm dosya ya da alt küme için kullanılabilir.

## 41. 5 sütun-sayısı x 4 satır-sayısı sentetik test grid'i (20 dosya) -- gerçek 10.000+ sütun ölçeğinde testler için

Kullanıcı, Bölüm 39/40'ta tespit edilen "sıkıştırma/eşzamanlılık
kararları hâlâ ~1.000 sütun ölçeğinde, 10.000+ ölçekte doğrulanmadı"
açık maddesini kapatmak için bir test grid'i istedi: **5 sütun-sayısı
tier'i (10k/20k/30k/40k/50k VERİ sütunu) x 4 satır sayısı (1k/5k/50k/
100k) = 20 dosya**.

**Sütun tasarımı** (her tier için N = veri sütunu sayısı):
- `timestamp` + `aircraft_type` -- EK olarak en başta (N'e dahil değil)
- %10 float64 -- HER ZAMAN N'in en başında, sıralı (f0, f1, ...)
- %10 karışık 0/1, %40 sabit-sıfır, %40 sabit-bir -- bu üçü BİRLİKTE
  rastgele karıştırılıyor (float'larla değil, sadece kendi aralarında)

**5 "uçak tipi"** sütun sayısına göre tanımlandı: `AIRCRAFT_10K`,
`AIRCRAFT_20K`, `AIRCRAFT_30K`, `AIRCRAFT_40K`, `AIRCRAFT_50K`.

**Verimlilik iyileştirmesi**: önceki generator'da (Bölüm 36) binary
sütunlar ayrı ayrı DataFrame'ler halinde üretilip sonra TÜM sütun
setine (float dahil) `reindex` uygulanıyordu. Bu sefer float'lar
sabit sırada tutulduğu için sadece binary blok, doğrudan hedef
(karıştırılmış) sırada bir numpy array'e yazılıp (`binary_arr[:,
one_positions]=1` gibi konum-bazlı atamalarla) tek adımda
oluşturuldu -- ayrı reindex adımına hiç gerek kalmadı.

**Sonuçlar (dosya boyutu/süre, satır sayısına göre artan)**:

| Tier | 1k satır | 5k satır | 50k satır | 100k satır | Tier toplam |
|---|---|---|---|---|---|
| 10k sütun | 1,7sn | 8,9sn | 93,1sn | 185,1sn | 4,16GB / 4,8dk |
| 20k sütun | 5,5sn | 27,5sn | 279,7sn | 561,7sn | 8,32GB / 14,6dk |
| 30k sütun | 10,1sn | 48,1sn | 486,2sn | 956,4sn | 12,48GB / 25,0dk |
| 40k sütun | 16,8sn | 83,9sn | 850,9sn | 1.736,3sn | 16,64GB / 44,8dk |
| 50k sütun | 38,3sn | 192,0sn | 1.889,1sn | 3.707,2sn (61,8dk) | 20,80GB / 61,8dk |

**Bulgu -- süre sütun sayısıyla DOĞRUSALDAN biraz daha hızlı artıyor**:
10k->20k (2x sütun) süreyi ~3x artırdı; 40k->50k (1,25x sütun) süreyi
de (son dosya artık tamamlandığı için kesinleşti) benzer bir oranda
artırdı. Muhtemel sebep: pandas'ın büyük DataFrame birleştirme
(`concat`)/CSV yazma maliyeti sütun sayısıyla tam doğrusal değil.

**TAMAMLANDI (2. oturumda)**: kullanıcı bilgisayarı kapattıktan sonra
geri dönüldü, ortam (Docker Desktop + 5 container) yeniden ayağa
kaldırıldı, hepsi sorunsuz döndü. Yarım kalan `synthetic_50k_100000.tab`
(kesilme anında 3,69GB'taydı) ve eşlik eden `_columns.json` silinip
üretici script (artık idempotent -- var olan dosyaları atlıyor)
yeniden çalıştırıldı. Sonuç: **son dosya 100.000 satır, 50.002 sütun,
13,333GB, 3.707,2sn (61,8dk) sürede tamamlandı** -- diğer 19 dosya
otomatik atlandı (`[atlanildi, zaten var]`), yeniden üretilmedi.

**20/20 dosya TAMAMLANDI, toplam 62,40GB, `/work/synthetic_grid/`
altında kalıcı olarak duruyor.** Üretici script kalıcı olarak
`scripts/gen_synthetic_grid.py`'e kaydedildi (idempotent hale
getirildi -- tekrar çalıştırılırsa sadece eksik dosyaları üretir).

**Henüz yapılmadı**: bu 20 dosyanın pipeline'dan (temizle->sıkıştır->
MinIO->ClickHouse->Postgres) geçirilmesi -- sadece ham `.tab` üretimi
tamamlandı. Sıkıştırma/eşzamanlılık testleri bu dosyalarla henüz
YAPILMADI, bu hâlâ açık bir takip maddesi -- Bölüm 39'daki "sıkıştırma
kararı ve worker/thread ayarları 10.000+ sütun ölçeğinde doğrulanmadı"
açık maddesini kapatmak için asıl amaç buydu.
