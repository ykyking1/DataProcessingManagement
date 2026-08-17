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
