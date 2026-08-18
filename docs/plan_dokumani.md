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
