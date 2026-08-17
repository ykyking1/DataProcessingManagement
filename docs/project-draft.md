Bu akış projenin A2 kısmı için geçerlidir


# AU-AIR Veri İşleme ve Kalite Dashboard'u

## Proje özeti

Bu projenin amacı, AU-AIR veri seti için tekrarlanabilir bir veri işleme
pipeline'ı ve bu pipeline'ın çıktılarını görünür hâle getiren bir dashboard
geliştirmektir.

Sistem; ham verinin doğrulanması, preprocessing ve augmentation işlemlerinin
uygulanması, sonuçların tekrar doğrulanması ve veri sürümlerinin takip edilmesi
adımlarını kapsayacaktır.

> Bu belge ilk mimari taslaktır. Teslim kapsamı ve kabul kriterleri mentör
> dokümantasyonu sonrasında güncellenecektir.

## Planlanan mimari

```text
AU-AIR dataset
      |
      v
DVC ile veri alma ve sürümleme
      |
      v
Dagster pipeline
      |
      +-- Ham veri ve annotation inceleme
      +-- Great Expectations ile validation
      +-- Preprocessing
      +-- Albumentations ile augmentation
      +-- İşlenmiş veri validation'ı
      +-- Metrik ve rapor üretimi
      +-- Çıktıları DVC ile sürümleme
      |
      v
Dashboard
      +-- Raw / processed karşılaştırması
      +-- Augmentation örnekleri
      +-- Sınıf ve bounding box dağılımları
      +-- Veri kalitesi sonuçları
      +-- Pipeline çalışma durumu
```

## Teknolojilerin sorumlulukları

| Teknoloji | Planlanan sorumluluk |
| --- | --- |
| Dagster | Pipeline orkestrasyonu, asset lineage, çalışma durumu ve retry yönetimi |
| DVC | Ham ve işlenmiş dataset sürümleri ile pipeline artifact'lerinin takibi |
| Great Expectations | Gerçek dataset üzerinde null, aralık, kategori, duplicate ve benzeri kalite kontrolleri |
| Pandera + pytest | DataFrame kullanan Python fonksiyonlarının unit testleri ve giriş/çıkış kontratları |
| OpenCV / Pillow | Bozuk görüntü, gerçek çözünürlük, blur ve görüntüye özel kontroller |
| Albumentations | Görüntü ve bounding box koordinatlarını birlikte dönüştüren augmentation işlemleri |
| Dashboard | Pipeline sonuçlarının ve veri değişimlerinin kullanıcıya sunulması |

## Validation yaklaşımı

AU-AIR görüntü tabanlı bir veri setidir ancak annotation ve sensör verileri
tabular bir yapıya dönüştürülebilir. Great Expectations temel dataset kalite
kapısı olarak kullanılacaktır.

Örnek kontroller:

- Zorunlu kolonların bulunması ve boş olmaması
- Sınıf değerlerinin tanımlı kategorilerden biri olması
- Bounding box genişlik ve yüksekliğinin pozitif olması
- Bounding box koordinatlarının görüntü sınırları içinde kalması
- Tekrarlanan annotation kayıtlarının tespit edilmesi
- Görüntü dosyasının mevcut ve okunabilir olması
- Metadata çözünürlüğünün gerçek görüntü çözünürlüğüyle eşleşmesi
- Preprocessing sonrasında kayıt ve sınıf dağılımının beklenmedik şekilde bozulmaması
- Train, validation ve test bölümlerinin boş kalmaması

OpenCV veya Pillow ile hesaplanan `readable`, `actual_width`, `actual_height`
ve `blur_score` gibi görüntü metrikleri tabloya eklenerek Great Expectations
kurallarıyla değerlendirilebilir.

Pandera aynı kuralların tamamını tekrar etmek için kullanılmayacaktır. Yalnızca
preprocessing fonksiyonlarının beklediği DataFrame yapısını doğrulamak ve bu
davranışları unit testlerle güvenceye almak için kullanılacaktır.

## Dataset ayrımı

AU-AIR ardışık video kareleri içerdiğinden train, validation ve test ayrımının
rastgele frame bazında yapılması veri sızıntısına neden olabilir. Mümkünse
ayrım video veya sequence bazında yapılmalıdır. Kesin strateji dataset yapısı
incelendikten ve proje gereksinimleri netleştikten sonra belirlenecektir.

## Dashboard için ilk kapsam

- Dataset ve aktif DVC sürüm bilgisi
- Toplam görüntü ve annotation sayısı
- Sınıf dağılımı
- Bounding box genişlik, yükseklik ve alan dağılımları
- Hatalı veya eksik kayıtların özeti
- Great Expectations başarı oranı ve başarısız kontroller
- Raw, preprocessed ve augmented görüntülerin yan yana gösterimi
- Dagster pipeline çalışma durumu ve son çalışma bilgisi

Dashboard'un yalnızca sonuçları gösterip göstermeyeceği veya pipeline'ı
tetikleyip parametreleri değiştirmeye de izin verip vermeyeceği henüz açık bir
karardır.

## Future work

DVC ile izlenen raw veya processed dataset değişiklikleri için Semantic Release
tabanlı bir sürümleme aracı geliştirilebilir. Validation başarıyla tamamlandığında
çalışan bir Semantic Release pipeline'ı ile dataset sürümü ve release notes
otomatik olarak üretilebilir.

Bu otomasyon kapsamında sürüm numarası değişikliğin türüne göre belirlenebilir:

- `PATCH`: Annotation düzeltmeleri
- `MINOR`: Yeni veri veya augmentation çıktıları
- `MAJOR`: Annotation şeması, sınıflar veya çıktı formatında geriye uyumsuz değişiklikler

Otomatik release notes; eklenen veya silinen görüntü sayısını, annotation ve
sınıf dağılımı değişikliklerini, validation sonuçlarını ve ilgili DVC dataset
kimliğini içerebilir.


LAKEFS



## İlk uygulama önerisi

Gereksinimler netleşene kadar sistem lokal geliştirmeye uygun fakat merkezi
ortama taşınabilir şekilde tasarlanmalıdır:

- Lokal geliştirme için Docker Compose
- Ayarlanabilir DVC remote ve storage katmanı
- Ortam değişkenleriyle yapılandırma
- Veri yollarının kod içine sabitlenmemesi
- Pipeline adımlarının bağımsız ve test edilebilir olması
- Önce küçük bir AU-AIR örneklemiyle uçtan uca MVP hazırlanması
