"""
Paylaşılan Partition Tanımı

raw_uav_telemetry -> processed_telemetry -> clickhouse_telemetry
zincirindeki tüm asset'ler bu günlük partition'ı kullanır.

clickhouse_telemetry, processed_telemetry'nin çıktısını ClickHouse'un
kendi depolama motorundaki (MergeTree) bir tabloya INSERT eder;
dashboard bu tabloyu sorgu zamanında okur. processed_telemetry ayrıca
aynı veriyi yedek/denetim amacıyla data/processed/ klasörüne parquet
olarak da yazar (ClickHouse tarafından kullanılmaz).

Bunun sağladığı şeyler:

    - Dagster UI üzerinden belirli bir tarih aralığı için BACKFILL
      çalıştırılabilir (Runs > Backfills veya asset sayfasındaki
      "Materialize" panelinden tarih aralığı seçilerek).
    - Her run yalnızca kendi partition'ına (gününe) ait veriyi işler,
      böylece backfill sırasında farklı günler birbirine karışmaz.

start_date, AU-AIR örnek verisinin başladığı tarihe göre ayarlandı.
Gerçek veri kaynağınızın en eski tarihine göre güncelleyin.
"""

from dagster import DailyPartitionsDefinition

#SADECE BU TARİHTEN GÜNÜMÜZE KADAR OLAN KAYITLAR İÇİN RUN ÇALIŞTIRILIR
daily_partitions = DailyPartitionsDefinition(
    start_date="2026-01-01",
    timezone="UTC",
)
