"""
Paylaşılan Partition Tanımı

raw_uav_telemetry -> processed_telemetry -> clickhouse_telemetry
zincirindeki tüm asset'ler bu günlük partition'ı kullanır.

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
