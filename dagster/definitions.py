from pathlib import Path

from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
)
from dotenv import load_dotenv

# CLICKHOUSE_* / ALERT_WEBHOOK_URL gibi ortam değişkenleri bu dosyadan
# okunur. assets/clickhouse.py ve alerting.py bu değerleri sadece
# çalışma zamanında (fonksiyon çağrıldığında) os.environ üzerinden
# okuduğu için, .env'in en geç bu modül import edilirken yüklenmiş
# olması yeterlidir. "dagster dev" nereden çalıştırılırsa çalıştırılsın
# doğru dosyayı bulmak için mutlak yol kullanılıyor.
load_dotenv(
    Path(__file__).resolve().parent / ".env"
)

import assets

from schedules.telemetry_sensor import telemetry_sensor
from schedules.mx_tab_sensor import mx_tab_sensor

# İKİ HOOK'U DA İÇERİ AKTAR
from alerting import alert_on_failure, clear_alert_on_success


# ===========================================================================
# ASSETS
# ===========================================================================

all_assets = load_assets_from_modules([assets])


# ===========================================================================
# JOB
# ===========================================================================

uav_data_pipeline_job = define_asset_job(
    name="uav_data_pipeline_job",
    # extended_telemetry_load KASITLI OLARAK dışarıda bırakılıyor:
    # partitions_def'i yok ve file_path config'i zorunlu (varsayılanı
    # yok) -- AssetSelection.all() içinde kalsaydı, sensor'ün günlük
    # partition'lı dosyalar için oluşturduğu HER RunRequest bu asset'i
    # de seçip config eksikliğinden run'ı hemen başlamadan
    # başarısız ederdi. Bu asset yalnızca Dagster UI'dan elle
    # "Materialize" edilip file_path verilerek çalıştırılmalı (bkz.
    # assets.py::extended_telemetry_load).
    selection=AssetSelection.all()
    - AssetSelection.assets(
        assets.extended_telemetry_load,
        assets.spark_processed_tab,
        assets.spark_validated_tab,
        assets.dvc_published_mx_tab,
    ),
    hooks={
        alert_on_failure,
        clear_alert_on_success, # YENİ EKLENEN KISIM
    },
)

mx_tab_quality_job = define_asset_job(
    name="mx_tab_quality_job",
    selection=AssetSelection.assets(
        assets.spark_processed_tab,
        assets.spark_validated_tab,
        assets.dvc_published_mx_tab,
    ),
)


# ===========================================================================
# DEFINITIONS
# ===========================================================================

defs = Definitions(
    assets=all_assets,

    jobs=[
        uav_data_pipeline_job,
        mx_tab_quality_job,
    ],

    sensors=[
        telemetry_sensor,
        mx_tab_sensor,
    ],
)
