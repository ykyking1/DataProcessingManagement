from pathlib import Path

from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
)
from dotenv import load_dotenv

# ALERT_WEBHOOK_URL gibi ortam değişkenleri bu dosyadan okunur.
# alerting.py bu değerleri sadece çalışma zamanında (fonksiyon
# çağrıldığında) os.environ üzerinden okuduğu için, .env'in en geç bu
# modül import edilirken yüklenmiş olması yeterlidir. "dagster dev"
# nereden çalıştırılırsa çalıştırılsın doğru dosyayı bulmak için mutlak
# yol kullanılıyor.
load_dotenv(
    Path(__file__).resolve().parent / ".env"
)

from assets import ingestion
from assets import processing
from assets import publishing

from schedules.telemetry_sensor import telemetry_sensor

# İKİ HOOK'U DA İÇERİ AKTAR
from alerting import alert_on_failure, clear_alert_on_success


# ===========================================================================
# ASSETS
# ===========================================================================

all_assets = load_assets_from_modules(
    [
        ingestion,
        processing,
        publishing,
    ]
)


# ===========================================================================
# JOB
# ===========================================================================

uav_data_pipeline_job = define_asset_job(
    name="uav_data_pipeline_job",
    selection=AssetSelection.all(),
    hooks={
        alert_on_failure,
        clear_alert_on_success, # YENİ EKLENEN KISIM
    },
)


# ===========================================================================
# DEFINITIONS
# ===========================================================================

defs = Definitions(
    assets=all_assets,

    jobs=[
        uav_data_pipeline_job,
    ],

    sensors=[
        telemetry_sensor,
    ],
)
