from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
)

# 1. İlgili modüllerin içe aktarılması (Noktalar kaldırıldı)
from assets import ingestion, processing
from assets.alerting import alert_on_failure 
from schedules.telemetry_sensor import telemetry_sensor

# 2. Asset'lerin klasörlerden yüklenmesi
ingestion_assets = load_assets_from_modules([ingestion])
processing_assets = load_assets_from_modules([processing])
all_assets = ingestion_assets + processing_assets

# 3. Job tanımlaması ve Alert sisteminin bağlanması
uav_data_pipeline_job = define_asset_job(
    name="uav_data_pipeline_job",
    selection="*",
    hooks={alert_on_failure} 
)

# 4. Definitions objesi (Schedule yerine Sensor kullanılarak)
defs = Definitions(
    assets=all_assets,
    jobs=[uav_data_pipeline_job],
    sensors=[telemetry_sensor],
)