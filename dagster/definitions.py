from dagster import (
    Definitions,
    define_asset_job,
    load_assets_from_modules,
)

from assets import ingestion
from assets import processing
from assets import clickhouse

from schedules.telemetry_sensor import telemetry_sensor

from alerting import alert_on_failure


# ===========================================================================
# ASSETS
# ===========================================================================

all_assets = load_assets_from_modules(
    [
        ingestion,
        processing,
        clickhouse,
    ]
)


# ===========================================================================
# JOB
# ===========================================================================

uav_data_pipeline_job = define_asset_job(
    name="uav_data_pipeline_job",
    selection="*",
    hooks={
        alert_on_failure,
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