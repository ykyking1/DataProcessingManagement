from dagster import Definitions, define_asset_job

from assets.ingestion import raw_telemetry
from assets.processing import processed_telemetry


telemetry_job = define_asset_job(
    name="telemetry_pipeline",
    selection=[
        raw_telemetry,
        processed_telemetry,
    ],
)


defs = Definitions(
    assets=[
        raw_telemetry,
        processed_telemetry,
    ],
    jobs=[
        telemetry_job,
    ],
)