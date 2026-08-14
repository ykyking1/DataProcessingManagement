from dagster import asset, MaterializeResult


@asset(
    group_name="processing",
    description="Raw telemetri verisini işleyerek curated katmana hazırlar.",
)
def processed_telemetry(raw_telemetry):
    """
    A2 preprocessing pipeline'ını çalıştırır.

    raw_telemetry:
        A1 asset'inin çıktısı.
    """

    print("A2 preprocessing çalıştırılıyor...")

    # TODO:
    #
    # from processing.preprocess import preprocess_telemetry
    #
    # result = preprocess_telemetry(raw_telemetry)

    return MaterializeResult(
        metadata={
            "status": "success",
            "layer": "curated",
            "source": "raw_telemetry",
        }
    )