from dagster import asset, MaterializeResult


@asset(
    group_name="ingestion",
    description="AU-AIR ham verilerini veri gölüne alır."
)
def raw_telemetry():
    """
    A1 ingestion pipeline'ını çalıştırır.

    TODO:
    Burada A1 tarafından geliştirilen gerçek
    ingestion fonksiyonu çağrılacak.
    """

    # Örnek:
    #
    # from ingestion.ingest import ingest_telemetry
    #
    # result = ingest_telemetry()

    print("A1 ingestion çalıştırılıyor...")

    return {
        "source": "AU-AIR",
        "status": "success",
        "data": "test_data",
    }