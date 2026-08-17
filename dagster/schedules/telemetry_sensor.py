from pathlib import Path

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    sensor,
)


# ---------------------------------------------------------
# İzlenecek veri klasörü
# ---------------------------------------------------------

DATA_DIR = Path("data/raw")


# ---------------------------------------------------------
# Telemetry Sensor
# ---------------------------------------------------------

@sensor(
    job_name="uav_data_pipeline_job",
    minimum_interval_seconds=10,
)
def telemetry_sensor(context: SensorEvaluationContext):

    # Dizin yoksa oluştur
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Hem Parquet hem CSV dosyalarını kontrol et
    files = sorted(
        list(DATA_DIR.glob("*.parquet"))
        + list(DATA_DIR.glob("*.csv"))
    )

    if not files:
        context.log.info(
            f"Yeni veri dosyası bulunamadı: {DATA_DIR.resolve()}"
        )
        return

    # Daha önce işlenmiş dosyaları cursor'dan al
    processed_files = set()

    if context.cursor:
        processed_files = set(
            context.cursor.split("|")
        )

    # Henüz işlenmemiş dosyalar
    new_files = [
        file
        for file in files
        if str(file) not in processed_files
    ]

    if not new_files:
        context.log.info(
            "Yeni telemetry dosyası bulunamadı."
        )
        return

    # Her yeni dosya için RunRequest oluştur
    for file in new_files:

        context.log.info(
            f"Yeni telemetry dosyası tespit edildi: {file}"
        )

        yield RunRequest(
            run_key=str(file),
            run_config={},
        )

    # İşlenen dosyaları cursor'a kaydet
    processed_files.update(
        str(file)
        for file in new_files
    )

    context.update_cursor(
        "|".join(sorted(processed_files))
    )