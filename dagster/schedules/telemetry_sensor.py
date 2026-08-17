import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    sensor,
)

from partitions import daily_partitions


# ---------------------------------------------------------
# İzlenecek veri klasörü
# ---------------------------------------------------------

DATA_DIR = Path("data/raw")


# ---------------------------------------------------------
# Dosyanın ait olduğu partition tarihini belirleme
# ---------------------------------------------------------

_DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _infer_partition_date(file_path: Path) -> str:
    """
    Dosyanın ait olduğu partition tarihini (YYYY-MM-DD) belirler.

    Öncelik sırası:

        1. Dosya adında YYYY-MM-DD deseni varsa onu kullan
           (örn. telemetry_2026-08-01.parquet).
        2. Dosya içeriğindeki 'time' kolonunun ilk değerinden tarih çıkar.
        3. İkisi de başarısız olursa dosyanın değiştirilme (mtime)
           tarihini kullan.

    Bu, raw_uav_telemetry asset'inin doğru günlük partition'a
    materialize edilmesini sağlar (bkz. ingestion.py, partitions.py).
    """

    match = _DATE_PATTERN.search(file_path.name)

    if match:
        return match.group(1)

    try:

        if file_path.suffix == ".parquet":
            df = pd.read_parquet(
                file_path,
                columns=["time"],
            )
        else:
            df = pd.read_csv(
                file_path,
                usecols=["time"],
                nrows=1,
            )

        first_time = pd.to_datetime(
            df["time"].iloc[0]
        )

        return first_time.strftime("%Y-%m-%d")

    except Exception:
        pass

    mtime = datetime.fromtimestamp(
        file_path.stat().st_mtime,
        tz=timezone.utc,
    )

    return mtime.strftime("%Y-%m-%d")


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

    valid_partition_keys = set(
        daily_partitions.get_partition_keys()
    )

    # Her yeni dosya için, dosyanın kendisini raw_uav_telemetry asset'ine
    # config olarak ileten bir RunRequest oluştur. Böylece sensor'ün
    # bulduğu dosya ile işlenen dosya arasındaki bağlantı garanti edilir
    # (önceden asset her zaman sabit kodlanmış tek bir dosyayı okuyordu).
    for file in new_files:

        partition_date = _infer_partition_date(file)

        if partition_date not in valid_partition_keys:

            context.log.warning(
                f"{file} dosyasının tarihi ({partition_date}) tanımlı "
                f"partition aralığının (daily_partitions start_date) "
                f"dışında kaldığı için run oluşturulamadı."
            )

            continue

        context.log.info(
            f"Yeni telemetry dosyası tespit edildi: {file} "
            f"(partition: {partition_date})"
        )

        yield RunRequest(
            run_key=str(file),
            partition_key=partition_date,
            run_config={
                "ops": {
                    "raw_uav_telemetry": {
                        "config": {
                            "file_path": str(file)
                        }
                    }
                }
            },
        )

    # İşlenen dosyaları cursor'a kaydet
    processed_files.update(
        str(file)
        for file in new_files
    )

    context.update_cursor(
        "|".join(sorted(processed_files))
    )
