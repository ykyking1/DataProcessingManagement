from pathlib import Path
from dagster import (
    RunRequest,
    SensorEvaluationContext,
    sensor,
)

# İşlenecek raw dosyalarının yolu
DATA_DIR = Path("data/raw")

# job_name parametresi ile definitions.py içindeki job adını hedefliyoruz
@sensor(
    job_name="uav_data_pipeline_job",
    minimum_interval_seconds=10,
)
def telemetry_sensor(context: SensorEvaluationContext):
    
    # Dizin yoksa oluştur
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # .csv uzantılı dosyaları bul
    files = sorted(DATA_DIR.glob("*.csv"))
    
    if not files:
        return

    # Daha önce işlenmiş dosyaları cursor'da tutuyoruz
    processed_files = set()
    if context.cursor:
        processed_files = set(context.cursor.split("|"))

    # Henüz işlenmemiş dosyaları bul
    new_files = [
        file for file in files if str(file) not in processed_files
    ]

    if not new_files:
        return

    # Her yeni dosya için bir RunRequest oluştur
    for file in new_files:
        context.log.info(f"New telemetry file detected: {file}")
        
        yield RunRequest(
            run_key=str(file),
            # Asset tabanlı sistem kullandığımız için run_config'i kendi 
            # asset konfigürasyonlarına göre uyarlayabilir veya boş bırakabilirsin.
            run_config={}, 
        )

    # İşlenen dosyaları cursor'a kaydet
    processed_files.update(str(file) for file in new_files)
    context.update_cursor("|".join(sorted(processed_files)))