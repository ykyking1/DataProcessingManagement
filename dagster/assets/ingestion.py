from dagster import asset

# DailyPartitionsDefinition kısımlarını ve partition_def parametresini kaldırdık
@asset(compute_kind="python", group_name="raw_layer")
def raw_uav_telemetry(context):
    
    # Tarih aramak yerine direkt işlemin başladığını logluyoruz
    context.log.info("Yeni İHA telemetri dosyası algılandı ve çekiliyor (A1 Katmanı)...")
    
    # Burada veri çekme (Ingestion) ve MinIO'ya yazma mantığın yer alacak
    return [1, 2, 3] # Örnek veri döndürüyoruz