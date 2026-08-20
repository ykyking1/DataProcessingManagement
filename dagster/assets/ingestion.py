import pandas as pd
from pathlib import Path

from dagster import asset, Config, MaterializeResult, MetadataValue

from partitions import daily_partitions
from metadata_store import record_asset_metadata


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class RawTelemetryConfig(Config):
    """
    Hangi kaynak dosyanın okunacağını belirtir.

    - Sensor tarafından tetiklenen run'larda telemetry_sensor.py bu alanı
      bulduğu dosyanın tam yoluyla doldurur (run_config üzerinden).
    - Manuel çalıştırma veya backfill'de belirtilmezse varsayılan örnek
      dosya kullanılır.
    """

    file_path: str = "data/au_air/telemetry.parquet"

    flight_id: str = ""
    """
    Bu dosyanın ait olduğu uçuşun kimliği (örn. "flight_1", "ucus_003").

    Boş bırakılırsa dosya adının uzantısız hali (path.stem) kullanılır.
    Sensor tarafından tetiklenen run'larda genelde boş bırakılır; dosya
    adı zaten uçuşu ayırt etmeye yeter (örn. telemetry_013.parquet ->
    flight_id = "telemetry_013").
    """


# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------

@asset(
    compute_kind="python",
    group_name="raw_layer",
    partitions_def=daily_partitions,
    description=(
        "AU-AIR telemetri verisini kaynaktan (sensor'ün bulduğu dosya "
        "veya varsayılan örnek dosya) okur; günlük partition'a göre "
        "filtreler."
    ),
)
def raw_uav_telemetry(context, config: RawTelemetryConfig):

    path = Path(config.file_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Telemetri dosyası bulunamadı: {path}"
        )

    # -----------------------------------------------------------------------
    # Dosya formatına göre okuma
    # -----------------------------------------------------------------------
    #
    # telemetry_sensor.py hem .parquet hem .csv dosyalarını izliyor,
    # bu yüzden burada da her iki formatı da desteklemek gerekiyor.
    # Aksi halde CSV dosyası "Parquet magic bytes not found" hatasıyla
    # patlar.

    suffix = path.suffix.lower()

    if suffix == ".csv":

        # Ayracı otomatik algıla (sep=None + engine="python").
        #
        # Bu özellikle Türkçe Windows/Excel ortamında kaydedilen CSV'ler
        # için önemli: Excel'in Türkçe yerel ayarları CSV'yi virgül (,)
        # yerine noktalı virgül (;) ile kaydeder. Sabit sep="," kullanmak,
        # böyle bir dosyayı tek kolon olarak okur ve "time" kolonu hiç
        # bulunamaz (KeyError: 'time').

        df = pd.read_csv(
            path,
            sep=None,
            engine="python",
        )

        # Kolon adlarındaki baştaki/sondaki boşlukları temizle
        # (bazı export araçları "time " gibi boşluklu adlar üretebiliyor).
        df.columns = df.columns.str.strip()

    elif suffix == ".parquet":
        df = pd.read_parquet(path)

    else:
        raise ValueError(
            f"Desteklenmeyen dosya formatı: '{suffix}' ({path}). "
            f"Yalnızca .csv ve .parquet destekleniyor."
        )

    # -----------------------------------------------------------------------
    # Beklenen kolonların varlığını doğrula
    # -----------------------------------------------------------------------
    #
    # "time" kolonu bulunamazsa aşağıdaki satır zaten KeyError fırlatırdı,
    # ama hata mesajı hangi dosyadan hangi kolonların okunduğunu
    # göstermiyordu. Burada daha okunabilir bir hata veriyoruz.

    if "time" not in df.columns:
        raise ValueError(
            f"'{path}' dosyasında 'time' kolonu bulunamadı. "
            f"Bulunan kolonlar: {list(df.columns)}. "
            f"CSV ise ayracın (virgül/noktalı virgül) ve başlık "
            f"satırının doğru olduğundan emin olun."
        )

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce",
    )

    # -----------------------------------------------------------------------
    # Uçuş kimliği (flight_id)
    # -----------------------------------------------------------------------
    #
    # Her kaynak dosya bir "uçuşu" temsil eder. Bu kolon, dashboard'daki
    # "Veri Gözat / Dışa Aktar" ekranında kullanıcının belirli uçuşları
    # seçip her biri için ayrı filtrelenmiş CSV indirebilmesini sağlar.

    flight_id = config.flight_id.strip() or path.stem

    df["flight_id"] = flight_id

    # -----------------------------------------------------------------------
    # Partition filtresi
    # -----------------------------------------------------------------------
    #
    # Bu asset günlük partition'lı olduğu için her run yalnızca kendi
    # partition'ına (gününe) ait satırları döndürmeli. Bu sayede:
    #
    #   - Backfill sırasında her gün ayrı ayrı ve doğru şekilde işlenir.
    #   - Aynı kaynak dosyada birden fazla günün verisi olsa bile
    #     partition'lar birbirine karışmaz.

    partition_date = context.partition_key

    day_start = pd.Timestamp(partition_date)
    day_end = day_start + pd.Timedelta(days=1)

    df = df[
        (df["time"] >= day_start)
        & (df["time"] < day_end)
    ]

    context.log.info(
        f"AU-AIR verisi okundu (partition={partition_date}, "
        f"flight_id={flight_id}, dosya={path}): {len(df)} satır"
    )

    context.log.info(
        f"Kolonlar: {list(df.columns)}"
    )

    # -----------------------------------------------------------------------
    # Şema metadata'sı (kolon adı -> tip)
    # -----------------------------------------------------------------------

    schema = {
        column: str(dtype)
        for column, dtype in df.dtypes.items()
    }

    record_asset_metadata(
        context,
        group_name="raw_layer",
        flight_id=flight_id,
        row_count=len(df),
        metadata={
            "partition": partition_date,
            "flight_id": flight_id,
            "source_file": str(path),
            "row_count": len(df),
            "column_count": len(df.columns),
            "schema": schema,
        },
    )

    return MaterializeResult(
        value=df,
        metadata={
            "partition": partition_date,
            "flight_id": flight_id,
            "source_file": str(path),
            "row_count": len(df),
            "column_count": len(df.columns),
            "schema": MetadataValue.json(schema),
        },
    )
