import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "AU-AIR telemetri verisini kaynaktan (CSV veya Parquet) okur, "
            "günlük partition'a göre filtreler ve flight_id kolonunu ekler."
        )
    )
    parser.add_argument("--file-path", required=True, type=Path)
    parser.add_argument("--partition-date", required=True)
    parser.add_argument("--flight-id", default="")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--metadata-out", required=True, type=Path)
    return parser.parse_args()


def read_source(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()

    if suffix == ".csv":
        # Ayracı otomatik algıla (sep=None + engine="python").
        #
        # Bu özellikle Türkçe Windows/Excel ortamında kaydedilen CSV'ler
        # için önemli: Excel'in Türkçe yerel ayarları CSV'yi virgül (,)
        # yerine noktalı virgül (;) ile kaydeder. Sabit sep="," kullanmak,
        # böyle bir dosyayı tek kolon olarak okur ve "time" kolonu hiç
        # bulunamaz (KeyError: 'time').
        df = pd.read_csv(path, sep=None, engine="python")
        df.columns = df.columns.str.strip()
        return df

    if suffix == ".parquet":
        return pd.read_parquet(path)

    raise ValueError(
        f"Desteklenmeyen dosya formatı: '{suffix}' ({path}). "
        f"Yalnızca .csv ve .parquet destekleniyor."
    )


def main() -> None:
    args = parse_args()

    path = args.file_path
    if not path.exists():
        raise FileNotFoundError(f"Telemetri dosyası bulunamadı: {path}")

    df = read_source(path)

    if "time" not in df.columns:
        raise ValueError(
            f"'{path}' dosyasında 'time' kolonu bulunamadı. "
            f"Bulunan kolonlar: {list(df.columns)}. "
            f"CSV ise ayracın (virgül/noktalı virgül) ve başlık "
            f"satırının doğru olduğundan emin olun."
        )

    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    flight_id = args.flight_id.strip() or path.stem
    df["flight_id"] = flight_id

    day_start = pd.Timestamp(args.partition_date)
    day_end = day_start + pd.Timedelta(days=1)
    df = df[(df["time"] >= day_start) & (df["time"] < day_end)]

    print(
        f"AU-AIR verisi okundu (partition={args.partition_date}, "
        f"flight_id={flight_id}, dosya={path}): {len(df)} satır"
    )
    print(f"Kolonlar: {list(df.columns)}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_file = args.output_dir / f"{flight_id}_{args.partition_date}.parquet"
    df.to_parquet(output_file, index=False)

    print(f"Filtrelenmiş veri yazıldı: {output_file}")

    schema = {column: str(dtype) for column, dtype in df.dtypes.items()}

    metadata = {
        "partition": args.partition_date,
        "flight_id": flight_id,
        "source_file": str(path),
        "output_file": str(output_file),
        "row_count": len(df),
        "column_count": len(df.columns),
        "schema": schema,
    }

    args.metadata_out.parent.mkdir(parents=True, exist_ok=True)
    args.metadata_out.write_text(
        json.dumps(metadata, ensure_ascii=False),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
