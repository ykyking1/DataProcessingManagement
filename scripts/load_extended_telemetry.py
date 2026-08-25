"""
Geniş şemalı (binlerce sütunlu) telemetri dosyalarını ClickHouse'a yükler.

AU-AIR'in standart 17 sütunluk şeması (bkz. ingest_telemetry.py /
load_clickhouse.py) dışında, çok daha geniş şemalı kaynak dosyalar da
(örn. "auair_2Mx10K_merged.tab.gz" -- 2015 sütun, kanal başına birden
fazla sensör metriği) işlenebilmesi gerekiyor; bu bizim için istisna
değil, normal bir veri şekli. Mevcut ingest_telemetry.py/process_
telemetry.py/load_clickhouse.py üçlüsü AU-AIR'in sabit 17 sütununa
(velocity_x, roll, box_x, ...) sıkı sıkıya bağlı olduğu için bu geniş
dosyalar o hattan geçemiyor -- bu script bunun yerine, dosyanın GERÇEK
sütunlarından şemayı kendisi çıkarıp ayrı bir ClickHouse tablosuna
(varsayılan: telemetry_extended) yazan, AU-AIR şemasından bağımsız bir
alternatif sağlıyor. Mevcut `telemetry` tablosuna hiç dokunulmaz.

Bellek prensibi: dosya tek seferde belleğe alınmaz -- --chunk-rows
kadar satırlık parçalar halinde okunup ClickHouse'a INSERT edilir
(gigabaytlarca dosyayı bile sabit bellekle işler).

Kullanım:
    python load_extended_telemetry.py --file-path dataset.tab.gz \
        --metadata-out meta.json
"""

import argparse
import gzip
import json
import time
from pathlib import Path

import pandas as pd
from clickhouse_driver import Client

from load_clickhouse import (
    _get_clickhouse_database,
    _get_clickhouse_native_host,
    _get_clickhouse_native_port,
    _get_clickhouse_password,
    _get_clickhouse_user,
)


# "time" doğrudan yoksa bu adaylardan ilk bulunanı zaman sütunu olarak
# kabul edilip "time" adına yeniden adlandırılır -- dashboard'daki
# filtreler (bkz. dashboard/app.py::build_clickhouse_where) "time"
# sütun adını sabit kodlanmış olarak bekliyor.
TIME_COLUMN_CANDIDATES = ["time", "timestamp_utc", "timestamp", "date"]

# Bir sütundaki dolu (null olmayan) değerlerin en az bu oranı sayısala
# çevrilebiliyorsa Float64 kabul edilir; aksi halde String -- bu geniş
# dosyalardaki sütunlar seyrek (çoğu satırda boş) olabildiği için katı
# "hepsi sayısal olmalı" kuralı yerine bir eşik kullanılıyor.
NUMERIC_FRACTION_THRESHOLD = 0.9


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Geniş şemalı (AU-AIR'in sabit 17 sütununa uymayan) bir "
            "telemetri dosyasını, şemasını dosyadan çıkararak ayrı bir "
            "ClickHouse tablosuna yükler."
        )
    )
    parser.add_argument("--file-path", required=True, type=Path)
    parser.add_argument("--table-name", default="telemetry_extended")
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=50_000,
        help="Her INSERT'te ClickHouse'a gönderilecek satır sayısı.",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=50_000,
        help="Sütun tiplerini (Float64/String) çıkarmak için okunacak örnek satır sayısı.",
    )
    parser.add_argument("--metadata-out", required=True, type=Path)
    return parser.parse_args()


def _open_text(path: Path):
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def _detect_separator(path: Path) -> str:
    with _open_text(path) as f:
        header = f.readline()

    if "\t" in header:
        return "\t"

    if ";" in header:
        return ";"

    return ","


def _drop_trailing_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bazı .tab dosyalarının her satırının SONUNDA fazladan bir ayraç
    karakteri olabiliyor (bkz. scripts/clean_tab_trailing_tab.py); bu
    pandas'ta isimsiz/boş başlıklı hayali bir sondaki sütun olarak
    görünür. Böyle bir sütun yoksa bu fonksiyon no-op'tur.
    """

    empty_columns = [
        column
        for column in df.columns
        if column == "" or str(column).startswith("Unnamed:")
    ]

    if empty_columns:
        df = df.drop(columns=empty_columns)

    return df


def _infer_schema(path: Path, sep: str, sample_rows: int):
    """
    Dosyanın ilk sample_rows satırından şemayı çıkarır.

    Döner: (time_column_or_None, {sütun_adı: "Float64"|"String"}, tüm_sütunlar)
    time_column, ClickHouse tablosunda "time" adıyla yazılacak kaynak
    sütunun adıdır (TIME_COLUMN_CANDIDATES'teki ilk eşleşen).
    """

    sample = pd.read_csv(path, sep=sep, nrows=sample_rows, low_memory=False)
    sample.columns = sample.columns.str.strip()
    sample = _drop_trailing_empty_columns(sample)

    time_column = None
    for candidate in TIME_COLUMN_CANDIDATES:
        if candidate in sample.columns:
            time_column = candidate
            break

    column_types = {}

    for column in sample.columns:

        if column == time_column:
            continue

        non_null = sample[column].dropna()

        if non_null.empty:
            column_types[column] = "String"
            continue

        numeric = pd.to_numeric(non_null, errors="coerce")
        numeric_fraction = numeric.notna().mean()

        column_types[column] = (
            "Float64"
            if numeric_fraction >= NUMERIC_FRACTION_THRESHOLD
            else "String"
        )

    return time_column, column_types, list(sample.columns)


def main() -> None:
    args = parse_args()

    path = args.file_path

    if not path.exists():
        raise FileNotFoundError(f"Telemetri dosyası bulunamadı: {path}")

    t_start = time.time()

    sep = _detect_separator(path)
    print(f"Ayraç algılandı: {sep!r}")

    time_column, column_types, all_columns = _infer_schema(
        path, sep, args.sample_rows
    )

    if time_column is None:
        raise ValueError(
            f"'{path}' dosyasında zaman sütunu bulunamadı. Aranan "
            f"adaylar: {TIME_COLUMN_CANDIDATES}. Bulunan sütunlar: "
            f"{all_columns}"
        )

    ordered_columns = ["time"] + list(column_types.keys())

    print(
        f"Şema çıkarıldı: {len(ordered_columns)} sütun "
        f"(zaman sütunu kaynağı: '{time_column}')."
    )

    client = Client(
        host=_get_clickhouse_native_host(),
        port=_get_clickhouse_native_port(),
        user=_get_clickhouse_user(),
        password=_get_clickhouse_password(),
        database=_get_clickhouse_database(),
    )

    database = _get_clickhouse_database()
    table_fqn = f"{database}.{args.table_name}"

    # Her sütun Nullable: bu geniş dosyalardaki kanallar seyrek olabiliyor
    # (çoğu satırda boş) -- bkz. NUMERIC_FRACTION_THRESHOLD notu.
    col_defs = ["`time` DateTime64(3)"] + [
        f"`{column}` Nullable({column_type})"
        for column, column_type in column_types.items()
    ]

    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn}
        (
            {", ".join(col_defs)}
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(time)
        ORDER BY time
        """
    )

    insert_columns_sql = ", ".join(f"`{column}`" for column in ordered_columns)
    insert_sql = f"INSERT INTO {table_fqn} ({insert_columns_sql}) VALUES"

    total_rows = 0
    chunk_index = 0

    for chunk in pd.read_csv(
        path, sep=sep, chunksize=args.chunk_rows, low_memory=False
    ):

        chunk.columns = chunk.columns.str.strip()
        chunk = _drop_trailing_empty_columns(chunk)

        chunk["time"] = pd.to_datetime(chunk[time_column], errors="coerce")

        for column, column_type in column_types.items():
            if column_type == "Float64":
                chunk[column] = pd.to_numeric(chunk[column], errors="coerce")

        chunk = chunk[ordered_columns]
        chunk = chunk.astype(object).where(pd.notna(chunk), None)

        rows = list(chunk.itertuples(index=False, name=None))

        if rows:
            client.execute(insert_sql, rows)
            total_rows += len(rows)

        chunk_index += 1
        elapsed = time.time() - t_start
        rate = total_rows / elapsed if elapsed > 0 else 0

        print(
            f"  Parça {chunk_index}: {len(rows):,} satır "
            f"(toplam {total_rows:,}, {rate:,.0f} satır/sn, "
            f"{elapsed:.1f}sn)",
            flush=True,
        )

    elapsed_total = time.time() - t_start

    print(
        f"Tamamlandı: {total_rows:,} satır, {len(ordered_columns)} "
        f"sütun, {elapsed_total:.1f}sn ({table_fqn})."
    )

    schema_rows = client.execute(f"DESCRIBE TABLE {table_fqn}")
    schema = {row[0]: row[1] for row in schema_rows}

    metadata = {
        "source_file": str(path),
        "table": table_fqn,
        "database": database,
        "row_count": total_rows,
        "column_count": len(ordered_columns),
        "chunk_count": chunk_index,
        "elapsed_seconds": round(elapsed_total, 1),
        "time_column_source": time_column,
        "schema": schema,
    }

    args.metadata_out.parent.mkdir(parents=True, exist_ok=True)
    args.metadata_out.write_text(
        json.dumps(metadata, ensure_ascii=False),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
