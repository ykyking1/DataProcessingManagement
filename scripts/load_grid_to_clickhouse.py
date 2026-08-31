"""
Dagster-uyumlu ince sarmalayıcı: pipeline_grid_to_clickhouse.py'nin
KENDİSİNİ (tek-dosya modunda, `python3 pipeline_grid_to_clickhouse.py
<n_cols_k> <n_rows>`) subprocess olarak, HİÇBİR DEĞİŞİKLİK YAPMADAN
çağırır -- ardından sonucu conversion_manifest'ten okuyup dagster/
assets.py'nin bekleyebileceği --metadata-out JSON sözleşmesine çevirir.

Neden AYRI bir script (pipeline_grid_to_clickhouse.py'yi doğrudan import
etmek ya da değiştirmek yerine): o dosyanın en altındaki tek-/çoklu-
dosya modu seçim mantığı `if __name__ == "__main__":` KORUMASI OLMADAN
modül seviyesinde çalışıyor -- doğrudan `import` etmek, argv'ye bağlı
olarak istemeden TÜM 20 dosyayı işleyen toplu modu tetikleyebilir. Bu
yüzden BU script onu bir alt süreç olarak çağırıyor (tıpkı script'in
kendi CLI'ından elle çalıştırmak gibi) -- 40+ oturumda kanıtlanmış
davranışına hiç dokunulmuyor.

Kullanım:
    python load_grid_to_clickhouse.py --n-cols-k 20 --n-rows 50000 \
        --metadata-out meta.json
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras

SCRIPT_DIR = Path(__file__).resolve().parent
MAIN_PIPELINE = SCRIPT_DIR / "pipeline_grid_to_clickhouse.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Tek bir sentetik grid dosyasını (n_cols_k/n_rows ile "
            "belirtilen) pipeline_grid_to_clickhouse.py'nin kanıtlanmış "
            "tek-dosya moduyla ClickHouse'a yükler."
        )
    )
    parser.add_argument("--n-cols-k", required=True, type=int)
    parser.add_argument("--n-rows", required=True, type=int)
    parser.add_argument("--metadata-out", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tag = f"{args.n_cols_k}k_{args.n_rows}"
    tab_file_name = f"synthetic_{tag}.tab"

    print(f"pipeline_grid_to_clickhouse.py {args.n_cols_k} {args.n_rows} çalıştırılıyor...", flush=True)

    result = subprocess.run(
        [sys.executable, str(MAIN_PIPELINE), str(args.n_cols_k), str(args.n_rows)],
        capture_output=True,
        text=True,
    )

    if result.stdout:
        print(result.stdout, flush=True)

    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr, flush=True)
        raise RuntimeError(
            f"pipeline_grid_to_clickhouse.py başarısız oldu "
            f"(exit code {result.returncode})."
        )

    # Sonucu doğrudan conversion_manifest'ten oku -- ana pipeline zaten
    # her denemenin tam detayını (süreler, boyutlar, satır sayıları) o
    # tabloya yazıyor, burada tekrar hesaplamıyoruz.
    conn = psycopg2.connect(
        host="postgres", dbname="telemetry_meta", user="postgres", password="pg123"
    )
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM conversion_manifest WHERE tab_file_name = %s",
            (tab_file_name,),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    if row is None:
        raise RuntimeError(
            f"conversion_manifest'te '{tab_file_name}' satırı bulunamadı "
            "(pipeline exit 0 döndü ama kayıt yazılmamış görünüyor)."
        )

    if row["status"] != "done":
        raise RuntimeError(
            f"'{tab_file_name}' başarıyla tamamlanmadı (status="
            f"{row['status']}). error_detail={row['error_detail']}"
        )

    metadata = {
        "tab_file_name": row["tab_file_name"],
        "aircraft_type": row["aircraft_type"],
        "row_count_tab": row["row_count_tab"],
        "row_count_clickhouse": row["row_count_clickhouse"],
        "column_count": row["column_count"],
        "original_size_bytes": row["original_size_bytes"],
        "tab_zst_size_bytes": row["tab_zst_size_bytes"],
        "compress_duration_seconds": row["compress_duration_seconds"],
        "minio_upload_duration_seconds": row["minio_upload_duration_seconds"],
        "clickhouse_load_duration_seconds": row["clickhouse_load_duration_seconds"],
        "clickhouse_table_name": row["clickhouse_table_name"],
        "clickhouse_disk_bytes": row["clickhouse_disk_bytes"],
        "tab_zst_object_key": row["tab_zst_object_key"],
        "status": row["status"],
    }

    args.metadata_out.parent.mkdir(parents=True, exist_ok=True)
    args.metadata_out.write_text(
        json.dumps(metadata, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    print(
        f"Tamamlandı: {row['row_count_clickhouse']:,} satır, "
        f"{row['column_count']} sütun ({row['clickhouse_table_name']}).",
        flush=True,
    )


if __name__ == "__main__":
    main()
