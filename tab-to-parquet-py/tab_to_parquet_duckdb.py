"""
.tab -> .parquet donusturucu -- DuckDB kullanarak (2026-08-17). Tek bir
COPY komutuyla okuma+tip donusumu+sikistirma+yazma yapiyor -- Python
tarafinda hicbir satir/deger islenmiyor, tamamen DuckDB'nin C++ motoruna
devrediliyor.

docs/plan_dokumani.md Bolum 3.6: trailing-tab formatini DuckDB'nin
sikici CSV sniffer'i null_padding=true, strict_mode=false ile asiyor --
fazladan bir hayalet sutun olusuyor, disari yaziLIRKEN EXCLUDE ediliyor.

--threads: worker-per-process mimarisiyle adil karsilastirma icin --
1 verilirse DuckDB kendi ic paralelligini kapatiyor (pyarrow.csv'deki
use_threads=False ile ayni mantik -- 6 worker paralel calisirken cifte
paralellik/OOM riskini onlemek icin).

Kullanim:
    python3 tab_to_parquet_duckdb.py --input sample.tab --output sample.parquet --threads 1
"""
import argparse
import os
import time

import duckdb


def convert(tab_path, parquet_path, threads, row_group_size):
    con = duckdb.connect()
    if threads is not None:
        con.sql(f"PRAGMA threads={threads}")
    # threads=None ise hicbir PRAGMA verilmiyor -- DuckDB'nin kendi
    # varsayilanina (genelde mevcut CPU sayisi) birakiliyor.

    # Gercek sutun adlarini DOGRUDAN dosyadan oku -- DuckDB'nin hayalet
    # (trailing-tab) sutununa verdigi otomatik isim ("column1001" gibi)
    # surumden surume degisebilir, bu yuzden DuckDB'nin kendi isimlendirmesine
    # guvenmek yerine header satirini kendimiz parse ediyoruz (2026-08-17
    # duzeltmesi -- ilk denemede "c != ''" kontrolu hayalet sutunu
    # yakalayamamisti, DuckDB ona bos string degil "column1001" vermisti).
    with open(tab_path, "r", encoding="utf-8") as f:
        header_line = f.readline().rstrip("\n").rstrip("\r")
        if header_line.endswith("\t"):
            header_line = header_line[:-1]
        real_cols = header_line.split("\t")
    select_expr = ", ".join(f'"{c}"::DOUBLE AS "{c}"' for c in real_cols)

    row_group_clause = f", ROW_GROUP_SIZE {row_group_size}" if row_group_size is not None else ""
    t0 = time.time()
    con.sql(f"""
        COPY (
            SELECT {select_expr}
            FROM read_csv('{tab_path}', delim='\t', header=true,
                           null_padding=true, strict_mode=false)
        ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD{row_group_clause});
    """)
    elapsed = time.time() - t0

    row_count = con.sql(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    return {"row_count": row_count, "elapsed": elapsed}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--threads", type=int, default=None)
    p.add_argument("--row-group-size", type=int, default=None)
    args = p.parse_args()

    result = convert(args.input, args.output, args.threads, args.row_group_size)

    tab_size = os.path.getsize(args.input)
    parquet_size = os.path.getsize(args.output)

    print(f"Tamamlandı: {result['row_count']:,} satır, {result['elapsed']:.1f} sn")
    print(f".tab boyutu:     {tab_size/(1024*1024):.1f} MB")
    print(f".parquet boyutu: {parquet_size/(1024*1024):.1f} MB")
    print(f"Sıkıştırma oranı: {tab_size/parquet_size:.2f}x")
