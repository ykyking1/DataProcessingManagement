"""6 dosyayi TEK bir DuckDB baglantisi/sureciyle, kendi ic paralelligine
birakarak (biz manuel 6 ayri OS sureci acmadan) sirayla islemek --
'DuckDB'ye kendi karar versin' senaryosu."""
import duckdb
import time
import sys

files = [f"/host/testdata/dataset_{i:02d}.tab" for i in range(1, 7)]
out_dir = "/work/duckdb_single_out"

con = duckdb.connect()
# threads PRAGMA verilmiyor -- DuckDB'nin tam kendi varsayilanina birakiliyor.

t_start = time.time()
for tab_path in files:
    name = tab_path.split("/")[-1].replace(".tab", ".parquet")
    out_path = f"{out_dir}/{name}"

    with open(tab_path, "r", encoding="utf-8") as f:
        header_line = f.readline().rstrip("\n").rstrip("\r")
        if header_line.endswith("\t"):
            header_line = header_line[:-1]
        real_cols = header_line.split("\t")
    select_expr = ", ".join(f'"{c}"::DOUBLE AS "{c}"' for c in real_cols)

    t0 = time.time()
    con.sql(f"""
        COPY (
            SELECT {select_expr}
            FROM read_csv('{tab_path}', delim='\t', header=true,
                           null_padding=true, strict_mode=false)
        ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
    """)
    elapsed = time.time() - t0
    row_count = con.sql(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    print(f"{name}: {row_count:,} satır, {elapsed:.1f}sn", flush=True)

total = time.time() - t_start
print(f"\nTOPLAM (tek surec, 6 dosya sirayla, DuckDB kendi ic paralelligiyle): {total:.1f}sn")
