"""DuckDB ile .tab -> .parquet donusumu -- Rust/Python ile karsilastirma icin."""
import duckdb
import sys
import time
import os

input_path = sys.argv[1]
output_path = sys.argv[2]
threads = int(sys.argv[3]) if len(sys.argv) > 3 else None

con = duckdb.connect()
if threads:
    con.sql(f"PRAGMA threads={threads}")

t0 = time.time()
con.sql(f"""
COPY (
  SELECT COLUMNS(* EXCLUDE (column1001))::DOUBLE
  FROM read_csv('{input_path}', delim='\t', header=true, null_padding=true, strict_mode=false)
) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")
elapsed = time.time() - t0

row_count = con.sql(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
tab_size = os.path.getsize(input_path)
parquet_size = os.path.getsize(output_path)

print(f"Tamamlandi: {row_count:,} satir, {elapsed:.1f} sn")
print(f".tab boyutu:     {tab_size/(1024*1024):.1f} MB")
print(f".parquet boyutu: {parquet_size/(1024*1024):.1f} MB")
print(f"Sikistirma orani: {tab_size/parquet_size:.2f}x")
print(f"Throughput: {row_count/elapsed:.0f} satir/sn")
