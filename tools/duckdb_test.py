"""DuckDB ile .tab -> .parquet donusumunu test etmek icin kucuk deneme."""
import duckdb
import sys

input_path = sys.argv[1]
output_path = sys.argv[2]

con = duckdb.connect()
con.sql(f"""
COPY (
  SELECT COLUMNS(* EXCLUDE (column1001))::DOUBLE
  FROM read_csv('{input_path}', delim='\t', header=true, null_padding=true, strict_mode=false)
) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")

row_count = con.sql(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
cols = con.sql(f"DESCRIBE SELECT * FROM read_parquet('{output_path}')").fetchall()
print("parquet satir sayisi:", row_count)
print("sutun sayisi:", len(cols))
print("ilk 3 sutun tipi:", cols[:3])
print("son 3 sutun tipi:", cols[-3:])
