"""DuckDB'nin binary (0/1) sutunlarda neden daha iyi sikistigini
izole bir deneyle arastirmak icin -- tek sutunlu, gurultusuz test."""
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import os

N = 500_000
rng = np.random.default_rng(42)
vals = rng.integers(0, 2, size=N).astype(np.float64)

print(f"=== {N:,} satirlik tek bir binary (0/1) sutun ===\n")

# --- 1. pyarrow varsayilan ---
table = pa.table({"b0": vals})
pq.write_table(table, "/tmp/pa_default.parquet", compression="zstd")
size1 = os.path.getsize("/tmp/pa_default.parquet")
pf = pq.ParquetFile("/tmp/pa_default.parquet")
col = pf.metadata.row_group(0).column(0)
print(f"1) pyarrow varsayilan: boyut={size1} encodings={col.encodings} "
      f"comp={col.total_compressed_size} uncomp={col.total_uncompressed_size}")

# --- 2. pyarrow, use_dictionary=True acik ---
pq.write_table(table, "/tmp/pa_dict.parquet", compression="zstd", use_dictionary=True)
size2 = os.path.getsize("/tmp/pa_dict.parquet")
pf = pq.ParquetFile("/tmp/pa_dict.parquet")
col = pf.metadata.row_group(0).column(0)
print(f"2) pyarrow use_dictionary=True: boyut={size2} encodings={col.encodings} "
      f"comp={col.total_compressed_size} uncomp={col.total_uncompressed_size}")

# --- 3. pyarrow, data_page_version=2.0 ---
pq.write_table(table, "/tmp/pa_v2.parquet", compression="zstd", data_page_version="2.0")
size3 = os.path.getsize("/tmp/pa_v2.parquet")
pf = pq.ParquetFile("/tmp/pa_v2.parquet")
col = pf.metadata.row_group(0).column(0)
print(f"3) pyarrow data_page_version=2.0: boyut={size3} encodings={col.encodings} "
      f"comp={col.total_compressed_size} uncomp={col.total_uncompressed_size}")

# --- 4. DuckDB ile ayni veri ---
con = duckdb.connect()
con.register("t", table)
con.sql("COPY (SELECT * FROM t) TO '/tmp/duckdb_same.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)")
size4 = os.path.getsize("/tmp/duckdb_same.parquet")
pf = pq.ParquetFile("/tmp/duckdb_same.parquet")
col = pf.metadata.row_group(0).column(0)
print(f"4) DuckDB (ayni veri): boyut={size4} encodings={col.encodings} "
      f"comp={col.total_compressed_size} uncomp={col.total_uncompressed_size}")

print(f"\nHam (sikistirilmamis) boyut: {N*8:,} byte")
