"""Iki parquet dosyasinin satir sayisini ve sutun toplamlarini DuckDB'nin
kendi SQL toplama fonksiyonlariyla (Python'a veri cekmeden) karsilastirir."""
import sys
import duckdb

a, b = sys.argv[1], sys.argv[2]
con = duckdb.connect()

ra = con.sql(f"SELECT COUNT(*) FROM read_parquet('{a}')").fetchone()[0]
rb = con.sql(f"SELECT COUNT(*) FROM read_parquet('{b}')").fetchone()[0]
print(f"row_count A={ra} B={rb} ESLESTI={ra == rb}")

cols = con.sql(f"SELECT * FROM read_parquet('{a}') LIMIT 0").columns
sum_exprs = ", ".join(f"SUM(\"{c}\") AS s_{i}" for i, c in enumerate(cols))
sums_a = con.sql(f"SELECT {sum_exprs} FROM read_parquet('{a}')").fetchone()
sums_b = con.sql(f"SELECT {sum_exprs} FROM read_parquet('{b}')").fetchone()

import numpy as np
arr_a = np.array(sums_a, dtype=np.float64)
arr_b = np.array(sums_b, dtype=np.float64)
max_diff = np.max(np.abs(arr_a - arr_b))
print(f"sutun sayisi: {len(cols)}")
print(f"max mutlak fark: {max_diff}")
print(f"COL SUM ESLESTI (rtol=1e-9, gorece tolerans): {np.allclose(arr_a, arr_b, rtol=1e-9)}")
