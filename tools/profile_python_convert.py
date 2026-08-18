"""
tab_to_parquet.py'nin (Python) hangi asamasinin gercekte ne kadar
surdugunu olcmek icin gecici profiling script'i. Kalici koda dokunmuyor,
ayni mantigi ic ice zaman olcumleriyle sarmalyor.

Uc asama olculuyor:
  1) oku+bol   -- dosyadan satir okuma + split_tab_line + buf'a ekleme
  2) donustur  -- numpy string->float64 array insasi (bizim optimize
                  ettigimiz adim)
  3) yaz       -- Arrow array insasi + writer.write_batch (zstd sikistirma
                  + parquet encode + diske yazma DAHIL)
"""
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def split_tab_line(line):
    line = line.rstrip("\n").rstrip("\r")
    if line.endswith("\t"):
        line = line[:-1]
    return line.split("\t")


def convert(tab_path, parquet_path, chunk_rows, max_row_group_rows):
    t_read_split = 0.0
    t_convert = 0.0
    t_write = 0.0

    with open(tab_path, "r", encoding="utf-8") as f:
        t0 = time.time()
        header = split_tab_line(f.readline())
        ts_col_name = header[0]
        column_names = header[1:]
        num_columns = len(column_names)
        t_read_split += time.time() - t0

        schema = pa.schema(
            [(ts_col_name, pa.float64())] + [(name, pa.float64()) for name in column_names]
        )
        writer = pq.ParquetWriter(parquet_path, schema, compression="zstd")

        row_count = 0
        buf_ts, buf_vals = [], []
        chunk_idx = 0
        t_start = time.time()

        def flush():
            nonlocal row_count, chunk_idx, t_convert, t_write
            if not buf_vals:
                return
            t0 = time.time()
            ts_arr = np.array(buf_ts, dtype=np.float64)
            vals_arr = np.array(buf_vals, dtype=np.float64)
            t_convert += time.time() - t0

            t0 = time.time()
            arrays = [pa.array(ts_arr)] + [pa.array(vals_arr[:, i]) for i in range(num_columns)]
            batch = pa.record_batch(arrays, schema=schema)
            writer.write_batch(batch, row_group_size=max_row_group_rows)
            t_write += time.time() - t0

            row_count += len(buf_ts)
            chunk_idx += 1
            if chunk_idx % 10 == 0:
                elapsed = time.time() - t_start
                print(f"  Parça {chunk_idx}: toplam {row_count:,} satır, {elapsed:.0f}sn "
                      f"(oku+böl={t_read_split:.0f}s, dönüştür={t_convert:.0f}s, yaz={t_write:.0f}s)",
                      flush=True)

            buf_ts.clear()
            buf_vals.clear()

        t0 = time.time()
        line = f.readline()
        while line:
            t_read_split += time.time() - t0
            parts = split_tab_line(line)
            buf_ts.append(parts[0])
            buf_vals.append(parts[1:])
            if len(buf_vals) >= chunk_rows:
                flush()
            t0 = time.time()
            line = f.readline()
        t_read_split += time.time() - t0
        flush()
        writer.close()

    total = t_read_split + t_convert + t_write
    print()
    print(f"Toplam: {row_count:,} satır")
    print(f"  oku+böl (I/O + split):        {t_read_split:8.1f}s  ({100*t_read_split/total:.1f}%)")
    print(f"  dönüştür (numpy str->float64): {t_convert:8.1f}s  ({100*t_convert/total:.1f}%)")
    print(f"  yaz (Arrow+zstd+parquet+disk): {t_write:8.1f}s  ({100*t_write/total:.1f}%)")
    print(f"  TOPLAM (üç aşama):             {total:8.1f}s")


if __name__ == "__main__":
    convert(sys.argv[1], sys.argv[2], 50000, 50000)
