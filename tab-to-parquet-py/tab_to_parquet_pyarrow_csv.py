"""
.tab -> .parquet donusturucu -- pyarrow.csv.open_csv (C++ CSV okuyucusu)
kullanarak. tab_to_parquet.py'deki elle satir/deger ayristirmasi yerine,
okuma+sayisal donusum tamamen Arrow'un C++ koduna devrediliyor -- Rust'a
daha yakin bir performans denemesi (2026-08-17).

docs/plan_dokumani.md Bolum 3.6: her satir sonunda fazladan bir '\t' var --
pyarrow.csv bunu otomatik olarak adi bos ('') bir "hayalet" sutun olarak
okuyor (deger her zaman null), bu sutun disariya yazilmadan once atiliyor.

Kullanim:
    python3 tab_to_parquet_pyarrow_csv.py --input sample.tab --output sample.parquet \
        --chunk-rows 50000 --max-row-group-rows 50000
"""
import argparse
import hashlib
import json
import os
import time

import numpy as np
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq


def convert(tab_path, parquet_path, chunk_rows, compression, max_row_group_rows, compression_level=None):
    # NOT (2026-08-17 duzeltmesi): block_size = chunk_rows * 20_000 ~= 1GB'lik
    # TEK bir okuma blogu demekti -- OOM'a yol acti (ilk chunk'a bile
    # ulasamadan SIGKILL). pyarrow'un kendi makul varsayilanina (~1MB) yakin
    # sabit bir deger kullaniliyor; chunk_rows kontrolu zaten asagidaki
    # buf_rows >= chunk_rows mantigiyla sagliyoruz, block_size'in bununla
    # birebir eslesmesi gerekmiyor.
    # use_threads=False (2026-08-17): pyarrow.csv varsayilan olarak KENDI
    # ICINDE de coklu thread kullanabiliyor -- worker-per-process
    # mimarimizde (6 ayri Python sureci) her surecin AYRICA icten
    # coklu-thread acmasi beklenmedik bellek/CPU rekabetine yol acabilir
    # (6-worker OOM denemesi sonrasi eklendi, bkz. docs/plan_dokumani.md).
    read_options = pcsv.ReadOptions(block_size=4 * 1024 * 1024, use_threads=False)  # 4MB
    parse_options = pcsv.ParseOptions(delimiter="\t")
    convert_options = pcsv.ConvertOptions(strings_can_be_null=False)

    reader = pcsv.open_csv(
        tab_path, read_options=read_options, parse_options=parse_options, convert_options=convert_options
    )

    # Hayalet (trailing-tab) sutunu tespit et -- adi bos string.
    real_names = [n for n in reader.schema.names if n != ""]
    num_columns = len(real_names) - 1  # timestamp haric

    target_schema = pa.schema([(n, pa.float64()) for n in real_names])
    # use_dictionary=True (2026-08-17 denemesi): DuckDB'nin sikistirma
    # avantajinin (Bolum 15) dictionary encoding'den geldigi bulundu --
    # burada aciktan zorlanarak ayni kazanc pyarrow.csv'de de denenecek.
    writer = pq.ParquetWriter(
        parquet_path, target_schema, compression=compression, use_dictionary=True,
        compression_level=compression_level,
    )

    row_count = 0
    col_sum = np.zeros(num_columns, dtype=np.float64)
    col_min = np.full(num_columns, np.inf, dtype=np.float64)
    col_max = np.full(num_columns, -np.inf, dtype=np.float64)

    t0 = time.time()
    chunk_idx = 0
    buf_batches = []
    buf_rows = 0

    def flush():
        nonlocal row_count, chunk_idx, buf_batches, buf_rows
        if not buf_batches:
            return
        table = pa.Table.from_batches(buf_batches)
        table = table.select(real_names).cast(target_schema)

        vals_arr = np.column_stack([table.column(i).to_numpy() for i in range(1, len(real_names))])
        col_sum[:] += vals_arr.sum(axis=0)
        np.minimum(col_min, vals_arr.min(axis=0), out=col_min)
        np.maximum(col_max, vals_arr.max(axis=0), out=col_max)

        writer.write_table(table, row_group_size=max_row_group_rows)

        row_count += buf_rows
        chunk_idx += 1
        elapsed = time.time() - t0
        rate = row_count / elapsed if elapsed > 0 else 0
        print(f"  Parça {chunk_idx}: {buf_rows:,} satır (toplam {row_count:,}, {rate:,.0f} satır/sn)", flush=True)

        buf_batches = []
        buf_rows = 0

    for batch in reader:
        buf_batches.append(batch)
        buf_rows += batch.num_rows
        if buf_rows >= chunk_rows:
            flush()
    flush()
    writer.close()

    fingerprint_input = json.dumps({
        "row_count": row_count,
        "col_sum": [round(x, 4) for x in col_sum.tolist()],
    }, sort_keys=True)
    fingerprint = hashlib.sha256(fingerprint_input.encode()).hexdigest()

    return {"row_count": row_count, "content_fingerprint": fingerprint}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--chunk-rows", type=int, default=50_000)
    p.add_argument("--compression", default="zstd")
    p.add_argument("--max-row-group-rows", type=int, default=100_000)
    p.add_argument("--compression-level", type=int, default=None)
    args = p.parse_args()

    t_start = time.time()
    result = convert(
        args.input, args.output, args.chunk_rows, args.compression, args.max_row_group_rows,
        args.compression_level,
    )
    elapsed_total = time.time() - t_start

    tab_size = os.path.getsize(args.input)
    parquet_size = os.path.getsize(args.output)

    print()
    print(f"Tamamlandı: {result['row_count']:,} satır, {elapsed_total:.1f} sn")
    print(f".tab boyutu:     {tab_size/(1024*1024):.1f} MB")
    print(f".parquet boyutu: {parquet_size/(1024*1024):.1f} MB")
    print(f"Sıkıştırma oranı: {tab_size/parquet_size:.2f}x küçülme")
    print(f"İçerik parmak izi (Postgres manifest için): {result['content_fingerprint'][:16]}...")
