"""
.tab -> .parquet dönüştürücü (Python/pyarrow) -- tab-to-parquet/ (Rust) ile
DOĞRUDAN KARŞILAŞTIRMA için yazıldı (2026-08-15). prototypes/tab_to_parquet.py
ile karıştırılmasın -- o dosya donmuş bir referans/prototip, bu dosya ise
gerçek formatla (trailing-tab) çalışan, Rust ile aynı CLI sözleşmesine sahip
bir eşdeğer.

Girdi: .tab dosyası (tab-ayraçlı, ilk satır header, ilk sütun timestamp,
geri kalan sütunlar sayısal değerler, HER SATIR SONUNDA FAZLADAN BİR '\t' --
docs/plan_dokumani.md Bölüm 3.6).
Çıktı: .parquet dosyası (tüm sayısal sütunlar Float64, zstd sıkıştırma).

Bellek prensibi: --chunk-rows kadar satır okunup bir Arrow RecordBatch'e
dönüştürülür, parquet writer'a akış halinde yazılır.

--max-row-group-rows: Rust tarafındaki aynı isimli parametreyle eşdeğer
davranış için var. NOT: pyarrow'un varsayılan davranışı parquet-rs'den
farklı -- her write_batch() çağrısı KENDİ BAŞINA bir row-group olur
(parquet-rs gibi çoklu write() çağrısını max_row_group_size'a kadar TEK
row-group'ta biriktirmez). Yani --chunk-rows == --max-row-group-rows
olduğu sürece (bu karşılaştırmada öyle) davranış zaten eşdeğer; farklı
olsalar iki taraf da ayrıca ayarlanmalı.

Kullanım:
    python3 tab_to_parquet.py --input sample.tab --output sample.parquet \
        --chunk-rows 50000 --max-row-group-rows 50000
"""
import argparse
import hashlib
import json
import os
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def split_tab_line(line):
    """docs/plan_dokumani.md Bölüm 3.6: her satır sonunda fazladan bir '\t'
    var -- split'ten önce temizlenmezse sütun hizalaması kayar. Rust
    tarafındaki split_tab_line ile birebir aynı mantık."""
    line = line.rstrip("\n").rstrip("\r")
    if line.endswith("\t"):
        line = line[:-1]
    return line.split("\t")


def convert(tab_path, parquet_path, chunk_rows, compression, max_row_group_rows):
    with open(tab_path, "r", encoding="utf-8") as f:
        header = split_tab_line(f.readline())
        ts_col_name = header[0]
        column_names = header[1:]
        num_columns = len(column_names)

        schema = pa.schema(
            [(ts_col_name, pa.float64())] + [(name, pa.float64()) for name in column_names]
        )

        writer = pq.ParquetWriter(parquet_path, schema, compression=compression)

        row_count = 0
        col_sum = np.zeros(num_columns, dtype=np.float64)
        col_min = np.full(num_columns, np.inf, dtype=np.float64)
        col_max = np.full(num_columns, -np.inf, dtype=np.float64)

        buf_ts, buf_vals = [], []
        t0 = time.time()
        chunk_idx = 0

        def flush():
            nonlocal row_count, chunk_idx
            if not buf_vals:
                return
            ts_arr = np.array(buf_ts, dtype=np.float64)
            vals_arr = np.array(buf_vals, dtype=np.float64)  # (n, num_columns)

            col_sum[:] = col_sum + vals_arr.sum(axis=0)
            np.minimum(col_min, vals_arr.min(axis=0), out=col_min)
            np.maximum(col_max, vals_arr.max(axis=0), out=col_max)

            arrays = [pa.array(ts_arr)] + [pa.array(vals_arr[:, i]) for i in range(num_columns)]
            batch = pa.record_batch(arrays, schema=schema)
            # row_group_size: bkz. dosya başı NOT -- write_batch zaten kendi
            # başına bir row-group oluşturuyor, bu parametre bunu Rust'taki
            # --max-row-group-rows ile aynı sözleşmeye açıkça bağlıyor.
            writer.write_batch(batch, row_group_size=max_row_group_rows)

            row_count += len(buf_ts)
            chunk_idx += 1
            elapsed = time.time() - t0
            rate = row_count / elapsed if elapsed > 0 else 0
            print(f"  Parça {chunk_idx}: {len(buf_ts):,} satır (toplam {row_count:,}, {rate:,.0f} satır/sn)", flush=True)

            buf_ts.clear()
            buf_vals.clear()

        for line_no, line in enumerate(f, start=2):
            parts = split_tab_line(line)
            if len(parts) != num_columns + 1:
                raise ValueError(
                    f"sütun sayısı uyuşmazlığı (satır {line_no}): "
                    f"beklenen {num_columns + 1} bulunan {len(parts)}"
                )
            buf_ts.append(float(parts[0]))
            buf_vals.append([float(x) for x in parts[1:]])
            if len(buf_vals) >= chunk_rows:
                flush()
        flush()
        writer.close()

    # İçerik parmak izi -- bkz. Rust tarafındaki aynı mantık (tolerans için
    # 4 ondalığa yuvarlanmış col_sum + row_count).
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
    p.add_argument("--chunk-rows", type=int, default=10_000)
    p.add_argument("--compression", default="zstd")
    p.add_argument("--max-row-group-rows", type=int, default=100_000)
    args = p.parse_args()

    t_start = time.time()
    result = convert(
        args.input, args.output, args.chunk_rows, args.compression, args.max_row_group_rows
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
