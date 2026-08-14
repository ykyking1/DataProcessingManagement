"""
.tab -> .parquet dönüştürücü (streaming, zstd sıkıştırmalı).

*** PROTOTİP -- ÜRETİMDE KULLANILMIYOR ***
Üretim implementasyonu tab-to-parquet/ (Rust) altında. Bu dosya, gerçek
.tab verisiyle henüz doğrulanmamış Rust çevirisinin referans/karşılaştırma
kaynağı olarak tutuluyor -- bkz. docs/plan_dokumani.md Bölüm 4.

Girdi: .tab dosyası (tab-ayraçlı, ilk satır header, ilk sütun timestamp,
geri kalan sütunlar sayısal değerler).
Çıktı: .parquet dosyası (tüm sayısal sütunlar Float64, zstd sıkıştırma --
konuştuğumuz gibi string DEĞİL, binary numeric).

Bellek prensibi: --chunk-rows kadar satır okunup bir Arrow RecordBatch'e
dönüştürülür, parquet writer'a akış halinde yazılır -- .tab dosyasının
tamamı asla RAM'de tutulmaz (10GB'lık bir .tab'ı bile sabit bellekle işler).

Ayrıca Postgres manifest tablosuna (conversion_manifest) yazılacak alanları
(satır sayısı, içerik parmak izi) hesaplayıp raporluyor.

NOT: .tab formatı şu an bizim varsayımımız (tab-ayraçlı, float64 round-trip
formatlı) -- gerçek exe'nin çıktı formatı netleştiğinde parse_line()
fonksiyonunu ona göre güncellemeniz gerekebilir.

Kullanım:
    python3 tab_to_parquet.py --input sample.tab --output sample.parquet
"""
import argparse
import hashlib
import json
import os
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def convert(tab_path, parquet_path, chunk_rows=10_000, compression="zstd"):
    with open(tab_path, "r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        ts_col_name = header[0]
        column_names = header[1:]
        num_columns = len(column_names)

        # Tüm sayısal sütunlar Float64 -- string DEĞİL (konuştuğumuz gibi
        # ClickHouse/Parquet'te string hem daha büyük hem sıralama/filtre
        # açısından yanlış sonuç veriyor)
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
            writer.write_batch(batch)

            row_count += len(buf_ts)
            chunk_idx += 1
            elapsed = time.time() - t0
            rate = row_count / elapsed if elapsed > 0 else 0
            print(f"  Parça {chunk_idx}: {len(buf_ts):,} satır (toplam {row_count:,}, {rate:,.0f} satır/sn)")

            buf_ts.clear()
            buf_vals.clear()

        for line in f:
            parts = line.rstrip("\n").split("\t")
            buf_ts.append(float(parts[0]))
            buf_vals.append([float(x) for x in parts[1:]])
            if len(buf_vals) >= chunk_rows:
                flush()
        flush()
        writer.close()

    # İçerik parmak izi -- Postgres manifest'teki content_fingerprint alanına
    # yazılacak. NOT: küçük FP birikim farklarını absorbe etmek için 4 ondalık
    # basamağa yuvarlanıyor -- exact hash yerine tolerans temelli karşılaştırma
    # önerilir (verify_conversion.py'daki gibi).
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
    args = p.parse_args()

    result = convert(args.input, args.output, args.chunk_rows, args.compression)

    tab_size = os.path.getsize(args.input)
    parquet_size = os.path.getsize(args.output)

    print()
    print(f"Tamamlandı: {result['row_count']:,} satır")
    print(f".tab boyutu:     {tab_size/(1024*1024):.1f} MB")
    print(f".parquet boyutu: {parquet_size/(1024*1024):.1f} MB")
    print(f"Sıkıştırma oranı: {tab_size/parquet_size:.2f}x küçülme")
    print(f"İçerik parmak izi (Postgres manifest için): {result['content_fingerprint'][:16]}...")
