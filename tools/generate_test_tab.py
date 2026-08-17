"""
Sentetik .tab test dosyası üretici (Python/pandas, vektörleştirilmiş CSV yazımı).

*** SADECE YEREL TEST/BENCHMARK AMAÇLI *** -- gerçek .ham/.tab verisinin
stand-in'i değil, sadece tab-to-parquet (Rust) dönüştürücüsünü büyük
dosyalarla (streaming/sabit bellek davranışı, throughput) sınamak için.

NOT: Bu görevde önce bir Rust üretici (tab-to-parquet/src/bin/generate_test_tab.rs)
yazıldı, derlendi ve çalıştı -- ama bu makinede her seferinde birkaç saniye
içinde silindi (Trellix Endpoint Security (HX) EDR'ı: imzasız, taze derlenmiş,
kısa sürede çok veri yazan bir native exe'yi şüpheli buluyor). Admin/UAC
onayı verilemediği için istisna eklenemedi. Bu yüzden zaten güvenilir/imzalı
olan Python yorumlayıcısı kullanılıyor -- pandas'ın C seviyesindeki to_csv()
yazıcısıyla satır bazlı Python döngüsünden çok daha hızlı.

docs/plan_dokumani.md Bölüm 3.6'daki gerçek .tab formatına uygun
(tab-ayraçlı, her satır sonunda fazladan '\t', ilk sütun timestamp) --
to_csv trailing tab eklemediği için her chunk'ta '\n' -> '\t\n' ile
düzeltiliyor.

Sütun şeması (2026-08-15 güncelleme): --float-columns + --binary-columns
karışımı -- örn. 300 float64 (rastgele, -range..range) + 700 binary
(0 ya da 1) sütun. Binary sütunlar da Float64 olarak yazılır (0.0/1.0
formatında değil, düz "0"/"1" -- tab_to_parquet zaten tüm sütunları
Float64'e parse ediyor, plan Bölüm 3.1'deki "her şey Float64" kararıyla
tutarlı).

Kullanım:
    python3 tools/generate_test_tab.py --output testdata/dataset_01.tab \
        --target-size-gb 10 --float-columns 300 --binary-columns 700
"""
import argparse
import time

import numpy as np
import pandas as pd


def generate(
    output_path,
    target_bytes,
    float_columns,
    binary_columns,
    value_range,
    dt,
    seed,
    block_rows,
    progress_every,
):
    rng = np.random.default_rng(seed)
    float_names = [f"f{i}" for i in range(float_columns)]
    binary_names = [f"b{i}" for i in range(binary_columns)]
    columns_names = ["timestamp"] + float_names + binary_names

    bytes_written = 0
    row_count = 0
    ts = 0.0
    t0 = time.time()
    next_progress = progress_every

    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        header = "\t".join(columns_names) + "\t\n"
        f.write(header)
        bytes_written += len(header.encode("utf-8"))

        while bytes_written < target_bytes:
            ts_col = ts + dt * np.arange(block_rows)
            ts += dt * block_rows

            float_vals = np.round(
                rng.uniform(-value_range, value_range, size=(block_rows, float_columns)), 6
            )
            binary_vals = rng.integers(0, 2, size=(block_rows, binary_columns))

            df = pd.DataFrame(
                np.column_stack([ts_col, float_vals]), columns=["timestamp"] + float_names
            )
            # NOT: float_format= vermek pandas'ın hızlı C yazıcısını devre dışı
            # bırakıyor (bilinen bir pandas davranışı) -- bu yüzden float
            # değerler yazımdan önce np.round ile 6 ondalığa yuvarlanıyor.
            df_bin = pd.DataFrame(binary_vals, columns=binary_names)
            df = pd.concat([df, df_bin], axis=1)

            chunk = df.to_csv(sep="\t", index=False, header=False, lineterminator="\n")
            # plan Bölüm 3.6: her satır sonunda fazladan bir '\t' var -- to_csv
            # bunu eklemiyor, tek geçişte düzeltiliyor.
            chunk = chunk.replace("\n", "\t\n")

            f.write(chunk)
            bytes_written += len(chunk.encode("utf-8"))
            row_count += block_rows

            if row_count >= next_progress:
                elapsed = time.time() - t0
                mb = bytes_written / (1024 * 1024)
                rate = mb / elapsed if elapsed > 0 else 0
                print(f"  {row_count:,} satır, {mb:,.0f} MB yazıldı ({rate:.1f} MB/sn)", flush=True)
                next_progress += progress_every

    elapsed = time.time() - t0
    gb = bytes_written / (1024 ** 3)
    rate = (bytes_written / (1024 * 1024)) / elapsed if elapsed > 0 else 0
    print()
    print(f"Tamamlandı: {row_count:,} satır, {gb:.2f} GB, {elapsed:.1f} sn ({rate:.1f} MB/sn)")
    print(f"Sütun sayısı: {float_columns} float64 + {binary_columns} binary = {float_columns + binary_columns}")
    print(f"Çıktı: {output_path}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True)
    p.add_argument("--target-size-gb", type=float, default=10.0)
    p.add_argument("--float-columns", type=int, default=300)
    p.add_argument("--binary-columns", type=int, default=700)
    p.add_argument("--range", type=float, default=500.0, dest="value_range")
    p.add_argument("--dt", type=float, default=0.004)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--block-rows", type=int, default=50_000)
    p.add_argument("--progress-every", type=int, default=200_000)
    args = p.parse_args()

    target_bytes = int(args.target_size_gb * (1024 ** 3))
    generate(
        args.output,
        target_bytes,
        args.float_columns,
        args.binary_columns,
        args.value_range,
        args.dt,
        args.seed,
        args.block_rows,
        args.progress_every,
    )
