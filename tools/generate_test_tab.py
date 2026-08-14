"""
Sentetik .tab test dosyası üretici (Python/pandas, vektörleştirilmiş CSV yazımı).

*** SADECE YEREL TEST/BENCHMARK AMAÇLI *** -- gerçek .ham/.tab verisinin
stand-in'i değil, sadece tab-to-parquet (Rust) dönüştürücüsünü büyük
dosyalarla (streaming/sabit bellek davranışı, throughput) sınamak için.

NOT: Bu görevde önce bir Rust üretici (tab-to-parquet/src/bin/generate_test_tab.rs)
yazıldı, derlendi ve çalıştı -- ama bu makinede her seferinde birkaç saniye
içinde silindi (muhtemelen Windows Defender'ın davranış temelli/ransomware
koruması: imzasız, taze derlenmiş, kısa sürede çok veri yazan bir native exe
şüpheli görünüyor olabilir). Admin/UAC onayı verilemediği için Defender
istisnası eklenemedi. Bu yüzden zaten güvenilir/imzalı olan Python
yorumlayıcısı kullanılıyor -- pandas'ın C seviyesindeki to_csv() yazıcısıyla
satır bazlı Python döngüsünden çok daha hızlı.

docs/plan_dokumani.md Bölüm 3.6'daki gerçek .tab formatına uygun
(tab-ayraçlı, her satır sonunda fazladan '\t', ilk sütun timestamp) --
to_csv trailing tab eklemediği için her chunk'ta '\n' -> '\t\n' ile
düzeltiliyor.

Kullanım:
    python3 tools/generate_test_tab.py --output testdata/big.tab \
        --target-size-gb 10 --columns 300
"""
import argparse
import time

import numpy as np
import pandas as pd


def generate(output_path, target_bytes, columns, value_range, dt, seed, block_rows, progress_every):
    rng = np.random.default_rng(seed)
    columns_names = ["timestamp"] + [f"col{i}" for i in range(columns)]

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
            vals = rng.uniform(-value_range, value_range, size=(block_rows, columns))
            ts_col = ts + dt * np.arange(block_rows)
            ts += dt * block_rows

            block = np.round(np.column_stack([ts_col, vals]), 6)
            df = pd.DataFrame(block, columns=columns_names)
            # NOT: float_format= vermek pandas'ın hızlı C yazıcısını devre dışı
            # bırakıyor (bilinen bir pandas davranışı) -- bu yüzden değerler
            # yazımdan önce np.round ile 6 ondalığa yuvarlanıyor, to_csv
            # varsayılan (hızlı) yoldan yazıyor.
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
    print(f"Sütun sayısı: {columns}")
    print(f"Çıktı: {output_path}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True)
    p.add_argument("--target-size-gb", type=float, default=10.0)
    p.add_argument("--columns", type=int, default=300)
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
        args.columns,
        args.value_range,
        args.dt,
        args.seed,
        args.block_rows,
        args.progress_every,
    )
