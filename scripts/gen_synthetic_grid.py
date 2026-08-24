# -*- coding: utf-8 -*-
"""
5 sutun-sayisi (ucak tipi) x 4 satir-sayisi = 20 sentetik test dosyasi
uretir.

Sutun tasarimi (her "tip" icin N = veri sutunu sayisi, timestamp ve
ucak_tipi bunun USTUNE eklenir):
  - %10 float64          (N*0.10 sutun) -- HER ZAMAN EN BASTA, sirali
  - %10 karisik 0/1       (N*0.10 sutun) -- geri kalanlarla KARISTIRILMIS
  - %40 sabit sifir       (N*0.40 sutun) -- geri kalanlarla KARISTIRILMIS
  - %40 sabit bir         (N*0.40 sutun) -- geri kalanlarla KARISTIRILMIS

Nihai sutun sirasi: timestamp, ucak_tipi, f0..f{n_float-1} (sirali),
sonra [karisik/sabit-sifir/sabit-bir turlerinin RASTGELE karistirilmis
sirasi].

5 "ucak tipi" sutun sayisina gore tanimlanir: AIRCRAFT_10K .. AIRCRAFT_50K.

NOT (2026-08-22): ilk calistirmada son dosya (50k sutun x 100k satir)
bilgisayar kapatildigi icin yarim kaldi -- /work/synthetic_grid/
synthetic_50k_100000.tab ve esllik eden _columns.json dosyasi silinip
bu script yeniden calistirilmali (diger 19 dosya zaten var oldugu
icin -- eger idempotent/atlama mantigi eklenmemisse hepsini yeniden
uretir, gerekirse OS.path.exists kontrolu eklenebilir).
"""
import time
import os
import json
import numpy as np
import pandas as pd

OUT_DIR = "/work/synthetic_grid"
os.makedirs(OUT_DIR, exist_ok=True)

COLUMN_TIERS = [10_000, 20_000, 30_000, 40_000, 50_000]
ROW_COUNTS = [1_000, 5_000, 50_000, 100_000]
SEED = 42

def aircraft_label(n_cols):
    return f"AIRCRAFT_{n_cols // 1000}K"

def gen_one_file(n_data_cols, n_rows, rng):
    aircraft = aircraft_label(n_data_cols)
    n_float = round(n_data_cols * 0.10)
    n_mixed = round(n_data_cols * 0.10)
    n_zero = round(n_data_cols * 0.40)
    n_one = n_data_cols - n_float - n_mixed - n_zero  # kalan (yuvarlama farkini burada emiyoruz)

    float_cols = [f"f{i}" for i in range(n_float)]

    # Binary-turu sutunlarin (mixed/zero/one) adlarini ve TIPLERINI
    # birlikte olusturup KARISTIR (float'lar ayri, sabit sirali kaliyor)
    binary_names = (
        [f"m{i}" for i in range(n_mixed)]
        + [f"z{i}" for i in range(n_zero)]
        + [f"o{i}" for i in range(n_one)]
    )
    binary_types = (
        ["mixed"] * n_mixed + ["zero"] * n_zero + ["one"] * n_one
    )
    perm = rng.permutation(len(binary_names))
    shuffled_binary_names = [binary_names[i] for i in perm]
    shuffled_binary_types = [binary_types[i] for i in perm]
    shuffled_binary_types = np.array(shuffled_binary_types)

    mixed_positions = np.where(shuffled_binary_types == "mixed")[0]
    one_positions = np.where(shuffled_binary_types == "one")[0]
    # zero_positions: hicbir sey yapmaya gerek yok, array zaten 0 ile basliyor

    all_cols = ["timestamp", "aircraft_type"] + float_cols + shuffled_binary_names
    total_cols = len(all_cols)

    out_path = os.path.join(OUT_DIR, f"synthetic_{n_data_cols//1000}k_{n_rows}.tab")
    manifest_path = os.path.join(OUT_DIR, f"synthetic_{n_data_cols//1000}k_{n_rows}_columns.json")

    manifest = {
        "seed": SEED,
        "n_rows": n_rows,
        "n_data_columns": n_data_cols,
        "n_total_columns": total_cols,
        "aircraft_type": aircraft,
        "n_float64": n_float,
        "n_mixed_binary": n_mixed,
        "n_constant_zero": n_zero,
        "n_constant_one": n_one,
        "column_order": all_cols,
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)

    n_binary_total = len(shuffled_binary_names)
    CHUNK = min(10_000, n_rows)

    t0 = time.time()
    with open(out_path, "w", encoding="utf-8", newline="\n") as fout:
        fout.write("\t".join(all_cols) + "\n")

        written = 0
        ts_counter = 0.0
        while written < n_rows:
            this_chunk = min(CHUNK, n_rows - written)

            ts_block = np.arange(ts_counter, ts_counter + this_chunk, dtype=np.float64)
            ts_counter += this_chunk
            ts_df = pd.DataFrame({"timestamp": ts_block})
            aircraft_df = pd.DataFrame({"aircraft_type": [aircraft] * this_chunk})

            if n_float > 0:
                floats = rng.normal(loc=0, scale=100, size=(this_chunk, n_float)).round(6)
                float_df = pd.DataFrame(floats, columns=float_cols)
            else:
                float_df = pd.DataFrame(index=range(this_chunk))

            binary_arr = np.zeros((this_chunk, n_binary_total), dtype=np.uint8)
            if len(one_positions) > 0:
                binary_arr[:, one_positions] = 1
            if len(mixed_positions) > 0:
                binary_arr[:, mixed_positions] = rng.integers(
                    0, 2, size=(this_chunk, len(mixed_positions)), dtype=np.uint8
                )
            binary_df = pd.DataFrame(binary_arr, columns=shuffled_binary_names)

            chunk_df = pd.concat([ts_df, aircraft_df, float_df, binary_df], axis=1)
            chunk_df.to_csv(fout, sep="\t", header=False, index=False, lineterminator="\n")

            written += this_chunk

    elapsed = time.time() - t0
    final_size = os.path.getsize(out_path)
    print(
        f"  [{aircraft}] {out_path}: {n_rows:,} satir, {total_cols:,} sutun, "
        f"{final_size/(1024**3):.3f}GB, {elapsed:.1f}sn",
        flush=True,
    )
    return final_size, elapsed


print(f"Toplam {len(COLUMN_TIERS)} sutun tier'i x {len(ROW_COUNTS)} satir sayisi = "
      f"{len(COLUMN_TIERS)*len(ROW_COUNTS)} dosya uretilecek.", flush=True)
print(flush=True)

grand_t0 = time.time()
total_size = 0
for n_cols in COLUMN_TIERS:
    print(f"=== Tier: {n_cols:,} veri sutunu ({aircraft_label(n_cols)}) ===", flush=True)
    tier_t0 = time.time()
    tier_size = 0
    for n_rows in ROW_COUNTS:
        out_path = os.path.join(OUT_DIR, f"synthetic_{n_cols//1000}k_{n_rows}.tab")
        if os.path.exists(out_path):
            print(f"  [atlanildi, zaten var] {out_path}", flush=True)
            tier_size += os.path.getsize(out_path)
            continue
        rng = np.random.default_rng(SEED + n_cols + n_rows)  # her dosya icin ayri ama tekrar-uretilebilir seed
        size, elapsed = gen_one_file(n_cols, n_rows, rng)
        tier_size += size
    tier_elapsed = time.time() - tier_t0
    total_size += tier_size
    print(f"  -- tier toplam: {tier_size/(1024**3):.2f}GB, {tier_elapsed/60:.1f}dk", flush=True)
    print(flush=True)

grand_elapsed = time.time() - grand_t0
print(f"=== TÜMÜ TAMAMLANDI ===", flush=True)
print(f"Toplam boyut: {total_size/(1024**3):.2f}GB", flush=True)
print(f"Toplam süre: {grand_elapsed/60:.1f}dk", flush=True)
