"""
Sikistirma/ClickHouse testleri icin sentetik genis-sutunlu dosya ureteci.

Sema (toplam 45.000 sutun):
  - 1.000 sutun: float64 (rastgele, normal dagilim, 6 ondalik)
  - 20.000 sutun: SABIT sifir (her satirda "0")
  - 20.000 sutun: SABIT bir (her satirda "1")
  - 4.000  sutun: KARISIK sifir/bir (her hucrede rastgele 0 ya da 1)

Sutun SIRASI rastgele karistirilir (float/sifir/bir/karisik turleri
dosya boyunca dagilmis olur, bloklar halinde gruplanmaz) -- gercek
telemetri dosyalarindaki duzensiz sutun dizilimini taklit eder.

Satir sayisi DUZGUN/YUVARLAK bir sayi olacak sekilde sabitlenmistir
(100.000), boyut hedefi ~10GB civarinda cikar (kesin 10GB'a
yuvarlamak icin satir sayisini "cirkin" bir sayiya zorlamiyoruz).

Ayrica bir "column manifest" (JSON) dosyasi da uretilir -- hangi
sutunun hangi turde oldugunu kaydeder, ileride otomatik siniflandirma
testleri (bkz. plan Bolum 31.5) ve dogrulama icin kullanilabilir.

Tekrar uretilebilirlik: SEED sabit (42), aynı parametrelerle tekrar
calistirildiginda birebir ayni dosya/manifest uretilir.
"""
import time
import json
import numpy as np
import pandas as pd
import os

SEED = 42
N_FLOAT = 1000
N_ZERO = 20000
N_ONE = 20000
N_MIXED = 4000
N_TOTAL = N_FLOAT + N_ZERO + N_ONE + N_MIXED  # 45000
N_ROWS = 100000  # duzgun/yuvarlak sayi (kullanicinin istegi)
CHUNK_SIZE = 10000

OUT_TAB = "/work/mixed_wide_test.tab"
OUT_MANIFEST = "/work/mixed_wide_test_columns.json"

rng = np.random.default_rng(SEED)

# --- Blok-sirali sutun isimleri (once turlere gore gruplu) ---
float_cols = [f"f{i}" for i in range(N_FLOAT)]
zero_cols = [f"z{i}" for i in range(N_ZERO)]
one_cols = [f"o{i}" for i in range(N_ONE)]
mixed_cols = [f"m{i}" for i in range(N_MIXED)]
block_cols = float_cols + zero_cols + one_cols + mixed_cols
assert len(block_cols) == N_TOTAL

# --- Rastgele karistirma (SEED ile tekrar-uretilebilir) ---
perm = rng.permutation(N_TOTAL)
shuffled_cols = [block_cols[i] for i in perm]

# --- Manifest: sutun adi -> tur ---
col_type = {}
for c in float_cols:
    col_type[c] = "float64"
for c in zero_cols:
    col_type[c] = "constant_zero"
for c in one_cols:
    col_type[c] = "constant_one"
for c in mixed_cols:
    col_type[c] = "mixed_binary"

manifest = {
    "seed": SEED,
    "n_rows": N_ROWS,
    "n_columns": N_TOTAL,
    "n_float64": N_FLOAT,
    "n_constant_zero": N_ZERO,
    "n_constant_one": N_ONE,
    "n_mixed_binary": N_MIXED,
    "column_order": shuffled_cols,
    "column_types": col_type,
}
with open(OUT_MANIFEST, "w", encoding="utf-8") as f:
    json.dump(manifest, f, ensure_ascii=False, indent=1)
print(f"Manifest yazildi: {OUT_MANIFEST}", flush=True)

# --- Uretim ---
t0 = time.time()
with open(OUT_TAB, "w", encoding="utf-8", newline="\n") as fout:
    fout.write("\t".join(shuffled_cols) + "\n")

    written = 0
    while written < N_ROWS:
        this_chunk = min(CHUNK_SIZE, N_ROWS - written)

        floats = rng.normal(loc=0, scale=100, size=(this_chunk, N_FLOAT)).round(6)
        floats_df = pd.DataFrame(floats, columns=float_cols)

        zeros_df = pd.DataFrame(
            np.zeros((this_chunk, N_ZERO), dtype=np.int8), columns=zero_cols
        )
        ones_df = pd.DataFrame(
            np.ones((this_chunk, N_ONE), dtype=np.int8), columns=one_cols
        )
        mixed_df = pd.DataFrame(
            rng.integers(0, 2, size=(this_chunk, N_MIXED), dtype=np.int8),
            columns=mixed_cols,
        )

        chunk_df = pd.concat([floats_df, zeros_df, ones_df, mixed_df], axis=1)
        chunk_df = chunk_df[shuffled_cols]  # rastgele sutun sirasina uygula

        chunk_df.to_csv(fout, sep="\t", header=False, index=False, lineterminator="\n")

        written += this_chunk
        elapsed = time.time() - t0
        cur_size = os.path.getsize(OUT_TAB)
        print(
            f"  {written:,}/{N_ROWS:,} satir yazildi, {elapsed:.1f}sn, "
            f"mevcut boyut {cur_size/(1024**3):.2f}GB",
            flush=True,
        )

total_time = time.time() - t0
final_size = os.path.getsize(OUT_TAB)
print(flush=True)
print(f"Toplam uretim suresi: {total_time:.1f}sn", flush=True)
print(f"Nihai dosya boyutu: {final_size/(1024**3):.3f}GB ({final_size:,} byte)", flush=True)
print(f"Satir sayisi: {N_ROWS:,}", flush=True)
print(
    f"Sutun sayisi: {N_TOTAL:,} "
    f"({N_FLOAT} float64 + {N_ZERO} sabit-sifir + {N_ONE} sabit-bir + {N_MIXED} karisik)",
    flush=True,
)
