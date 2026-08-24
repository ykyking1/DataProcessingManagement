# -*- coding: utf-8 -*-
"""
Ham .tab dosyalarindaki bilinen bir sorunu duzeltir: her satirin
SONUNDA fazladan bir tab karakteri var (orn. "...b699\t\n"), bu da
gercek sutun sayisindan bir fazla ("hayali" bos bir sutun) alan
sayilmasina yol aciyor. Bu script, isteğe bagli olarak sadece ilk
N_ROWS satiri alarak (kucuk test alt kumesi icin) veya tum dosyayi
(N_ROWS=None) temizler.

Kullanim: dosya yollarini ve N_ROWS degerini asagida degistirip
calistir. Ileride bu, tam pipeline scriptine (pipeline_tab_to_clickhouse.py)
entegre edilecek bir on-isleme adimi.
"""
import time

SRC = r"C:\Users\PC_4150_YD26\DataProcessingManagement\testdata\dataset_01.tab"
OUT = r"C:\Users\PC_4150_YD26\DataProcessingManagement\testdata\dataset_01_clean.tab"
N_ROWS = None  # None = tum dosya; kucuk test icin ornegin 2000 yap

t0 = time.time()
written = 0
with open(SRC, "rb") as fin, open(OUT, "wb") as fout:
    header = fin.readline()
    fout.write(header.rstrip(b"\t\r\n") + b"\n")
    while N_ROWS is None or written < N_ROWS:
        line = fin.readline()
        if not line:
            break
        fout.write(line.rstrip(b"\t\r\n") + b"\n")
        written += 1
        if written % 1_000_000 == 0:
            print(f"  {written:,} satir islendi, {time.time()-t0:.1f}sn", flush=True)

elapsed = time.time() - t0
print(f"{written:,} satir yazildi, {elapsed:.1f}sn")
print(f"Cikti: {OUT}")
