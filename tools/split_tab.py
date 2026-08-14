"""
Bir .tab dosyasını N parçaya böler (her parçada aynı header + satırların bir
alt kümesi) -- çoklu-dosya paralellik testi için (plan Bölüm 3.3/3.4'teki
worker havuzu senaryosunu simüle etmek amaçlı). Streaming okur/yazar, tüm
dosyayı RAM'e almaz.

Kullanım:
    python3 tools/split_tab.py --input testdata/big.tab --outdir testdata/parts --n 12
"""
import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("--input", required=True)
p.add_argument("--outdir", required=True)
p.add_argument("--n", type=int, required=True)
args = p.parse_args()

os.makedirs(args.outdir, exist_ok=True)

with open(args.input, encoding="utf-8") as f:
    header = f.readline()

    # Hedef: N parçaya kabaca eşit satır sayısı. Toplam satır sayısını önceden
    # bilmiyoruz (streaming), o yüzden basit round-robin: her satırı sırayla
    # bir sonraki parçaya yaz -- parçalar arası fark en fazla 1 satır olur.
    outs = [
        open(os.path.join(args.outdir, f"part_{i:02d}.tab"), "w", encoding="utf-8", newline="\n")
        for i in range(args.n)
    ]
    for o in outs:
        o.write(header)

    counts = [0] * args.n
    for idx, line in enumerate(f):
        i = idx % args.n
        outs[i].write(line)
        counts[i] += 1

    for o in outs:
        o.close()

print("Toplam satır (header hariç):", sum(counts))
for i, c in enumerate(counts):
    print(f"  part_{i:02d}.tab: {c:,} satır")
