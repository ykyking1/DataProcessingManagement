#!/usr/bin/env python3
"""
auair_sim.py — AU-AIR benzeri sentetik İHA telemetri üreteci (.tab çıktısı)

Gerçek AU-AIR (32.823 satır, 8 uçuş oturumu, 5 Hz) anotasyon dosyasından
çıkarılan istatistiklere göre kalibre edilmiştir:

  konum      : Aarhus civarı (56.2064 N, 10.1886 E), ~100 m'lik bir alanda dolaşma
  irtifa     : 2806 - 30625 mm (kalkış rampası + seyir + iniş)
  linear_*   : ort. ~0.0, std 0.24-0.38, tepe ±4  (gövde eksenli hız, m/s)
  angle_phi  : std 0.105   angle_theta: ort 0.084 std 0.074   angle_psi: [-pi, pi]
  nesne sayısı: 1'de tepe yapan, uzun kuyruklu dağılım (maks 56)
  sınıf dağılımı: Car %78, Truck %7, Van %7, Human %4, Trailer %2, ...

Satırlar bağımsız rastgele DEĞİL: konum/irtifa/açı sinyalleri değer-gürültüsü
(value noise, çok oktavlı) ile üretilir, dolayısıyla haritaya çizildiğinde
nokta bulutu değil gerçek bir uçuş rotası çıkar. Hız ve yönelim doğrudan
rotanın türevinden hesaplanır, yani kolonlar birbiriyle tutarlıdır.

Kullanım
--------
    python auair_sim.py --rows 1000000 --cols 500
    python auair_sim.py --rows 5000 --cols 30 --out-dir ./data --preview 5
    python auair_sim.py --rows 200000 --cols 2000 --flights 12 --split-flights --gzip

Çıktı dosyası adı:  <satır>_<sütun>_<flight_id>.tab
    tek dosya    : 1000000_500_flight_1_2019-08-29.tab   (ad, ilk uçuşun kimliği)
    --split-flights : her uçuş ayrı dosya, ör. 125000_500_flight_3_2019-08-29.tab

Kolon düzeni: flight_id ve time en solda bir kez yer alır; kalan AU-AIR
kolonları istenen sütun sayısına ulaşana kadar sağa doğru numaralandırılarak
tekrarlanır:

    flight_id, time,
    image_name,  image_width,  image_height,  ..., obj_trailer,
    image_name1, image_width1, image_height1, ..., obj_trailer1,
    image_name2, image_width2, image_height2, ...

Son blok gerekirse yarım kalır. Her blok bağımsız bir sensör/uçuş varyantı
olarak üretilir (aynı şema, farklı tohum), yani kolonlar birbirinin kopyası
değildir; dağılımlar aynı, değerler farklıdır. --cols 24'ten küçükse ilk blok
sağdan kırpılır, flight_id ve time her zaman korunur.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import sys
import time
from datetime import datetime, timedelta

import numpy as np

# --------------------------------------------------------------------------
# AU-AIR sabitleri (gerçek veri setinden ölçüldü)
# --------------------------------------------------------------------------

CATEGORIES = ["Human", "Car", "Truck", "Van", "Motorbike", "Bicycle", "Bus", "Trailer"]
CLASS_COUNTS = np.array([5158, 102619, 9545, 9995, 319, 1128, 729, 2538], dtype=np.float64)
CLASS_P = CLASS_COUNTS / CLASS_COUNTS.sum()

LAT0, LON0 = 56.206403, 10.188591      # oturumların ağırlık merkezi
PLATFORM = "Parrot Bebop 2"
IMG_W, IMG_H = 1920, 1080
HZ_DEFAULT = 5.0                        # gerçek veri 200 ms adımlarla örneklenmiş
ALT_MIN_MM, ALT_MAX_MM = 2806.0, 30625.0

M_PER_DEG_LAT = 111_320.0
M_PER_DEG_LON = 111_320.0 * math.cos(math.radians(LAT0))

# flight_id ve time'dan SONRA gelen temel kolonlar (bu sırayla)
BASE_COLUMNS = [
    "image_name",
    "image_width",
    "image_height",
    "platform",
    "longtitude",        # AU-AIR'deki orijinal yazım (typo) korundu
    "latitude",
    "altitude",
    "linear_x",
    "linear_y",
    "linear_z",
    "angle_phi",
    "angle_theta",
    "angle_psi",
    "num_objects",
] + ["obj_" + c.lower() for c in CATEGORIES]

INT_COLUMNS = {"image_width", "image_height", "num_objects"} | {
    "obj_" + c.lower() for c in CATEGORIES
}
STR_COLUMNS = {"flight_id", "time", "image_name", "platform"}

FIXED_COLUMNS = ["flight_id", "time"]           # her zaman en solda, asla kırpılmaz
N_BASE_TOTAL = len(FIXED_COLUMNS) + len(BASE_COLUMNS)



# --------------------------------------------------------------------------
# Deterministik gürültü çekirdeği (chunk'tan bağımsız, rastgele erişilebilir)
# --------------------------------------------------------------------------

def _hash01(seed: int, idx: np.ndarray) -> np.ndarray:
    """splitmix64 tabanlı, (seed, idx) -> [0,1) deterministik değer."""
    x = idx.astype(np.uint64) + np.uint64((seed * 0x9E3779B97F4A7C15 + 0x165667B19E3779F9) & 0xFFFFFFFFFFFFFFFF)
    x = x ^ (x >> np.uint64(30))
    x = x * np.uint64(0xBF58476D1CE4E5B9)
    x = x ^ (x >> np.uint64(27))
    x = x * np.uint64(0x94D049BB133111EB)
    x = x ^ (x >> np.uint64(31))
    return (x >> np.uint64(11)).astype(np.float64) / float(1 << 53)


def value_noise(seed: int, idx: np.ndarray, period: float) -> np.ndarray:
    """Kontrol noktaları arasında smoothstep ile yumuşatılmış [0,1) sinyal."""
    p = idx / float(period)
    i0 = np.floor(p).astype(np.int64)
    f = p - i0
    w = f * f * (3.0 - 2.0 * f)
    a = _hash01(seed, i0)
    b = _hash01(seed, i0 + 1)
    return a * (1.0 - w) + b * w


def fbm(seed: int, idx: np.ndarray, period: float, octaves: int = 3) -> np.ndarray:
    """Çok oktavlı değer gürültüsü, [-1, 1] aralığına normalize."""
    out = np.zeros(idx.shape, dtype=np.float64)
    amp, norm, per = 1.0, 0.0, float(period)
    for o in range(octaves):
        out += amp * (value_noise(seed + 7919 * o, idx, per) - 0.5) * 2.0
        norm += amp
        amp *= 0.5
        per *= 0.5
    return out / norm


# --------------------------------------------------------------------------
# Uçuş tanımı
# --------------------------------------------------------------------------

def make_flight_id(index: int, start_dt: datetime) -> str:
    return f"flight_{index + 1}_{start_dt.strftime('%Y-%m-%d')}"


class Flight:
    """Tek bir uçuş oturumu: rota, irtifa profili ve sahne yoğunluğu."""

    def __init__(self, index: int, n_rows: int, start_dt: datetime, hz: float, seed: int,
                 variant: int = 0):
        self.index = index
        self.variant = variant                                   # tekrarlanan kolon bloğu no
        self.n_rows = n_rows
        self.start_dt = start_dt
        self.dt = 1.0 / hz
        self.seed = seed * 1000 + index * 17 + variant * 104729

        rng = np.random.default_rng(self.seed)
        self.lat0 = LAT0 + rng.normal(0, 3e-4)
        self.lon0 = LON0 + rng.normal(0, 5e-4)
        self.radius_m = float(rng.uniform(35.0, 70.0))          # dolaşma yarıçapı
        self.period = float(rng.uniform(750, 1500))             # ana rota periyodu (örnek)
        self.cruise_mm = float(rng.uniform(9_000, 27_000))      # seyir irtifası
        self.scene_scale = float(rng.uniform(0.6, 1.25))         # trafik yoğunluğu çarpanı
        self.yaw0 = float(rng.uniform(-1.0, 2.2))               # tercih edilen bakış yönü (rad)

        self.session = start_dt.strftime("%Y%m%d%H%M%S")
        self.flight_id = make_flight_id(index, start_dt)
        self.seq0 = int(rng.integers(100, 400))                 # ilk kare numarası

    # -- rota ------------------------------------------------------------
    def xy(self, idx: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Yerel düzlemde (metre) konum. Çok oktavlı gürültü -> pürüzsüz rota."""
        x = self.radius_m * fbm(self.seed + 11, idx, self.period, octaves=4)
        y = self.radius_m * fbm(self.seed + 23, idx, self.period * 1.13, octaves=4)
        return x, y

    def altitude_mm(self, idx: np.ndarray) -> np.ndarray:
        n = max(self.n_rows - 1, 1)
        t = np.clip(idx / n, 0.0, 1.0)
        ramp = np.minimum(t / 0.04, 1.0) * np.minimum((1.0 - t) / 0.04, 1.0)   # kalkış/iniş
        ramp = np.clip(ramp, 0.0, 1.0)
        wobble = fbm(self.seed + 37, idx, self.period * 0.6, octaves=3)
        alt = self.cruise_mm * (0.30 + 0.70 * ramp) + 6_500.0 * wobble * ramp
        return np.clip(alt, ALT_MIN_MM, ALT_MAX_MM)

    def scene_lambda(self, idx: np.ndarray) -> np.ndarray:
        base = value_noise(self.seed + 53, idx, 220.0)          # sahne yavaş değişir
        burst = value_noise(self.seed + 59, idx, 40.0)
        return np.clip(self.scene_scale * (0.5 + 6.5 * base ** 2 + 9.0 * burst ** 4), 0.05, 40.0)


# --------------------------------------------------------------------------
# Chunk üretimi
# --------------------------------------------------------------------------

def build_block(fl: Flight, start: int, n: int, rng: np.random.Generator,
                wanted: set, suffix: str = "") -> dict:
    """[start, start+n) aralığı için bir AU-AIR kolon bloğu üretir.

    suffix boş ise blok, flight_id ve time kolonlarını da içerir; tekrarlanan
    bloklarda ("1", "2", ...) sadece diğer kolonlar üretilir ve isimlerinin
    sonuna blok numarası eklenir (image_name1, image_width1, ...).
    """
    idx = np.arange(start, start + n, dtype=np.int64)
    prev = idx - 1                                              # türev için bir örnek geri

    x, y = fl.xy(idx)
    xp, yp = fl.xy(prev)
    alt = fl.altitude_mm(idx)
    altp = fl.altitude_mm(prev)

    lat = fl.lat0 + y / M_PER_DEG_LAT
    lon = fl.lon0 + x / M_PER_DEG_LON

    # dünya eksenli hız (m/s)
    vx = (x - xp) / fl.dt
    vy = (y - yp) / fl.dt
    vz = (alt - altp) / (1000.0 * fl.dt)

    # yönelim: yaw uçuşa özgü bir tercih edilen yön etrafında yavaş salınır
    # (AU-AIR'de psi dağılımı ort 0.74 / std 1.10, yani rotadan bağımsız ve toplu),
    # roll dönüş hızına, pitch ileri ivmeye bağlanır
    course = np.arctan2(vy, vx)
    speed = np.hypot(vx, vy)
    dcourse = np.diff(course, prepend=course[0])
    turn = np.arctan2(np.sin(dcourse), np.cos(dcourse)) / fl.dt
    accel = np.diff(speed, prepend=speed[0]) / fl.dt

    phi = np.clip(-0.12 * turn * np.clip(speed, 0, 3) + 0.30 * fbm(fl.seed + 71, idx, 40, 3)
                  - 0.029, -0.54, 0.58)
    theta = np.clip(0.084 + 0.20 * accel + 0.21 * fbm(fl.seed + 83, idx, 45, 3), -0.43, 0.46)
    psi = fl.yaw0 + 1.55 * fbm(fl.seed + 89, idx, 260, 3) + 0.02 * fbm(fl.seed + 97, idx, 20, 2)
    psi = np.arctan2(np.sin(psi), np.cos(psi))

    # doğrusal hızlar (odom/ENU çerçevesi) + ölçüm gürültüsü + seyrek tepe değerler
    lin_x = vx + rng.normal(0, 0.12, n)
    lin_y = vy + rng.normal(0, 0.14, n)
    lin_z = vz + rng.normal(0, 0.18, n)
    for arr, p, mag in ((lin_x, 0.0016, 2.6), (lin_y, 0.0016, 2.6), (lin_z, 0.0022, 3.0)):
        m = rng.random(n) < p
        arr[m] += rng.normal(0, mag, int(m.sum()))
    np.clip(lin_x, -3.9, 3.4, out=lin_x)
    np.clip(lin_y, -3.9, 3.1, out=lin_y)
    np.clip(lin_z, -4.4, 2.3, out=lin_z)

    # nesneler (sadece istenirse hesaplanır)
    need_obj = any(("num_objects" + suffix) == w or w.startswith("obj_") for w in wanted)
    counts = np.zeros((n, len(CATEGORIES)), dtype=np.int64)
    num = np.zeros(n, dtype=np.int64)
    if need_obj:
        lam = fl.scene_lambda(idx)
        num = rng.poisson(lam).astype(np.int64)
        np.clip(num, 0, 56, out=num)
        total = int(num.sum())
        if total:
            cls = rng.choice(len(CATEGORIES), size=total, p=CLASS_P)
            row = np.repeat(np.arange(n, dtype=np.int64), num)
            flat = np.bincount(row * len(CATEGORIES) + cls, minlength=n * len(CATEGORIES))
            counts = flat.reshape(n, len(CATEGORIES))

    # kare adı (pahalı: sadece istenirse üretilir)
    if ("image_name" + suffix) in wanted:
        seq = fl.seq0 + idx
        image_name = np.char.add(np.char.add(f"frame_{fl.session}_x_",
                                             np.char.mod("%07d", seq)), ".jpg")
    else:
        image_name = np.zeros(n, dtype="U1")

    out = {
        "image_name": image_name,
        "image_width": np.full(n, IMG_W, dtype=np.int64),
        "image_height": np.full(n, IMG_H, dtype=np.int64),
        "platform": np.full(n, PLATFORM),
        "longtitude": lon,
        "latitude": lat,
        "altitude": alt,
        "linear_x": lin_x,
        "linear_y": lin_y,
        "linear_z": lin_z,
        "angle_phi": phi,
        "angle_theta": theta,
        "angle_psi": psi,
        "num_objects": num,
    }
    for i, cat in enumerate(CATEGORIES):
        out["obj_" + cat.lower()] = counts[:, i]

    out = {k + suffix: v for k, v in out.items() if (k + suffix) in wanted}
    if not suffix:                                   # sabit kolonlar sadece ilk blokta
        ms = np.round(idx * fl.dt * 1000.0).astype("int64")
        t0 = np.datetime64(fl.start_dt.replace(microsecond=0).isoformat(), "ms")
        tstamp = t0 + ms.astype("timedelta64[ms]")
        out["flight_id"] = np.full(n, fl.flight_id)
        out["time"] = tstamp.astype("datetime64[ms]").astype(str)
    return out


# --------------------------------------------------------------------------
# Yazma
# --------------------------------------------------------------------------

def _str_block(values: np.ndarray) -> np.ndarray | None:
    """Sabit uzunluklu metin kolonunu (n, w) bayt matrisine çevirir."""
    s = np.asarray(values, dtype=str)
    lens = np.char.str_len(s)
    b = s.astype("S")
    w = b.dtype.itemsize
    if w == 0 or int(lens.min()) != w:      # chunk içinde uzunluk sabit değilse
        return None
    return np.frombuffer(b.tobytes(), dtype=np.uint8).reshape(len(s), w)


def _num_block(v: np.ndarray, dec: int) -> np.ndarray:
    """Sayısal kolonu sabit genişlikli ASCII bayt matrisine çevirir (vektörize)."""
    scaled = np.rint(np.asarray(v, dtype=np.float64) * (10.0 ** dec)).astype(np.int64) \
        if dec else np.asarray(v, dtype=np.int64)
    absv = np.abs(scaled)
    has_neg = bool((scaled < 0).any())
    mx = int(absv.max(initial=0)) // (10 ** dec)
    ip = max(1, len(str(mx)))
    w = (1 if has_neg else 0) + ip + (1 + dec if dec else 0)
    out = np.empty((scaled.shape[0], w), dtype=np.uint8)
    col = w
    work = absv.copy()
    for _ in range(dec):
        col -= 1
        out[:, col] = 48 + (work % 10).astype(np.uint8)
        work //= 10
    if dec:
        col -= 1
        out[:, col] = 46                                  # '.'
    for _ in range(ip):
        col -= 1
        out[:, col] = 48 + (work % 10).astype(np.uint8)
        work //= 10
    if has_neg:
        out[:, 0] = np.where(scaled < 0, 45, 48)          # '-' / '0'
    return out


def encode_chunk_fast(cols: dict, names: list[str], precision: int) -> bytes | None:
    """Sabit genişlikli (sıfır dolgulu) hızlı kodlayıcı. Uygun değilse None döner."""
    blocks = []
    for name in names:
        v = cols[name]
        if name in STR_COLUMNS or v.dtype.kind in "US":
            b = _str_block(v)
            if b is None:
                return None
        else:
            b = _num_block(v, 0 if v.dtype.kind in "iu" else precision)
        blocks.append(b)
    n = blocks[0].shape[0]
    total = sum(b.shape[1] for b in blocks) + len(blocks)
    line = np.empty((n, total), dtype=np.uint8)
    pos = 0
    for i, b in enumerate(blocks):
        w = b.shape[1]
        line[:, pos:pos + w] = b
        pos += w
        line[:, pos] = 10 if i == len(blocks) - 1 else 9      # '\n' / '\t'
        pos += 1
    return line.tobytes()


def encode_chunk_exact(cols: dict, names: list[str], precision: int) -> bytes:
    """Tam biçimli (dolgusuz) kodlayıcı: pandas varsa onu, yoksa numpy'ı kullanır."""
    fmt = "%." + str(precision) + "f"
    try:
        import pandas as pd
        df = pd.DataFrame({k: cols[k] for k in names}, columns=names)
        return df.to_csv(sep="\t", index=False, header=False,
                         float_format=fmt, lineterminator="\n").encode()
    except ImportError:
        parts = []
        for name in names:
            v = cols[name]
            if name in STR_COLUMNS or v.dtype.kind in "US":
                parts.append(np.asarray(v, dtype=str))
            elif v.dtype.kind in "iu":
                parts.append(np.char.mod("%d", v))
            else:
                parts.append(np.char.mod(fmt, v))
        arr = np.stack(parts, axis=1)
        return ("\n".join("\t".join(r) for r in arr.tolist()) + "\n").encode()


def encode_chunk(cols: dict, names: list[str], precision: int, fast: bool) -> bytes:
    if fast:
        out = encode_chunk_fast(cols, names, precision)
        if out is not None:
            return out
    return encode_chunk_exact(cols, names, precision)


def resolve_columns(n_cols: int) -> tuple[list[str], list[tuple[str, list[str]]]]:
    """İstenen sütun sayısına göre kolon adlarını ve blok yapısını belirler.

    İlk blok orijinal AU-AIR adlarını kullanır; sonraki bloklarda flight_id ve
    time dışındaki kolonlar sağa doğru numaralandırılarak tekrarlanır:
    image_name1, image_width1, ... image_name2, image_width2, ...
    Son blok gerekirse yarım kalır.
    """
    if n_cols < len(FIXED_COLUMNS):
        raise ValueError(f"--cols en az {len(FIXED_COLUMNS)} olmalı (flight_id + time)")

    names = list(FIXED_COLUMNS)
    blocks: list[tuple[str, list[str]]] = []
    remaining = n_cols - len(FIXED_COLUMNS)
    block_no = 0
    while remaining > 0:
        suffix = "" if block_no == 0 else str(block_no)
        take = min(remaining, len(BASE_COLUMNS))
        cols = [c + suffix for c in BASE_COLUMNS[:take]]
        blocks.append((suffix, cols))
        names.extend(cols)
        remaining -= take
        block_no += 1
    if not blocks:                                   # sadece flight_id + time
        blocks.append(("", []))
    return names, blocks


def split_rows(total: int, parts: int) -> list[int]:
    base, rem = divmod(total, parts)
    return [base + (1 if i < rem else 0) for i in range(parts)]


def _flight_stream(fh, fi: int, n_rows: int, session_dt: datetime, hz: float, seed: int,
                   names: list, blocks: list, precision: int, fast: bool,
                   chunk_size: int, progress=None) -> None:
    """Bir uçuş oturumunu chunk chunk üretip açık dosyaya yazar."""
    flights = [Flight(fi, n_rows, session_dt, hz, seed, variant=b) for b in range(len(blocks))]
    rngs = [np.random.default_rng(seed * 7919 + fi * 131 + b) for b in range(len(blocks))]
    wanted = [set(cols) for _, cols in blocks]
    done = 0
    while done < n_rows:
        n = min(chunk_size, n_rows - done)
        chunk = {}
        for b, (suffix, _) in enumerate(blocks):
            chunk.update(build_block(flights[b], done, n, rngs[b], wanted[b], suffix))
        fh.write(encode_chunk(chunk, names, precision, fast))
        done += n
        if progress is not None:
            progress(n)


def _flight_worker(task) -> str:
    """multiprocessing girişi: tek uçuşu geçici dosyaya yazar."""
    (fi, n_rows, session_iso, hz, seed, names, blocks, precision, fast, chunk_size, out_path) = task
    with open(out_path, "wb", buffering=1 << 22) as fh:
        _flight_stream(fh, fi, n_rows, datetime.fromisoformat(session_iso), hz, seed,
                       names, blocks, precision, fast, chunk_size)
    return out_path


def session_times(counts: list[int], start_dt: datetime, hz: float) -> list[datetime]:
    """Oturum başlangıçları: uçuş süresi + 20-60 dk yer molası."""
    out, cur = [], start_dt
    for i, n in enumerate(counts):
        out.append(cur)
        cur = cur + timedelta(seconds=n / hz + 60 * (20 + (7 * i) % 40))
    return out


def generate(rows: int, cols: int, out_dir: str, flights: int, hz: float, seed: int,
             start: str, chunk_size: int, precision: int, header: bool,
             compress: bool, preview: int, write_meta: bool, fast: bool, jobs: int,
             split: bool) -> list[str]:
    names, blocks = resolve_columns(cols)
    counts = [c for c in split_rows(rows, flights) if c > 0]
    starts = session_times(counts, datetime.fromisoformat(start), hz)
    ids = [make_flight_id(fi, starts[fi]) for fi in range(len(counts))]

    os.makedirs(out_dir, exist_ok=True)
    opener = (lambda p: gzip.open(p, "wb", compresslevel=6)) if compress else \
             (lambda p: open(p, "wb", buffering=1 << 22))

    # split: her uçuş ayrı dosya; aksi halde tek dosya, adında ilk uçuşun kimliği
    if split:
        groups = [([fi], counts[fi], ids[fi]) for fi in range(len(counts))]
    else:
        groups = [(list(range(len(counts))), rows, ids[0])]

    t_begin = time.time()
    state = {"n": 0}

    def progress(k: int) -> None:
        state["n"] += k
        if state["n"] % (chunk_size * 10) == 0:
            el = time.time() - t_begin
            print(f"  {state['n']:,}/{rows:,} satır  ({state['n'] / max(el, 1e-9):,.0f} satır/s)",
                  file=sys.stderr)

    paths = []
    for members, n_file, fid in groups:
        stem = f"{n_file}_{cols}_{fid}"
        fname = stem + ".tab" + (".gz" if compress else "")
        path = os.path.join(out_dir, fname)
        paths.append(path)

        if jobs > 1 and len(members) > 1:
            import multiprocessing as mp
            import shutil
            import tempfile
            tmpdir = tempfile.mkdtemp(prefix="auair_sim_")
            try:
                tasks = [(fi, counts[fi], starts[fi].isoformat(), hz, seed, names, blocks,
                          precision, fast, chunk_size, os.path.join(tmpdir, f"part_{fi:04d}.tab"))
                         for fi in members]
                with mp.Pool(min(jobs, len(members))) as pool:
                    parts = pool.map(_flight_worker, tasks)
                with opener(path) as fh:
                    if header:
                        fh.write(("\t".join(names) + "\n").encode())
                    for part in parts:
                        with open(part, "rb") as src:
                            shutil.copyfileobj(src, fh, 1 << 22)
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
        else:
            with opener(path) as fh:
                if header:
                    fh.write(("\t".join(names) + "\n").encode())
                for fi in members:
                    _flight_stream(fh, fi, counts[fi], starts[fi], hz, seed, names, blocks,
                                   precision, fast, chunk_size, progress)

        size = os.path.getsize(path)
        if preview:
            op = gzip.open if compress else open
            with op(path, "rb") as fh:
                for _ in range(preview + (1 if header else 0)):
                    line = fh.readline().decode().rstrip("\n")
                    if not line:
                        break
                    print(line[:400] + (" ..." if len(line) > 400 else ""), file=sys.stderr)
            preview = 0                                  # sadece ilk dosya için

        print(f"{path}  ({n_file:,} satır x {cols:,} sütun | {size / 1e6:,.1f} MB)", file=sys.stderr)

        if write_meta:
            meta = {
                "file": fname, "rows": n_file, "cols": cols,
                "flights": [ids[fi] for fi in members],
                "hz": hz, "seed": seed, "start": starts[members[0]].isoformat(),
                "bytes": size, "format": "fast" if fast else "exact",
                "column_blocks": len(blocks),
                "columns": names[:64] + (["..."] if len(names) > 64 else []),
                "source_profile": "AU-AIR 2019 (32823 satır, 8 oturum, 5 Hz)",
            }
            with open(os.path.join(out_dir, stem + ".meta.json"), "w") as f:
                json.dump(meta, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - t_begin
    print(f"\ntoplam {rows:,} satır x {cols:,} sütun | {len(counts)} uçuş | "
          f"{len(paths)} dosya | {elapsed:,.1f} sn | {rows / max(elapsed, 1e-9):,.0f} satır/s",
          file=sys.stderr)
    return paths


# --------------------------------------------------------------------------

def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="AU-AIR benzeri sentetik İHA telemetrisi üretir (.tab)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--rows", type=int, default=100_000, help="üretilecek satır sayısı")
    p.add_argument("--cols", type=int, default=64, help="toplam sütun sayısı (flight_id ve time dahil)")
    p.add_argument("--out-dir", default="dagster/data/raw", help="çıktı klasörü")
    p.add_argument("--flights", type=int, default=8, help="uçuş oturumu sayısı")
    p.add_argument("--hz", type=float, default=HZ_DEFAULT, help="örnekleme frekansı")
    p.add_argument("--seed", type=int, default=42, help="rastgelelik tohumu (tekrarlanabilirlik)")
    p.add_argument("--start", default="2019-08-29T09:11:11", help="ilk oturumun başlangıç zamanı (ISO)")
    p.add_argument("--chunk-size", type=int, default=50_000, help="bellekte tutulan satır bloğu")
    p.add_argument("--precision", type=int, default=6, help="ondalık basamak sayısı")
    p.add_argument("--no-header", action="store_true", help="başlık satırını yazma")
    p.add_argument("--gzip", action="store_true", help="çıktıyı .tab.gz olarak sıkıştır")
    p.add_argument("--preview", type=int, default=0, help="ilk N satırı stderr'e yazdır")
    p.add_argument("--fast", action="store_true",
                   help="hızlı kodlayıcı: sabit genişlikli, sıfır dolgulu sayılar (~4x hızlı)")
    p.add_argument("--split-flights", action="store_true",
                   help="her uçuşu ayrı dosyaya yaz (satır_sütun_flightid.tab)")
    p.add_argument("--jobs", type=int, default=1,
                   help="paralel süreç sayısı (uçuş başına bölünür; büyük dosyalar için)")
    p.add_argument("--no-meta", action="store_true", help="yan meta.json dosyasını yazma")
    a = p.parse_args(argv)

    if a.rows <= 0:
        p.error("--rows pozitif olmalı")
    if a.flights <= 0:
        p.error("--flights pozitif olmalı")

    try:
        resolve_columns(a.cols)
    except ValueError as exc:
        p.error(str(exc))

    generate(a.rows, a.cols, a.out_dir, min(a.flights, a.rows), a.hz, a.seed, a.start,
             a.chunk_size, a.precision, not a.no_header, a.gzip, a.preview, not a.no_meta,
             a.fast, a.jobs, a.split_flights)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
