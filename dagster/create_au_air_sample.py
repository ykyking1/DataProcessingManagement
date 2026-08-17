import numpy as np
import pandas as pd
from pathlib import Path

rng = np.random.default_rng(42)

n = 5000

df = pd.DataFrame({
    "time": pd.date_range(
        "2026-08-01 08:00:00",
        periods=n,
        freq="1s"
    ),

    "latitude":
        39.93 + np.cumsum(rng.normal(0, 0.00001, n)),

    "longitude":
        32.85 + np.cumsum(rng.normal(0, 0.00001, n)),

    "altitude":
        np.clip(
            100 + np.cumsum(rng.normal(0, 0.2, n)),
            0,
            500
        ),

    "velocity_x":
        rng.normal(10, 2, n),

    "velocity_y":
        rng.normal(5, 1.5, n),

    "velocity_z":
        rng.normal(0, 0.5, n),

    "roll":
        rng.normal(0, 5, n),

    "pitch":
        rng.normal(0, 5, n),

    "yaw":
        np.mod(rng.normal(180, 30, n), 360),

    "image_name":
        [f"image_{i:06d}.jpg" for i in range(n)],

    "box_x":
        rng.uniform(0, 1200, n),

    "box_y":
        rng.uniform(0, 700, n),

    "box_w":
        rng.uniform(20, 300, n),

    "box_h":
        rng.uniform(20, 300, n),

    "class":
        rng.choice(
            ["car", "pedestrian", "truck", "bus"],
            size=n
        ),
})

output_path = Path("data/au_air/telemetry.parquet")

df.to_parquet(
    output_path,
    index=False
)

print(f"Parquet oluşturuldu: {output_path}")
print(f"Satır sayısı: {len(df)}")
print(f"Kolonlar: {list(df.columns)}")