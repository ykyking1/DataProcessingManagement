from pathlib import Path

import pandas as pd
import pandera.pandas as pa


DATA_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "test_data" / "test_data.csv"
)

schema = pa.DataFrameSchema(
    {
        "feat_A": pa.Column(
            int,
            checks=pa.Check.in_range(1, 100),
            nullable=False,
        ),
        "feat_B": pa.Column(
            int,
            checks=pa.Check.in_range(1, 100),
            nullable=False,
        ),
    },
    strict=True,
)


def validate_data() -> pd.DataFrame:
    dataframe = pd.read_csv(DATA_PATH)
    return schema.validate(dataframe, lazy=True)


if __name__ == "__main__":
    validated_data = validate_data()
    print(f"Validation başarılı: {len(validated_data)} satır kontrol edildi.")
