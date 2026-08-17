import json
from pathlib import Path

import great_expectations as gx
import pandas as pd


DATA_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "test_data" / "test_data.csv"
)
REPORT_PATH = (
    Path(__file__).resolve().parents[1]
    / "reports"
    / "ge_validation_result.json"
)


def validate_data():
    dataframe = pd.read_csv(DATA_PATH)
    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas(name="test_data_source")
    data_asset = data_source.add_dataframe_asset(name="test_data")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="whole_dataframe"
    )

    suite = context.suites.add(gx.ExpectationSuite(name="test_data_suite"))

    # Kural 1: Tabloda yalnızca beklenen iki kolon bulunmalı.
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=["feat_A", "feat_B"],
            exact_match=True,
        )
    )

    # Kural 2: feat_A değerleri 1-100 arasında olmalı.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="feat_A",
            min_value=1,
            max_value=100,
        )
    )

    # Kural 3: feat_B değerleri 1-100 arasında olmalı.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="feat_B",
            min_value=1,
            max_value=100,
        )
    )

    # Kural 4: feat_A ortalaması basit dağılım eşiğinde kalmalı.
    suite.add_expectation(
        gx.expectations.ExpectColumnMeanToBeBetween(
            column="feat_A",
            min_value=30,
            max_value=60,
        )
    )

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="test_data_validation",
            data=batch_definition,
            suite=suite,
        )
    )
    return validation_definition.run(batch_parameters={"dataframe": dataframe})


def write_validation_report(validation_result) -> None:
    statistics = validation_result.to_json_dict()["statistics"]
    report = {
        "success": bool(validation_result.success),
        "evaluated_expectations": statistics["evaluated_expectations"],
        "successful_expectations": statistics["successful_expectations"],
        "unsuccessful_expectations": statistics["unsuccessful_expectations"],
        "success_percent": statistics["success_percent"],
    }

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    validation_result = validate_data()
    write_validation_report(validation_result)
    print(validation_result.describe())

    if not validation_result.success:
        raise SystemExit(1)
