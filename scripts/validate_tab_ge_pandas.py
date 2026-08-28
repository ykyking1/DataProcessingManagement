"""Spark'sız (pandas tabanlı) Great Expectations doğrulaması.

scripts/validate_tab_spark_ge.py ile AYNI fikri (timestamp/aircraft_type/
feature sütunlarının doluluğu, aircraft_type eşleşmesi) uyguluyor, ama
Spark yerine pandas kullanıyor -- bu ortamda pyspark kurulu olsa da JVM
(java) hiç kurulu olmadığı için Spark yolu çalışmıyor (bkz. proje
belleği / 2026-08-28 inceleme notu).

Geniş şemalı (10K-50K sütun) dosyalarda tüm dosyayı pandas'a okumak
YAVAŞ ve bellek açısından riskli olurdu -- bu yüzden sadece --sample-rows
kadar satır okunur (doğrulama amaçlı örnekleme, tam yükleme değil).
Aynı dosyanın ClickHouse'a TAM yüklenmesi hâlâ load_extended_telemetry.py
(scripts/load_extended_telemetry.py) ile yapılır; bu script sadece
kalite kontrolü içindir.
"""

from __future__ import annotations

import argparse
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import great_expectations as gx
import zstandard as zstd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT_DIR = PROJECT_ROOT / "reports" / "validation"
TIMESTAMP_COLUMN = "timestamp"
AIRCRAFT_TYPE_COLUMN = "aircraft_type"
DEFAULT_SAMPLE_ROWS = 2000


def _default_report_path() -> Path:
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return DEFAULT_REPORT_DIR / f"ge_validation_pandas_{run_timestamp}.json"


def write_validation_report(
    validation: dict[str, Any],
    output_path: Path | str | None = None,
) -> Path:
    """scripts/validate_tab_spark_ge.py::write_validation_report ile aynı
    (atomik yazma) desen -- iki raporun şekli aynı kalsın diye."""

    report_path = (
        _default_report_path() if output_path is None else Path(output_path).resolve()
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = report_path.with_suffix(report_path.suffix + ".tmp")
    temporary_path.write_text(
        json.dumps(validation, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_path.replace(report_path)
    return report_path


def _open_tab_file(path: Path):
    lower = path.name.lower()
    if lower.endswith(".zst") or lower.endswith(".zstd"):
        raw = path.open("rb")
        return zstd.ZstdDecompressor().stream_reader(raw)
    if lower.endswith(".gz"):
        return gzip.open(path, "rb")
    return path.open("rb")


def read_tab_sample(path: Path, *, sample_rows: int) -> pd.DataFrame:
    """Dosyanın başlığını + ilk sample_rows veri satırını okur.

    dtype=str: load_extended_telemetry.py'nin hızlı yolundaki gibi, tip
    çıkarımı GE'nin kendi expectation'larına bırakılıyor (ör.
    ExpectColumnValuesToNotBeNull ham metin üzerinde de çalışır).
    """

    with _open_tab_file(path) as file_obj:
        return pd.read_csv(
            file_obj,
            sep="\t",
            dtype=str,
            nrows=sample_rows,
            engine="c",
            on_bad_lines="error",
        )


def _default_feature_sample(feature_columns: Sequence[str]) -> list[str]:
    """scripts/validate_tab_spark_ge.py::_default_feature_sample ile aynı
    fikir -- ilk, orta, son özellik sütunu (binlerce expectation'dan
    kaçınmak için)."""

    if not feature_columns:
        raise ValueError("At least one feature column is required for validation.")
    indexes = (0, len(feature_columns) // 2, len(feature_columns) - 1)
    return list(dict.fromkeys(feature_columns[index] for index in indexes))


def validate_tab_dataframe(
    dataframe: pd.DataFrame,
    *,
    expected_aircraft_type: str | None = None,
    feature_columns: Sequence[str] | None = None,
    result_format: str = "BASIC",
    report_path: Path | str | None = None,
    write_report: bool = True,
) -> dict[str, Any]:
    columns = list(dataframe.columns)
    required_prefix = [TIMESTAMP_COLUMN, AIRCRAFT_TYPE_COLUMN]
    if columns[:2] != required_prefix:
        raise ValueError(
            f"The first two columns must be {required_prefix}; received {columns[:2]}."
        )

    available_features = columns[2:]
    selected_features = (
        _default_feature_sample(available_features)
        if feature_columns is None
        else list(dict.fromkeys(feature_columns))
    )
    missing = [c for c in selected_features if c not in available_features]
    if missing:
        raise ValueError(f"Unknown feature columns requested: {missing}")

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name="tab_pandas_source")
    data_asset = data_source.add_dataframe_asset(name="tab_pandas_dataframe")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="tab_pandas_batch"
    )

    expectations: list[Any] = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=TIMESTAMP_COLUMN),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=AIRCRAFT_TYPE_COLUMN),
    ]

    if expected_aircraft_type is not None:
        normalized = expected_aircraft_type.strip().upper()
        if not normalized:
            raise ValueError("expected_aircraft_type cannot be blank.")
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=AIRCRAFT_TYPE_COLUMN,
                value_set=[normalized],
            )
        )
        # dosyadaki değerler baştaki/sondaki boşluklarla gelebiliyor
        # (bkz. generate_small_mx_tab_fixtures.py'deki gibi kirli veri
        # senaryosu) -- karşılaştırmadan önce normalize ediyoruz.
        dataframe = dataframe.copy()
        dataframe[AIRCRAFT_TYPE_COLUMN] = (
            dataframe[AIRCRAFT_TYPE_COLUMN].str.strip().str.upper()
        )

    expectations.extend(
        gx.expectations.ExpectColumnValuesToNotBeNull(column=column_name)
        for column_name in selected_features
    )

    suite = gx.ExpectationSuite(name="tab_pandas_suite", expectations=expectations)
    context.suites.add(suite)
    validation_definition = gx.ValidationDefinition(
        name="tab_pandas_validation",
        data=batch_definition,
        suite=suite,
    )
    validation_result = validation_definition.run(
        batch_parameters={"dataframe": dataframe},
        result_format=result_format,
    )

    ge_result = validation_result.to_json_dict()
    output: dict[str, Any] = {
        "success": bool(ge_result["success"]),
        "statistics": ge_result["statistics"],
        "validated_feature_columns": selected_features,
        "sampled_row_count": len(dataframe),
        "total_column_count": len(columns),
        "result": ge_result,
    }
    if write_report:
        written_report = write_validation_report(output, report_path)
        output["report_path"] = str(written_report)
    else:
        output["report_path"] = None
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a .tab/.tab.zst file's sample rows with GE (pandas backend, no Spark)."
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--expected-aircraft-type")
    parser.add_argument("--feature-column", action="append", dest="feature_columns")
    parser.add_argument("--result-format", default="BASIC")
    parser.add_argument("--sample-rows", type=int, default=DEFAULT_SAMPLE_ROWS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataframe = read_tab_sample(args.input, sample_rows=args.sample_rows)
    validation = validate_tab_dataframe(
        dataframe,
        expected_aircraft_type=args.expected_aircraft_type,
        feature_columns=args.feature_columns,
        result_format=args.result_format,
        report_path=args.report,
    )
    print(
        f"Validation {'passed' if validation['success'] else 'failed'}: "
        f"{validation['statistics']} (sampled {validation['sampled_row_count']} rows, "
        f"{validation['total_column_count']} columns)"
    )
    if not validation["success"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
