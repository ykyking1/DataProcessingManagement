from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import great_expectations as gx
import pandas as pd

from scripts.ge_data_docs import (
    build_data_docs_bundle,
    configure_data_docs_site,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class GeDataDocsBundleTest(unittest.TestCase):
    def test_builds_complete_site_and_locates_validation_page(self):
        temporary_root = PROJECT_ROOT / "tmp" / "tests"
        temporary_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temporary_root) as temporary:
            output_directory = Path(temporary) / "data-docs"
            context = gx.get_context(mode="ephemeral")
            configure_data_docs_site(context, output_directory)

            data_source = context.data_sources.add_pandas(name="docs_source")
            data_asset = data_source.add_dataframe_asset(name="docs_asset")
            batch_definition = data_asset.add_batch_definition_whole_dataframe(
                name="docs_batch"
            )
            suite = gx.ExpectationSuite(
                name="docs_suite",
                expectations=[
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="value")
                ],
            )
            context.suites.add(suite)
            validation = gx.ValidationDefinition(
                name="docs_validation",
                data=batch_definition,
                suite=suite,
            )
            validation.run(
                batch_parameters={"dataframe": pd.DataFrame({"value": [1, None]})}
            )

            bundle = build_data_docs_bundle(context, output_directory)

            self.assertEqual(bundle.index_path, "index.html")
            self.assertTrue(bundle.validation_path.startswith("validations/"))
            self.assertTrue((output_directory / bundle.index_path).is_file())
            self.assertTrue((output_directory / bundle.validation_path).is_file())
            self.assertTrue(
                (output_directory / "static/styles/data_docs_default_styles.css").is_file()
            )
            self.assertGreater(bundle.file_count, 3)


if __name__ == "__main__":
    unittest.main()
