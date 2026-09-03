from __future__ import annotations

import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "dashboard"))

from data_docs_viewer import (
    DataDocsReferenceError,
    inline_data_docs_bundle,
    validate_data_docs_html_key,
)


class DataDocsViewerTest(unittest.TestCase):
    def test_inlines_private_assets_and_preserves_navigation(self):
        bundle_prefix = "validation/auair/batch/abc123"
        html_key = (
            f"{bundle_prefix}/validations/suite/__none__/run/asset.html"
        )
        objects = {
            f"{bundle_prefix}/static/styles/site.css": (
                b"@font-face{src:url('../fonts/report.woff2')}"
            ),
            f"{bundle_prefix}/static/fonts/report.woff2": b"font-bytes",
            f"{bundle_prefix}/static/report.js": b"window.reportReady=true;",
            f"{bundle_prefix}/static/images/logo.png": b"png-bytes",
        }
        html = b"""
        <html><head>
          <link rel="stylesheet" href="../../../../static/styles/site.css">
          <link rel="stylesheet" href="https://cdn.example/report.css">
          <script src="../../../../static/report.js"></script>
        </head><body>
          <img src="../../../../static/images/logo.png">
          <a href="../../../../index.html">Home</a>
          <a href="#details">Details</a>
        </body></html>
        """

        rendered = inline_data_docs_bundle(
            html,
            html_key=html_key,
            bundle_prefix=bundle_prefix,
            cache_buster="etag-1",
            fetch_object=objects.__getitem__,
        )

        self.assertIn("@font-face", rendered)
        self.assertIn("data:font/woff2;base64,", rendered)
        self.assertIn("window.reportReady=true;", rendered)
        self.assertIn("data:image/png;base64,", rendered)
        self.assertIn('href="https://cdn.example/report.css"', rendered)
        self.assertIn(
            f'data-ge-doc-key="{bundle_prefix}/index.html"',
            rendered,
        )
        self.assertIn('params.set("ge_docs_version", "etag-1")', rendered)

    def test_rejects_html_outside_validation_bundle(self):
        with self.assertRaises(DataDocsReferenceError):
            validate_data_docs_html_key("../private/report.html")


if __name__ == "__main__":
    unittest.main()
