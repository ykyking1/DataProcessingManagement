from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

import zstandard as zstd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.preprocess_auair_tab_polars import (
    polars_readable_tab_inputs,
    preprocess_auair_lazyframe,
    read_auair_lazyframe,
    write_processed_tab_zstd,
)


class AuairPolarsPreprocessingTest(unittest.TestCase):
    def test_zstd_input_and_output_preserve_processing_contract(self):
        columns = [
            "flight_id",
            "time",
            "image_name",
            "image_width",
            "image_height",
            "platform",
            "longtitude",
            "latitude",
            "altitude",
            "linear_x",
            "linear_y",
            "linear_z",
            "angle_phi",
            "angle_theta",
            "angle_psi",
            "num_objects",
            "obj_human",
        ]
        rows = [
            [
                " flight_1_2019-08-29 ",
                " 2019-08-29T09:11:11.125 ",
                " frame.jpg ",
                "640",
                "480",
                " drone ",
                "32.2",
                "39.9",
                "120.5",
                "1.0",
                "2.0",
                "3.0",
                "0.1",
                "0.2",
                "0.3",
                "2",
                "1",
            ],
            [
                "flight_1_2019-08-29",
                "not-a-time",
                "frame-2.jpg",
                "not-an-int",
                "480",
                "drone",
                "32.2",
                "39.9",
                "120.5",
                "1.0",
                "2.0",
                "3.0",
                "0.1",
                "0.2",
                "0.3",
                "0",
                "0",
            ],
        ]

        temporary_root = PROJECT_ROOT / "tmp" / "tests"
        temporary_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temporary_root) as temporary_directory:
            temporary = Path(temporary_directory)
            plain_input = temporary / "input.tab"
            with plain_input.open("w", newline="", encoding="utf-8") as output:
                writer = csv.writer(output, delimiter="\t", lineterminator="\n")
                writer.writerow(columns)
                writer.writerows(rows)

            compressed_input = temporary / "input.tab.zst"
            compressor = zstd.ZstdCompressor(level=12)
            with plain_input.open("rb") as source, compressed_input.open("wb") as target:
                compressor.copy_stream(source, target)
            plain_input.unlink()

            output_path = temporary / "processed"
            with polars_readable_tab_inputs(compressed_input) as inputs:
                source = read_auair_lazyframe(inputs)
                processed = preprocess_auair_lazyframe(source)
                part_count = write_processed_tab_zstd(processed, output_path)

            self.assertEqual(part_count, 1)
            part_path = output_path / "part-00000.tab.zst"
            self.assertTrue(part_path.is_file())
            with part_path.open("rb") as compressed_file:
                with zstd.ZstdDecompressor().stream_reader(compressed_file) as reader:
                    decompressed = reader.read()

            parsed = list(
                csv.reader(
                    decompressed.decode("utf-8").splitlines(),
                    delimiter="\t",
                )
            )
            self.assertEqual(parsed[0], columns)
            self.assertEqual(len(parsed), 3)
            self.assertEqual(parsed[1][0], "flight_1_2019-08-29")
            self.assertEqual(parsed[1][1], "2019-08-29T09:11:11.125")
            self.assertEqual(parsed[1][2], "frame.jpg")
            self.assertEqual(parsed[1][3], "640")
            self.assertEqual(parsed[2][1], "")
            self.assertEqual(parsed[2][3], "")


if __name__ == "__main__":
    unittest.main()
