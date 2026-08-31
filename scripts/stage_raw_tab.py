"""Stream-clean raw MX tab data and produce a ZSTD-compressed byte stream.

The transformation core accepts a readable binary stream and exposes another
readable stream containing the cleaned, compressed data. A Dagster asset can
therefore connect MinIO ``get_object`` directly to MinIO ``put_object`` without
writing the raw, cleaned, or compressed dataset to local disk.

The ``stage_raw_tab`` path-based function remains as a convenience for manual
and CLI use. It uses the same streaming core and creates only the requested
final output file, not raw or cleaned intermediate copies.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import BinaryIO, Iterator

import zstandard as zstd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.clean_tab_trailing_tab import (
    _remove_extra_trailing_fields,
    _remove_surplus_empty_fields,
    _split_and_trim,
)


DEFAULT_ZSTD_LEVEL = 12
DEFAULT_STREAM_CHUNK_SIZE = 64 * 1024 * 1024


@dataclass(frozen=True)
class StagedTabResult:
    """Measurements collected after a staging stream is fully consumed."""

    row_count: int
    column_count: int
    fields_trimmed: int
    trailing_fields_removed: int
    surplus_empty_fields_removed: int
    blank_lines_skipped: int
    raw_size_bytes: int
    cleaned_size_bytes: int
    staged_size_bytes: int
    raw_to_staged_ratio: float
    cleaned_to_staged_ratio: float
    compression: str
    zstd_level: int
    zstd_threads: int


def _compression_ratio(source_size: int, compressed_size: int) -> float:
    if compressed_size == 0:
        return 0.0
    return round(source_size / compressed_size, 6)


class StagedTabStream(io.RawIOBase):
    """Readable stream that lazily cleans and ZSTD-compresses raw tab bytes."""

    def __init__(
        self,
        source: BinaryIO,
        *,
        zstd_level: int = DEFAULT_ZSTD_LEVEL,
        zstd_threads: int = 0,
        stream_chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
    ) -> None:
        super().__init__()
        if zstd_threads < 0:
            raise ValueError("zstd_threads cannot be negative.")
        if stream_chunk_size <= 0:
            raise ValueError("stream_chunk_size must be greater than zero.")
        if not hasattr(source, "read"):
            raise TypeError("source must be a readable binary stream.")

        self._source = source
        self._zstd_level = zstd_level
        self._zstd_threads = zstd_threads
        self._stream_chunk_size = stream_chunk_size
        self._compressor = zstd.ZstdCompressor(
            level=zstd_level,
            threads=zstd_threads,
            write_content_size=False,
        ).compressobj()
        self._cleaned_chunks = self._iter_cleaned_chunks()
        self._compressed_buffer = bytearray()
        self._finished = False

        self._row_count = 0
        self._column_count = 0
        self._fields_trimmed = 0
        self._trailing_fields_removed = 0
        self._surplus_empty_fields_removed = 0
        self._blank_lines_skipped = 0
        self._raw_size_bytes = 0
        self._cleaned_size_bytes = 0
        self._staged_size_bytes = 0

    def readable(self) -> bool:
        return True

    def _iter_raw_lines(self) -> Iterator[bytes]:
        pending = b""
        while True:
            chunk = self._source.read(self._stream_chunk_size)
            if not chunk:
                break
            if not isinstance(chunk, bytes):
                raise TypeError("source.read() must return bytes.")

            self._raw_size_bytes += len(chunk)
            lines = (pending + chunk).split(b"\n")
            pending = lines.pop()
            for line in lines:
                yield line + b"\n"

        if pending:
            yield pending

    def _iter_cleaned_lines(self) -> Iterator[bytes]:
        raw_lines = self._iter_raw_lines()
        try:
            raw_header = next(raw_lines)
        except StopIteration as error:
            raise ValueError("Input .tab stream is empty.") from error

        header, trimmed = _split_and_trim(raw_header)
        self._fields_trimmed += trimmed
        self._trailing_fields_removed += _remove_extra_trailing_fields(header)
        header_empty_count = sum(field == b"" for field in header)
        if header_empty_count:
            header[:] = [field for field in header if field != b""]
            self._surplus_empty_fields_removed += header_empty_count
        if not header:
            raise ValueError("Header has no named columns after cleanup.")

        self._column_count = len(header)
        cleaned_header = b"\t".join(header) + b"\n"
        self._cleaned_size_bytes += len(cleaned_header)
        yield cleaned_header

        for line_number, raw_line in enumerate(raw_lines, start=2):
            if not raw_line.strip():
                self._blank_lines_skipped += 1
                continue

            fields, trimmed = _split_and_trim(raw_line)
            self._fields_trimmed += trimmed
            self._trailing_fields_removed += _remove_extra_trailing_fields(
                fields, self._column_count
            )
            self._surplus_empty_fields_removed += _remove_surplus_empty_fields(
                fields, self._column_count
            )
            if len(fields) != self._column_count:
                raise ValueError(
                    f"Line {line_number} has {len(fields):,} columns after "
                    f"cleanup; expected {self._column_count:,}."
                )

            cleaned_line = b"\t".join(fields) + b"\n"
            self._cleaned_size_bytes += len(cleaned_line)
            self._row_count += 1
            yield cleaned_line

    def _iter_cleaned_chunks(self) -> Iterator[bytes]:
        buffer = bytearray()
        for cleaned_line in self._iter_cleaned_lines():
            if buffer and len(buffer) + len(cleaned_line) > self._stream_chunk_size:
                yield bytes(buffer)
                buffer.clear()

            buffer.extend(cleaned_line)
            if len(buffer) >= self._stream_chunk_size:
                yield bytes(buffer)
                buffer.clear()

        if buffer:
            yield bytes(buffer)

    def _produce_compressed_bytes(self) -> None:
        if self._finished:
            return

        try:
            cleaned_chunk = next(self._cleaned_chunks)
            compressed = self._compressor.compress(cleaned_chunk)
        except StopIteration:
            compressed = self._compressor.flush()
            self._finished = True

        if compressed:
            self._compressed_buffer.extend(compressed)
            self._staged_size_bytes += len(compressed)

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed staging stream.")
        if size == 0:
            return b""

        if size is None or size < 0:
            while not self._finished:
                self._produce_compressed_bytes()
            data = bytes(self._compressed_buffer)
            self._compressed_buffer.clear()
            return data

        while len(self._compressed_buffer) < size and not self._finished:
            self._produce_compressed_bytes()

        data = bytes(self._compressed_buffer[:size])
        del self._compressed_buffer[:size]
        return data

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def result(self) -> StagedTabResult:
        """Return final metrics after the stream has reached end-of-file."""

        if not self._finished or self._compressed_buffer:
            raise RuntimeError(
                "Staging result is unavailable until the output stream is fully consumed."
            )

        return StagedTabResult(
            row_count=self._row_count,
            column_count=self._column_count,
            fields_trimmed=self._fields_trimmed,
            trailing_fields_removed=self._trailing_fields_removed,
            surplus_empty_fields_removed=self._surplus_empty_fields_removed,
            blank_lines_skipped=self._blank_lines_skipped,
            raw_size_bytes=self._raw_size_bytes,
            cleaned_size_bytes=self._cleaned_size_bytes,
            staged_size_bytes=self._staged_size_bytes,
            raw_to_staged_ratio=_compression_ratio(
                self._raw_size_bytes, self._staged_size_bytes
            ),
            cleaned_to_staged_ratio=_compression_ratio(
                self._cleaned_size_bytes, self._staged_size_bytes
            ),
            compression="zstd",
            zstd_level=self._zstd_level,
            zstd_threads=self._zstd_threads,
        )


def stage_raw_tab_stream(
    source: BinaryIO,
    *,
    zstd_level: int = DEFAULT_ZSTD_LEVEL,
    zstd_threads: int = 0,
    stream_chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
) -> StagedTabStream:
    """Create a lazy cleaned-and-compressed stream over raw tab input."""

    return StagedTabStream(
        source,
        zstd_level=zstd_level,
        zstd_threads=zstd_threads,
        stream_chunk_size=stream_chunk_size,
    )


def stage_raw_tab(
    input_path: Path | str,
    output_path: Path | str,
    *,
    zstd_level: int = DEFAULT_ZSTD_LEVEL,
    zstd_threads: int = 0,
    stream_chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
) -> StagedTabResult:
    """Convenience wrapper that streams one local file into another."""

    source = Path(input_path).resolve()
    destination = Path(output_path).resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Raw .tab input not found: {source}")
    if source == destination:
        raise ValueError("Input and output paths must be different.")

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = destination.with_suffix(destination.suffix + ".tmp")
    temporary_output.unlink(missing_ok=True)

    try:
        with source.open("rb") as input_file, temporary_output.open("wb") as output_file:
            staged_stream = stage_raw_tab_stream(
                input_file,
                zstd_level=zstd_level,
                zstd_threads=zstd_threads,
                stream_chunk_size=stream_chunk_size,
            )
            while chunk := staged_stream.read(stream_chunk_size):
                output_file.write(chunk)
            result = staged_stream.result()

        temporary_output.replace(destination)
    except Exception:
        temporary_output.unlink(missing_ok=True)
        raise

    return result


def _write_metadata(path: Path, result: StagedTabResult) -> None:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(
        json.dumps(asdict(result), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary_path.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream-clean a raw MX .tab file and compress it as .tab.zst."
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--metadata-output",
        type=Path,
        help="Optional JSON file receiving cleanup and compression measurements.",
    )
    parser.add_argument("--zstd-level", type=int, default=DEFAULT_ZSTD_LEVEL)
    parser.add_argument(
        "--zstd-threads",
        type=int,
        default=0,
        help="ZSTD worker threads; 0 uses the library's single-threaded mode.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = stage_raw_tab(
        args.input,
        args.output,
        zstd_level=args.zstd_level,
        zstd_threads=args.zstd_threads,
    )
    if args.metadata_output is not None:
        _write_metadata(args.metadata_output, result)
    print(json.dumps(asdict(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
