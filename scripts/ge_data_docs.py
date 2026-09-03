"""Build and describe a complete Great Expectations Data Docs bundle."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


DATA_DOCS_SITE_NAME = "auair_validation_docs"


@dataclass(frozen=True)
class DataDocsBundle:
    """Paths within one generated Data Docs site."""

    index_path: str
    validation_path: str
    file_count: int


def configure_data_docs_site(context, output_directory: Path | str) -> Path:
    """Attach a filesystem Data Docs site to an existing GX context."""

    destination = Path(output_directory).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        raise ValueError(f"Data Docs output directory must be empty: {destination}")

    context.add_data_docs_site(
        site_name=DATA_DOCS_SITE_NAME,
        site_config={
            "class_name": "SiteBuilder",
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": str(destination),
            },
        },
    )
    return destination


def build_data_docs_bundle(context, output_directory: Path | str) -> DataDocsBundle:
    """Build the configured site and locate its single validation page."""

    destination = Path(output_directory).resolve()
    context.build_data_docs(site_names=[DATA_DOCS_SITE_NAME])

    index_path = destination / "index.html"
    if not index_path.is_file():
        raise RuntimeError(f"GX Data Docs index was not created: {index_path}")

    validation_pages = sorted(destination.glob("validations/**/*.html"))
    if len(validation_pages) != 1:
        raise RuntimeError(
            "Expected exactly one GX Data Docs validation page; "
            f"received {len(validation_pages)} in {destination}."
        )

    generated_files = [path for path in destination.rglob("*") if path.is_file()]
    return DataDocsBundle(
        index_path=index_path.relative_to(destination).as_posix(),
        validation_path=validation_pages[0].relative_to(destination).as_posix(),
        file_count=len(generated_files),
    )
