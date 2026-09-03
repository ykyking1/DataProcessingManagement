"""Render a private MinIO-hosted GX Data Docs bundle inside Streamlit."""

from __future__ import annotations

import base64
import json
import mimetypes
import posixpath
import re
from collections.abc import Callable
from pathlib import PurePosixPath
from urllib.parse import urlsplit

from bs4 import BeautifulSoup


_CSS_URL_RE = re.compile(
    r"url\(\s*(?P<quote>['\"]?)(?P<reference>[^)'\"]+)(?P=quote)\s*\)",
    re.IGNORECASE,
)


class DataDocsReferenceError(ValueError):
    """Raised when a requested Data Docs object is outside its bundle."""


def validate_data_docs_html_key(
    object_key: str,
    *,
    validation_prefix: str = "validation/",
) -> tuple[str, str]:
    """Return a normalized HTML key and its dataset/batch/etag bundle root."""

    normalized_key = object_key.strip("/")
    key = PurePosixPath(normalized_key)
    prefix = PurePosixPath(validation_prefix.strip("/"))
    if (
        key.is_absolute()
        or ".." in key.parts
        or key.suffix.lower() != ".html"
        or key.parts[: len(prefix.parts)] != prefix.parts
        or len(key.parts) < len(prefix.parts) + 4
    ):
        raise DataDocsReferenceError(
            f"Invalid GX Data Docs object key: {object_key}"
        )

    bundle_parts = key.parts[: len(prefix.parts) + 3]
    bundle_prefix = PurePosixPath(*bundle_parts).as_posix()
    return key.as_posix(), bundle_prefix


def _local_object_key(
    current_key: str,
    reference: str,
    bundle_prefix: str,
) -> str | None:
    parsed = urlsplit(reference)
    if (
        parsed.scheme
        or parsed.netloc
        or reference.startswith("//")
        or not parsed.path
    ):
        return None

    resolved = posixpath.normpath(
        posixpath.join(posixpath.dirname(current_key), parsed.path)
    ).strip("/")
    if resolved == bundle_prefix or resolved.startswith(f"{bundle_prefix}/"):
        return resolved
    return None


def _local_asset_candidates(
    current_key: str,
    reference: str,
    bundle_prefix: str,
) -> list[str]:
    """Resolve a resource and tolerate GX's occasionally short static path."""

    primary = _local_object_key(current_key, reference, bundle_prefix)
    candidates = [primary] if primary else []
    parsed = urlsplit(reference)
    if parsed.scheme or parsed.netloc or reference.startswith("//"):
        return candidates

    path_parts = PurePosixPath(parsed.path).parts
    if "static" in path_parts:
        static_index = path_parts.index("static")
        fallback = str(
            PurePosixPath(bundle_prefix, *path_parts[static_index:])
        )
        if fallback not in candidates:
            candidates.append(fallback)
    return candidates


def _fetch_local_asset(
    current_key: str,
    reference: str,
    bundle_prefix: str,
    fetch_object: Callable[[str], bytes],
) -> tuple[str | None, bytes | None]:
    for object_key in _local_asset_candidates(
        current_key,
        reference,
        bundle_prefix,
    ):
        try:
            return object_key, fetch_object(object_key)
        except Exception:
            continue
    return None, None


def _content_type(object_key: str) -> str:
    overrides = {
        ".css": "text/css",
        ".html": "text/html",
        ".js": "application/javascript",
        ".otf": "font/otf",
        ".svg": "image/svg+xml",
        ".ttf": "font/ttf",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
    }
    suffix = PurePosixPath(object_key).suffix.lower()
    return overrides.get(
        suffix,
        mimetypes.guess_type(object_key)[0] or "application/octet-stream",
    )


def _data_uri(content: bytes, object_key: str) -> str:
    encoded = base64.b64encode(content).decode("ascii")
    return f"data:{_content_type(object_key)};base64,{encoded}"


def _inline_css_assets(
    css: str,
    *,
    css_key: str,
    bundle_prefix: str,
    fetch_object: Callable[[str], bytes],
) -> str:
    def replace_url(match: re.Match) -> str:
        reference = match.group("reference").strip()
        if reference.startswith(("data:", "#")):
            return match.group(0)
        object_key, content = _fetch_local_asset(
            css_key,
            reference,
            bundle_prefix,
            fetch_object,
        )
        if object_key is None or content is None:
            return match.group(0)
        return f'url("{_data_uri(content, object_key)}")'

    return _CSS_URL_RE.sub(replace_url, css)


def inline_data_docs_bundle(
    html: bytes | str,
    *,
    html_key: str,
    bundle_prefix: str,
    cache_buster: str,
    fetch_object: Callable[[str], bytes],
) -> str:
    """Inline private bundle assets while preserving GX's HTML and scripts."""

    html_text = html.decode("utf-8") if isinstance(html, bytes) else html
    soup = BeautifulSoup(html_text, "html.parser")

    for link in list(soup.find_all("link", href=True)):
        reference = str(link.get("href"))
        object_key, content = _fetch_local_asset(
            html_key,
            reference,
            bundle_prefix,
            fetch_object,
        )
        if object_key is None or content is None:
            continue

        relations = {str(value).lower() for value in (link.get("rel") or [])}
        if "stylesheet" in relations:
            css = _inline_css_assets(
                content.decode("utf-8"),
                css_key=object_key,
                bundle_prefix=bundle_prefix,
                fetch_object=fetch_object,
            )
            style = soup.new_tag("style")
            style.string = css
            link.replace_with(style)
        elif relations.intersection({"icon", "shortcut"}):
            link["href"] = _data_uri(content, object_key)

    for script in list(soup.find_all("script", src=True)):
        reference = str(script.get("src"))
        object_key, content = _fetch_local_asset(
            html_key,
            reference,
            bundle_prefix,
            fetch_object,
        )
        if object_key is None or content is None:
            continue
        script.attrs.pop("src", None)
        script.string = content.decode("utf-8")

    for tag_name, attribute in (("img", "src"), ("source", "src")):
        for tag in soup.find_all(tag_name, **{attribute: True}):
            reference = str(tag.get(attribute))
            object_key, content = _fetch_local_asset(
                html_key,
                reference,
                bundle_prefix,
                fetch_object,
            )
            if object_key is None or content is None:
                continue
            tag[attribute] = _data_uri(content, object_key)

    for anchor in soup.find_all("a", href=True):
        reference = str(anchor.get("href"))
        if reference.startswith("#"):
            continue
        object_key = _local_object_key(html_key, reference, bundle_prefix)
        if object_key and object_key.lower().endswith(".html"):
            anchor["href"] = "#"
            anchor["data-ge-doc-key"] = object_key
        elif object_key is None and urlsplit(reference).scheme in {"http", "https"}:
            anchor["target"] = "_blank"
            anchor["rel"] = "noopener noreferrer"

    safe_cache_buster = json.dumps(cache_buster).replace("</", "<\\/")
    navigation_script = soup.new_tag("script")
    navigation_script.string = f"""
document.addEventListener("click", function (event) {{
  const link = event.target.closest("a[data-ge-doc-key]");
  if (!link) return;
  event.preventDefault();
  const params = new URLSearchParams();
  params.set("ge_data_docs", link.dataset.geDocKey);
  params.set("ge_docs_version", {safe_cache_buster});
  window.parent.location.href = window.parent.location.pathname + "?" + params;
}});
"""
    (soup.body or soup).append(navigation_script)
    return str(soup)
