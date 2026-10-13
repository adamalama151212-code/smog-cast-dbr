"""GIOŚ yearly archive → landing/gios_archive/raw/ (zip per year + station metadata xlsx).

Only downloads; parsing is done by smogcast-bronze. Files already present are kept
(landing acts as a cache: a 50–75 MB zip per year is downloaded once).

Safety check: the archive web page shows labels shifted against the download links, so
every download verifies that the server-provided file name matches what we asked for
(the year for yearly zips, "Metadane" for the metadata file). A wrong file is never
written to landing.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from urllib.parse import unquote

import requests

log = logging.getLogger(__name__)

USER_AGENT = "smogcast/0.1 (data platform course project)"
METADATA_FILE = "metadata.xlsx"


class ArchiveFileMismatch(RuntimeError):
    """The server returned a different file than the one the configured id should point to."""


def zip_path(raw_dir: str | Path, year: int) -> Path:
    return Path(raw_dir) / f"{year}.zip"


def server_file_name(content_disposition: str | None) -> str:
    """File name from a Content-Disposition header (GIOŚ sends Latin-2 bytes; we only need ASCII parts)."""
    if not content_disposition:
        return ""
    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', content_disposition)
    return unquote(m.group(1)) if m else ""


def check_expected_file(name: str, expected_token: str) -> None:
    if expected_token.lower() not in name.lower():
        raise ArchiveFileMismatch(
            f"Expected a file containing {expected_token!r}, server sent {name!r}. "
            "The archive ids in conf/gios.yaml are probably shifted — re-check them (ARCHITECTURE.md, section 11.4.1)."
        )


def _download(url: str, target: Path, expected_token: str, retries: int = 3) -> None:
    tmp = target.with_suffix(target.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=120, headers={"User-Agent": USER_AGENT}) as r:
                r.raise_for_status()
                check_expected_file(server_file_name(r.headers.get("Content-Disposition")), expected_token)
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            # Rename only after a complete download: a broken transfer never looks like a cached file.
            tmp.replace(target)
            return
        except ArchiveFileMismatch:
            raise
        except requests.RequestException as e:
            log.warning("download %s failed (attempt %d/%d): %s", url, attempt, retries, e)
            if attempt == retries:
                raise
            time.sleep(5 * attempt)


def download_archive(cfg: dict, landing_raw_dir: str) -> list[Path]:
    """Download missing yearly zips for ``run.archive_years`` and the station metadata."""
    arch = cfg["gios"]["archive"]
    raw = Path(landing_raw_dir)
    raw.mkdir(parents=True, exist_ok=True)
    new: list[Path] = []
    for year in cfg["run"]["archive_years"]:
        file_id = arch["yearly_file_ids"].get(year)
        if file_id is None:
            raise ValueError(f"No archive file id for {year} in conf/gios.yaml")
        target = zip_path(raw, year)
        if target.exists():
            log.info("archive %s: cached (%s)", year, target.name)
            continue
        log.info("archive %s: downloading file id %s", year, file_id)
        _download(arch["download_url"].format(file_id=file_id), target, str(year))
        new.append(target)
    meta = raw / METADATA_FILE
    if not meta.exists():
        _download(arch["download_url"].format(file_id=arch["metadata_file_id"]), meta, "Metadane")
        new.append(meta)
    return new
