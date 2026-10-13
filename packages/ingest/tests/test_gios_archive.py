"""Archive download guards: the file name sent by the server must match the requested year."""

import pytest

from smogcast.ingest.gios_archive import ArchiveFileMismatch, check_expected_file, server_file_name


def test_file_name_from_content_disposition():
    assert server_file_name('attachment; filename="2024.zip"') == "2024.zip"
    assert server_file_name(None) == ""


def test_matching_year_passes():
    check_expected_file("Wyniki pomiarów z 2022 roku.zip", "2022")


def test_shifted_id_is_detected():
    # The web page labels are shifted: id 644 is labelled 2024 but is 2025.zip.
    with pytest.raises(ArchiveFileMismatch, match="shifted"):
        check_expected_file("2025.zip", "2024")
