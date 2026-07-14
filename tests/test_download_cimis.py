"""Integration tests for download_cimis.py.

These tests connect to the real CIMIS SFTP server and require the
CIMIS_PASSWORD environment variable to be set.  They are skipped
automatically when the variable is absent.

Run with:
    pytest tests/test_download_cimis.py -m integration -v
"""

import os
import pytest
import pandas as pd

from dms_datastore.download_cimis import CIMIS

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

PASSWORD = os.environ.get("CIMIS_PASSWORD")

skip_no_password = pytest.mark.skipif(
    not PASSWORD,
    reason="CIMIS_PASSWORD environment variable not set",
)


@pytest.fixture(scope="module")
def cimis(tmp_path_factory):
    """Single CIMIS connection shared by all tests in this module."""
    base = tmp_path_factory.mktemp("cimis_data")
    cx = CIMIS(base_dir=str(base), password=PASSWORD)
    yield cx
    cx.close()


@pytest.fixture(scope="module")
def active_station(cimis):
    """Return the station number of the first active station."""
    dfstations = cimis.get_stations_info()
    active = dfstations[dfstations["Status"] == "Active"]["Station Number"]
    assert len(active) > 0, "No active stations found in catalog"
    return int(active.iloc[0])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@skip_no_password
def test_connection(cimis):
    """SFTP connection opens successfully."""
    # CIMIS_DOWNLOAD_DIR is created lazily on first download, not at __init__.
    assert cimis.sftp is not None
    transport = cimis.ssh.get_transport()
    assert transport is not None and transport.is_active()


@pytest.mark.integration
@skip_no_password
def test_get_stations_info(cimis):
    """Station catalog downloads and has expected columns."""
    df = cimis.get_stations_info()
    assert not df.empty
    for col in ("Station Number", "Status", "Connect"):
        assert col in df.columns, f"Missing column: {col}"
    active = df[df["Status"] == "Active"]
    assert len(active) > 0, "Expected at least one active station"


@pytest.mark.integration
@skip_no_password
def test_download_current_month_single_station(cimis, active_station):
    """Downloading the current month CSV for one active station creates a file."""
    cimis.download_current_month([active_station], hourly=True)
    _, current_year = cimis.get_months_and_current_year()
    dest = os.path.join(cimis.CIMIS_DOWNLOAD_DIR, str(current_year))
    assert os.path.isdir(dest), f"Expected download dir {dest}"
    files = os.listdir(dest)
    assert any(f.endswith(".csv") for f in files), (
        f"No CSV files found in {dest}; files present: {files}"
    )


@pytest.mark.integration
@skip_no_password
def test_download_current_year_monthly_zips(cimis):
    """download_current_year downloads and unzips at least one monthly zip."""
    cimis.download_current_year(hourly=True)
    _, current_year = cimis.get_months_and_current_year()
    dest = os.path.join(cimis.CIMIS_DOWNLOAD_DIR, str(current_year))
    files = os.listdir(dest) if os.path.isdir(dest) else []
    csv_files = [f for f in files if f.endswith(".csv")]
    # We expect at least some per-station CSVs extracted from the monthly zip.
    assert len(csv_files) > 0, (
        f"No CSV files extracted in {dest}; files present: {files}"
    )


@pytest.mark.integration
@skip_no_password
def test_load_station_active(cimis, active_station):
    """load_station returns a non-empty, sorted, unique-index DataFrame."""
    df = cimis.load_station(active_station, load_current_year=True, hourly=True)
    assert isinstance(df, pd.DataFrame), "Expected a DataFrame"
    assert not df.empty, f"Empty result for active station {active_station}"
    assert isinstance(df.index, pd.DatetimeIndex), "Expected DatetimeIndex"
    assert df.index.is_monotonic_increasing, "Index is not sorted"
    assert df.index.is_unique, "Index has duplicate timestamps"


@pytest.mark.integration
@skip_no_password
def test_load_station_inactive_no_crash(cimis):
    """load_station for station 102 (often has gaps) should not raise; may return empty."""
    # This reproduces the original reported error where station 102 produced
    # WinError 2 (file not found) for janhourly102.csv / hourly102.csv.
    # The correct behaviour is to warn and return whatever data is available,
    # not to propagate FileNotFoundError.
    try:
        df = cimis.load_station(102, load_current_year=True, hourly=True)
    except Exception as exc:
        pytest.fail(
            f"load_station(102) raised unexpectedly: {type(exc).__name__}: {exc}"
        )
    # Result may be empty if the station has no downloaded data at all, but
    # it must be a DataFrame (no crash).
    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert df.index.is_monotonic_increasing
        assert df.index.is_unique


@pytest.mark.integration
@skip_no_password
def test_reconnect_survives_closed_socket(cimis, active_station):
    """Manually close the underlying socket; the next download should reconnect."""
    # Force-close the transport to simulate a dropped connection.
    try:
        cimis.ssh.get_transport().close()
    except Exception:
        pass

    # download() should detect "Socket is closed" and reconnect automatically.
    _, current_year = cimis.get_months_and_current_year()
    dest = os.path.join(cimis.CIMIS_DOWNLOAD_DIR, str(current_year))
    os.makedirs(dest, exist_ok=True)
    # A small text file that is always present on the server.
    local = cimis.download("/pub2/readme-ftp-Revised5units.txt")
    assert os.path.isfile(local), "File not downloaded after reconnect"
