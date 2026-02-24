import io
import pandas as pd
import pytest
from dms_datastore.write_ts import write_ts_csv


@pytest.fixture
def sample_ts():
    index = pd.date_range("2020-01-01", periods=5, freq="h", name="datetime")
    df = pd.DataFrame(
        {
            "value": [1.1, 2.2, 3.3, 4.4, 5.5],
            "user_flag": pd.array([0, 0, 1, 0, 2], dtype="Int64"),
        },
        index=index,
    )
    return df


def test_write_ts_csv_stringio(sample_ts):
    buf = io.StringIO()
    write_ts_csv(sample_ts, buf)

    contents = buf.getvalue()

    # Header line is present
    assert contents.startswith("# format: dwr-dms-1.0")

    # date_formatted line is present
    assert "# date_formatted:" in contents

    # Column headers are written
    assert "datetime" in contents
    assert "value" in contents
    assert "user_flag" in contents

    # All five data rows are present
    for val in ["1.1", "2.2", "3.3", "4.4", "5.5"]:
        assert val in contents

    # Data is parseable back to a DataFrame
    buf.seek(0)
    lines = [line for line in buf if not line.startswith("#")]
    roundtrip = pd.read_csv(
        io.StringIO("".join(lines)),
        index_col="datetime",
        parse_dates=True,
    )
    assert list(roundtrip.index) == list(sample_ts.index)
    assert list(roundtrip["value"]) == list(sample_ts["value"])


def test_write_ts_csv_stringio_with_metadata(sample_ts):
    buf = io.StringIO()
    metadata = {"station_id": "ABC", "units": "ft"}
    write_ts_csv(sample_ts, buf, metadata=metadata)

    contents = buf.getvalue()
    assert "station_id: ABC" in contents
    assert "units: ft" in contents
    assert "value" in contents
