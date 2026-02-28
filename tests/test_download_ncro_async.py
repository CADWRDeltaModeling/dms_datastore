import asyncio
import json

import pandas as pd

from dms_datastore import download_ncro


class _FakeResponse:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        return None


class _FlakyAsyncClient:
    def __init__(self, payload_text):
        self.payload_text = payload_text
        self.attempts = 0

    async def get(self, url, timeout=None):
        self.attempts += 1
        if self.attempts < 3:
            raise RuntimeError("temporary network error")
        return _FakeResponse(self.payload_text)


def test_async_download_trace_retries_and_succeeds(monkeypatch):
    payload = {
        "return": {
            "traces": [
                {
                    "site": "B95370",
                    "site_details": {"name": "Old River"},
                    "trace_details": {"unit": "cfs", "desc": "Flow"},
                    "trace": [{"t": "20240101000000", "v": "1.0", "q": "0"}],
                }
            ]
        }
    }
    client = _FlakyAsyncClient(json.dumps(payload))

    async def _no_sleep(_):
        return None

    monkeypatch.setattr(download_ncro.asyncio, "sleep", _no_sleep)

    text = asyncio.run(
        download_ncro._async_download_trace(
            client,
            "B95370",
            "flow RAW",
            pd.Timestamp("2024-01-01"),
            pd.Timestamp("2024-01-02"),
        )
    )

    assert text is not None
    assert client.attempts == 3


def test_ncro_download_writes_file_with_async_pipeline(monkeypatch, tmp_path):
    stations = pd.DataFrame(
        [
            {
                "agency_id": "B95370",
                "station_id": "orm",
                "src_var_id": "Flow",
                "param": "flow",
            }
        ]
    )

    inventory = pd.DataFrame(
        [
            {
                "site": "B95370",
                "param": "Flow",
                "start_time": pd.Timestamp("2020-01-01"),
                "end_time": pd.Timestamp("2030-01-01"),
                "trace": "flow RAW",
            }
        ]
    )

    monkeypatch.setattr(download_ncro, "load_inventory", lambda: inventory)
    monkeypatch.setattr(download_ncro.dstore_config, "station_dbase", lambda: pd.DataFrame())

    async def _fake_chunked(_client, site, trace, stime, etime):
        df = pd.DataFrame(
            {"value": [1.2, 1.4], "qaqc_flag": ["0", "0"]},
            index=pd.to_datetime(["2024-01-01 00:00", "2024-01-01 01:00"]),
        )
        df.index.name = "datetime"
        site_details = {"name": "Old River at Bacon"}
        trace_details = {"unit": "cfs", "desc": "Flow"}
        return site, site_details, trace_details, df

    monkeypatch.setattr(download_ncro, "_async_download_trace_chunked", _fake_chunked)

    failures = download_ncro.ncro_download(
        stations,
        str(tmp_path),
        start=pd.Timestamp("2024-01-01"),
        end=pd.Timestamp("2024-01-02"),
        overwrite=True,
    )

    output_file = tmp_path / "ncro_orm_b95370_flow_2024_2024.csv"
    assert failures == []
    assert output_file.exists()

    content = output_file.read_text(encoding="utf-8")
    assert "station_id: orm" in content
    assert "agency_station_id: B95370" in content
