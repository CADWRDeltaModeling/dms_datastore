# dms_datastore/tests/test_yaml_headers.py

from pathlib import Path

import pytest
from dms_datastore.read_ts import parse_yaml_header
from dms_datastore.write_ts import prep_header
from pathlib import Path

import pandas as pd

from dms_datastore.read_ts import parse_yaml_header
from dms_datastore.write_ts import prep_header, write_ts_csv

#pytestmark = pytest.mark.skip(reason="Temporarily disabled on CI due to reorder_metadata recursion fix")

def leading_commented_header(text: str, comment: str = "#") -> str:
    # Use the same rule as extract_commented_header, but on an in-memory string
    lines = text.splitlines(keepends=True)
    out = []
    for line in lines:
        if line.startswith(comment):
            out.append(line)
        else:
            break
    return "".join(out)

def load_header_cases():
    cases = {}
    current_name = None
    current_lines = []
    # This tedious way of doing things is to avoid some GiHub-side substitutions of $SRC that were not expanded
    _candidates = [
            Path("tests/data/header_data.txt"),
            Path("data/header_data.txt"),
            Path(__file__).parent.resolve() / "data" / "header_data.txt",
        ]

    for candidate in _candidates:
        if candidate.is_file():
            DATA_FILE = candidate
            break
    else:   # else with a for loop executes if the loop completes without hitting a break
        raise FileNotFoundError(
                "Could not find header_data.txt. Tried:\n" +
                "\n".join(str(p) for p in _candidates)
        )
        
            
    for line in DATA_FILE.read_text().splitlines(keepends=True):
        if line.startswith("!"):
            if current_name is not None:
                cases[current_name] = "".join(current_lines)
            current_name = line[1:].strip()
            current_lines = []
        else:
            current_lines.append(line)

    if current_name is not None:
        cases[current_name] = "".join(current_lines)

    return cases


@pytest.fixture(scope="module")
def cases():
    return load_header_cases()

@pytest.fixture(scope="module")
def good_cases(cases):
    return {k: v for k, v in cases.items() if "corrupted" not in k.lower()}

@pytest.fixture(scope="module")
def bad_cases(cases):
    return {k: v for k, v in cases.items() if "corrupted" in k.lower()}


def test_parse_good_headers(good_cases):
    for name, text in good_cases.items():
        header_text = leading_commented_header(text)
        meta = parse_yaml_header(header_text)
        assert isinstance(meta, dict), name
        assert "format" in meta, name


def test_header_round_trip(good_cases):
    for name, text in good_cases.items():
        header_text = leading_commented_header(text)
        meta1 = parse_yaml_header(header_text)
        text2 = prep_header(meta1)
        meta2 = parse_yaml_header(text2)
        assert meta1 == meta2, name

def test_header_idempotent(good_cases):
    for name, text in good_cases.items():
        header_text = leading_commented_header(text)
        text1 = prep_header(parse_yaml_header(header_text))
        text2 = prep_header(parse_yaml_header(text1))
        assert text1 == text2, name
    
def test_original_header_survives(cases):
    for name in [
        "usgs dutch slough formatted example",
        "usgs screened example",
        "Division of Environmental Services at DWR (des)",
    ]:
        text = cases[name]
        header_text = leading_commented_header(text)
        meta1 = parse_yaml_header(header_text)
        assert "original_header" in meta1, name

        text2 = prep_header(meta1)
        meta2 = parse_yaml_header(text2)

        assert meta1["original_header"] == meta2["original_header"], name
    
    
def test_bad_headers_fail(bad_cases):
    for name, text in bad_cases.items():
        with pytest.raises(ValueError):
            parse_yaml_header(text)
        



def leading_commented_header(text: str, comment: str = "#") -> str:
    # Use the same rule as extract_commented_header, but on an in-memory string
    lines = text.splitlines(keepends=True)
    out = []
    for line in lines:
        if line.startswith(comment):
            out.append(line)
        else:
            break
    return "".join(out)


def load_header_cases():
    cases = {}
    current_name = None
    current_lines = []
    _candidates = [
        Path("tests/data/header_data.txt"),
        Path("data/header_data.txt"),
        Path(__file__).parent.resolve() / "data" / "header_data.txt",
    ]

    for candidate in _candidates:
        if candidate.is_file():
            DATA_FILE = candidate
            break
    else:
        raise FileNotFoundError(
            "Could not find header_data.txt. Tried:\n" +
            "\n".join(str(p) for p in _candidates)
        )

    for line in DATA_FILE.read_text().splitlines(keepends=True):
        if line.startswith("!"):
            if current_name is not None:
                cases[current_name] = "".join(current_lines)
            current_name = line[1:].strip()
            current_lines = []
        else:
            current_lines.append(line)

    if current_name is not None:
        cases[current_name] = "".join(current_lines)

    return cases


@pytest.fixture(scope="module")
def cases():
    return load_header_cases()


@pytest.fixture(scope="module")
def good_cases(cases):
    return {k: v for k, v in cases.items() if "corrupted" not in k.lower()}


@pytest.fixture(scope="module")
def bad_cases(cases):
    return {k: v for k, v in cases.items() if "corrupted" in k.lower()}


def test_parse_good_headers(good_cases):
    for name, text in good_cases.items():
        header_text = leading_commented_header(text)
        meta = parse_yaml_header(header_text)
        assert isinstance(meta, dict), name
        assert "format" in meta, name


def test_header_round_trip(good_cases):
    for name, text in good_cases.items():
        header_text = leading_commented_header(text)
        meta1 = parse_yaml_header(header_text)
        text2 = prep_header(meta1)
        meta2 = parse_yaml_header(text2)
        assert meta1 == meta2, name


def test_header_idempotent(good_cases):
    for name, text in good_cases.items():
        header_text = leading_commented_header(text)
        text1 = prep_header(parse_yaml_header(header_text))
        text2 = prep_header(parse_yaml_header(text1))
        assert text1 == text2, name


def test_original_header_survives(cases):
    for name in [
        "usgs dutch slough formatted example",
        "usgs screened example",
        "Division of Environmental Services at DWR (des)",
    ]:
        text = cases[name]
        header_text = leading_commented_header(text)
        meta1 = parse_yaml_header(header_text)
        assert "original_header" in meta1, name

        text2 = prep_header(meta1)
        meta2 = parse_yaml_header(text2)

        assert meta1["original_header"] == meta2["original_header"], name


def test_bad_headers_fail(bad_cases):
    for name, text in bad_cases.items():
        with pytest.raises(ValueError):
            parse_yaml_header(text)


def test_prep_header_preserves_original_header_block_and_utf8():
    meta = {
        "format": "dwr-dms-1.0",
        "param": "ec",
        "agency_unit": "µS/cm",
        "original_header": (
            "# date_formatted: 2026-04-11T21:25:27\n"
            "# agency_unit_name : µS/cm\n"
            "# agency_equipment_name : Schneider\n"
        ),
    }

    text = prep_header(meta)

    assert "# original_header: |" in text
    assert "µS/cm" in text
    assert "ÂµS/cm" not in text
    assert "# original_header: '" not in text

    reparsed = parse_yaml_header(text)
    assert reparsed["agency_unit"] == "µS/cm"
    assert reparsed["original_header"] == meta["original_header"].rstrip("\n")


def test_write_ts_csv_injects_date_formatted_for_dict_metadata(tmp_path):
    idx = pd.date_range("1984-01-01", periods=20, freq="15min")
    ts = pd.DataFrame({"value": range(len(idx))}, index=idx)

    fpath = tmp_path / "single.csv"
    metadata = {
        "format": "dwr-dms-1.0",
        "param": "ec",
        "station_id": "abc",
    }

    write_ts_csv(ts, fpath, metadata=metadata, chunk_years=False)

    text = fpath.read_text(encoding="utf-8")
    header = leading_commented_header(text)
    meta = parse_yaml_header(header)

    assert "date_formatted" in meta
    assert meta["format"] == "dwr-dms-1.0"
    assert meta["param"] == "ec"
    assert meta["station_id"] == "abc"


def test_write_ts_csv_year_metadata_map_applies_per_shard(tmp_path):
    idx = pd.date_range("1984-12-31 20:00:00", periods=40, freq="15min")
    ts = pd.DataFrame({"value": range(len(idx))}, index=idx)

    fpath = tmp_path / "des_mrz@upper_40_ec.csv"
    metadata = {
        1984: {
            "format": "dwr-dms-1.0",
            "param": "ec",
            "station_id": "mrz",
            "agency_unit": "µS/cm",
            "original_header": (
                "# agency_unit_name : µS/cm\n"
                "# agency_equipment_name : Schneider\n"
            ),
        },
        1985: {
            "format": "dwr-dms-1.0",
            "param": "ec",
            "station_id": "mrz",
            "agency_unit": "mg/L",
            "original_header": (
                "# agency_unit_name : mg/L\n"
                "# agency_equipment_name : YSI Sonde\n"
            ),
        },
    }

    write_ts_csv(ts, fpath, metadata=metadata, chunk_years=True)

    f1984 = tmp_path / "des_mrz@upper_40_ec_1984.csv"
    f1985 = tmp_path / "des_mrz@upper_40_ec_1985.csv"

    assert f1984.exists()
    assert f1985.exists()

    t1984 = f1984.read_text(encoding="utf-8")
    t1985 = f1985.read_text(encoding="utf-8")

    h1984 = leading_commented_header(t1984)
    h1985 = leading_commented_header(t1985)

    m1984 = parse_yaml_header(h1984)
    m1985 = parse_yaml_header(h1985)

    assert m1984["agency_unit"] == "µS/cm"
    assert m1985["agency_unit"] == "mg/L"

    assert "Schneider" in m1984["original_header"]
    assert "YSI Sonde" in m1985["original_header"]

    assert "ÂµS/cm" not in t1984
    assert "ÂµS/cm" not in t1985


def test_write_ts_csv_year_metadata_map_requires_complete_coverage(tmp_path):
    idx = pd.date_range("1984-12-31 23:45:00", periods=40, freq="15min")
    ts = pd.DataFrame({"value": range(len(idx))}, index=idx)

    fpath = tmp_path / "des_mrz@upper_40_ec.csv"
    metadata = {
        1984: {
            "format": "dwr-dms-1.0",
            "param": "ec",
            "station_id": "mrz",
        }
    }

    with pytest.raises(ValueError, match="Missing|cover all output shard years"):
        write_ts_csv(ts, fpath, metadata=metadata, chunk_years=True)