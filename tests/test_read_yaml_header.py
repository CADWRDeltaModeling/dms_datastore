from pathlib import Path

import pandas as pd
import pytest

from dms_datastore.read_ts import (
    extract_commented_header,
    parse_yaml_header,
    read_yaml_header,
)

DATA_FILE = Path(__file__).parent / "data" / "header_data.txt"


def load_cases():
    cases = {}
    current_name = None
    current_lines = []

    for line in DATA_FILE.read_text(encoding="utf-8").splitlines(keepends=True):
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


def split_header_and_body(text: str, comment: str = "#") -> tuple[str, str]:
    lines = text.splitlines(keepends=True)
    header = []
    i = 0
    for i, line in enumerate(lines):
        if line.startswith(comment):
            header.append(line)
        else:
            break
    else:
        i = len(lines)

    return "".join(header), "".join(lines[i:])


CASES = load_cases()

BAD_CASE_NAMES = {
    "ncro corrupted file with extra space [fails]",
}

GOOD_CASES = {
    name: text for name, text in CASES.items() if name not in BAD_CASE_NAMES
}
BAD_CASES = {
    name: text for name, text in CASES.items() if name in BAD_CASE_NAMES
}


@pytest.mark.parametrize("name,text", GOOD_CASES.items(), ids=GOOD_CASES.keys())
def test_extract_commented_header_matches_leading_block(tmp_path, name, text):
    fpath = tmp_path / f"{name}.csv"
    fpath.write_text(text, encoding="utf-8")

    expected_header, body = split_header_and_body(text)
    actual_header = extract_commented_header(fpath)

    assert actual_header == expected_header
    assert body.startswith("datetime,")


@pytest.mark.parametrize("name,text", GOOD_CASES.items(), ids=GOOD_CASES.keys())
def test_read_yaml_header_on_real_cases(tmp_path, name, text):
    fpath = tmp_path / f"{name}.csv"
    fpath.write_text(text, encoding="utf-8")

    meta = read_yaml_header(fpath)

    assert isinstance(meta, dict)
    assert meta["format"] == "dwr-dms-1.0"
    assert "param" in meta
    assert "station_id" in meta or "agency_id" in meta


@pytest.mark.parametrize("name,text", GOOD_CASES.items(), ids=GOOD_CASES.keys())
def test_parse_yaml_header_matches_file_reader(name, text):
    header_text, _ = split_header_and_body(text)

    meta_from_text = parse_yaml_header(header_text)

    assert isinstance(meta_from_text, dict)
    assert meta_from_text["format"] == "dwr-dms-1.0"


@pytest.mark.parametrize("name,text", BAD_CASES.items(), ids=BAD_CASES.keys())
def test_bad_headers_fail_to_parse(name, text):
    header_text, _ = split_header_and_body(text)
    with pytest.raises(ValueError):
        parse_yaml_header(header_text)


def test_extract_commented_header_stops_before_csv_header(tmp_path):
    text = (
        "# format: dwr-dms-1.0\n"
        "# param: ec\n"
        "# station_id: abc\n"
        "datetime,value\n"
        "2020-01-01T00:00:00,1.0\n"
    )
    fpath = tmp_path / "simple.csv"
    fpath.write_text(text, encoding="utf-8")

    header = extract_commented_header(fpath)

    assert header == (
        "# format: dwr-dms-1.0\n"
        "# param: ec\n"
        "# station_id: abc\n"
    )


def test_read_yaml_header_simple_file(tmp_path):
    text = (
        "# format: dwr-dms-1.0\n"
        "# param: ec\n"
        "# station_id: abc\n"
        "datetime,value\n"
        "2020-01-01T00:00:00,1.0\n"
    )
    fpath = tmp_path / "simple.csv"
    fpath.write_text(text, encoding="utf-8")

    meta = read_yaml_header(fpath)

    assert meta == {
        "format": "dwr-dms-1.0",
        "param": "ec",
        "station_id": "abc",
    }