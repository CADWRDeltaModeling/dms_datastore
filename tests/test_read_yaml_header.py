from pathlib import Path

import pandas as pd
import pytest

from dms_datastore.read_ts import (
    extract_commented_header,
    parse_yaml_header,
    read_yaml_header,
)



def load_cases():
    cases = {}
    current_name = None
    current_lines = []
    # This tedious way of doing things is to avoid some GiHub-side substitutions of $SRC that were not expanded
    _candidates = [
            Path(__file__).parent.resolve() / "data" / "header_data.txt",        
            Path("tests/data/header_data.txt"),
            Path("data/header_data.txt"),

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



@pytest.fixture(scope="module")
def cases():
    return load_cases()

@pytest.fixture(scope="module")
def good_cases(cases):
    bad_case_names = {
        "ncro corrupted file with extra space [fails]",
    }
    return {name: text for name, text in cases.items() if name not in bad_case_names}

@pytest.fixture(scope="module")
def bad_cases(cases):
    bad_case_names = {
        "ncro corrupted file with extra space [fails]",
    }
    return {name: text for name, text in cases.items() if name in bad_case_names}




def test_extract_commented_header_matches_leading_block(tmp_path, good_cases):
    for name, text in good_cases.items():
        fpath = tmp_path / f"{name}.csv"
        fpath.write_text(text, encoding="utf-8")

        expected_header, body = split_header_and_body(text)
        actual_header = extract_commented_header(fpath)

        assert actual_header == expected_header, name
        assert body.startswith("datetime,"), name


def test_read_yaml_header_on_real_cases(tmp_path, good_cases):
    for name, text in good_cases.items():
        fpath = tmp_path / f"{name}.csv"
        fpath.write_text(text, encoding="utf-8")

        meta = read_yaml_header(fpath)

        assert isinstance(meta, dict), name
        assert meta["format"] == "dwr-dms-1.0", name
        assert "param" in meta, name
        assert "station_id" in meta or "agency_id" in meta, name


def test_parse_yaml_header_matches_file_reader(good_cases):
    for name, text in good_cases.items():
        header_text, _ = split_header_and_body(text)
        meta_from_text = parse_yaml_header(header_text)

        assert isinstance(meta_from_text, dict), name
        assert meta_from_text["format"] == "dwr-dms-1.0", name

def test_bad_headers_fail_to_parse(bad_cases):
    for name, text in bad_cases.items():
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