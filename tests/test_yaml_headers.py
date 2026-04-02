# dms_datastore/tests/test_yaml_headers.py

from pathlib import Path

import pytest
from dms_datastore.read_ts import parse_yaml_header
from dms_datastore.write_ts import prep_header

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
        
  