from io import StringIO
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

from dms_datastore.read_ts import read_flagged, read_yaml_header
from dms_datastore.write_ts import write_ts_csv

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


def body_to_dataframe(body_text: str) -> pd.DataFrame:
    return pd.read_csv(
        StringIO(body_text),
        index_col=0,
        parse_dates=[0],
    )


CASES = load_cases()

BAD_CASE_NAMES = {
    "ncro corrupted file with extra space [fails]",
}

GOOD_CASES = {
    name: text for name, text in CASES.items() if name not in BAD_CASE_NAMES
}

UNSCREENED_CASE_NAMES = [
    "usgs dutch slough formatted example",
    "CDEC",
    "Division of Environmental Services at DWR (des)",
    "NOAA",
    "DWR North Central Regional Office",
    "NCRO another example",
]

SCREENED_CASE_NAMES = [
    "usgs screened example",
    "DES screened example",
    "noaa screened",
]


@pytest.mark.parametrize("name", UNSCREENED_CASE_NAMES)
def test_write_ts_csv_roundtrip_unscreened(tmp_path, name):
    text = GOOD_CASES[name]
    src = tmp_path / f"{name}_src.csv"
    out = tmp_path / f"{name}_out.csv"
    src.write_text(text, encoding="utf-8")

    meta1 = read_yaml_header(src)
    _, body1 = split_header_and_body(text)
    df1 = body_to_dataframe(body1)

    write_ts_csv(df1, out, metadata=meta1)

    meta2 = read_yaml_header(out)
    df2 = pd.read_csv(
        out,
        comment="#",
        index_col=0,
        parse_dates=[0],
    )

    assert meta1 == meta2
    pdt.assert_frame_equal(df1, df2, check_dtype=False)


@pytest.mark.parametrize("name", SCREENED_CASE_NAMES)
def test_write_ts_csv_roundtrip_screened(tmp_path, name):
    text = GOOD_CASES[name]
    src = tmp_path / f"{name}_src.csv"
    out = tmp_path / f"{name}_out.csv"
    src.write_text(text, encoding="utf-8")

    meta1, df1 = read_flagged(
        str(src),
        apply_flags=False,
        return_flags=True,
        return_meta=True,
    )

    write_ts_csv(df1, out, metadata=meta1)

    meta2, df2 = read_flagged(
        str(out),
        apply_flags=False,
        return_flags=True,
        return_meta=True,
    )

    assert meta1 == meta2
    pdt.assert_frame_equal(df1, df2, check_dtype=False)


@pytest.mark.parametrize("name", UNSCREENED_CASE_NAMES[:2] + SCREENED_CASE_NAMES[:1])
def test_write_ts_csv_is_idempotent_for_selected_cases(tmp_path, name):
    text = GOOD_CASES[name]
    src = tmp_path / f"{name}_src.csv"
    out1 = tmp_path / f"{name}_out1.csv"
    out2 = tmp_path / f"{name}_out2.csv"
    src.write_text(text, encoding="utf-8")

    if name in SCREENED_CASE_NAMES:
        meta1, df1 = read_flagged(
            str(src),
            apply_flags=False,
            return_flags=True,
            return_meta=True,
        )
    else:
        meta1 = read_yaml_header(src)
        _, body1 = split_header_and_body(text)
        df1 = body_to_dataframe(body1)

    write_ts_csv(df1, out1, metadata=meta1)

    if name in SCREENED_CASE_NAMES:
        meta2, df2 = read_flagged(
            str(out1),
            apply_flags=False,
            return_flags=True,
            return_meta=True,
        )
    else:
        meta2 = read_yaml_header(out1)
        df2 = pd.read_csv(
            out1,
            comment="#",
            index_col=0,
            parse_dates=[0],
        )

    write_ts_csv(df2, out2, metadata=meta2)

    assert out1.read_text(encoding="utf-8") == out2.read_text(encoding="utf-8")
    